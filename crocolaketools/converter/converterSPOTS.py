import os
import warnings
import dask.dataframe as dd
import gsw
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import xarray as xr
from crocolaketools import db_params
from crocolaketools.converter.converter import Converter

#######################################################################################################


class ConverterSPOTS(Converter):

    """class ConverterSPOTS: methods to generate parquet schemas for SPOTS database
    
    """

    # ----------------------------------------------------------------------------------------------- #
    # Constructors/Destructors                                                                        #
    # ----------------------------------------------------------------------------------------------- #

    def __init__(self, config = None, db_type = None):

        if config is not None and not config['db'] == "SPOTS":
            raise ValueError("Database must be SPOTS.")
        elif ((config is None) and (db_type is not None)):
            config = {
                'db': 'SPOTS',
                'db_type': db_type.upper(),
            }

        Converter.__init__(self, config)


    # ----------------------------------------------------------------------------------------------- #
    # Methods                                                                                         #
    # ------------------------------------------------------------------------------------------------#

#-------------------------------------------------------------------------------------------------------------#
## Read database into dask dataframe
    def read_to_ddf(self, flist = None, lock = None):
        if len(flist) > 1:
            raise ValueErros("SPOTS database must be read from a single file. Please check your input.")

        df = self.read_to_df(flist[0], lock)
        if isinstance(df, pd.DataFrame):
            return dd.from_pandas(df)
        elif isinstance(df, dd.DataFrame):
            return df
        else:
            raise TypeError("read_to_df must return a panadas or dask dataframe, not: ", type(df))
        
#-------------------------------------------------------------------------------------------------------------#
## Read file to convert into a pandas dataframe
    def read_to_df(self, filename = None, lock = None):
        """Read file into a pandas dataframe

        Argument:
        filename -- file name, including relative path

        Returns:
        df -- pandas dataframe
        """

        if filename is None:
            filename = "spots.csv"
            print("Using default filename: ", filename)

        input_fname = self.input_path + filename
        print("Reading SPOTS file: ", input_fname)

        # low_memory = False as SPOTS is a small db
        ddf = dd.read_csv(
            input_fname, 
            assume_missing = True,
            delimiter = ",",
            header = 0,
            low_memory = False,
            dtype_backend = 'pyarrow'
        )

        return self.standardize_data(ddf)
#-------------------------------------------------------------------------------------------------------------#
## Convert parquet schema to pandas
    def standardize_data(self,ddf):
        """Standardize dask dataframe to schema consistent across databases

        Argument:
        ddf -- dask dataframe

        Returns:
        ddf -- homogenized (dask) dataframe
        """

        #ddf = self.add_profile_id(ddf)
        # don't have add_profile_id() yet


        ### convert SPOTS multiple time columns to one datetime
        # add time as midnight for rows missing a time
        spots_subset["TIME"] = spots_subset["TIME"].fillna(0)
   
        date_str = ddf["DATE"].astype("Int64").astype(str)
        time_str = ddf["TIME"].astype("Int64").astype(str).str.zfill(4)

        ddf["JULD"] = pd.to_datetime(
            date_str + time_str,
            format = "%Y%m%d%H%M",
            errors = "coerce"
        )
        ddf = ddf.persist()

        # keep only good QC values
        params_to_check =[]
        for param in db_params.params["SPOTS2CROCOLAKE"].keys(): 
            if param.endswith("_FLAG_W") and param in ddf.columns:
                ddf = ddf.map_partitions(
                    self.keep_best_values, param
                )
                params_to_check.append(param[:-7])

        # remove rows containing all NAs
        ddf = ddf.map_partitions(
            super().remove_all_NAs, params_to_check
        )
        ddf = ddf.persist()

        ddf["date_update"] = np.datetime64("2024-02-22T00:00:00.00000000")

        # add error parameters
        error_params = [
            "SALNTY",
            "OXYGEN",
            "NITRAT",
            "PHSPHT",
            "SILCAT",
            "ALKALI",
            "PH_TOT",
        ]
        for value_name in error_params:
            ddf = self.add_error_column(ddf, value_name)


        # return standardized dataframe
        return super().standardize_data(ddf)

#-------------------------------------------------------------------------------------------------------------#
## this is where add_profile_id() can go

#-------------------------------------------------------------------------------------------------------------#
## Keep best values for each row
    def keep_best_values(self, df, param):
        """Keep the best observation available for each row

        Arguments:
        df -- a row or a partition of a pandas dataframe
        param -- name of the qc variable of the parameter

        Returns:
        df -- updated dataframe

        """

        # SPOTS' quality control columns end with "_FLAG_W" (e.g. "NITRAT_FLAG_W")
        # and 2 means usable
        condition = ~df[param].isin([2])

        # Find bad QC values
        df.loc[condition, param] = pd.NA
        df.loc[condition, param[:-7]] = pd.NA

        return df
#-------------------------------------------------------------------------------------------------------------#
## Add error column
    def add_error_column(df, value_name):
        """Add error column to pandas dataframe
        
        Argument:
        df -- pandas dataframe
        value_name -- name of parameter
        
        Returns:
        df -- pandas dataframe
        """
        
        precision_col = value_name + "_P"
        accuracy_col = value_name + "_A"
        error_col = value_name + "_ERROR"
        
        df[error_col] = pd.NA
            
        has_p = df[precision_col].notna()
        has_a = df[accuracy_col].notna()
        
        # for rows with precision but no accuracy, put precision value into error column
        df.loc[has_p & ~has_a, error_col] = df.loc[has_p & ~has_a, precision_col]
        
        # for any row where accuracy exists, put accuracy value into error column
        df.loc[has_a, error_col] = df.loc[has_a, accuracy_col]

        return df
     
#######################################################################################################
#if __name__ == "__main__":
#    ConverterSPOTS()