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
            raise ValueError("SPOTS database must be read from a single file. Please check your input.")

        df = self.read_to_df(flist[0], lock)
        if isinstance(df, pd.DataFrame):
            return dd.from_pandas(df)
        elif isinstance(df, dd.DataFrame):
            return df
        else:
            raise TypeError("read_to_df must return a pandas or dask dataframe, not: ", type(df))
        
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

        input_fname = self.input_path / filename
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

        ddf = self.add_profile_id(ddf)


        ### convert SPOTS multiple time columns to one datetime
        # add time as midnight for rows missing a time
        ddf["TIME"] = ddf["TIME"].fillna(0)
   
        ddf = ddf.map_partitions(self.add_datetime)
        ddf = ddf.persist()

        # keep only good QC values
        params_to_check =[]
        for param in db_params.params["SPOTS2CROCOLAKE"].keys(): 
            if param.endswith("_FLAG_W") and param in ddf.columns:
                ddf = ddf.map_partitions(
                    self.keep_best_values, param
                )
                params_to_check.append(param[:-7])


        # Use bottle salinity when available, otherwise use CTD salinity
        use_salnty = ~ddf["SALNTY"].isna()
        
        ddf["PSAL"] = ddf["SALNTY"].fillna(ddf["CTDSAL"])
        
        if "CTDSAL_FLAG_W" in ddf.columns:
            ddf["PSAL_QC"] = ddf["SALNTY_FLAG_W"].where(
                use_salnty,
                ddf["CTDSAL_FLAG_W"]
            )
        else:
            ddf["PSAL_QC"] = ddf["SALNTY_FLAG_W"].where(
                use_salnty, pd.NA
            )
            
        params_to_check.append("PSAL")
        
        ddf = ddf.map_partitions(
            self.add_error_column,
            "SALNTY",
            "PSAL"
        )

        
        # remove rows containing all NAs
        ddf = ddf.map_partitions(
            super().remove_all_NAs, params_to_check
        )
        ddf = ddf.persist()

        
        ddf["date_update"] = np.datetime64("2024-02-22T00:00:00.00000000")

        
        # add error parameters
        error_params = [
            "OXYGEN",
            "NITRAT",
            "PHSPHT",
            "SILCAT",
            "ALKALI",
            "PH_TOT",
        ]
        for value_name in error_params:
            ddf = ddf.map_partitions(
                self.add_error_column,
                value_name
            )

        
        # Replace SPOTS fill values with missing values
        ddf = ddf.map_partitions(self.replace_fill_values)
        
        # return standardized dataframe
        return super().standardize_data(ddf)

#-------------------------------------------------------------------------------------------------------------#
## Replace fill values
    def replace_fill_values(self, df):
        """Replace SPOTS fill values without relying on deprecated downcasting."""

        return df.mask(df.eq(-999), np.nan)

#-------------------------------------------------------------------------------------------------------------#
## Add datetime
    def add_datetime(self, df):
        """Add a single datetime column from SPOTS date and time columns."""

        date_str = df["DATE"].astype("Int64").astype(str)
        time_str = df["TIME"].astype("Int64").astype(str).str.zfill(4)
        df["JULD"] = pd.to_datetime(
            date_str + time_str,
            format="%Y%m%d%H%M",
            errors="coerce",
        )
        return df

#-------------------------------------------------------------------------------------------------------------#
## Add profile ID
    def add_profile_id(self, ddf):
        """Create deterministic profile identifiers from SPOTS cast metadata."""

        profile_columns = ["TimeSeriesSite", "CRUISE", "STNNBR", "CASTNO"]
        missing = [column for column in profile_columns if column not in ddf.columns]
        if missing:
            raise ValueError(
                "SPOTS input must contain profile-identifying columns: "
                + ", ".join(missing)
            )

        def add_profile_key(df):
            df = df.copy()
            df["_profile_key"] = (
                df[profile_columns]
                .fillna("")
                .astype("string")
                .agg("|".join, axis=1)
            )
            return df

        ddf = ddf.map_partitions(add_profile_key)
        unique_profiles = (
            ddf[profile_columns + ["_profile_key"]]
            .drop_duplicates()
            .compute()
            .sort_values(profile_columns)
        )
        unique_profiles["profile_nb"] = (
            unique_profiles.groupby("TimeSeriesSite").cumcount() + 1
        ).astype("int32")
        profile_numbers = dict(
            zip(unique_profiles["_profile_key"], unique_profiles["profile_nb"])
        )

        def assign_profile_number(df):
            df = df.copy()
            df["profile_nb"] = (
                df["_profile_key"].map(profile_numbers).astype("int32")
            )
            return df.drop(columns="_profile_key")

        meta = ddf._meta.drop(columns="_profile_key")
        meta["profile_nb"] = pd.Series(dtype="int32")
        return ddf.map_partitions(assign_profile_number, meta=meta)

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
    def add_error_column(self, df, value_name, error_name = None):
        """Add error column to pandas dataframe
        
        Argument:
        df -- pandas dataframe
        value_name -- name of parameter
        error_name -- optional name for output error column
        
        Returns:
        df -- pandas dataframe
        """

        # The optional error_name argument handles cases where the source parameter is renamed
        # before output, e.g., SALNTY_P/SALNTY_A to create PSAL_ERROR
        
        if error_name is None:
            error_name = value_name

            
        precision_col = value_name + "_P"
        accuracy_col = value_name + "_A"
        error_col = error_name + "_ERROR"
        
        df[error_col] = pd.NA
            
        has_p = df[precision_col].notna()
        has_a = df[accuracy_col].notna()
        
        # If precision exists but accuracy does not, use precision
        df.loc[has_p & ~has_a, error_col] = df.loc[has_p & ~has_a, precision_col]
        
        # If accuracy exists, use accuracy
        df.loc[has_a, error_col] = df.loc[has_a, accuracy_col]

        return df
     
#######################################################################################################
if __name__ == "__main__":
    ConverterSPOTS()
