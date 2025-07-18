#!/usr/bin/env python3

## @file converterArgoQC.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Tue 04 Feb 2025

##########################################################################
import os
import warnings
import dask.dataframe as dd
from dask.distributed import Lock
import gsw
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import xarray as xr
from crocolakeloader import params
from crocolaketools.converter.converter import Converter
##########################################################################

class ConverterArgoQC(Converter):

    """class ConverterSprayGliders: methods to generate parquet schemas for
    Spray Gliders netCDF files

    """

    # ------------------------------------------------------------------ #
    # Constructors/Destructors                                           #
    # ------------------------------------------------------------------ #

    def __init__(self, config=None, db_type=None):

        if config is not None and not config['db'] == "ARGO":
            raise ValueError("Database must be ARGO.")
        elif ((config is None) and (db_type is not None)):
            config = {
                'db': 'ARGO',
                'db_type': db_type.upper(),
            }

        Converter.__init__(self, config)

    # ------------------------------------------------------------------ #
    # Methods                                                            #
    # ------------------------------------------------------------------ #

    def convert(self, filenames=None):

        if isinstance(filenames,str) or filenames is None:
            ddf_c = self.read_to_df(filename=filenames)
        elif isinstance(filenames,list):
            ddf_c = dd.from_map( self.read_to_df, filenames )

        if self.add_derived_vars:
            ddf_c = self.add_derived_variables(ddf_c)

        ddf_c = ddf_c.repartition(partition_size="300MB")
        self.to_parquet(ddf_c)

#------------------------------------------------------------------------------#
## Read file to convert into a pandas dataframe
    def read_to_df(self, filename=None, lock=None):
        """Read ARGO parquet database into a dask dataframe and update its
        columns to match the format for higher QC data

        Arguments:
        filename  --  path to parquet database
        lock      --  (unused, needed for compatibility with super.read_to_df() )

        Returns:
        ddf -- dask dataframe containing ARGO observations for high QC version
        """

        print("read_pq...")
        print(filename)
        ddf = self.read_pq(filename=filename)
        print("read.")

        print("update_cols...")
        ddf = self.update_cols(ddf)
        print("updated.")
        self.generate_schema(ddf.columns.to_list())

        return ddf

#------------------------------------------------------------------------------#
## Read ARGO parque database
    def read_pq(self,filename=None):
        """Generate filters and basenames, and read in the ARGO database

        Returns:
        ddf -- dask dataframe containing ARGO observations read from parquet
        """

        filters, self.param_basenames = self.generate_qc_schema_filters()

        if filename is None:
            filename = self.input_path # read whole database

        print("Reading ARGO database from ", filename)
        ddf = dd.read_parquet(
            filename,
            engine="pyarrow",
            dtype_backend = "pyarrow",
            filters = filters,
            split_row_groups = False,
        )

        cols_cat = []
        for param in ddf.columns:
            if "DATA_MODE" in param:
                cols_cat.append(param)
        cols_cat.append("DIRECTION")
        ddf = ddf.categorize(columns=cols_cat)

        return ddf

#------------------------------------------------------------------------------#
## Update columns
    def update_cols(self,ddf):
        """Update columns to keep the best values for each row, add database
        name, and remove extra columns

        Argument:
        ddf  --  dask dataframe containing ARGO observations read from parquet

        Returns:
        ddf -- updated dask dataframe
        """

        data_mode_col = "DATA_MODE"
        meta = {}
        for col in ddf.columns:
            meta[col] = ddf.dtypes[col]

        #generate meta for each parameter
        for param in self.param_basenames:
            for p in [param, param+"_QC", param+"_ERROR"]:
                if p == param:
                    meta[p] = "float32[pyarrow]"
                elif p == param+"_QC":
                    meta[p] = "uint8[pyarrow]"
                else:
                    meta[p] = "float32[pyarrow]"

        # keep best values for each parameter
        ddf = ddf.map_partitions(self.keep_best_values, self.param_basenames, self.db_type)

        # remove extra columns
        cols_to_drop = [f"{param}{suffix}"
                        for param in self.param_basenames
                        for suffix in ["_ADJUSTED", "_ADJUSTED_QC", "_ADJUSTED_ERROR"]
                        if f"{param}{suffix}" in ddf.columns]
        ddf = ddf.drop(columns=cols_to_drop)

        # keep only rows with good POSITION_QC and JULD_QC
        ddf = ddf.map_partitions(self.keep_pos_juld_best_values, self.param_basenames)

        # remove rows with only NAs
        ddf = ddf.map_partitions(super().remove_all_NAs, self.param_basenames)

        # add database name if not present
        if "DB_NAME" not in ddf.columns:
            ddf["DB_NAME"] = self.db
            categories = pd.Series(params.databases, dtype='string[pyarrow]')
            ddf["DB_NAME"] = ddf["DB_NAME"].astype(pd.CategoricalDtype(categories=categories, ordered=False))

        # convert DATA_MODEs to categorical
        categories_dm = pd.Series(["R","A","D"], dtype='string[pyarrow]')
        data_mode_columns = [col for col in ddf.columns if "DATA_MODE" in col]
        ddf[data_mode_columns] = ddf[data_mode_columns].astype(pd.CategoricalDtype(categories=categories_dm, ordered=False))

        return ddf

#------------------------------------------------------------------------------#
## Keep best values for each row
    def keep_best_values(self, df, param_basenames, db_type):
        """Keep the best observation available for each row

        Arguments:
        df    --  a row of a pandas dataframe containing Argo observations
        param_basename -- base name of the parameters to keep (i.e. <PARAM> in Argo
                          convention)
        db_type -- database type, either "PHY" or "BGC"

        Returns:
        df  --  updated dataframe
        """

        # PHY has one DATA_MODE variable for all variables of each row
        # BGC has one DATA_MODE variable for each variable of each row

        # Find good QC values
        for param in param_basenames:
            data_mode_col = param + "_DATA_MODE" if db_type == "BGC" else "DATA_MODE"

            condition_1 = ( ~df[param+"_ADJUSTED"].isna() ) & ( df[param + "_ADJUSTED_QC"].isin([1, 2, 5, 8]) ) & ( df[data_mode_col].isin(["A", "D"]) )
            condition_2 = ( ~df[param].isna() ) & ( df[param+"_QC"].isin([1, 2, 5, 8]) ) & (df[data_mode_col] == "R")
            condition_3 = ~(condition_1 | condition_2)

            # Keep best values reducing the number of columns
            df.loc[condition_1, param] = df.loc[condition_1, param+"_ADJUSTED"]
            df.loc[condition_1, param+"_QC"] = df.loc[condition_1, param+"_ADJUSTED_QC"]
            df.loc[condition_1, param+"_ERROR"] = df.loc[condition_1, param+"_ADJUSTED_ERROR"]

            # Fill param columns with NA values otherwise
            df.loc[condition_2, param+"_ERROR"] = pd.NA
            df.loc[condition_3, param] = pd.NA
            df.loc[condition_3, param+"_QC"] = pd.NA
            df.loc[condition_3, param+"_ERROR"] = pd.NA

            # Convert columns to the right type
            if df[param].dtypes != "float32[pyarrow]":
                df[param] = df[param].astype("float32[pyarrow]")
            if df[param+"_QC"].dtypes != "uint8[pyarrow]":
                df[param+"_QC"] = df[param+"_QC"].astype("uint8[pyarrow]")
            if df[param+"_ERROR"].dtypes != "float32[pyarrow]":
                df[param+"_ERROR"] = df[param+"_ERROR"].astype("float32[pyarrow]")

        return df

#------------------------------------------------------------------------------#
## Keep only rows with good POSITION_QC and JULD_QC
    def keep_pos_juld_best_values(self,df,cols_used):
        """Discard observations if we don't have a good position and time

        Arguments:
        df    --  a row of a pandas dataframe containing Argo observations
        cols_used -- list of columns to check for NA values later

        Returns:
        df  --  updated dataframe
        """

        # Find good QC values
        condition_pos_juld = ( df["POSITION_QC"].isin([1, 2, 5, 8]) ) & ( df["JULD_QC"].isin([1, 2, 5, 8]) )

        # Fill whole row with NA values if POSITION_QC or JULD_QC are not good
        # (rows with all NAs will be removed later)
        df.loc[~condition_pos_juld, cols_used] = pd.NA

        return df

#------------------------------------------------------------------------------#
## Generate schema and filters for ARGO QC
    def generate_qc_schema_filters(self):
        """ Generate schema and filters for ARGO QC from ARGO standard names

        Returns:
        filterQC -- list of tuples to filter by QC and data mode
        param_basenames -- list of base names of parameters (i.e. <PARAM> in Argo)
        """

        db_schema = pq.read_schema(self.input_path+"/_common_metadata")

        filters = []
        filter_qc = []
        filter_adj_qc = []
        param_basenames = []
        skip = ["POSITION","JULD"]
        for param in db_schema.names:
            filter_loc = []
            if "_ADJUSTED_QC" in param:
                second_last_index = param.rfind("_", 0, param.rfind("_") )
                param_base_name = param[:second_last_index]
                if param_base_name in skip:
                    continue
                if param_base_name not in param_basenames:
                    param_basenames.append(param_base_name)
                filter_loc.append( (param, "in", [1,2]) )
                if self.db_type == "BGC":
                    param_data_mode = param_base_name + "_DATA_MODE"
                    filter_loc.append( (param_data_mode, "in", ["A","D"]) )
            elif "_QC" in param and "POSITION" not in param:
                last_index = param.rfind("_")
                param_base_name = param[:last_index]
                if param_base_name in skip:
                    continue
                if param_base_name not in param_basenames:
                    param_basenames.append(param_base_name)
                filter_loc.append( (param, "in", [1,2]) )
                if self.db_type == "BGC":
                    param_data_mode = param_base_name + "_DATA_MODE"
                    filter_loc.append( (param_data_mode, "==", "R") )

            if len(filter_loc) > 0:
                filters.append(filter_loc)

        return filters, param_basenames


##########################################################################
if __name__ == "__main__":
    ConverterArgoQC()
