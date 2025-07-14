#!/usr/bin/env python3

## @file converterSaildrones.py
#
#  Converter for Saildrone NetCDF data to TRITON-compliant Parquet format
#
## @author David Nady <davidnady4yad@gmail.com>
##         Adapted from Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Sat 18 Apr 2025

##########################################################################
import os
import warnings
import dask
import dask.dataframe as dd
from dask.distributed import Lock
import gsw
import numpy as np
import pandas as pd
from pandas import ArrowDtype
import pyarrow as pa
import xarray as xr
from collections import defaultdict
from crocolakeloader import params
from crocolaketools.converter.converter import Converter
##########################################################################

class ConverterSaildrones(Converter):
    """class ConverterSaildrones: methods to generate parquet schemas for
    Saildrones NetCDF files"""

    # ------------------------------------------------------------------ #
    # Constructors/Destructors                                           #
    # ------------------------------------------------------------------ #

    def __init__(self, config=None, db_type=None):
        if config is not None and config.get("db") != "Saildrones":
            raise ValueError("Database must be 'Saildrones'.")
        elif config is None and db_type is not None:
            config = {
                "db": "Saildrones",
                "db_type": db_type.upper()
            }

        super().__init__(config)

    # ------------------------------------------------------------------ #
    # Methods                                                            #
    # ------------------------------------------------------------------ #

#------------------------------------------------------------------------------#
## Read netcdf files and convert them to dask dataframe
    def read_to_ddf(self, flist=None, lock=None):
        """Read list of netCDF files and generate list of delayed objects with
        processed data

        Arguments:
        flist -- list of files to process
        lock  -- dask lock to use for concurrency

        Returns:
        results -- list of dask dataframes
        """

        if lock is None:
            warnings.warn("No lock provided. This might lead to concurrency or segmentation fault errors.")

        results = []
        for fname in flist:
            if not fname.endswith(".nc"):
                raise ValueError(f"{fname} does not end with '.nc'.")
            read_result = self.read_to_df(fname, lock)
            proc_result = self.process_df_chunked(read_result[0], read_result[1])
            results.append(proc_result)

        # combine all results into a single dask dataframe
        ddf = dd.from_delayed(results)

        # Stores the intermediate result in memory
        # This prevents the task graph from becoming too large
        ddf = ddf.persist()

        self.call_guess_schema = True

        return ddf

#------------------------------------------------------------------------------#
## Read file to convert into a pandas dataframe
    @dask.delayed(nout=2)
    def read_to_df(self, filename=None, lock=None):
        """Read file into a pandas dataframe

        Argument:
        filename -- file name, excluding relative path
        lock     -- dask lock to use for concurrency

        Returns
        df     -- pandas dataframe
        invars -- list of variables in df
        """

        if lock is None:
            warnings.warn("No lock provided. This might lead to concurrency or segmentation fault errors.")

        if filename is None:
            raise ValueError("No filename provided for Saildrone database.")

        input_fname = self.input_path + filename
        print("Reading file: ", input_fname)

        # Hold lock for the entire NetCDF operation to prevent race conditions
        with lock:
            try:          
                ds = xr.open_dataset(input_fname, engine="netcdf4", cache=False)
                try:
                    invars = list(set(params.params["Saildrones"]) & set(ds.data_vars))
                    data = {var: ds[var].values for var in invars}
                    df = pd.DataFrame(data)
                    wmo_id = ds.attrs["wmo_id"] # use wmo_id as a PLATFORM_NUMBER
                    mission_start_year = pd.to_datetime(ds.time.min().item()).year
                finally: # Ensure dataset is always closed
                    ds.close()
                    
            except Exception as e:
                print(f"Error reading file {input_fname}: {e}")
                raise

        # Assign wmo_id from global attributes
        df["wmo_id"] = wmo_id

        # Compute CYCLE_NUMBER (cycle 0 starts in 2017)
        cycle_number = mission_start_year - 2017
        # 2020 skipped due to mission pause
        if mission_start_year >= 2021:
            cycle_number += 1
        df["CYCLE_NUMBER"] = cycle_number

        return df, invars

#------------------------------------------------------------------------------#
## Process large dataframe in chunks
    @dask.delayed(nout=1)
    def process_df_chunked(self, df, invars, rows_per_chunk=50000):
        """Process a large dataframe in chunks to manage memory
        
        Arguments:
        df     -- pandas dataframe as generated from .nc file
        invars -- list of variables in df

        Returns:
        df    -- pandas dataframe with standardized schema
        """
        
        # check if we need to chunk this dataframe
        if len(df) > rows_per_chunk:
            chunk_size = rows_per_chunk
            chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
            
            # create delayed objects for parallel processing
            delayed_chunks = []
            for chunk in chunks:
                delayed_chunk = dask.delayed(self.process_df)(chunk, invars)
                delayed_chunks.append(delayed_chunk)
            
            # compute all chunks in parallel
            processed_chunks = dask.compute(*delayed_chunks)
            
            # combine all processed chunks
            df = pd.concat(processed_chunks, ignore_index=True)
            return df
        else:
            return self.process_df(df, invars)

#------------------------------------------------------------------------------#
## Process pandas dataframe to standardize it to CrocoLake schema
    def process_df(self, df, invars):
        """Process pandas dataframe to standardize it to CrocoLake schema

        Arguments:
        df     -- pandas dataframe as generated from .nc file
        invars -- list of variables in df

        Returns:
        df    -- pandas dataframe with standardized schema
        """
        
        # Assign depth column
        df = self.assign_depths(df)

        # Ensure all identifiers are non-null AFTER depth assignment
        df.dropna(subset=["time", "latitude", "longitude", "depth"], inplace=True)

        # Group source columns by target variable
        reverse_map = defaultdict(list)
        for sensor_var, croco_var in params.params["Saildrones2CROCOLAKE"].items():
            reverse_map[croco_var].append(sensor_var)

        # Merge reads from multiple source (sensors) columns into a single croco column
        for croco_var, sensor_vars in reverse_map.items():
            existing = [v for v in sensor_vars if v in df.columns]
            if len(existing) > 1:
                df[croco_var] = df[existing].bfill(axis=1).iloc[:, 0]
                df.drop(columns=[col for col in existing if col != croco_var], inplace=True, errors='ignore')

        # make df consistent with CrocoLake schema
        df = self.standardize_data(df)

        # remove rows that are all NAs
        cols_to_check = ["TEMP", "PSAL"]
        if self.db_type == "BGC":
            cols_to_check += ["DOXY", "CHLA", "CDOM", "BBP700"]
        cols_to_check = [col for col in cols_to_check if col in df.columns]
        df = super().remove_all_NAs(df, cols_to_check)

        return df

#------------------------------------------------------------------------------#
## Assign depths to variables
    def assign_depths(self, df):
        """Assign depths to variables based on known sensor installation depths"""

        depth_map = {
            "TEMP_CTD_MEAN":               0.6,
            "TEMP_CTD_RBR_MEAN":           0.53,
            "TEMP_SBE37_MEAN":             1.7,
            "TEMP_DEPTH_HALFMETER_MEAN":   0.5,
            "O2_CONC_MEAN":                0.6,
            "O2_RBR_CONC_MEAN":            0.53,
            "O2_CONC_RBR_MEAN":            0.53,
            "O2_CONC_SBE37_MEAN":          1.7,
            "O2_CONC_UNCOR_MEAN":          0.6,
            "O2_AANDERAA_CONC_UNCOR_MEAN": 0.6,
            "O2_CONC_AANDERAA_MEAN":       0.6,
            "SAL_MEAN":                    0.6,
            "SAL_RBR_MEAN":                0.53,
            "SAL_SBE37_MEAN":              1.7,
            "CHLOR_MEAN":                  0.25,
            "CHLOR_RBR_MEAN":              0.53,
            "CHLOR_WETLABS_MEAN":          1.9,
            "CDOM_MEAN":                   1.9,
            "BKSCT_RED_MEAN":              1.9,
        }

        id_vars = ["time", "latitude", "longitude", "wmo_id", "CYCLE_NUMBER"]
        value_vars = [var for var in depth_map if var in df.columns]

        if not value_vars:
            return pd.DataFrame(columns=id_vars + ["depth"])

        df_long = df.melt(id_vars=id_vars, value_vars=value_vars, var_name='variable', value_name='value')
        df_long.dropna(subset=['value'], inplace=True)
        df_long['depth'] = df_long['variable'].map(depth_map)

        df_pivoted = df_long.pivot_table(
            index=id_vars + ['depth'], 
            columns='variable', 
            values='value'
        ).reset_index()
        
        df_pivoted.columns.name = None
        return df_pivoted

#------------------------------------------------------------------------------#
## Convert parquet schema to xarray
    def standardize_data(self, df):
        """Standardize xarray dataset to schema consistent across databases

        Argument:
        ds -- xarray dataset

        Returns:
        df -- homogenized dataframe
        """

        # convert depth to pressure using the Gibbs SeaWater (GSW) Oceanographic
        # Toolbox of TEOS-10
        df["PRES"] = gsw.p_from_z(-df["depth"], df["latitude"])
        df["PRES"] = df["PRES"].astype("float32[pyarrow]")

        # standardize data and generate schemas
        df = super().standardize_data(df)

        # add QC flag = 1 for some variables that exist in the dataframe
        df = super().add_qc_flags(df, ["TEMP","PSAL","PRES"], 1)

        df = df[sorted(df.columns.tolist())]

        return df

#------------------------------------------------------------------------------#
## Convert file
    def convert(self, filenames=None, filepath=None):
        """Override convert to handle single file to compute delayed operations, 
        and delegate to base class for multiple files.
        """
        
        if filenames is None:
            guess_path = filepath or self.input_path
            filenames = os.listdir(guess_path)

        if isinstance(filenames, str):
            filenames = [filenames]

        lock = Lock()
        # Handle single file to compute delayed results
        if len(filenames) == 1:
            print("Reading single file")
            df_delayed, invars_delayed = self.read_to_df(filenames[0])
            processed_delayed = self.process_df(df_delayed, invars_delayed)
            df = dask.compute(processed_delayed)[0]
            ddf = dd.from_pandas(df, npartitions=1)
            self.call_guess_schema = True
            
            if self.add_derived_vars:
                print("Adding derived variables")
                ddf = self.compute_derived_variables(ddf)
            ddf = self.reorder_columns(ddf)
            ddf = ddf.drop_duplicates()
            self.to_parquet(ddf)
        else: # Multiple files, delegate to base class for Dask processing
            super().convert(filenames=filenames, filepath=filepath)

##########################################################################
if __name__ == "__main__":
    ConverterSaildrones()
