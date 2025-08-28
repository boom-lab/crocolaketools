#!/usr/bin/env python3

## @file converterOleanderXBT.py
#
#
## @author David Nady <davidnady4yad@gmail.com>
##         Adapted from Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Wed 16 Jul 2025

##########################################################################
import glob
import os
import warnings
import dask
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

class ConverterOleanderXBT(Converter):

    """class ConverterOleanderXBT: methods to generate parquet schemas for
    OleanderXBT netCDF files

    """

    # ------------------------------------------------------------------ #
    # Constructors/Destructors                                           #
    # ------------------------------------------------------------------ #

    def __init__(self, config=None, db_type=None):

        if config is not None and not config['db'] == "OleanderXBT":
            raise ValueError("Database must be OleanderXBT.")
        elif ((config is None) and (db_type is not None)):
            config = {
                'db': 'OleanderXBT',
                'db_type': db_type.upper(),
            }

        Converter.__init__(self, config)

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
            read_result = self.read_to_df(fname, lock)
            proc_result = self.process_df(read_result[0], read_result[1])
            results.append(proc_result)

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
            raise ValueError("No filename provided for OleanderXBT database.")

        input_fname = self.input_path + filename
        print("Reading file: ", input_fname)

        lock.acquire(timeout=600)
        try:
            with xr.open_dataset(input_fname,cache=True,chunks=None,engine="netcdf4") as ds:
                ds_vars = list(ds.data_vars) + list(ds.coords)
                invars = list(set(params.params["OleanderXBT"]) & set(ds_vars))
                df = ds[invars].to_dataframe()
                df["date_update"] = pd.to_datetime(
                    ds.attrs["date_created"]
                )

        except Exception as e:
            print(f"Error reading file {input_fname}: {e}")
            raise

        finally: # always release lock in case of error in try block
            lock.release()

        return df, invars

#------------------------------------------------------------------------------#
## Process pandas dataframe to standardize it to CrocoLake schema
    @dask.delayed(nout=1)
    def process_df(self,df,invars):
        """Process pandas dataframe to standardize it to CrocoLake schema

        Arguments:
        df     -- pandas dataframe as generated from .nc file
        invars -- list of variables in df

        Returns:
        df    -- pandas dataframe with standardized schema
        """

        df = df.reset_index(drop=False)

        # make df consistent with CrocoLake schema
        df = self.standardize_data(df)

        # remove rows with NAs TEMP
        df = super().remove_all_NAs(df,["TEMP"])

        return df

#------------------------------------------------------------------------------#
## Convert parquet schema to xarray
    def standardize_data(self,df):
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

        # add qc flag = 1 for temperature and salinity
        df = super().add_qc_flags(df, ["TEMP", "PRES"], 1)

        df = df[sorted(df.columns.tolist())]

        return df

##########################################################################
if __name__ == "__main__":
    ConverterOleanderXBT()
