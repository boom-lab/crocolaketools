#!/usr/bin/env python3

## @file converterIOOSGliders.py
#
#  Converter for IOOS Glider DAC parquet files (downloaded from ERDDAP by
#  DownloaderIOOSGliders) to CrocoLake-compliant parquet format.
#
## @author Mahi Sarwar Anol <anol.mahi@gmail.com>
#
## @date sat 01 Aug 2026

##########################################################################
import os
import warnings
from collections import defaultdict

import dask
import dask.dataframe as dd
import gsw
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from crocolaketools import db_params
from crocolaketools.converter.converter import Converter
##########################################################################

class ConverterIOOSGliders(Converter):

    """
        class ConverterIOOSGliders: methods to convert IOOS Glider DAC parquet
        files to CrocoLake schema
    """

    # ------------------------------------------------------------------ #
    # Constructors/Destructors                                           #
    # ------------------------------------------------------------------ #

    def __init__(self, config=None, db_type=None):

        if config is not None and not config['db'] == "IOOS_GLIDERS":
            raise ValueError("Database must be IOOS_GLIDERS.")
        elif ((config is None) and (db_type is not None)):
            config = {
                'db': 'IOOS_GLIDERS',
                'db_type': db_type.upper(),
            }

        Converter.__init__(self, config)

        # The 'overwrite' key of the IOOS_GLIDERS_* config sections belongs to
        # the downloader (overwrite=True forces a full re-download); the
        # converter's output overwrite behavior is controlled by the separate
        # 'overwrite_pq' key. Converter.__init__ merges the on-disk config
        # into `config`, so the key is available here.
        if "overwrite_pq" in config:
            self.overwrite = config["overwrite_pq"]

        # DOXY sources are raw optode outputs in μmol/L and must become μmol/kg
        self.cols_to_convert = {
            "DOXY": "micromol/L2micromol/kg"
        }

    # ------------------------------------------------------------------ #
    # Methods                                                            #
    # ------------------------------------------------------------------ #

#------------------------------------------------------------------------------#

## Reading parquet files and convert them to dask dataframe
    def read_to_ddf(self, flist=None, lock=None, path=None):

        if path is None:
            path = self.input_path

        if flist is None:
            flist = os.listdir(path)
            print("List of files not provided, guessing from input path: ", path)

        flist = [f for f in flist if f.endswith(".parquet")]
        if len(flist) == 0:
            raise ValueError(f"No parquet files found in {path}.")

        print("List of files to process: ", flist)

        results = []
        for fname in flist:
            input_fname = os.path.join(path, fname)
            metadata = pq.read_metadata(input_fname)
            for rg in range(metadata.num_row_groups):
                read_result = self.read_to_df(fname, rg, path)
                proc_result = self.process_df(read_result[0], read_result[1])
                results.append(proc_result)

        ddf = dd.from_delayed(results)

        self.call_guess_schema = True

        return ddf

#------------------------------------------------------------------------------#
## Read one row group of a parquet file into a pandas dataframe
    @dask.delayed(nout=2)
    def read_to_df(self, filename=None, row_group=0, path=None):
        """Read one row group of a parquet file into a pandas dataframe

        Argument:
        filename  -- file name, excluding relative path
        row_group -- index of the row group to read
        path      -- path to file (defaults to self.input_path)

        Returns
        df     -- pandas dataframe
        invars -- list of variables in df
        """

        if filename is None:
            raise ValueError("No filename provided for IOOS Gliders database.")

        if path is None:
            path = self.input_path

        input_fname = os.path.join(path, filename)
        print(f"Reading file: {input_fname} (row group {row_group})")

        try:
            pf = pq.ParquetFile(input_fname)
            file_vars = pf.schema_arrow.names
            invars = [v for v in db_params.params["IOOS_GLIDERS"] if v in file_vars]
            df = pf.read_row_group(row_group, columns=invars).to_pandas()
        except Exception as e:
            print(f"Error reading file {input_fname}: {e}")
            raise

        # the download (file modification) time is the best available proxy
        # for the last update of the dataset
        df["date_update"] = pd.to_datetime(
            os.path.getmtime(input_fname), unit="s"
        ).floor("s")
        invars = invars + ["date_update"]

        return df, invars

#------------------------------------------------------------------------------#
## Process pandas dataframe in bounded-memory sub-chunks
    @dask.delayed(nout=1)
    def process_df(self, df, invars, rows_per_chunk=250000):
        """Process a row group in sub-chunks to bound peak memory: the BGC
        schema is wide (~120 columns), and standardizing a full ~1M-row row
        group at once can exceed the worker memory limit

        Arguments:
        df     -- pandas dataframe as read from the parquet file
        invars -- list of variables in df

        Returns:
        df    -- pandas dataframe with standardized schema
        """

        if len(df) > rows_per_chunk:
            # sub-chunks are processed sequentially on purpose: the goal here
            # is to bound memory, parallelism comes from the per-row-group tasks
            pieces = []
            for i in range(0, len(df), rows_per_chunk):
                piece = df.iloc[i:i + rows_per_chunk].reset_index(drop=True)
                pieces.append(self._process_df(piece, invars))
            return pd.concat(pieces, ignore_index=True)

        return self._process_df(df, invars)

#------------------------------------------------------------------------------#
## Process pandas dataframe to standardize it to CrocoLake schema
    def _process_df(self, df, invars):
        """Process pandas dataframe to standardize it to CrocoLake schema

        Arguments:
        df     -- pandas dataframe as read from the parquet file
        invars -- list of variables in df

        Returns:
        df    -- pandas dataframe with standardized schema
        """

        # ERDDAP serves time as timezone-aware (UTC); CrocoLake stores
        # timezone-naive UTC timestamps
        for col in ["time", "date_update"]:
            if col in df.columns and isinstance(df[col].dtype, pd.DatetimeTZDtype):
                df[col] = df[col].dt.tz_convert("UTC").dt.tz_localize(None)

        # fill missing pressure from depth (TEOS-10) before renaming, as some
        # deployments carry only depth or have sparse pressure records
        if "pressure" not in df.columns:
            df["pressure"] = np.nan
        if "depth" in df.columns:
            missing = df["pressure"].isna() & df["depth"].notna()
            if missing.any():
                df.loc[missing, "pressure"] = gsw.p_from_z(
                    -df.loc[missing, "depth"],
                    df.loc[missing, "latitude"]
                ).astype(df["pressure"].dtype)

        # merge source columns that map to the same CrocoLake variable
        # (e.g. 'chlorophyll' and 'fluorescence' both map to CHLA); first
        # non-null value wins, in the order defined in the rename map
        df = self.merge_alternate_columns(df)

        # only keep variables of interest (source names still to be renamed,
        # plus CrocoLake names already produced by the merge above)
        rename_map = db_params.params["IOOS_GLIDERS2CROCOLAKE"]
        keep = [c for c in df.columns
                if c in rename_map or c in rename_map.values()]
        df = df[keep]

        # make df consistent with CrocoLake schema
        df = self.standardize_data(df)

        # remove rows where all measurements are NAs
        cols_to_check = [
            "TEMP",
            "PSAL",
        ]
        if self.db_type == "BGC":
            cols_to_check += [
                "DOXY", "CHLA", "CDOM", "BBP470", "BBP532", "BBP700",
                "TURBIDITY", "PH_IN_SITU_TOTAL", "NITRATE", "TOT_ALKALINITY",
                "DOWNWELLING_PAR",
            ]
        cols_to_check = [col for col in cols_to_check if col in df.columns]

        df = super(ConverterIOOSGliders, self).remove_all_NAs(df, cols_to_check)

        return df

#------------------------------------------------------------------------------#
## Merge alternate source names into a single column
    def merge_alternate_columns(self, df):
        """Merge source columns that map to the same CrocoLake name into a
        single column named after the CrocoLake target; columns with a single
        source are left untouched (they are renamed later by standardize_data)

        Argument:
        df -- pandas dataframe with IOOS Glider DAC source names

        Returns:
        df -- dataframe with at most one source column per CrocoLake name
        """

        reverse_map = defaultdict(list)
        for src_var, croco_var in db_params.params["IOOS_GLIDERS2CROCOLAKE"].items():
            reverse_map[croco_var].append(src_var)

        for croco_var, src_vars in reverse_map.items():
            existing = [v for v in src_vars if v in df.columns]
            if len(existing) > 1:
                df[croco_var] = df[existing].bfill(axis=1).iloc[:, 0]
                df = df.drop(columns=[c for c in existing if c != croco_var])

        return df

#------------------------------------------------------------------------------#
## Standardize dataframe to CrocoLake schema
    def standardize_data(self, df):
        """Standardize dataframe to schema consistent across databases

        Argument:
        df -- pandas dataframe with IOOS Glider DAC data

        Returns:
        df -- homogenized dataframe
        """

        # standardize data and generate schemas
        df = super().standardize_data(df)

        # NB: quality flags are propagated as-is: the IOOS Glider DAC uses the
        # same 0-9 flag convention as Argo

        df = df[sorted(df.columns.tolist())]

        return df

##########################################################################
if __name__ == "__main__":
    ConverterIOOSGliders()
