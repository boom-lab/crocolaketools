#!/usr/bin/env python3

## @file converterArgoGDAC.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Tue 11 Feb 2025

##########################################################################
import dask
import dask.dataframe as dd
from dask.distributed import Client
import numpy as np
import os
import pandas as pd
from pathlib import Path
import pyarrow as pa
import time
import xarray as xr
from crocolakeloader import params
from crocolaketools.converter.converter import Converter
from crocolaketools.converter.dask_tools import daskTools
from crocolaketools.converter.generate_schema import generateSchema
##########################################################################

class ConverterArgoGDAC(Converter):

    """class ConverterSprayGliders: methods to generate parquet schemas for
    Spray Gliders netCDF files

    """

    # ------------------------------------------------------------------ #
    # Constructors/Destructors                                           #
    # ------------------------------------------------------------------ #

    def __init__(self, flists=None, metadata=None, db_types=None, outdir_parquet=None, schema_path=None, db=None):
        if not db == "ARGO-GDAC":
            raise ValueError("Database must be ARGO-GDAC.")

        if flists is None:
            raise ValueError("flists should contain two lists.")
        self.flist_phy = flists[0]
        self.flist_bgc = flists[1]

        self.metadata_phy = metadata[0]
        self.metadata_bgc = metadata[1]

        self.db_types = db_types

        self.outdir_parquet = outdir_parquet
        self.schema_path = schema_path

        self.chunk_size = 1000

    # ------------------------------------------------------------------ #
    # Methods                                                            #
    # ------------------------------------------------------------------ #

## Convert series of argo files
    @staticmethod
    def convert_dask_tools(flists, metadata, db_names, outdir_parquet, schema_path):
        """Performs conversion by building compute graph and triggering
        operations; note that this conversion is quite different from the other
        converters because of the specificity of Argo's data (multiple files
        needing some sort of parallel processing)

        Arguments:
        flist    -- list of paths to files to convert
        chunk    -- number of files processed at a time

        """

        flist_phy = flists[0]
        flist_bgc = flists[1]

        metadata_phy = metadata[0]
        metadata_bgc = metadata[1]

        for k in range(len(db_names)):

            start_time = time.time()

            db_name = db_names[k].upper()
            print("Converting " + db_name + " database...")
            genSchema = generateSchema(outdir=schema_path, db=db_name)
            schema_fname = genSchema.schema_fname
            print("Schema file for " + db_name + " database: " + schema_fname)

            if db_name=="PHY":
                flist = flist_phy
                metadata = metadata_phy
                nw = 18
                tw = 1
                memlim = "5.5GB"
            elif db_name=="BGC":
                flist = flist_bgc
                metadata = metadata_bgc
                nw = 9
                tw = 1
                memlim = "11GB"

            print("(nw, tw)")
            print( (nw, tw) )

            # convert metadata
            if len(metadata) > 0:
                metadata_dir = os.path.join(outdir_parquet, "metadata/")
                Path(metadata_dir).mkdir(parents = True, exist_ok = True)
                parquet_filename = os.path.join(metadata_dir, "Argo" + db_name + "_metadata.parquet")
                metadata.to_parquet(parquet_filename)
                print("Metadata stored to " + str(parquet_filename) + ".")

            client = Client(
                n_workers=nw,
                threads_per_worker=tw,
                processes=True,
                memory_limit=memlim,
            )

            chunksize = 1000

            daskConverter = daskTools(
                db_type = db_name,
                out_dir = outdir_parquet,
                flist = flist,
                schema_path = schema_fname,
                chunk = chunksize,
            )

            daskConverter.convert_to_parquet()

            client.shutdown()

            elapsed_time = time.time() - start_time
            print("Time to convert " + db_name + " database: " + str(elapsed_time))


##########################################################################
if __name__ == "__main__":
    ConverterArgoGDAC()
