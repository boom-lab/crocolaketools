#!/usr/bin/env python3

## @file glodap2parquet.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Wed 30 Oct 2024

##########################################################################
import argparse
import os
from warnings import simplefilter
from datetime import datetime

import argparse
import glob
import pandas as pd
# ignore pandas "educational" performance warnings
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)
from dask.distributed import Client, Lock
from crocolaketools.config import config_paths as cfgp
from crocolaketools.converter.converterSprayGliders import ConverterSprayGliders

import functools
print = functools.partial(print, flush=True)
##########################################################################

def _convert_db_type(db_type, client, spray_path, outdir_pqt, fname_pq, use_config_file):
    """Convert one database type (PHY or BGC). Returns the active client."""

    if not use_config_file:
        print("Using user-defined configuration")
        config = {
            'db': 'SprayGliders',
            'db_type': db_type,
            'input_path': spray_path,
            'outdir_pq': outdir_pqt,
            'outdir_schema': './schemas/SprayGliders/',
            'fname_pq': fname_pq,
            'add_derived_vars': True,
            'overwrite': False,
            'tmp_path': './tmp_spray/',
        }
        converter = ConverterSprayGliders(config)

    else: # reads from file
        print("Using configuration from datasets.yaml")
        converter = ConverterSprayGliders(db_type=db_type.lower())

    print("Creating temporary files...")
    converter.prepare_data(
        lock=Lock()
    )

    print("Temporary files created.")

    # Restarting the server forces dask to free the memory
    client.shutdown()
    client = Client(**cfgp.get_config_cluster_db_dict("SPRAY_GLIDERS"))

    print("Converting temporary files to parquet...")
    converter.convert(
        filepath=converter.tmp_path,
    )

    print("Temporary files converted to parquet.")
    print(db_type + " files converted to parquet.")
    print("Removing temporary files...")
    converter.cleanup()
    del converter
    print("done.")

    return client

def spray2parquet(spray_path=None, outdir_pqt=None, fname_pq=None, use_config_file=None, db_type=None):
    """Convert Spray Gliders to parquet.

    db_type  --  "PHY" or "BGC" to convert only that type; None converts PHY,
                 then BGC
    """

    db_types = ["PHY", "BGC"] if db_type is None else [db_type.upper()]

    client = Client(**cfgp.get_config_cluster_db_dict("SPRAY_GLIDERS"))

    for j, dbt in enumerate(db_types):
        if j > 0:
            print("Working on " + dbt + " files...")
            # Restarting the server forces dask to free the memory
            client.shutdown()
            client = Client(**cfgp.get_config_cluster_db_dict("SPRAY_GLIDERS"))

        client = _convert_db_type(
            dbt, client, spray_path, outdir_pqt, fname_pq, use_config_file
        )

    client.shutdown()

    return

#------------------------------------------------------------------------------#
def main():
    parser = argparse.ArgumentParser(description='Script to convert Spray Gliders database to parquet')
    parser.add_argument('-i', help="Path to Spray Gliders data", required=False)
    parser.add_argument('-o', help="Destination path for parquet format database", required=False)
    parser.add_argument("-f", help="Basename for output files", required=False, default="1200_PHY_SPRAY-DEV.parquet")
    parser.add_argument('-d', '--db-type', type=str.upper, choices=["PHY", "BGC"], help="Convert only this type (PHY or BGC); default: PHY, then BGC", required=False, default=None)
    parser.add_argument('--config', action='store_true', help="Use config files instead of parsing arguments", required=False, default=None)


    cfgp.add_config_dir_argument(parser)
    args = parser.parse_args()

    cfgp.apply_config_dir_argument(args)

    spray2parquet(args.i,args.o,args.f,args.config,args.db_type)

##########################################################################
if __name__ == "__main__":
    print(datetime.now())
    print()
    main()
    print("spray2parquet.py executed successfully")
    print()
    print(datetime.now())
    print(" ")
