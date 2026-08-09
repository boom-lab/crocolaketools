#!/usr/bin/env python3

## @file ioos_gliders2parquet.py
#
#  Convert IOOS Glider DAC parquet files (downloaded from ERDDAP by
#  download_ioos_gliders) to CrocoLake-compliant parquet datasets.
#
## @author Mahi Sarwar Anol <anol.mahi@gmail.com>
#
## @date Tue 04 Aug 2026

##########################################################################
import argparse
import importlib.resources
from datetime import datetime
from warnings import simplefilter

import pandas as pd
import yaml

from dask.distributed import Client
from crocolaketools.converter.converterIOOSGliders import ConverterIOOSGliders

import functools
print = functools.partial(print, flush=True)
##########################################################################

def ioos_gliders2parquet(ioos_path=None, outdir_pqt=None, fname_pq=None, use_config_file=None, db_types=None):

    if db_types is None:
        db_types = ["PHY", "BGC"]

    config_path = importlib.resources.files("crocolaketools.config").joinpath("config_cluster.yaml")
    config_cluster = yaml.safe_load(open(config_path))

    # A single client is used for all db_types: repeatedly shutting down and
    # re-creating clients in the same process can deadlock; workers 
    # restarts between runs instead to free their memory (restarting worked for me when I faced memeory error)

    client = Client(**config_cluster["IOOS_GLIDERS"])

    try:
        for i, db_type in enumerate(db_types):

            if i > 0:
                print("Restarting dask workers to free memory...")
                client.restart()

            print(f"Working on {db_type} files...")

            if not use_config_file:
                print("Using user-defined configuration")
                config = {
                    'db': 'IOOS_GLIDERS',
                    'db_type': db_type,
                    'input_path': ioos_path,
                    'outdir_pq': outdir_pqt.rstrip("/") + f"_{db_type}/",
                    'outdir_schema': './schemas/IOOSGliders/',
                    'fname_pq': fname_pq,
                    'add_derived_vars': True,
                    'overwrite_pq': True,
                    'tmp_path': None,
                }
                converter = ConverterIOOSGliders(config)
            else: # reads from config.yaml
                print("Using configuration from config.yaml")
                converter = ConverterIOOSGliders(db_type=db_type)

            print(f"Converting {db_type} files to parquet...")
            converter.convert()
            print(f"{db_type} files converted to parquet.")

            del converter

    finally:
        client.close()

    return

#------------------------------------------------------------------------------#
def main():
    parser = argparse.ArgumentParser(description='Script to convert IOOS Glider DAC database to parquet')
    parser.add_argument('-i', help="Path to IOOS Glider DAC parquet files", required=False)
    parser.add_argument('-o', help="Destination path for parquet format database", required=False)
    parser.add_argument("-f", help="Basename for output files", required=False, default="IOOS_GLIDERS.parquet")
    parser.add_argument("-t", help="Database type(s) to generate: phy, bgc, or both", required=False, default="both", choices=["phy", "bgc", "both"])
    parser.add_argument('--config', action='store_true', help="Use config files instead of parsing arguments", required=False, default=None)

    args = parser.parse_args()

    if args.t == "both":
        db_types = ["PHY", "BGC"]
    else:
        db_types = [args.t.upper()]

    ioos_gliders2parquet(args.i, args.o, args.f, args.config, db_types)

##########################################################################
if __name__ == "__main__":
    print(datetime.now())
    print()
    main()
    print("ioos_gliders2parquet.py executed successfully")
    print()
    print(datetime.now())
    print(" ")
