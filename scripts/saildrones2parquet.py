#!/usr/bin/env python3

## @file saildrones2parquet.py
#
# Script to convert Saildrone NetCDF files to CROCOLAKE-compliant Parquet format
#
## @author David Nady <davidnady4yad@gmail.com>
## @date Sat 18 Apr 2025

##########################################################################
import argparse
import os
import importlib.resources
import yaml
from warnings import simplefilter
from datetime import datetime

import glob
import pandas as pd
# ignore pandas "educational" performance warnings
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)
from dask.distributed import Client, Lock
from crocolaketools.converter.converterSaildrones import ConverterSaildrones

import functools
print = functools.partial(print, flush=True)
##########################################################################

def saildrones2parquet(saildrones_path=None, outdir_pqt=None, fname_pq=None, use_config_file=None):

    config_path = importlib.resources.files("crocolaketools.config").joinpath("config_cluster.yaml")
    config_cluster = yaml.safe_load(open(config_path))
    client = Client(**config_cluster["SAILDRONES"])

    if not use_config_file:
        print("Using user-defined configuration")
        config = {
            'db': 'Saildrones',
            'db_type': 'PHY',
            'input_path': saildrones_path,
            'outdir_pq': outdir_pqt,
            'outdir_schema': './schemas/Saildrones/',
            'fname_pq': fname_pq,
            'add_derived_vars': True,
            'overwrite': False,
            'tmp_path': './tmp_saildrones/',
        }
        ConverterPHY = ConverterSaildrones(config)

    else: # reads from file
        print("Using configuration from config.yaml")
        ConverterPHY = ConverterSaildrones(db_type='phy')
    print("Converting PHY files to parquet...")
    ConverterPHY.convert()
    print("PHY files converted to parquet.")
    del ConverterPHY
    print("done.")

    print("Working on BGC files...")

    # Restarting the server forces dask to free the memory
    client.shutdown()
    client = Client(**config_cluster["SAILDRONES"])

    if not use_config_file:
        print("Using user-defined configuration")
        config = {
            'db': 'Saildrones',
            'db_type': 'BGC',
            'input_path': saildrones_path,
            'outdir_pq': outdir_pqt,
            'outdir_schema': './schemas/Saildrones/',
            'fname_pq': fname_pq,
            'add_derived_vars': True,
            'overwrite': False,
            'tmp_path': './tmp_saildrones/',
        }
        ConverterBGC = ConverterSaildrones(config)

    else: # reads from file
        print("Using configuration from config.yaml")
        ConverterBGC = ConverterSaildrones(db_type='bgc')

    print("Converting BGC files to parquet...")
    ConverterBGC.convert()
    print("BGC files converted to parquet.")
    del ConverterBGC
    print("done.")

    client.shutdown()

    return

##########################################################################
def main():
    parser = argparse.ArgumentParser(description='Script to convert Saildrones database to parquet')
    parser.add_argument('-i', help="Path to Saildrones data", required=False)
    parser.add_argument('-o', help="Destination path for parquet format database", required=False)
    parser.add_argument("-f", help="Basename for output files", required=False, default="demo_SAILDRONES.parquet")
    parser.add_argument('--config', action='store_true', help="Use config files instead of parsing arguments", required=False, default=None)

    args = parser.parse_args()

    saildrones2parquet(args.i, args.o, args.f, args.config)

##########################################################################
if __name__ == "__main__":
    print(datetime.now())
    print()
    main()
    print("saildrones2parquet.py executed successfully")
    print()
    print(datetime.now())
    print(" ")