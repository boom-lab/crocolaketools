#!/usr/bin/env python3

## @file oleanderXBT2parquet.py
#
# Script to convert OleanderXBT NetCDF files to CROCOLAKE-compliant Parquet format
#
## @author David Nady <davidnady4yad@gmail.com>
## @date Wed 16 Jul 2025

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
from crocolaketools.converter.converterOleanderXBT import ConverterOleanderXBT

import functools
print = functools.partial(print, flush=True)
##########################################################################

def oleanderXBT2parquet(oleanderXBT_path=None, outdir_pqt=None, fname_pq=None, use_config_file=None):

    config_path = importlib.resources.files("crocolaketools.config").joinpath("config_cluster.yaml")
    config_cluster = yaml.safe_load(open(config_path))
    client = Client(**config_cluster["OLEANDER_XBT"])

    if not use_config_file:
        print("Using user-defined configuration")
        config = {
            'db': 'OleanderXBT',
            'db_type': 'PHY',
            'input_path': oleanderXBT_path,
            'outdir_pq': outdir_pqt,
            'outdir_schema': './schemas/OleanderXBT/',
            'fname_pq': fname_pq,
            'add_derived_vars': True,
            'overwrite': False,
        }
        ConverterPHY = ConverterOleanderXBT(config)

    else: # reads from file
        print("Using configuration from config.yaml")
        ConverterPHY = ConverterOleanderXBT(db_type='phy')
    print("Converting PHY files to parquet...")
    ConverterPHY.convert()
    print("PHY files converted to parquet.")
    del ConverterPHY
    print("done.")

    client.shutdown()

    return

##########################################################################
def main():
    parser = argparse.ArgumentParser(description='Script to convert OleanderXBT database to parquet')
    parser.add_argument('-i', help="Path to OleanderXBT data", required=False)
    parser.add_argument('-o', help="Destination path for parquet format database", required=False)
    parser.add_argument("-f", help="Basename for output files", required=False, default="demo_OLEANDERXBT.parquet")
    parser.add_argument('--config', action='store_true', help="Use config files instead of parsing arguments", required=False, default=None)

    args = parser.parse_args()

    oleanderXBT2parquet(args.i, args.o, args.f, args.config)

##########################################################################
if __name__ == "__main__":
    print(datetime.now())
    print()
    main()
    print("oleanderXBT2parquet.py executed successfully")
    print()
    print(datetime.now())
    print(" ")