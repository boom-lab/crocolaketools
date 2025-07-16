#!/usr/bin/env python3

## @file oleander2parquet.py
#
# Script to convert Oleander NetCDF files to CROCOLAKE-compliant Parquet format
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
from crocolaketools.converter.converterOleander import ConverterOleander

import functools
print = functools.partial(print, flush=True)
##########################################################################

def oleander2parquet(oleander_path=None, outdir_pqt=None, fname_pq=None, use_config_file=None):

    config_path = importlib.resources.files("crocolaketools.config").joinpath("config_cluster.yaml")
    config_cluster = yaml.safe_load(open(config_path))
    client = Client(**config_cluster["OLEANDER"])

    if not use_config_file:
        print("Using user-defined configuration")
        config = {
            'db': 'Oleander',
            'db_type': 'PHY',
            'input_path': oleander_path,
            'outdir_pq': outdir_pqt,
            'outdir_schema': './schemas/Oleander/',
            'fname_pq': fname_pq,
            'add_derived_vars': True,
            'overwrite': False,
        }
        ConverterPHY = ConverterOleander(config)

    else: # reads from file
        print("Using configuration from config.yaml")
        ConverterPHY = ConverterOleander(db_type='phy')
    print("Converting PHY files to parquet...")
    ConverterPHY.convert()
    print("PHY files converted to parquet.")
    del ConverterPHY
    print("done.")

    client.shutdown()

    return

##########################################################################
def main():
    parser = argparse.ArgumentParser(description='Script to convert Oleander database to parquet')
    parser.add_argument('-i', help="Path to Oleander data", required=False)
    parser.add_argument('-o', help="Destination path for parquet format database", required=False)
    parser.add_argument("-f", help="Basename for output files", required=False, default="demo_OLEANDER.parquet")
    parser.add_argument('--config', action='store_true', help="Use config files instead of parsing arguments", required=False, default=None)

    args = parser.parse_args()

    oleander2parquet(args.i, args.o, args.f, args.config)

##########################################################################
if __name__ == "__main__":
    print(datetime.now())
    print()
    main()
    print("oleander2parquet.py executed successfully")
    print()
    print(datetime.now())
    print(" ")