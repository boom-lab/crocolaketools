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
import importlib.resources
import yaml
from warnings import simplefilter
from datetime import datetime

import argparse
import glob
import pandas as pd
# ignore pandas "educational" performance warnings
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)
from dask.distributed import Client, Lock
from crocolaketools.converter.converterSprayGliders import ConverterSprayGliders

import functools
print = functools.partial(print, flush=True)
##########################################################################

def spray2parquet(spray_path=None, outdir_pqt=None, fname_pq=None, use_config_file=None):

    config_path = importlib.resources.files("crocolaketools.config").joinpath("config_cluster.yaml")
    config_cluster = yaml.safe_load(open(config_path))
    client = Client(**config_cluster["SPRAY_GLIDERS"])

    if not use_config_file:
        print("Using user-defined configuration")
        config = {
            'db': 'GLODAP',
            'db_type': 'PHY',
            'input_path': spray_path,
            'outdir_pq': outdir_pqt,
            'outdir_schema': './schemas/SprayGliders/',
            'fname_pq': fname_pq,
            'add_derived_vars': True,
            'overwrite': False,
            'tmp_path': './tmp_spray/',
        }
        ConverterPHY = ConverterSprayGliders(config)

    else: # reads from file
        print("Using configuration from config.yaml")
        ConverterPHY = ConverterSprayGliders(db_type='phy')

    print("Creating temporary files...")
    ConverterPHY.prepare_data(
        lock=Lock()
    )

    print("Temporary files created.")

    # Restarting the server forces dask to free the memory
    client.shutdown()
    client = Client(**config_cluster["SPRAY_GLIDERS"])
    #client.restart()

    print("Converting temporary files to parquet...")
    ConverterPHY.convert(
        filepath=ConverterPHY.tmp_path,
    )

    print("Temporary files converted to parquet.")
    print("PHY files converted to parquet.")
    print("Removing temporary files...")
    ConverterPHY.cleanup()
    del ConverterPHY
    print("done.")

    print("Working on BGC files...")

    # Restarting the server forces dask to free the memory
    client.shutdown()
    client = Client(**config_cluster["SPRAY_GLIDERS"])

    if not use_config_file:
        print("Using user-defined configuration")
        config = {
            'db': 'GLODAP',
            'db_type': 'BGC',
            'input_path': spray_path,
            'outdir_pq': outdir_pqt,
            'outdir_schema': './schemas/SprayGliders/',
            'fname_pq': fname_pq,
            'add_derived_vars': True,
            'overwrite': False,
            'tmp_path': './tmp_spray/',
        }
        ConverterBGC = ConverterSprayGliders(config)

    else: # reads from file
        print("Using configuration from config.yaml")
        ConverterBGC = ConverterSprayGliders(db_type='bgc')


    # The following step is not needed if you already created all the temporary
    # files earlier with ConverterPHY and you did not call the cleanup()
    # function
    print("Creating temporary files...")
    ConverterBGC.prepare_data(
        lock=Lock()
    )

    print("Temporary files created.")

    print("Converting temporary files to parquet...")
    ConverterBGC.convert(
        filepath=ConverterBGC.tmp_path,
    )
    print("BGC files converted to parquet.")
    print("Removing temporary files...")
    ConverterBGC.cleanup()
    print("done.")

    client.shutdown()

    return

#------------------------------------------------------------------------------#
def main():
    parser = argparse.ArgumentParser(description='Script to convert Spray Gliders database to parquet')
    parser.add_argument('-i', help="Path to Spray Gliders data", required=False)
    parser.add_argument('-o', help="Destination path for parquet format database", required=False)
    parser.add_argument("-f", help="Basename for output files", required=False, default="1200_PHY_SPRAY-DEV.parquet")
    parser.add_argument('--config', action='store_true', help="Use config files instead of parsing arguments", required=False, default=None)

    args = parser.parse_args()

    spray2parquet(args.i,args.o,args.f,args.config)

##########################################################################
if __name__ == "__main__":
    print(datetime.now())
    print()
    main()
    print("spray2parquet.py executed successfully")
    print()
    print(datetime.now())
    print(" ")
