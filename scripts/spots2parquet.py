#!/usr/bin/env python3

## @file spots2parquet.py
#
#
## @author Kalea Holdren <kalea.holdren@whoi.edu>
#
## @date Thurs 6 Aug 2026

##########################################################################
import argparse
import importlib.resources
import yaml
from dask.distributed import Client


from datetime import datetime
from warnings import simplefilter
import pandas as pd
# ignore pandas "educational" performance warnings
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)
from crocolaketools.converter.converterSPOTS import ConverterSPOTS
##########################################################################

def spots2parquet(spots_path = None, spots_name = None, outdir_pqt_phy = None, outdir_pqt_bgc = None, fname_pq = None, use_config_file = None):
    """Convert SPOTS data to parquet format"""

    config_path = importlib.resources.files("crocolaketools.config").joinpath("config_cluster.yaml")
    config_cluster = yaml.safe_load(open(config_path))
    client = Client(**config_cluster["SPOTS"])
    print("Dask client dashboard link:", client.dashboard_link)

    if not use_config_file:
        print("Using user-defined configuration")
        config = {
            'db': 'SPOTS',
            'db_type': 'PHY',
            'input_path': spots_path,
            'outdir_pq': outdir_pqt_phy,
            'outdir_schema': './schemas/SPOTS/',
            'fname_pq': fname_pq, 
            'add_derived_vars': True,
            'overwrite': False,
        }
        ConverterPHY = ConverterSPOTS(config)
        
    else: # reads from file
        print("Using configuration from config.yaml")
        ConverterPHY = ConverterSPOTS(db_type = 'phy')

    ConverterPHY.convert()
    del ConverterPHY

    if not use_config_file:
        print("Using user-defined configuration")
        config = {
            'db': 'SPOTS',
            'db_type': 'BGC',
            'input_path': spots_path,
            'outdir_pq': outdir_pqt_bgc,
            'outdir_schema': './schemas/SPOTS/',
            'fname_pq': fname_pq,
            'add_derived_vars': True,
            'overwrite': False,
        }
        ConverterBGC = ConverterSPOTS(config)

    else: # reads from file
        print("Using configuration from config.yaml")
        ConverterBGC = ConverterSPOTS(db_type = 'bgc')

    ConverterBGC.convert()
    del ConverterBGC

    return

#------------------------------------------------------------------------------#
def main():
    parser = argparse.ArgumentParser(description = 'Script to convert SPOTS csv file to parquet')
    parser.add_argument('-i', help = "Path to SPOTS csv file", required = False, default = None)
    parser.add_argument('-n', help = "Name of SPOTS csv file", required = False, default = "spots.csv")
    parser.add_argument('--phy', help = "Destination path for physical-variables database", required = False, default = None)
    parser.add_argument('--bgc', help = "Destination path for bgc-variables database", required = False, default = None)
    parser.add_argument('-b', help = "Basename for output files", required = False, default = None)
    parser.add_argument('--config', action = 'store_true', help = "Use config files instead of parsing arguments", required = False, default = None)

    args = parser.parse_args()

    if args.b is None and args.n == "spots.csv":
        basename = args.n[:-4]
    else:
        raise ValueError("Please provide a basename for the output files.")

    spots2parquet(args.i, args.n, args.phy, args.bgc, args.b, args.config)

##########################################################################
if __name__ == "__main__":
    print(datetime.now())
    print()
    main()
    print("spots2parquet.py executed successfully")
    print()
    print(datetime.now())
    print(" ")