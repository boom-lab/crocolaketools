#!/usr/bin/env python3

## @file glodap2parquet.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Wed 30 Oct 2024

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
from crocolaketools.converter.converterGLODAP import ConverterGLODAP
##########################################################################

def glodap2parquet(glodap_path=None, glodap_name=None, outdir_pqt_phy=None, outdir_pqt_bgc=None, fname_pq=None, use_config_file=None):
    """Convert GLODAP data to parquet format"""

    config_path = importlib.resources.files("crocolaketools.config").joinpath("config_cluster.yaml")
    config_cluster = yaml.safe_load(open(config_path))
    client = Client(**config_cluster["GLODAP"])
    print("Dask client dashboard link:", client.dashboard_link)

    if not use_config_file:
        print("Using user-defined configuration")
        config = {
            'db': 'GLODAP',
            'db_type': 'PHY',
            'input_path': glodap_path,
            'outdir_pq': outdir_pqt_phy,
            'outdir_schema': './schemas/GLODAP/',
            'fname_pq': fname_pq,
            'add_derived_vars': True,
            'overwrite': False,
        }
        ConverterPHY = ConverterGLODAP(config)

    else: # reads from file
        print("Using configuration from config.yaml")
        ConverterPHY = ConverterGLODAP(db_type='phy')

    ConverterPHY.convert()
    del ConverterPHY

    if not use_config_file:
        print("Using user-defined configuration")
        config = {
            'db': 'GLODAP',
            'db_type': 'BGC',
            'input_path': glodap_path,
            'outdir_pq': outdir_pqt_bgc,
            'outdir_schema': './schemas/GLODAP/',
            'fname_pq': fname_pq,
            'add_derived_vars': True,
            'overwrite': False,
        }
        ConverterBGC = ConverterGLODAP(config)

    else: # reads from file
        print("Using configuration from config.yaml")
        ConverterBGC = ConverterGLODAP(db_type='bgc')

    ConverterBGC.convert()
    del ConverterBGC

    return

#------------------------------------------------------------------------------#
def main():
    parser = argparse.ArgumentParser(description='Script to convert GLODAP csv master file to parquet')
    parser.add_argument('-i', help="Path to GLODAP csv master file", required=False, default=None)
    parser.add_argument('-n', help="Name of GLODAP csv master file", required=False, default="GLODAPv2.2023_Merged_Master_File.csv")
    parser.add_argument('--phy', help="Destination path for physical-variables database", required=False, default=None)
    parser.add_argument('--bgc', help="Destination path for bgc-variables database", required=False, default=None)
    parser.add_argument('-b', help="Basename for output files", required=False, default=None)
    parser.add_argument('--config', action='store_true', help="Use config files instead of parsing arguments", required=False, default=None)

    args = parser.parse_args()

    if args.b is None and args.n == "GLODAPv2.2023_Merged_Master_File.csv":
        basename = args.n[:-4]
    else:
        raise ValueError("Please provide a basename for the output files.")

    glodap2parquet(args.i, args.n, args.phy, args.bgc, args.b, args.config)

##########################################################################
if __name__ == "__main__":
    print(datetime.now())
    print()
    main()
    print("glodap2parquet.py executed successfully")
    print()
    print(datetime.now())
    print(" ")
