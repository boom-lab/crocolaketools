#!/usr/bin/env python3

## @file glodap2parquet.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Wed 30 Oct 2024

##########################################################################
from datetime import datetime
import importlib.resources
import yaml
from warnings import simplefilter
from dask.distributed import Client
import pandas as pd
import argparse
# ignore pandas "educational" performance warnings
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)
from crocolaketools.converter.converterArgoQC import ConverterArgoQC
##########################################################################

def argo2argoqc_phy(argo_path,outdir_pqt,fname_pq,use_config_file):
    """Subset ARGO to QC-ed only data"""

    # Set up config dask cluster from config file
    config_path = importlib.resources.files("crocolaketools.config").joinpath("config_cluster.yaml")
    config_cluster = yaml.safe_load(open(config_path))
    client = Client(**config_cluster["ARGO-QC_PHY"])

    if not use_config_file:
        if argo_path is None:
            raise ValueError("Path to ARGO data argo_path is required")
        if outdir_pqt is None:
            raise ValueError("Output path outdir_pqt is required")
        if fname_pq is None:
            raise ValueError("Output filename fname_pq is required")

        print("Using run-time user-defined configuration")
        config = {
            'db': 'ARGO',
            'db_type': 'PHY',
            'input_path': argo_path,
            'outdir_pq': outdir_pqt,
            'outdir_schema': './schemas/ARGO/',
            'fname_pq': fname_pq,
            'add_derived_vars': True,
            'overwrite': True,
        }
        ConverterPHY = ConverterArgoQC(config)

    else: # reads from file
        print("Using configuration from config.yaml")
        ConverterPHY = ConverterArgoQC(db_type='phy')

    ConverterPHY.convert()

    client.shutdown()

#------------------------------------------------------------------------------#
def main():
    parser = argparse.ArgumentParser(description='Script to convert ARGO PHY database to QC-ed only data')
    parser.add_argument('-i', help="Path to physical Argo parquet database", required=False)
    parser.add_argument('-o', help="Destination path for best-quality physical Argo parquet database", required=False)
    parser.add_argument('-f', help="Basename for output files", required=False, default="1002_PHY_ARGO-QC-DEV")
    parser.add_argument('--config', action='store_true', help="Use config files instead of parsing arguments", required=False, default=None)

    args = parser.parse_args()

    argo2argoqc_phy(args.i,args.o,args.f,args.config)

##########################################################################

if __name__ == "__main__":
    print(datetime.now())
    print()
    main()
    print("argo2argoqc_phy.py executed successfully")
    print()
    print(datetime.now())
    print(" ")
