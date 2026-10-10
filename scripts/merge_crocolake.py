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
import logging
import subprocess
from datetime import datetime
from pathlib import Path

from crocolaketools.loader.loader import Loader
from dask.distributed import Client

from crocolaketools.config import config_paths as cfgp
##########################################################################

#------------------------------------------------------------------------------#
def configure_logging(log_file, debug=False):
    """ Configure logging

    Args:
    log_file  -- file to save log to
    debug     -- if True, log debug info
    """

    if not debug:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, mode='w'),
                logging.StreamHandler()
            ]
        )
    else:
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s',
            handlers=[
                logging.FileHandler(log_file, mode='w'),
                logging.StreamHandler()
            ]
        )
    logging.info("Starting execution.")

#------------------------------------------------------------------------------#
def merge_crocolake(db_type,croco_path,outdir,croco_name):
    """Read existing CrocoLake made of individual sub databases and merge into unique database

    Args:
    db_type     --  PHY or BGC
    croco_path  --  path to existing CrocoLake
    outdir      --  path to output the merged CrocoLake
    croco_name  --  name of merged CrocoLake
    """

    client = Client(**cfgp.get_config_cluster_db_dict("SPRAY_GLIDERS"))

    logging.info("Client dashboard address: %s", client.dashboard_link)
    logging.info("Client scheduler address: %s", client.scheduler.address)

    crocoloader = Loader(
        db_type = db_type,
        db_rootpath=croco_path
    )
    print("getting dataframe")
    ddf = crocoloader.get_dataframe()
    crocoloader.add_units_to_schema()
    ddf = ddf.repartition(partition_size="300MB")

    name_function = lambda x: f"{croco_name}_{x:04d}.parquet"
    Path(outdir).mkdir(parents=True, exist_ok=True)

    print("writing parquet")
    ddf.to_parquet(
        outdir,
        engine="pyarrow",
        name_function=name_function,
        append=False,
        overwrite=True,
        write_metadata_file = True,
        write_index=False,
        schema=crocoloader.global_schema
    )

    client.shutdown()

#------------------------------------------------------------------------------#
def main():
    parser = argparse.ArgumentParser(description='Script to merge CrocoLake into one parquet database')
    parser.add_argument('-d', help="CrocoLake type (PHY or BGC)", required=True)
    parser.add_argument('-i', help="Path to CrocoLake", required=False)
    parser.add_argument('-o', help="Destination for merged CrocoLake", required=False)
    parser.add_argument('-f', help="Basename for output files", required=False, default="merge_crocolake_out")
    parser.add_argument('--config', action='store_true', help="Use config files instead of parsing arguments", required=False, default=None)


    cfgp.add_config_dir_argument(parser)
    args = parser.parse_args()

    cfgp.apply_config_dir_argument(args)

    if not (args.d.upper() == "PHY" or args.d.upper() == "BGC"):
        raise ValueError("CrocoLake type must be PHY or BGC.")

    if args.config:

        print("Using configuration from datasets.yaml")
        # generage symlinks
        with importlib.resources.as_file(
                importlib.resources.files("crocolaketools.config").joinpath("generate_crocolake_symlinks.sh")
        ) as sh_script:
            print("Executing script to generate symlinks for CrocoLake data...")
            variants = [args.d.upper()]
            # the merge must not run if the symlinks were not rebuilt
            subprocess.run(["bash", str(sh_script)] + variants, check=True)

        db_key = "CROCOLAKE_" + args.d.upper()
        # resolve_config_path already returns an absolute, normalised path;
        # ln_path is the `current` symlink dir, so it must not be dereferenced
        args.i = cfgp.get_config_paths_field(db_key, "ln_path")
        args.o = cfgp.get_config_paths_field(db_key, "outdir_pq")
        args.f = cfgp.get_config_paths_db_dict(db_key)["fname_pq"]

    # Configure logging
    configure_logging(args.f+".log")

    logging.info("CrocoLake type (PHY or BGC):  %s", args.d)
    logging.info("Path to CrocoLake:            %s", args.i)
    logging.info("Merged CrocoLake output path: %s", args.o)
    logging.info("Basename for output files:    %s", args.f)

    merge_crocolake(args.d.upper(),args.i,args.o,args.f)

##########################################################################

if __name__ == "__main__":
    print(datetime.now())
    print()
    main()
    print("merge_crocolake.py executed successfully")
    print()
    print(datetime.now())
    print(" ")
