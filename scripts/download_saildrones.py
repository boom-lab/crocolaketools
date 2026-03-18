#!/usr/bin/env python3

## @file download_saildrones.py
#
#
## @author Alieldin Alaa <alieldinalaa04@gmail.com>
#
# @date Wed 18 Mar 2026

##########################################################################
import os
import argparse
from crocolaketools.downloader.downloader_saildrones import DownloaderSaildrones
from datetime import datetime

def download_saildrones(out_dir, search_for="TPOS", id_prefix="sd"):
    """Download OCS Saildrones files using Downloader Saildrones Submodule."""
    
    # Resolve the absolute path
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    out_dir = os.path.join(base_dir, out_dir) if not os.path.isabs(out_dir) else out_dir

    print(f"Initializing Saildrone downloader...")
    print(f"Target Output Directory: {out_dir}")

    # Instantiate the downloader and start downloading
    downloader = DownloaderSaildrones()
    downloader.saildrones_download(
        outdir_nc=out_dir, 
        search_for=search_for, 
        id_prefix=id_prefix,
        dryrun_flag=False
    )

    return

#------------------------------------------------------------------------------#
def main():
    parser = argparse.ArgumentParser(description="Download OCS Saildrones files from ERDDAP.")
    parser.add_argument(
        "--out_dir", 
        type=str, 
        default="data/original/Saildrones",
        help="Local directory to store downloaded NetCDF files."
    )
    parser.add_argument(
        "--search_for", 
        type=str, 
        default="TPOS",
        help="ERDDAP search keyword constraint."
    )
    parser.add_argument(
        "--id_prefix", 
        type=str, 
        default="sd",
        help="Dataset ID prefix required"
    )
    
    args = parser.parse_args()

    download_saildrones(args.out_dir, args.search_for, args.id_prefix)

##########################################################################

if __name__ == "__main__":
    print(datetime.now())
    print()
    main()
    print("download_saildrones.py executed successfully")
    print()
    print(datetime.now())
    print(" ")
