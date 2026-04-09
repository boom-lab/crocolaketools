#!/usr/bin/env python3

## @file download_saildrones.py
#
#
## @author Alieldin Alaa <alieldinalaa04@gmail.com>
#
# @date Wed 18 Mar 2026

##########################################################################
import argparse
from datetime import datetime

from crocolaketools.downloader.downloader_saildrones import DownloaderSaildrones
##########################################################################


def download_saildrones(
    config: dict = None,
    overwrite: bool = False,
) -> None:
    """Download Saildrones TPOS netCDF files using DownloaderSaildrones.

    Parameters
    ----------
    config    : configuration dict passed to DownloaderSaildrones.
                Must contain at least 'db' and 'db_type'.
                Defaults to {'db': 'Saildrones', 'db_type': 'PHY'}.
    overwrite : re-download even if file already exists.
    """
    downloader = DownloaderSaildrones(
        config=config,
        overwrite=overwrite,
    )
    downloader.saildrones_download()


#------------------------------------------------------------------------------#
def main():
    parser = argparse.ArgumentParser(
        description="Download Saildrones TPOS netCDF files from PMEL ERDDAP."
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help="Re-download even if file already exists on disk",
    )
    parser.add_argument(
        "--config",
        action="store_true",
        default=False,
        help="Use config.yaml defaults for input_path instead of CLI arguments",
    )

    args = parser.parse_args()

    # when --config is passed, let DownloaderSaildrones read input_path from
    # config.yaml; otherwise use the default {'db': 'Saildrones', 'db_type': 'PHY'}
    config = None if args.config else {'db': 'Saildrones', 'db_type': 'PHY'}

    download_saildrones(
        config=config,
        overwrite=args.overwrite,
    )

##########################################################################

if __name__ == "__main__":
    print(datetime.now())
    print()
    main()
    print("download_saildrones.py executed successfully")
    print()
    print(datetime.now())
    print(" ")
