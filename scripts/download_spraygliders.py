#!/usr/bin/env python3

## @file download_spraygliders.py
#
# CLI for downloading Spray Gliders Level-3 NetCDF files.
#
## @author mahi-anol
#
## @date Sat 21 Mar 2026

##########################################################################
import argparse
from datetime import datetime

from crocolaketools.downloader.downloaderSprayGliders import (
    SPRAY_BASE_URL,
    SPRAY_FILES,
    DownloaderSprayGliders,
)
##########################################################################


def download_spraygliders(
    config: dict = None,
    fnames: list = None,
    base_url: str = SPRAY_BASE_URL,
    overwrite: bool = False,
) -> list:
    """Download Spray Gliders Level-3 NetCDF files.

    Parameters
    ----------
    config   : configuration dict passed to DownloaderSprayGliders.
               Must contain at least 'db' and 'db_type'.
               Defaults to {'db': 'SprayGliders', 'db_type': 'PHY'}.
    fnames   : list of filenames to download. Defaults to SPRAY_FILES.
    base_url : base URL for the Level-3 files.
    overwrite: re-download even if file already exists.

    Returns
    -------
    list
        Paths to all downloaded files.
    """
    downloader = DownloaderSprayGliders(
        config=config,
        fnames=fnames,
        base_url=base_url,
        overwrite=overwrite,
    )
    return downloader.spray_download()


#------------------------------------------------------------------------------#

def main():
    parser = argparse.ArgumentParser(
        description="Download Spray Gliders Level-3 NetCDF files to a local directory."
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help="Re-download even if files already exist on disk",
    )
    parser.add_argument(
        "--config",
        action="store_true",
        default=False,
        help="Use config.yaml defaults for input_path",
    )
    parser.add_argument(
        "--files",
        nargs="+",
        default=None,
        help=(
            "Subset of filenames to download (e.g. --files CORC.nc GulfStream.nc). "
            "Must match keys in SPRAY_FILES. Defaults to all files."
        ),
    )

    args = parser.parse_args()

    config = None if args.config else {'db': 'SprayGliders', 'db_type': 'PHY'}

    # if --files provided, build a subset dict from SPRAY_FILES
    fnames = None
    if args.files:
        fnames = {f: SPRAY_FILES[f] for f in args.files if f in SPRAY_FILES}
        missing = [f for f in args.files if f not in SPRAY_FILES]
        if missing:
            print(f"Warning: unknown filenames ignored: {missing}")

    download_spraygliders(
        config=config,
        fnames=fnames,
        overwrite=args.overwrite,
    )


##########################################################################

if __name__ == "__main__":
    print(datetime.now())
    print()
    main()
    print("download_spraygliders.py executed successfully")
    print()
    print(datetime.now())