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
import sys
from datetime import datetime

from crocolaketools.config import config_paths as cfgp
from crocolaketools.downloader.downloaderSprayGliders import (
    SPRAY_BASE_URL,
    SPRAY_FILES,
    DownloaderSprayGliders,
)
##########################################################################


def download_spraygliders(
    config: dict = None,
    fnames: dict = None,
    base_url: str = SPRAY_BASE_URL,
    overwrite: bool = False,
) -> tuple:
    """Download Spray Gliders Level-3 NetCDF files.

    Parameters
    ----------
    config   : configuration dict passed to DownloaderSprayGliders.
               Must contain at least 'db' and 'db_type'.
               Defaults to {'db': 'SprayGliders', 'db_type': 'PHY'}.
    fnames   : dict mapping ERDDAP dataset ID to local filename.
               Defaults to SPRAY_FILES.
    base_url : base URL of the ERDDAP files/ service.
    overwrite: re-download even if the local file is unchanged.

    Returns
    -------
    tuple
        (paths of the up-to-date local files, dict of failed dataset ID ->
        reason).
    """
    downloader = DownloaderSprayGliders(
        config=config,
        fnames=fnames,
        base_url=base_url,
        overwrite=overwrite,
    )
    paths = downloader.spray_download()
    return paths, downloader.failed


def select_files(names: list) -> tuple:
    """Return the subset of SPRAY_FILES matching names, and the names that
    match nothing. A name matches a dataset ID or a local filename."""
    by_fname = {fname: dataset_id for dataset_id, fname in SPRAY_FILES.items()}
    selected = {}
    unknown = []
    for name in names:
        dataset_id = name if name in SPRAY_FILES else by_fname.get(name)
        if dataset_id is None:
            unknown.append(name)
        else:
            selected[dataset_id] = SPRAY_FILES[dataset_id]
    return selected, unknown


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
        help="Use datasets.yaml defaults for input_path",
    )
    parser.add_argument(
        "--files",
        nargs="+",
        default=None,
        help=(
            "Subset to download, as local filenames or ERDDAP dataset IDs "
            "(e.g. --files CORC.nc binnedGS). Defaults to all files."
        ),
    )


    cfgp.add_config_dir_argument(parser)
    args = parser.parse_args()

    cfgp.apply_config_dir_argument(args)

    config = None if args.config else {'db': 'SprayGliders', 'db_type': 'PHY'}

    # if --files provided, build a subset dict from SPRAY_FILES
    fnames = None
    if args.files:
        fnames, unknown = select_files(args.files)
        if unknown:
            print(f"Warning: unknown filenames or dataset IDs ignored: {unknown}")

    _, failed = download_spraygliders(
        config=config,
        fnames=fnames,
        overwrite=args.overwrite,
    )
    if failed:
        sys.exit(1)


##########################################################################

if __name__ == "__main__":
    print(datetime.now())
    print()
    main()
    print("download_spraygliders.py executed successfully")
    print()
    print(datetime.now())