#!/usr/bin/env python3

## @file download_glodap.py
#
# CLI for downloading the GLODAPv2 Merged Master File.
#
## @author mahi-anol
#
## @date Fri 13 Mar 2026

##########################################################################
import argparse
from datetime import datetime

from crocolaketools.downloader.downloaderGLODAP import (
    GLODAP_MASTER_FNAME,
    DownloaderGLODAP,
)
##########################################################################


def download_glodap(
    config: dict = None,
    fname: str = GLODAP_MASTER_FNAME,
    overwrite: bool = False,
) -> str:
    """Download the GLODAP merged master CSV file.

    Parameters
    ----------
    config    : configuration dict passed to DownloaderGLODAP.
                Must contain at least 'db' and 'db_type'.
                Defaults to {'db': 'GLODAP', 'db_type': 'PHY'}.
    fname     : filename to save on disk.
    overwrite : re-download even if file already exists.

    Returns
    -------
    str
        Path to the downloaded file.
    """
    downloader = DownloaderGLODAP(
        config=config,
        fname=fname,
        overwrite=overwrite,
    )
    return downloader.glodap_download()


#------------------------------------------------------------------------------#

def main():
    parser = argparse.ArgumentParser(
        description="Download the GLODAPv2 Merged Master File to a local directory."
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

    # when --config is passed, let DownloaderGLODAP read input_path from
    # config.yaml; otherwise use the default {'db': 'GLODAP', 'db_type': 'PHY'}
    config = None if args.config else {'db': 'GLODAP', 'db_type': 'PHY'}

    download_glodap(
        config=config,
        overwrite=args.overwrite,
    )


##########################################################################

if __name__ == "__main__":
    print(datetime.now())
    print()
    main()
    print("download_glodap.py executed successfully")
    print()
    print(datetime.now())