#!/usr/bin/env python3

## @file download_glodap.py
#
# CLI for downloading the GLODAPv2 Merged Master File.
#
## @author mahi-anol
#
## @date Fri 12 Mar 2026

##########################################################################
import argparse
from datetime import datetime

from crocolaketools.downloader.downloaderGLODAP import (
    GLODAP_MASTER_FNAME,
    GLODAP_URL_GEOMAR,
    GLODAP_URL_NCEI,
    DownloaderGLODAP,
)
##########################################################################


def download_glodap(
    config: dict = None,
    fname: str = GLODAP_MASTER_FNAME,
    url: str = GLODAP_URL_NCEI,
    fallback_url: str = GLODAP_URL_GEOMAR,
    overwrite: bool = False,
) -> str:
    """Download the GLODAP merged master CSV file.

    Parameters
    ----------
    config       : configuration dict passed to DownloaderGLODAP.
                   Must contain at least 'db' and 'db_type'.
                   Defaults to {'db': 'GLODAP', 'db_type': 'PHY'}.
    fname        : filename to save on disk.
    url          : primary download URL.
    fallback_url : mirror URL used if the primary fails.
    overwrite    : re-download even if file already exists.

    Returns
    -------
    str
        Path to the downloaded file.
    """
    downloader = DownloaderGLODAP(
        config=config,
        fname=fname,
        url=url,
        fallback_url=fallback_url,
        overwrite=overwrite,
    )
    return downloader.glodap_download()


#------------------------------------------------------------------------------#

def main():
    parser = argparse.ArgumentParser(
        description="Download the GLODAPv2 Merged Master File to a local directory."
    )
    parser.add_argument(
        "--url",
        type=str,
        default=GLODAP_URL_NCEI,
        help="Primary download URL (default: NOAA NCEI)",
    )
    parser.add_argument(
        "--fallback-url",
        type=str,
        default=GLODAP_URL_GEOMAR,
        help="Fallback/mirror URL tried if primary fails (default: GEOMAR)",
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
        url=args.url,
        fallback_url=args.fallback_url,
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
