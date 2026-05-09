#!/usr/bin/env python3

## @file download_oleanderXBT.py
#
# CLI for downloading OleanderXBT data.
#
## @author David Nady <davidnady4yad@gmail.com>
#         Refactored by Mahi Sarwar Anol <anol.mahi@gmail.com>
#
## @date Wed 23 Jul 2025

##########################################################################
import argparse
from datetime import datetime

from crocolaketools.downloader.downloaderOleanderXBT import (
    OLEANDER_BASE_URL,
    DownloaderURLList,
)
##########################################################################


def download_oleanderXBT(
    config=None,
    url_file=None,
    start_year=None,
    end_year=None,
    base_url=OLEANDER_BASE_URL,
    save_to=None,
    threads=4,
    dryrun=False,
    overwrite=False,
    log_file="oleanderXBT_download.log",
):
    """Resolve URLs and run the OleanderXBT downloader.
 
    Arguments
    ---------
    config     : configuration dict passed to DownloaderURLList.
                 If None, uses config.yaml defaults (triggered by --config flag).
    url_file   : path to a text file containing one URL per line.
    start_year : first year to download (inclusive).
    end_year   : last year to download (inclusive).
    base_url   : ERDDAP base URL for OleanderXBT.
    save_to    : directory to save downloaded files. If None, uses config.yaml.
    threads    : number of parallel download threads.
    dryrun     : if True, print summary without downloading.
    overwrite  : if True, re-download files already present.
    log_file   : path to log file.
    """
    urls = DownloaderURLList.resolve_urls(
        url_file=url_file,
        start_year=start_year,
        end_year=end_year,
        base_url=base_url,
    )
 
    if not urls:
        return
 
    downloader = DownloaderURLList(
        urls=urls,
        log_file=log_file,
        num_threads=threads,
        overwrite=overwrite,
        dryrun=dryrun,
        config=config,
        base_dir=save_to,
    )
 
    print(f"\nAttempting to download {len(urls)} files to: {downloader.base_dir}")
    completed, failed = downloader.download()
 
    print("\nOleanderXBT download finished.")
    if dryrun:
        print("Dry run complete. No files were actually downloaded.")
    else:
        print(f"Success: {completed}  Failed: {failed}")
        print(f"See '{log_file}' for details.")


def main():
    parser = argparse.ArgumentParser(
        description='Download OleanderXBT data from a list of URLs or by specifying years.'
    )
    parser.add_argument(
        '-u', '--url_file',
        help='Path to a text file containing a list of URLs to download.'
    )
    parser.add_argument(
        '--start_year',
        type=int,
        help='Start year for downloading OleanderXBT data (e.g., 2020).'
    )
    parser.add_argument(
        '--end_year',
        type=int,
        help='End year for downloading OleanderXBT data (e.g., 2024).'
    )
    parser.add_argument(
        '--base_url',
        type=str,
        default=OLEANDER_BASE_URL,
        help='Base URL for constructing download links (default: ERDDAP OleanderXBT).'
    )
    parser.add_argument(
        '--save_to',
        type=str,
        default=None,
        help='Directory to save downloaded and unzipped files.'
    )
    parser.add_argument(
        '--threads',
        type=int,
        default=4,
        help='Number of threads to use for downloading.'
    )
    parser.add_argument(
        '--dryrun',
        action='store_true',
        help='If set, no files are downloaded.'
    )
    parser.add_argument(
        '--overwrite',
        action='store_true',
        help='If set, overwrite existing files.'
    )
    parser.add_argument(
        '--log_file',
        type=str,
        default='oleanderXBT_download.log',
        help='Path to log file (default: oleanderXBT_download.log).'
    )

    parser.add_argument(
        '--config',
        action='store_true',
        default=False,
        help=(
            'Use config.yaml defaults for input_path. '
            'Can be combined with --start_year and --end_year to filter years.'
        ),
    )
    args = parser.parse_args()

    config = None if args.config else {'db': 'OleanderXBT', 'db_type': 'PHY'}

    download_oleanderXBT(
        config=config,
        url_file=args.url_file,
        start_year=args.start_year,
        end_year=args.end_year,
        base_url=args.base_url,
        save_to=args.save_to,
        threads=args.threads,
        dryrun=args.dryrun,
        overwrite=args.overwrite,
        log_file=args.log_file,
    )

##########################################################################

if __name__ == "__main__":
    print(datetime.now())
    print()
    main()
    print()
    print(datetime.now())