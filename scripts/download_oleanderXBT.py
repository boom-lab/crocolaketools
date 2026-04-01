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

    args = parser.parse_args()

    # --- Build URL list (OleanderXBT-specific logic stays in the script) ---

    available_years = DownloaderURLList.get_available_years(args.base_url)

    if not available_years:
        print("Could not fetch available years from the server. Exiting.")
        return

    min_year = min(available_years)
    max_year = max(available_years)

    if args.url_file:
        with open(args.url_file, 'r') as f:
            urls = [url.strip() for url in f if url.strip()]

    elif args.start_year and args.end_year:
        start_year = max(args.start_year, min_year)
        if args.start_year < min_year:
            print(f"Warning: start year {args.start_year} is before {min_year}. Adjusting.")
        years = range(start_year, args.end_year + 1)
        urls = DownloaderURLList.build_urls(years, args.base_url)

    else:
        print(
            f"\nWarning: No --url_file or --start_year/--end_year provided. "
            f"Defaulting to all available years ({min_year}-{max_year})."
        )
        response = input("Do you want to continue? (y/N): ").strip().lower()
        if response != 'y':
            print("Download cancelled.")
            return
        urls = DownloaderURLList.build_urls(available_years, args.base_url)

    # --- Hand off to the downloader ---

    downloader = DownloaderURLList(
        urls=urls,
        log_file=args.log_file,
        num_threads=args.threads,
        overwrite=args.overwrite,
        dryrun=args.dryrun,
        base_dir=args.save_to,
    )

    print(f"\nAttempting to download {len(urls)} files to: {downloader.base_dir}")
    completed, failed = downloader.download()

    print("\nOleanderXBT download finished.")
    if args.dryrun:
        print("Dry run complete. No files were actually downloaded.")
    else:
        print(f"Success: {completed}  Failed: {failed}")
        print(f"See '{args.log_file}' for details.")


##########################################################################

if __name__ == "__main__":
    print(datetime.now())
    print()
    main()
    print()
    print(datetime.now())