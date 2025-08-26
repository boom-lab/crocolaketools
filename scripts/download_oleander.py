#!/usr/bin/env python3

## @file download_oleander.py
#
# CLI for downloading Oleander data.
#
## @author David Nady <davidnady4yad@gmail.com>
#
## @date Wed 23 Jul 2025

############################################################################
import argparse
import importlib.resources
import yaml
from pprint import pprint
import requests
import re
import html
from crocolaketools.downloader.downloaderOleander import DownloaderURLList
############################################################################

def main():
    parser = argparse.ArgumentParser(description='Download Oleander data from a list of URLs or by specifying years.')
    parser.add_argument(
        '-u', 
        '--url_file', 
        help='Path to a text file containing a list of URLs to download.'
    )
    parser.add_argument(
        '--start_year', 
        type=int, 
        help='Start year for downloading Oleander data (e.g., 2020).'
    )
    parser.add_argument(
        '--end_year', 
        type=int, 
        help='End year for downloading Oleander data (e.g., 2024).'
    )
    parser.add_argument(
        '--base_url',
        type=str,
        default="http://erddap.oleander.bios.edu:8080/erddap/files/oleanderXbtNcFiles",
        help="The base URL for constructing download links for year-based downloads."
    )
    parser.add_argument(
        '--save_to', 
        type=str,
        help="Directory to save downloaded and unzipped files",
        required=False, default=None
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
        default="oleander_download.log",
        help='Path to log file (default: oleander_download.log).'
    )

    args = parser.parse_args()

    # only pass through when provided
    save_path = args.save_to

    # extract all years to determine min and max years available
    def extract_available_years(base_url):
        try:
            response = requests.get(base_url)
            response.raise_for_status()
            html_text = html.unescape(response.text)
            year_matches = re.findall(r'(\d{4})_xbt_nc\.zip', html_text)
            years = sorted(set(int(y) for y in year_matches if y.isdigit() and len(y) == 4))
            return years
        except requests.RequestException as e:
            print(f"Error fetching directory listing: {e}")
            return []
    
    min_year = min(extract_available_years(args.base_url))
    max_year = max(extract_available_years(args.base_url))

    # enforce minimum start_year
    start_year = args.start_year
    if start_year and start_year < min_year:
        print(f"Warning: Start year {start_year} is before {min_year}. Adjusting to {min_year}.")
        start_year = min_year

    # Determine the list of URLs to download
    if args.url_file:
        with open(args.url_file, 'r') as f:
            urls = [url.strip() for url in f.readlines() if url.strip()]
    elif start_year and args.end_year:
        years = range(start_year, args.end_year + 1)
        urls = [f"{args.base_url}/{year}_xbt_nc.zip" for year in years]
    else:
        print(f"\nWarning: No --url_file or --start_year/--end_year provided. Defaulting to download all Oleander XBT files ({min_year}-{max_year}).")
        response = input("Do you want to continue? (y/N): ").strip().lower()
        if response != 'y':
            print("Download cancelled.")
            return
        years = extract_available_years(args.base_url)
        urls = [f"{args.base_url}/{year}_xbt_nc.zip" for year in years]

    config = {
        'urls': urls,
        'base_dir': save_path,
        'log_file': args.log_file,
        'num_threads': args.threads,
        'overwrite': args.overwrite,
        'dryrun': args.dryrun,
    }

    print("Calling Oleander downloader with the following configuration:")
    pprint(config)

    print(f"\nAttempting to download from {len(urls)} URLs to: {save_path}")
    
    downloader = DownloaderURLList( **config )
    downloader.url_list_download()

    print("\nOleander download process finished.")
    if config['dryrun']:
        print("Dry run complete. No files were actually downloaded.")
    else:
        print(f"Review 'oleander_download.log' for details on the downloaded files.")


##########################################################################
if __name__ == "__main__":
    main()