#!/usr/bin/env python3

## @file download_oleander.py
#
# CLI for downloading Oleander data.
#
## @author David Nady <davidnady4yad@gmail.com>
#
## @date Wed 23 Jul 2025

import argparse
from pprint import pprint
from crocolaketools.downloader.downloaderOleander import DownloaderURLList

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
        '-p', 
        '--strip_prefix', 
        default='erddap/files/oleanderXbtNcFiles/', 
        help='Prefix to strip from the URL path to create the local directory structure.'
    )
    parser.add_argument(
        '--save_to', 
        type=str,
        help='Root folder where the dataset will be downloaded to.',
        required=True
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
        '--verbose', 
        action='store_true', 
        help='If set, print additional information (currently enabled by default through logging).'
    )

    args = parser.parse_args()

    config = {
        'save_to': args.save_to,
        'threads': args.threads,
        'dryrun': args.dryrun,
        'overwrite': args.overwrite,
        'verbose': args.verbose,
        'url_file': args.url_file,
        'start_year': args.start_year,
        'end_year': args.end_year,
        'base_url': args.base_url,
        'strip_prefix': args.strip_prefix,
    }

    print("Calling Oleander downloader with the following configuration:")
    pprint(config)

    # Determine the list of URLs to download
    if config['url_file']:
        with open(config['url_file'], 'r') as f:
            urls = [url.strip() for url in f.readlines() if url.strip()]
    elif config['start_year'] and config['end_year']:
        years = range(config['start_year'], config['end_year'] + 1)
        urls = [f"{config['base_url']}/{year}_xbt_nc.zip" for year in years]
    else:
        print("\nPlease provide either --url_file or both --start_year and --end_year.")
        return

    print(f"\nAttempting to download from {len(urls)} URLs to: {config['save_to']}")
    
    downloader = DownloaderURLList()
    downloader.url_list_download(
        urls=urls,
        base_dir=config['save_to'],
        strip_prefix=config['strip_prefix'],
        num_threads=config['threads'],
        overwrite=config['overwrite'],
        dryrun=config['dryrun']
    )

    print("\nOleander download process finished.")
    if config['dryrun']:
        print("Dry run complete. No files were actually downloaded.")
    else:
        print(f"Review 'oleander_download.log' for details on the downloaded files.")


if __name__ == "__main__":
    main()