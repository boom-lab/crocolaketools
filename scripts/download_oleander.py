#!/usr/bin/env python3

## @file download_oleander.py
#
# CLI for downloading Oleander data.
#
## @author David Nady <davidnady4yad@gmail.com>
#
## @date Wed 23 Jul 2025

import argparse
import importlib.resources
import yaml
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
        '--save_to', type=str,
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

    # Load configuration to get the default save path
    config_path = importlib.resources.files("crocolaketools.config").joinpath("config.yaml")
    config_converter = yaml.safe_load(open(config_path))
    default_save_path = config_converter["Oleander_PHY"]["input_path"]

    # Use the default save path if --save_to arg is not specified
    save_path = args.save_to if args.save_to is not None else default_save_path

    config = {
        'url_file': args.url_file,
        'start_year': args.start_year,
        'end_year': args.end_year,
        'base_url': args.base_url,
        'save_to': save_path,
        'threads': args.threads,
        'dryrun': args.dryrun,
        'overwrite': args.overwrite,
        'log_file': args.log_file,
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
        log_file=config['log_file'],
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