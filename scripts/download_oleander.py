#!/usr/bin/env python3

## @file download_oleander.py
#
# CLI for downloading Oleander data.
#
## @author David Nady <davidnady4yad@gmail.com>
#
## @date Wed 23 Jul 2025

import argparse
from crocolaketools.downloader.downloaderOleander import DownloaderURLList

def main():
    parser = argparse.ArgumentParser(description='Download Oleander data from a list of URLs or by specifying years.')
    parser.add_argument('-u', '--url_file', help='Path to a text file containing a list of URLs to download.')
    parser.add_argument('-d', '--destination', required=True, help='Destination folder to download the data to.')
    parser.add_argument('-p', '--strip_prefix', default='erddap/files/oleanderXbtNcFiles/', help='Prefix to strip from the URL path to create the local directory structure.')
    parser.add_argument('-t', '--threads', type=int, default=4, help='Number of threads to use for downloading.')
    parser.add_argument('--start_year', type=int, help='Start year for downloading Oleander data (e.g., 2020)')
    parser.add_argument('--end_year', type=int, help='End year for downloading Oleander data (e.g., 2024)')
    
    args = parser.parse_args()

    if args.url_file:
        with open(args.url_file, 'r') as f:
            urls = [url.strip() for url in f.readlines() if url.strip()]
    elif args.start_year and args.end_year:
        years = range(args.start_year, args.end_year + 1)
        urls = [f"http://erddap.oleander.bios.edu:8080/erddap/files/oleanderXbtNcFiles/{year}_xbt_nc.zip" for year in years]
    else:
        print("Please provide either --url_file or --start_year and --end_year")
        return

    downloader = DownloaderURLList()
    downloader.url_list_download(
        urls=urls,
        base_dir=args.destination,
        strip_prefix=args.strip_prefix,
        num_threads=args.threads
    )

if __name__ == "__main__":
    main()