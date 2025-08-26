#!/usr/bin/env python3

## @file downloaderOleander.py
#
# Downloader for Oleander netCDF files
#
## @author David Nady <davidnady4yad@gmail.com>
#         Adapted from Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Wed 23 Jul 2025

##########################################################################
import os
import requests
import argparse
import logging
import zipfile
import time
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from crocolaketools.downloader.downloader import Downloader

##########################################################################

class DownloaderURLList(Downloader):
    """class DownloaderURLList: methods to download files from a URL list"""

    # ------------------------------------------------------------------ #
    # Constructors/Destructors                                           #
    # ------------------------------------------------------------------ #

    def __init__(self):
        """Initialize the DownloaderURLList instance."""
        super().__init__()

    # ------------------------------------------------------------------ #
    # Methods                                                            #
    # ------------------------------------------------------------------ #

    def configure_logging(self, log_file):
        """Configure logging to both file and console.

        Args:
            log_file (str): Path to the log file.
        """
        # Avoid adding handlers multiple times if called repeatedly
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.FileHandler(log_file),
                    logging.StreamHandler()
                ]
            )

    def unzip_file(self, zip_path):
        """Unzip a file and delete the original zip file.

        Args:
            zip_path (str): Path to the zip file.
        """
        try:
            extract_dir = os.path.dirname(zip_path)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            logging.info("Unzipped %s to %s", zip_path, extract_dir)
            os.remove(zip_path)
        except Exception as e:
            logging.error("Error processing zip file %s: %s", zip_path, e)

    def download_file(self, url, output_path, timeout=60, overwrite=False, dryrun=False):
        """Download a file, save it, then unzip and delete the zip.

        Args:
            url (str): URL of the file to download.
            output_path (str): Path where the file will be saved.
            timeout (int): Timeout in seconds for the request.
            overwrite (bool): If True, overwrite existing files.
            dryrun (bool): If True, log the action without downloading.

        Returns:
            bool: True if download and unzip succeeded, False otherwise.
        """
        if dryrun:
            logging.info("DRY RUN: Would download %s to %s", url, output_path)
            return True

        # Check if .nc files from this zip already exist
        if not overwrite:
            extract_dir = os.path.dirname(output_path)
            year = os.path.basename(output_path)[:4]  # Extract year from zip filename
            if os.path.exists(extract_dir) and any(f.endswith('.nc') and f.startswith(year) for f in os.listdir(extract_dir)):
                logging.info("NetCDF files from %s already exist and overwrite is False. Skipping.", output_path)
                return True

        try:
            response = requests.get(url, stream=True, timeout=timeout)
            response.raise_for_status()  # Will raise an exception for HTTP errors

            # Check content type to ensure it's not an HTML error page (indicating file not found)
            content_type = response.headers.get('Content-Type', '').lower()
            if 'text/html' in content_type:
                logging.info("File not found (returned HTML), skipping %s", url)
                return False

            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            logging.info("Downloaded %s to %s", url, output_path)

            # Unzip and delete the zip file
            self.unzip_file(output_path)
            return True
        
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.RequestException) as e:
            logging.error("Error downloading %s: %s", url, str(e))
            return False
        
        except Exception as e:
            logging.error("Unexpected error downloading %s: %s", url, str(e))
            return False

    def url_list_download(self, urls, base_dir, log_file="oleander_download.log", num_threads=4, overwrite=False, dryrun=False):
        """Download files from a list of URLs.

        Args:
            urls (list): List of URLs to download.
            base_dir (str): Base directory to save downloaded files.
            log_file (str, optional): Path to the log file.
            num_threads (int, optional): Number of threads for downloading.
            overwrite (bool, optional): If True, overwrite existing files.
            dryrun (bool, optional): If True, simulate download without writing files.

        Returns:
            bool: True if download process completes with no failures, False otherwise.
        """
        self.configure_logging(log_file)
        
        if dryrun:
            logging.info("DRY RUN enabled. No files will be downloaded.")
        
        # Ensure base_dir exists
        if not dryrun and not os.path.exists(base_dir):
            os.makedirs(base_dir, exist_ok=True)

        logging.info("Starting download of %d files with %d threads", len(urls), num_threads)

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            future_to_url = {
                executor.submit(
                    self.download_file, 
                    url, 
                    os.path.join(base_dir, os.path.basename(urlparse(url).path)),
                    overwrite=overwrite,
                    dryrun=dryrun
                ): url
                for url in urls
            }

            completed = 0
            failed = 0
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    if future.result():
                        completed += 1
                    else:
                        failed += 1
                except Exception as e:
                    failed += 1
                    logging.error("Error processing %s: %s", url, e)

        logging.info("Download completed. Success: %d, Failed: %d", completed, failed)
        return failed == 0