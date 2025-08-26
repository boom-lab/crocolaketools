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

    def __init__(self, urls, base_dir, log_file="oleander_download.log", num_threads=4, overwrite=False, dryrun=False):
        """Initialize the DownloaderURLList instance with configuration."""
        super().__init__()
        self.urls = urls
        self.base_dir = base_dir
        self.log_file = log_file
        self.num_threads = num_threads
        self.overwrite = overwrite
        self.dryrun = dryrun
        self.configure_logging(self.log_file)
        if self.dryrun:
            logging.info("DRY RUN enabled. No files will be downloaded.")

        if not self.dryrun and not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir, exist_ok=True)

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

    def download_file(self, url, output_path):
        """Download a file, save it, then unzip and delete the zip.

        Args:
            url (str): URL of the file to download.
            output_path (str): Path where the file will be saved.

        Returns:
            bool: True if download and unzip succeeded, False otherwise.
        """
        if self.dryrun:
            logging.info("DRY RUN: Would download %s to %s", url, output_path)
            return True

        # Check if .nc files from this zip already exist
        if not self.overwrite:
            extract_dir = os.path.dirname(output_path)
            year = os.path.basename(output_path)[:4]  # Extract year from zip filename
            if os.path.exists(extract_dir) and any(f.endswith('.nc') and f.startswith(year) for f in os.listdir(extract_dir)):
                logging.info("NetCDF files from %s already exist and overwrite is False. Skipping.", output_path)
                return True

        try:
            response = requests.get(url, stream=True)
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

    def url_list_download(self):
        """Download files from a list of URLs."""
        logging.info("Starting download of %d files with %d threads", len(self.urls), self.num_threads)

        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            future_to_url = {
                executor.submit(
                    self.download_file, 
                    url, 
                    os.path.join(self.base_dir, os.path.basename(urlparse(url).path))
                ): url
                for url in self.urls
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