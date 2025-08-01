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
    """class DownloaderURLList: methods to download files from a URL list,
    preserving directory structure
    """

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

    def create_directory_structure(self, base_dir, url, strip_prefix):
        """Create the directory structure based on the URL, stripping the specified prefix.

        Args:
            base_dir (str): Base directory to save files.
            url (str): URL of the file to download.
            strip_prefix (str): Prefix to strip from the URL path.

        Returns:
            str: Full path where the file will be saved.
        """
        parsed_url = urlparse(url)
        path = parsed_url.path.lstrip('/')

        # Strip the unwanted prefix
        if strip_prefix and path.startswith(strip_prefix):
            path = path[len(strip_prefix):].lstrip('/')

        full_path = os.path.join(base_dir, path)
        directory = os.path.dirname(full_path)
        if not os.path.exists(directory):
            os.makedirs(directory)
        return full_path

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
        """
        if dryrun:
            logging.info("DRY RUN: Would download %s to %s", url, output_path)
            return
        
        if os.path.exists(output_path) and not overwrite:
            logging.info("File %s already exists and overwrite is False. Skipping.", output_path)
            return

        max_retries = 3
        retry_delay = 2  # seconds
        
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    time.sleep(retry_delay * attempt)
                    logging.info("Retrying download of %s (attempt %d/%d)", url, attempt + 1, max_retries)
                
                response = requests.get(url, stream=True, timeout=timeout)
                response.raise_for_status()
                
                with open(output_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                logging.info("Downloaded %s to %s", url, output_path)
                
                # Unzip and delete the zip file
                self.unzip_file(output_path)
                return  # Success, exit the retry loop
                
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.RequestException) as e:
                logging.warning("Error downloading %s (attempt %d/%d): %s", url, attempt + 1, max_retries, str(e))
                if attempt == max_retries - 1:
                    logging.error("Failed to download %s after %d attempts", url, max_retries)
                    break
            except Exception as e:
                logging.error("Unexpected error downloading %s: %s", url, str(e))
                break

    def url_list_download(self, urls, base_dir, log_file="oleander_download.log", strip_prefix="thredds/fileServer/oceansites/", num_threads=4, overwrite=False, dryrun=False):
        """Download files from a list of URLs, preserving directory structure.

        Args:
            urls (list): List of URLs to download.
            base_dir (str): Base directory to save downloaded files.
            log_file (str, optional): Path to the log file.
            strip_prefix (str, optional): Prefix to strip from URL paths.
            num_threads (int, optional): Number of threads for downloading.
            overwrite (bool, optional): If True, overwrite existing files.
            dryrun (bool, optional): If True, simulate download without writing files.

        Returns:
            bool: True if download process completes with no failures, False otherwise.
        """
        self.configure_logging(log_file)
        
        if dryrun:
            logging.info("DRY RUN enabled. No files will be downloaded.")
        
        logging.info("Starting download of %d files with %d threads", len(urls), num_threads)

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            future_to_url = {
                executor.submit(
                    self.download_file, 
                    url, 
                    self.create_directory_structure(base_dir, url, strip_prefix),
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
                    future.result()
                    completed += 1
                except Exception as e:
                    failed += 1
                    logging.error("Error processing %s: %s", url, e)

        logging.info("Download completed. Success: %d, Failed: %d", completed, failed)
        return failed == 0