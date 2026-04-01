#!/usr/bin/env python3

## @file downloaderOleanderXBT.py
#
# Downloader for OleanderXBT netCDF files.
#
## @author David Nady <davidnady4yad@gmail.com>
#         Adapted from Enrico Milanese <enrico.milanese@whoi.edu>
#         Refactored by mahi-anol
#
## @date Wed 23 Jul 2025

##########################################################################
import logging
import os
import re
import html as html_module
from urllib.parse import urlparse

import requests

from crocolaketools.downloader.downloader import Downloader
##########################################################################

# Base ERDDAP URL for OleanderXBT files
OLEANDER_BASE_URL = (
    "http://erddap.oleander.bios.edu:8080/erddap/files/oleanderXbtNcFiles"
)
##########################################################################


class DownloaderURLList(Downloader):
    """class DownloaderURLList: build and download a list of OleanderXBT URLs.

    This class handles OleanderXBT-specific logic: resolving which years
    are available on the ERDDAP server, constructing the zip URLs for
    each year, and calling the shared download_parallel() and unzip_file()
    methods from the Downloader base class.

    The base class provides all shared tools: _download_file(), unzip_file(),
    _is_already_downloaded(), and download_parallel().

    Typical usage
    -------------
    >>> downloader = DownloaderURLList(urls=urls, num_threads=4)
    >>> downloader.download()
    """

    # ------------------------------------------------------------------ #
    # Constructors/Destructors                                           #
    # ------------------------------------------------------------------ #

    def __init__(
        self,
        urls: list,
        log_file: str = "oleanderXBT_download.log",
        num_threads: int = 4,
        overwrite: bool = False,
        dryrun: bool = False,
        config: dict = None,
        base_dir: str = None,
    ):
        """Constructor.

        Arguments
        ---------
        urls        : list of zip file URLs to download.
        log_file    : path to log file.
        num_threads : number of concurrent download threads.
        overwrite   : if True, re-download files even if already present.
        dryrun      : if True, log what would be downloaded without fetching.
        config      : optional config dict with at least {'db', 'db_type'}.
                      Defaults to OleanderXBT PHY from config.yaml.
        base_dir    : optional destination directory. If None, uses the
                      input_path resolved by the base Downloader.
        """
        if config is None:
            config = {
                'db': 'OleanderXBT',
                'db_type': 'PHY',
            }
        super().__init__(config)

        self.urls = urls
        self.base_dir = base_dir if base_dir is not None else getattr(self, 'input_path', None)
        self.log_file = log_file
        self.num_threads = num_threads
        self.overwrite = overwrite
        self.dryrun = dryrun
        self._configure_logging(self.log_file)

    # ------------------------------------------------------------------ #
    # Public interface                                                     #
    # ------------------------------------------------------------------ #

    def download(self) -> tuple:
        """Download all URLs, unzip each archive, and return (completed, failed).

        For each URL, the zip is downloaded to base_dir, then extracted
        via the inherited unzip_file() method (which also deletes the zip
        and cleans up __MACOSX folders).

        Files whose corresponding NetCDF output already exists on disk are
        skipped unless overwrite=True.

        Returns
        -------
        tuple
            (completed, failed) counts.
        """
        # Build (url, local_zip_path) pairs, skipping already-present files
        url_path_pairs = []
        for url in self.urls:
            zip_fname = os.path.basename(urlparse(url).path)
            zip_path  = os.path.join(self.base_dir, zip_fname)

            if not self.overwrite and self._nc_files_exist(zip_path):
                logging.info(
                    "NetCDF files for %s already exist, skipping.", zip_fname
                )
                continue

            url_path_pairs.append((url, zip_path))

        if not url_path_pairs:
            logging.info("Nothing to download.")
            return 0, 0

        # Download all zips in parallel using the base class method
        completed, failed = self.download_parallel(
            url_path_pairs,
            num_threads=self.num_threads,
            dryrun=self.dryrun,
        )

        # Extract each downloaded zip
        if not self.dryrun:
            for _url, zip_path in url_path_pairs:
                if os.path.isfile(zip_path):
                    try:
                        self.unzip_file(zip_path)
                    except Exception as exc:
                        logging.error(
                            "Failed to extract %s: %s", zip_path, exc
                        )

        return completed, failed

    # ------------------------------------------------------------------ #
    # Private helpers                                                      #
    # ------------------------------------------------------------------ #

    def _nc_files_exist(self, zip_path: str) -> bool:
        """Return True if NetCDF files for this zip already exist locally.

        Checks the destination directory for .nc files whose names start
        with the same year prefix as the zip filename.

        Parameters
        ----------
        zip_path : expected local path of the zip archive.
        """
        extract_dir = os.path.dirname(zip_path)
        year = os.path.basename(zip_path)[:4]
        if not os.path.exists(extract_dir):
            return False
        return any(
            f.endswith('.nc') and f.startswith(year)
            for f in os.listdir(extract_dir)
        )

    @staticmethod
    def _configure_logging(log_file: str) -> None:
        """Set up logging to both file and console.

        Parameters
        ----------
        log_file : path to the log file.
        """
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.FileHandler(log_file),
                    logging.StreamHandler(),
                ],
            )

    # ------------------------------------------------------------------ #
    # Class methods (OleanderXBT-specific URL building)                   #
    # ------------------------------------------------------------------ #

    @staticmethod
    def get_available_years(base_url: str = OLEANDER_BASE_URL) -> list:
        """Query the ERDDAP directory listing and return available years.

        Parameters
        ----------
        base_url : ERDDAP files base URL for OleanderXBT.

        Returns
        -------
        list of int
            Sorted list of years for which zip files are available.
        """
        try:
            response = requests.get(base_url, timeout=30)
            response.raise_for_status()
            html_text = html_module.unescape(response.text)
            year_matches = re.findall(r'(\d{4})_xbt_nc\.zip', html_text)
            return sorted(set(int(y) for y in year_matches if y.isdigit() and len(y) == 4))
        except requests.RequestException as exc:
            logging.error("Error fetching directory listing: %s", exc)
            return []

    @staticmethod
    def build_urls(
        years: list,
        base_url: str = OLEANDER_BASE_URL,
    ) -> list:
        """Build download URLs for the given list of years.

        Parameters
        ----------
        years    : list of years to build URLs for.
        base_url : ERDDAP files base URL for OleanderXBT.

        Returns
        -------
        list of str
            One zip URL per year.
        """
        return [f"{base_url}/{year}_xbt_nc.zip" for year in years]

##########################################################################

if __name__ == "__main__":
    DownloaderURLList(urls=[])