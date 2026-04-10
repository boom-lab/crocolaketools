#!/usr/bin/env python3

## @file downloader_saildrones.py
#
#
## @author Alieldin Alaa <alieldinalaa04@gmail.com>
#
## @date Wed 18 Mar 2026

##########################################################################
import os
import time
import requests
from crocolaketools.downloader.downloader import Downloader
##########################################################################

# PMEL server base URL
SAILDRONES_SERVER = "https://www.pmel.noaa.gov/ocs/"
DEFAULT_PREFIX = "sites/default/files/"
LWR_PREFIX = "pubs-ocs/saildrone_lwr/"
DEFAULT_SUFFIX = "_1min.nc_.zip"
LWR_SUFFIX = "_1min.nc.zip"

# Saildrones TPOS dataset IDs
SAILDRONES_DATASET_IDS = [
    DEFAULT_PREFIX + "TPOS-2017_SD1005" + DEFAULT_SUFFIX,
    DEFAULT_PREFIX + "TPOS-2017_SD1006" + DEFAULT_SUFFIX,
    DEFAULT_PREFIX + "TPOS-2018_SD1005" + DEFAULT_SUFFIX,
    DEFAULT_PREFIX + "TPOS-2018_SD1006" + DEFAULT_SUFFIX,
    DEFAULT_PREFIX + "TPOS-2018_SD1029" + DEFAULT_SUFFIX,
    DEFAULT_PREFIX + "TPOS-2018_SD1030" + DEFAULT_SUFFIX,
    DEFAULT_PREFIX + "TPOS-2019_SD1066" + DEFAULT_SUFFIX,
    DEFAULT_PREFIX + "TPOS-2019_SD1067" + DEFAULT_SUFFIX,
    DEFAULT_PREFIX + "TPOS-2019_SD1068" + DEFAULT_SUFFIX,
    DEFAULT_PREFIX + "TPOS-2019_SD1069" + DEFAULT_SUFFIX,
    DEFAULT_PREFIX + "TPOS-2021_SD1065" + DEFAULT_SUFFIX,
    DEFAULT_PREFIX + "TPOS-2021_SD1066" + DEFAULT_SUFFIX,
    DEFAULT_PREFIX + "TPOS-2022_SD1033" + DEFAULT_SUFFIX,
    DEFAULT_PREFIX + "TPOS-2022_SD1052" + DEFAULT_SUFFIX,
    LWR_PREFIX + "TPOS-2022_SD1033_LWR" + LWR_SUFFIX,
    LWR_PREFIX + "TPOS-2022_SD1052_LWR" + LWR_SUFFIX,
    DEFAULT_PREFIX + "TPOS-2023_SD1030" + DEFAULT_SUFFIX,
    DEFAULT_PREFIX + "TPOS-2023_SD1033" + DEFAULT_SUFFIX,
    DEFAULT_PREFIX + "TPOS-2023_SD1079" + DEFAULT_SUFFIX,
    LWR_PREFIX + "TPOS-2023_SD1030_LWR" + LWR_SUFFIX,
    LWR_PREFIX + "TPOS-2023_SD1079_LWR" + LWR_SUFFIX,
    DEFAULT_PREFIX + "TPOS-2024_SD1033" + DEFAULT_SUFFIX,
    DEFAULT_PREFIX + "TPOS-2024_SD1090" + DEFAULT_SUFFIX,
]

# Download URLs for each dataset in zip format
SAILDRONES_URLS = [
    f"{SAILDRONES_SERVER}{did}" for did in SAILDRONES_DATASET_IDS
]
##########################################################################


class DownloaderSaildrones(Downloader):
    """class DownloaderSaildrones: methods to download Saildrones
    TPOS netCDF files (missions 1-7) directly from the PMEL website archives.
    """

    # ------------------------------------------------------------------ #
    # Constructors/Destructors                                           #
    # ------------------------------------------------------------------ #

    def __init__(
        self,
        config: dict = None,
        overwrite: bool = False,
    ):
        """Initialize the Saildrones downloader.

        Arguments:
        config    -- configuration dictionary (must contain 'db' and 'db_type').
        overwrite -- if True, re-download files that already exist.
        """
        if config is None:
            config = {
                'db': 'Saildrones',
                'db_type': 'PHY',
            }
            
        super().__init__(config)
        self.overwrite = overwrite

    # ------------------------------------------------------------------ #
    # Methods                                                            #
    # ------------------------------------------------------------------ #

    def saildrones_download(self) -> None:
        """Loop through the known Saildrones URLs, skip files that
        are already on disk, download the zips, and extract them.
        """
        start_time = time.time()
        print("Downloading Saildrones from PMEL website...")

        urls = self.get_url()

        for url in urls:
            zip_fname = os.path.basename(url)

            # Define what the extracted .nc file name should be
            if zip_fname.endswith('.nc_.zip'):
                nc_fname = zip_fname.replace('.nc_.zip', '.nc')
            elif zip_fname.endswith('.nc.zip'):
                nc_fname = zip_fname.replace('.nc.zip', '.nc')
            else:
                nc_fname = zip_fname.replace('.zip', '')

            local_nc_path = os.path.join(self.input_path, nc_fname)
            local_zip_path = os.path.join(self.input_path, zip_fname)


            if self._is_already_downloaded(local_nc_path):
                print(
                    f"File already present at {local_nc_path}. "
                    "Use overwrite=True or use '--overwrite' flag to force re-download."
                )
                continue

            print(f"Downloading from {url} ...")

            try:
                self._download_file(url, local_zip_path)
                print(f"Saved archive to {local_zip_path}, extracting...")
                self.unzip_file(local_zip_path)
                print(f"Extracted to {local_nc_path}")
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 404:
                    print(f"File {zip_fname} returned 404 error. Skipping.")
                else:
                    print(f"HTTP error occurred for {zip_fname}: {e}")
                    if os.path.exists(local_zip_path):
                        os.remove(local_zip_path)
            except Exception as e:
                print(f"Error downloading/extracting {zip_fname}: {e}")
                if os.path.exists(local_zip_path):
                    os.remove(local_zip_path)

        elapsed_time = time.time() - start_time
        print("done.")
        print("Time to download Saildrones database: " + str(elapsed_time))

    def get_url(self) -> list:
        """Returns the list of URLs."""
        return SAILDRONES_URLS

##########################################################################

if __name__ == "__main__":
    DownloaderSaildrones().saildrones_download()
