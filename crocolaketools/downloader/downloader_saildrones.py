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

# PMEL ERDDAP server base URL
SAILDRONES_SERVER = "https://data.pmel.noaa.gov/pmel/erddap"

# Saildrones TPOS dataset IDs
SAILDRONES_DATASET_IDS = [
    "sd1005_2017",
    "sd1005_2018",
    "sd1006_2017",
    "sd1006_2018",
    "sd1029_2018",
    "sd1030_2018",
    "sd1030_tpos_2023",
    "sd1030_tpos_2023_LWR",
    "sd1033_tpos_2022",
    "sd1033_tpos_2022_LWR",
    "sd1033_tpos_2023",
    "sd1033_tpos_2024",
    "sd1052_tpos_2022",
    "sd1052_tpos_2022_LWR",
    "sd1065_tpos_2021",
    "sd1066_2019",
    "sd1066_tpos_2021",
    "sd1067_2019",
    "sd1068_2019",
    "sd1069_2019",
    "sd1079_tpos_2023",
    "sd1079_tpos_2023_LWR",
    "sd1090_tpos_2024",
    "sd_tpos_2023_sbe56",
    "sd_tpos_2024_sbe56",
]

# Download URLs for each dataset in netCDF format
SAILDRONES_URLS = [
    f"{SAILDRONES_SERVER}/tabledap/{did}.nc"
    for did in SAILDRONES_DATASET_IDS
]
##########################################################################


class DownloaderSaildrones(Downloader):
    """class DownloaderSaildrones: methods to download Saildrones
    TPOS netCDF files (missions 1-7) from the PMEL ERDDAP server.
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
        are already on disk, and download the rest.
        """
        start_time = time.time()
        print("Downloading Saildrones from ERDDAP...")

        self.get_url()

        for url in SAILDRONES_URLS:
            fname = os.path.basename(url)
            local_path = os.path.join(self.input_path, fname)


            if self._is_already_downloaded(local_path):
                print(
                    f"File already present at {local_path}. "
                    "Use overwrite=True or use '--overwrite' flag to force re-download."
                )
                continue

            print(f"Downloading from {url} ...")

            try:
                self._download_file(url, local_path)
                print(f"Saved to {local_path}")
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 404:
                    print(f"File {fname} returned 404 error. Skipping.")
                else:
                    print(f"HTTP error occurred for {fname}: {e}")
                    if os.path.exists(local_path):
                        os.remove(local_path)
            except Exception as e:
                print(f"Error downloading {fname}: {e}")
                if os.path.exists(local_path):
                    os.remove(local_path)

        elapsed_time = time.time() - start_time
        print("done.")
        print("Time to download Saildrones database: " + str(elapsed_time))

    def get_url(self) -> str:
        """Send a HEAD request to the ERDDAP server to make sure it is
        up before we start downloading. Returns the server URL or
        raises RuntimeError if it cannot be reached.
        """
        try:
            response = requests.head(SAILDRONES_SERVER, timeout=10)
            if response.ok:
                return SAILDRONES_SERVER
        except requests.RequestException:
            pass
        raise RuntimeError(
            f"ERDDAP server is unreachable: {SAILDRONES_SERVER}"
        )

##########################################################################

if __name__ == "__main__":
    DownloaderSaildrones().saildrones_download()
