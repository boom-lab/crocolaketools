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
import urllib3
import pandas as pd
from datetime import datetime
from dateutil.parser import parse as parsedate
from io import StringIO
from crocolaketools.downloader.downloader import Downloader
##########################################################################

class DownloaderSaildrones(Downloader):
    """class DownloaderSaildrones: methods to generate mirror of Saildrones
    files (missions 1-7) from ERDDAP
    """

    # ------------------------------------------------------------------ #
    # Constructors/Destructors                                           #
    # ------------------------------------------------------------------ #

    def __init__(self, config=None):
        """
        Initialize the Saildrones Downloader inheriting from the base Downloader.
        """
        if config is None:
            # Fallback configuration if not explicitly provided
            config = {
                'db': 'Saildrones', 
                'db_type': 'PHY', 
                'input_path': 'data/original/Saildrones',
                'overwrite': False,
                'dryrun': False
            }
            
        super().__init__(config)
        # Set attributes required by base class methods (_is_already_downloaded)
        self.overwrite = config.get('overwrite', False)
        self.dryrun = config.get('dryrun', False)

    # ------------------------------------------------------------------ #
    # Methods                                                            #
    # ------------------------------------------------------------------ #

    #------------------------------------------------------------------------------#
    # Saildrones ERDDAP download function
    def download_from_erddap(self, verbose=True, checktime=True, search_for="TPOS", id_prefix="sd"):
        """
        Downloads Saildrones files from ERDDAP

        Arguments:
            verbose (bool): If True, print detailed logs of the download process.
            checktime (bool): If True, download file if it is newer than the file on disk.
            search_for (str): ERDDAP search keyword constraint.
            id_prefix (str): Dataset ID prefix required.
        """
        server = "https://data.pmel.noaa.gov/pmel/erddap"
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        dataset_ids = self.get_dataset_ids(server, search_for=search_for, id_prefix=id_prefix)
        
        if verbose:
            print(f"Found {len(dataset_ids)} Saildrone datasets on ERDDAP.")
            
        for dataset_id in dataset_ids:
            # Construct download URL (netcdf format)
            dataset_url = f"{server}/tabledap/{dataset_id}.nc"

            # Construct local path using the base class input_path
            filename = f"{dataset_id}.nc"
            localfile = os.path.join(self.input_path, filename)
            
            if verbose:
                print(f">>>> Destination file: {localfile}.")
                
            if os.path.exists(localfile):
                if checktime:
                    # Get the modification time of the local file and the file on the server to decide whether to download
                    current_file_time = datetime.fromtimestamp(os.path.getmtime(localfile))
                    new_file_time = self.get_time_url(dataset_id, server)
                    if new_file_time:
                        tz = new_file_time.tzinfo
                        current_file_time = current_file_time.replace(tzinfo=tz).astimezone(tz)
                        # Skip download if the file on the server is not newer than the local file OR keep downloading if it is newer
                        if not new_file_time > current_file_time:
                            if verbose:
                                print(f">>> File {filename} on server is not newer than local file. Skipping.")
                            continue
                        else:
                            if verbose:
                                print(f">>> File {filename} has a newer version on ERDDAP. Downloading...")
                elif self._is_already_downloaded(localfile):
                    if verbose:
                        print(f">>> File {filename} already exists. Skipping download.")
                    continue
                else:
                    if verbose:
                        print(f">>> File {filename} already exists. Overwriting.")
            

            # Skip actual download if dryrun
            if self.dryrun:
                if verbose:
                    print(f">>> (Dry-run) Would download {filename} from {dataset_url}")
                continue
            
            print(f">>> Downloading {filename} from {dataset_url}...")
            try:
                self._download_file(dataset_url, localfile)
                if verbose:
                    print(f">>> Successfully downloaded {filename}.")
            except requests.exceptions.HTTPError as e:
                # Handling status codes natively raised by _download_file's raise_for_status()
                if e.response.status_code == 404:
                    if verbose:
                        print(f">>> File {filename} returned 404 error during download.")
                else:
                    print(f"HTTP error occurred: {e}")
                    if os.path.exists(localfile):
                        os.remove(localfile)
            except Exception as e:
                print("The following error occurred:", e)
                if verbose:
                    print(f">>> An error occurred while trying to download {filename} from {dataset_url}.")
                    if os.path.exists(localfile):
                        os.remove(localfile)
                    
        if (not self.dryrun) and verbose:
            print("All requested files have been downloaded.")
            
        return

    def get_time_url(self, dataset_id, server="https://data.pmel.noaa.gov/pmel/erddap"):
        """
        Get the most recent modification time of the ERDDAP dataset.
        Ask the ERDDAP info.csv for the 'date_modified' global attribute.
        """
        try:
            info_url = f"{server}/info/{dataset_id}/index.csv"
            df = pd.read_csv(info_url)
            
            # Filter to NC_GLOBAL and search for 'date_modified' or 'date_created'
            global_attrs = df[df['Variable Name'] == 'NC_GLOBAL']
            
            mod_row = global_attrs[global_attrs['Attribute Name'] == 'date_modified']
            if not mod_row.empty:
                date_str = mod_row.iloc[0]['Value']
                return parsedate(date_str)
            
            creat_row = global_attrs[global_attrs['Attribute Name'] == 'date_created']
            if not creat_row.empty:
                date_str = creat_row.iloc[0]['Value']
                return parsedate(date_str)
                
            return None
            
        except Exception as e:
            print(f"Error fetching modification time for {dataset_id}: {e}")
            return None

    #------------------------------------------------------------------------------#
    # Function to get dataset IDs from ERDDAP based on search criteria
    def get_dataset_ids(self, server="https://data.pmel.noaa.gov/pmel/erddap", search_for="TPOS", id_prefix="sd"):
        """
        Query the ERDDAP server to get all matching dataset IDs.
        returns a list of dataset IDs that match the search criteria and start with the specified prefix.
        """
        search_url = f"{server}/search/index.csv?page=1&itemsPerPage=100000&searchFor={search_for}"
        try:
            response = requests.get(search_url, verify=False)
            df = pd.read_csv(StringIO(response.text))
            dataset_ids = [d for d in df["Dataset ID"].tolist() if str(d).startswith(id_prefix)]
            return dataset_ids
        except Exception as e_msg:
            print(f"Failed to fetch dataset IDs: {e_msg}")
            return []

    def saildrones_download(self, search_for="TPOS", id_prefix="sd"):
        start_time = time.time()
        print("Downloading Saildrones from ERDDAP...")
        
        self.download_from_erddap(
            verbose=True,
            checktime=True,
            search_for=search_for,
            id_prefix=id_prefix
        )

        print("done.")
        elapsed_time = time.time() - start_time
        print("Time to download Saildrones database: " + str(elapsed_time))
        
        return

##########################################################################

if __name__ == "__main__":
    DownloaderSaildrones().saildrones_download()
