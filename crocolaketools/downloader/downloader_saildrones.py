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
import shutil
import pandas as pd
from pathlib import Path
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

    def __init__(self):
        return

    # ------------------------------------------------------------------ #
    # Methods                                                            #
    # ------------------------------------------------------------------ #

    #------------------------------------------------------------------------------#
    # Saildrones ERDDAP download function
    def download_from_erddap(self, save_to='./', dryrun=False, verbose=True, overwrite=False, checktime=True, search_for="TPOS", id_prefix="sd"):
        """
        Downloads Saildrones files from ERDDAP

        Arguments:
            save_to (str): Local directory to save downloaded NetCDF files.
            dryrun (bool): If True, only print the files that would be downloaded without actually downloading them.
            verbose (bool): If True, print detailed logs of the download process.
            overwrite (bool): If True, overwrite existing files (neglected if checktime is true).
            checktime (bool): If True, download file if it is newer than the file on disk.
            search_for (str): ERDDAP search keyword constraint.
            id_prefix (str): Dataset ID prefix required.
        """
        server = "https://data.pmel.noaa.gov/pmel/erddap"

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        # Ensure the save directory exists
        Path(save_to).mkdir(parents=True, exist_ok=True)
        
        dataset_ids = self.get_dataset_ids(server, search_for=search_for, id_prefix=id_prefix)
        
        if verbose:
            print(f"Found {len(dataset_ids)} Saildrone datasets on ERDDAP.")
            
        for dataset_id in dataset_ids:
            # Construct download URL (netcdf format)
            dataset_url = f"{server}/tabledap/{dataset_id}.nc"

            # Construct local path for the file
            filename = f"{dataset_id}.nc"
            localfile = os.path.join(save_to, filename)
            
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
                elif not overwrite:
                    if verbose:
                        print(f">>> File {filename} already exists. Skipping download.")
                    continue
                else:
                    if verbose:
                        print(f">>> File {filename} already exists. Overwriting.")
            

            # Skip actual download if dryrun
            if dryrun:
                if verbose:
                    print(f">>> (Dry-run) Would download {filename} from {dataset_url}")
                continue
            
            print(f">>> Downloading {filename} from {dataset_url}...")
            try:
                response = requests.get(dataset_url, stream=True, verify=False)
                
                if response.status_code == 404:
                    if verbose:
                        print(f">>> File {filename} returned 404 error during download (requested URL: {dataset_url}).")
                    continue
                    
                with open(localfile, 'wb') as out_file:
                    shutil.copyfileobj(response.raw, out_file)
                    del response
                    
                if verbose:
                    print(f">>> Successfully downloaded {filename}.")
                    
            except Exception as e:
                print("The following error occurred:", e)
                if verbose:
                    print(f">>> An error occurred while trying to download {filename} from {dataset_url}.")
                    if os.path.exists(localfile):
                        os.remove(localfile)
                    
        if (not dryrun) and verbose:
            print("All requested files have been downloaded.")
            
        return

    #------------------------------------------------------------------------------#
    # Get url file modification time
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

    def saildrones_download(self, outdir_nc, search_for, id_prefix, dryrun_flag):

        start_time = time.time()
        print("Downloading Saildrones from ERDDAP...")
        
        self.download_from_erddap(
            save_to=outdir_nc,
            dryrun=dryrun_flag,
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
    DownloaderSaildrones().saildrones_download(outdir_nc='.', dryrun_flag=False)

