#!/usr/bin/env python3

## @file download_db.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Thu 27 Feb 2025

##########################################################################
import argparse
import os
import requests
from tqdm import tqdm
import zipfile
##########################################################################
# Dictionary of URLs
urls = {}
urls["0002_PHY_CROCOLAKE-QC-DEV"] = "https://whoi-my.sharepoint.com/:u:/g/personal/enrico_milanese_whoi_edu/EYd-6370NqtNskScY-6hytIByrMA5LEIUVONBgzop9IVog?e=MfHCX4&download=1"
urls["0002_BGC_CROCOLAKE-QC-DEV"] = "https://whoi-my.sharepoint.com/:u:/g/personal/enrico_milanese_whoi_edu/EbjTk9CJgCZJlkvPmwI38NsBQQvUL6MXkTLBAPV5jZutVg?e=ALIZvI&download=1"
urls["0006_PHY_CROCOLAKE-QC-MERGED-DEV"] = "https://whoi-my.sharepoint.com/:u:/g/personal/enrico_milanese_whoi_edu/ETVsmC-RKnlIpH_cWf1fSHcBCfeAPGT9QOCv7Qxxrbt4Mg?e=3ihaW7&download=1"
urls["0006_BGC_CROCOLAKE-QC-MERGED-DEV"] = "https://whoi-my.sharepoint.com/:u:/g/personal/enrico_milanese_whoi_edu/EYu01zZNqjJLi9ep8eM3SNwBAG98weAgQWqlmNbYeuncRg?e=ie04C4&download=1"
urls["1002_PHY_ARGO-QC-DEV"] = "https://whoi-my.sharepoint.com/:u:/g/personal/enrico_milanese_whoi_edu/Ebme_Sr3np9CtEpcTZAnLPUBGY1rI9WvabYRtv9IAeFprg?e=Pb81Cd&download=1"
urls["1002_BGC_ARGO-QC-DEV"] = "https://whoi-my.sharepoint.com/:u:/g/personal/enrico_milanese_whoi_edu/EYs1amvXcH9Ml_qTXzdQNNYBYWPMonx8mcA6Hd2sXqNItw?e=FNEiaq&download=1"
urls["1010_PHY_ARGO-CLOUD-DEV"] = "https://whoi-my.sharepoint.com/:u:/g/personal/enrico_milanese_whoi_edu/EdC3MXJfZQ5Ku1uMBpoizpUBFHzED10M9PmY9qte7lpIqw?e=wcfl1q&download=1"
urls["1010_BGC_ARGO-CLOUD-DEV"] = "https://whoi-my.sharepoint.com/:u:/g/personal/enrico_milanese_whoi_edu/EYs1amvXcH9Ml_qTXzdQNNYBYWPMonx8mcA6Hd2sXqNItw?e=6gvkbu&download=1"

#------------------------------------------------------------------------------#
def get_db_codename(db_name, db_type, qc):

    if db_name=="CROCOLAKE":
        if not qc:
            raise ValueError("CrocoLake database available only with QC.")
        if db_type=="PHY":
            return "0002_PHY_CROCOLAKE-QC-DEV"
        elif db_type=="BGC":
            return "0002_BGC_CROCOLAKE-QC-DEV"
        else:
            raise ValueError("Invalid database type. Must be 'PHY' or 'BGC'.")

    elif db_name=="ARGO":
        if db_type=="PHY":
            if qc:
                return "1002_PHY_ARGO-QC-DEV"
            else:
                return "1010_PHY_ARGO-CLOUD-DEV"
        elif db_type=="BGC":
            if qc:
                return "1002_BGC_ARGO-QC-DEV"
            else:
                return "1010_BGC_ARGO-CLOUD-DEV"
        else:
            raise ValueError("Invalid database type. Must be 'PHY' or 'BGC'.")

#------------------------------------------------------------------------------#
def download_file(url, local_filename):
    """Download a file from a URL and save it locally.

    Args:
    url (str)             --  URL of the file to download.
    local_filename (str)  --  Local filename to save the file to.
    """
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            total_size = int(r.headers.get('content-length', 0))
            with open(local_filename, "wb") as f, tqdm(
                desc=local_filename,
                total=total_size,
                unit='iB',
                unit_scale=True,
                unit_divisor=1024,
            ) as bar:
                for chunk in r.iter_content(chunk_size=8192):
                    size = f.write(chunk)
                    bar.update(size)
            # with open(local_filename, "wb") as f:
            #     for chunk in r.iter_content(chunk_size=8192):
            #         f.write(chunk)
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")

#------------------------------------------------------------------------------#
def unzip_file(zip_filepath, extract_to):
    """Unzip a file to a specified directory.

    Args:
    zip_filepath (str)  --  Path to the zip file to extract.
    extract_to (str)    --  Directory to extract the zip file to.
    """

    with zipfile.ZipFile(zip_filepath, "r") as zip_ref:
        zip_ref.extractall(extract_to)

#------------------------------------------------------------------------------#
def main():
    parser = argparse.ArgumentParser(description='Script to download CrocoLake and Argo databases.')
    parser.add_argument('-d', type=str, help="Name of database to download ('CrocoLake' or 'Argo')", required=True)
    parser.add_argument('-t', type=str, help="Type of variables ('PHY' or 'BGC')", required=True)
    parser.add_argument('--qc', help="Only best quality measurements are downloaded (default)", action=argparse.BooleanOptionalAction)
    parser.add_argument('--noqc', help="No QC is performed", action=argparse.BooleanOptionalAction)
    parser.add_argument('--destination', type=str, help="Destination folder to download data to (default: ./CrocoLake)", required=False, default="./CrocoLake")

    args = parser.parse_args()

    if not args.qc and not args.noqc:
        args.qc = True
    elif args.qc and args.noqc:
        raise ValueError("Cannot specify both --qc and --noqc")

    destination = args.destination

    db_type = args.t.upper()
    db_name = args.d.upper()

    db_codename = get_db_codename(db_name, db_type, args.qc)
    if not destination[-1] == "/":
        destination += "/"
    extract_to = destination + db_codename + "/"
    os.makedirs(extract_to, exist_ok=True)

    local_zipfile = extract_to + db_codename + ".zip"

    # Download the dataset
    print(f"Downloading dataset {db_name}, type {db_type} (identifier: {db_codename})...")
    download_file(urls[db_codename], local_zipfile)

    # Unzip the dataset
    print("Unzipping dataset...")
    unzip_file(local_zipfile, extract_to)

    # Clean up
    print("Cleaning up...")
    os.remove(local_zipfile)

    print("Database setup complete.")

##########################################################################
if __name__ == "__main__":
    main()
