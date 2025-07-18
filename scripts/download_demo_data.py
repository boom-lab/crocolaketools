#!/usr/bin/env python3

## @file download_demo_data.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Thu 29 May 2025

##########################################################################
import argparse
import importlib.resources
import os
from pathlib import Path
import requests
from tqdm import tqdm
import yaml
import zipfile
##########################################################################
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
    parser = argparse.ArgumentParser(description='Script to download demo datasets for CrocoLakeTools.')
    parser.add_argument('-d', type=str, help="Destination folder to download data to (default: ./crocolaketools/demo/)", required=False, default=None)

    args = parser.parse_args()

    if args.d is None:
        config_path = importlib.resources.files("crocolaketools.config").joinpath("config.yaml")
        config = yaml.safe_load(open(config_path))
        destination = config["demo"]["download_path"]
    else:
        destination = args.d

    destination = Path(destination)
    base_path = Path(importlib.resources.files("crocolaketools.config"))
    destination = (base_path / destination).resolve()
    destination.mkdir(parents = True, exist_ok = True)

    # Download the dataset
    print(f"Downloading test datasets...")
    local_zip_filename = os.path.join(destination,"crocolaketools_demo_data.zip")
    download_file(
        "https://zenodo.org/records/15556267/files/crocolaketools_demo_data.zip?download=1",
        local_zip_filename
    )

    # Unzip the dataset
    print("Unzipping dataset...")
    unzip_file(
        local_zip_filename,
        destination
    )

    # Clean up
    print("Cleaning up...")
    os.remove(local_zip_filename)

    print("Database setup complete.")

##########################################################################
if __name__ == "__main__":
    main()
