#!/usr/bin/env python3

## @file downloader.py
#
# Base class for CrocoLakeTools downloaders.
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Tue 11 Feb 2025

##########################################################################
import importlib.resources
import os
import shutil
import warnings
import zipfile

import requests
import yaml
from tqdm import tqdm

from crocolakeloader import params
##########################################################################


class Downloader:

    """class Downloader: common facilities to configure downloads for different
    databases, mirroring the Converter's path handling.
    """

    # ------------------------------------------------------------------ #
    # Constructors/Destructors                                           #
    # ------------------------------------------------------------------ #

    def __init__(self, config=None):
        """Constructor

        Arguments:

        config -- configuration dictionary. Must contain at least 'db' and
                  'db_type'. If any value is not specified, defaults in
                  config.yaml are used; user-provided values override them.

        Relevant fields used by Downloader implementations:
        db            -- database name (e.g., 'OleanderXBT')
        db_type       -- 'PHY' or 'BGC'
        input_path    -- destination path where original files are stored
        """

        if config is None:
            raise ValueError("No config argument provided to Downloader.")

        db = config['db']
        db_type = config['db_type'].upper()

        config_path = importlib.resources.files("crocolaketools.config").joinpath("config.yaml")
        base_path = importlib.resources.files("crocolaketools.config")
        config_disk = yaml.safe_load(open(config_path))
        config_disk = config_disk[db + "_" + db_type]

        config_user_keys = list(config.keys())
        config_disk_keys = list(config_disk.keys())

        read_keys = [k for k in config_disk_keys if k not in config_user_keys]
        if len(read_keys)>0:
            for k in ["db","db_type"]:
                if not config[k] == config_disk[k]:
                    warnings.warn(f"User-specified and config file are not matching at key {k} (got {config[k]} and {config_disk[k]}), the user-specified value {config[k]} is used")
        for k in read_keys:
            config[k] = config_disk[k]

        print("Downloader configuration:")
        print(config)

        # Basic validation and assignments
        if isinstance(db,str):
            if db in params.databases:
                self.db = db
                print("Setting up downloader for " + self.db + " database.")
            else:
                raise ValueError("Database db must be one of " + str(params.databases))
        elif db is not None:
            raise ValueError("Database db not a string.")
        else:
            raise ValueError("No database provided.")

        if isinstance(db_type,str):
            if db_type in ["PHY","BGC"]:
                self.db_type = db_type
                print("Using " + self.db_type + " parameters.")
            else:
                raise ValueError("Database db_type must be one of " + str(["PHY","BGC"]))
        elif db is not None:
            raise ValueError("Database type db_type not a string.")

        input_path = os.path.abspath(os.path.join(base_path, config["input_path"]))
        if input_path[-1] != "/":
            input_path = input_path + "/"
        # Ensure destination exists for downloads
        os.makedirs(input_path, exist_ok=True)
        self.input_path = input_path
        print("Original files will be stored at " + self.input_path)

    # ------------------------------------------------------------------ #
    # Methods                                                            #
    # ------------------------------------------------------------------ #

    def _is_already_downloaded(self, local_path: str) -> bool:
        """Return True if the file exists on disk and overwrite is False.

        Parameters
        ----------
        local_path : absolute path to the expected local file.

        Returns
        -------
        bool
            True if the file should be skipped (exists and overwrite=False).
        """
        return (not self.overwrite) and os.path.isfile(local_path)

    @staticmethod
    def _download_file(url: str, local_path: str) -> None:
        """Stream url to local_path with a tqdm progress bar.

        Parameters
        ----------
        url        : remote URL to fetch.
        local_path : destination file path (parent directory must exist).

        Raises
        ------
        requests.exceptions.RequestException
            Propagated from requests on any HTTP or connection error.
        """
        with requests.get(url, stream=True, timeout=120) as response:
            response.raise_for_status()
            total_size = int(response.headers.get("content-length", 0))
            with open(local_path, "wb") as fh, tqdm(
                desc=os.path.basename(local_path),
                total=total_size,
                unit="iB",
                unit_scale=True,
                unit_divisor=1024,
            ) as bar:
                for chunk in response.iter_content(chunk_size=8192):
                    size = fh.write(chunk)
                    bar.update(size)

    @staticmethod
    def unzip_file(zip_path: str) -> None:
        """Extract a zip archive to its parent directory and delete the zip.

        Cleans up any __MACOSX metadata folder that macOS-created archives
        may include.

        Parameters
        ----------
        zip_path : path to the zip file to extract.
        """
        extract_dir = os.path.dirname(zip_path)
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_dir)

        # Remove __MACOSX metadata folder if present
        macosx_path = os.path.join(extract_dir, "__MACOSX")
        if os.path.exists(macosx_path) and os.path.isdir(macosx_path):
            shutil.rmtree(macosx_path)

        os.remove(zip_path)

##########################################################################
if __name__ == "__main__":
    Downloader()