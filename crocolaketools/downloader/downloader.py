#!/usr/bin/env python3

## @file downloader.py
#
# Base class for CrocoLakeTools downloaders.
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#         Updated by David Nady <davidnady4yad@gmail.com>
#         Updated by Mahi Sarwar Anol <anol.mahi@gmail.com>
#
## @date Tue 11 Feb 2025

##########################################################################
import importlib.resources
import logging
import os
import shutil
import warnings
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
 
import requests
import yaml
from tqdm import tqdm

from crocolaketools import db_names
from crocolaketools.config import config_paths as cfgp
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
        overwrite     -- if True, re-download files already present (default: False)
        dryrun        -- if True, print summary without downloading (default: False)
        num_threads   -- number of parallel download threads (default: 4)
        """
 
        if config is None:
            raise ValueError("No config argument provided to Downloader.")
 
        db = config['db']
        db_type = config['db_type'].upper()
 
        base_path = cfgp.get_config_path()
        config_disk = cfgp.get_config_paths_db_dict(db + "_" + db_type)
 
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
            if db in db_names.databases:
                self.db = db
                print("Setting up downloader for " + self.db + " database.")
            else:
                raise ValueError("Database db must be one of " + str(db_names.databases))
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
 
        # Common download options -- subclasses may override these after
        # calling super().__init__() if they accept them as constructor args.
        self.overwrite = config.get("overwrite", False)
        self.dryrun = config.get("dryrun", False)
        self.num_threads = config.get("num_threads", 4)
 
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
 
        Downloads to a temporary file first and renames it to local_path
        only after the download completes successfully. This prevents
        corrupt partial files from being left on disk if the download
        fails or is interrupted mid-way. If the download fails, the
        temporary file is deleted automatically.
 
        Parameters
        ----------
        url        : remote URL to fetch.
        local_path : destination file path (parent directory must exist).
 
        Raises
        ------
        requests.exceptions.RequestException
            Propagated from requests on any HTTP or connection error.
        """
        tmp_path = local_path + ".tmp"
        try:
            with requests.get(url, stream=True, timeout=120) as response:
                response.raise_for_status()
                total_size = int(response.headers.get("content-length", 0))
                with open(tmp_path, "wb") as fh, tqdm(
                    desc=os.path.basename(local_path),
                    total=total_size,
                    unit="iB",
                    unit_scale=True,
                    unit_divisor=1024,
                ) as bar:
                    for chunk in response.iter_content(chunk_size=8192):
                        size = fh.write(chunk)
                        bar.update(size)
            os.replace(tmp_path, local_path)
        except Exception:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise
 
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
 
    def download_parallel(
        self,
        url_path_pairs: list,
        num_threads: int = 4,
        dryrun: bool = False,
    ) -> tuple:
        """Download multiple (url, local_path) pairs concurrently.
 
        Uses ThreadPoolExecutor to call _download_file() for each pair.
        Logs progress and returns counts of completed and failed downloads.
 
        In dryrun mode, sends a HEAD request to each URL to retrieve the
        expected file size and prints a summary of what would be downloaded
        and how much disk space it would require.
 
        Parameters
        ----------
        url_path_pairs : list of (url, local_path) tuples to download.
        num_threads    : number of concurrent download threads.
        dryrun         : if True, print a download summary without
                         fetching any files.
 
        Returns
        -------
        tuple
            (completed, failed) counts.
        """
        actual_threads = min(num_threads, len(url_path_pairs))
        logging.info(
            "Starting download of %d files with %d threads",
            len(url_path_pairs),
            actual_threads,
        )
 
        if dryrun:
            total_bytes = 0
            rows = []
            for url, local_path in url_path_pairs:
                already = os.path.isfile(local_path)
                size_bytes = 0
                size_str = "unknown"
                try:
                    head = requests.head(url, timeout=10, allow_redirects=True)
                    if "content-length" in head.headers:
                        size_bytes = int(head.headers["content-length"])
                        size_str = self._format_size(size_bytes)
                except requests.RequestException:
                    pass
 
                status = "already exists, would skip" if already else "would download"
                if not already:
                    total_bytes += size_bytes
                rows.append((os.path.basename(local_path), size_str, status))
 
            print("\nDry run summary:")
            for fname, size, status in rows:
                print(f"  {fname:<40} {size:>10}   ({status})")
            print(f"\nTotal to download: {self._format_size(total_bytes)} "
                  f"across {sum(1 for _, _, s in rows if 'would download' in s)} file(s).")
            print()
            return len(url_path_pairs), 0
 
        completed = 0
        failed = 0
 
        with ThreadPoolExecutor(max_workers=actual_threads) as executor:
            future_to_url = {
                executor.submit(self._download_file, url, local_path): url
                for url, local_path in url_path_pairs
            }
 
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    future.result()
                    completed += 1
                    logging.info("Downloaded %s", url)
                except Exception as exc:
                    failed += 1
                    logging.error("Failed to download %s: %s", url, exc)
 
        logging.info(
            "Download completed. Success: %d, Failed: %d", completed, failed
        )
        return completed, failed
 
    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """Return a human-readable string for a size in bytes.
 
        Parameters
        ----------
        size_bytes : size in bytes.
 
        Returns
        -------
        str
            Human-readable size string (e.g. '1.29 MB', '623 KB').
        """
        if size_bytes == 0:
            return "unknown"
        for unit in ["B", "KB", "MB", "GB"]:
            if size_bytes < 1024:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.2f} TB"
 
##########################################################################
if __name__ == "__main__":
    Downloader()
