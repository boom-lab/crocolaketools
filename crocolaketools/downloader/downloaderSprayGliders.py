#!/usr/bin/env python3

## @file downloaderSprayGliders.py
#
# Downloader for Spray Gliders Level-3 NetCDF files.
#
## @author mahi-anol
#
## @date Sat 21 Mar 2026

##########################################################################
import csv
import io
import os
import warnings
from pathlib import Path

import requests

from crocolaketools.downloader.downloader import Downloader
##########################################################################

# Base URL for Spray Gliders Level-3 files hosted on spraydata.ucsd.edu.
# The file listing of dataset <id> is at f"{SPRAY_BASE_URL}/<id>/.csv".
SPRAY_BASE_URL = "https://spraydata.ucsd.edu/erddap/files"

# ERDDAP query listing every binned (Level-3) dataset on the server. Used
# only to warn about datasets missing from SPRAY_FILES.
SPRAY_DATASETS_URL = (
    "https://spraydata.ucsd.edu/erddap/tabledap/allDatasets.csv"
    "?datasetID,title&datasetID=~%22binned.*%22"
)

# Mapping from ERDDAP dataset ID to local filename. The remote filename is
# read from the dataset's file listing at download time, because the server
# renames files (e.g. binnedFLEAT serves fleat_v1.1.nc). The local filenames
# are fixed: ConverterSprayGliders depends on them.
SPRAY_FILES = {
    "binnedCalypso":     "Calypso.nc",
    "binnedCORC":        "CORC.nc",
    "binnedCUGNalong":   "CUGN_along.nc",
    "binnedCUGN56":      "CUGN_line_56.nc",
    "binnedCUGN66":      "CUGN_line_66.nc",
    "binnedCUGN80":      "CUGN_line_80.nc",
    "binnedCUGN90":      "CUGN_line_90.nc",
    "binnedCUGN93":      "CUGN_line_93.nc",
    "binnedFLEAT":       "FLEAT.nc",
    "binnedGoM":         "GoM.nc",
    "binnedGS":          "GulfStream.nc",
    "binnedHawaii":      "Hawaii.nc",
    "binnedMabSwot":     "mab_swot.nc",
    "binnedNASCar":      "NASCar.nc",
    "binnedNLIWI_IWISE": "NLIWI_IWISE.nc",
    "binnedOKMC":        "OKMC.nc",
    "binnedPEACH":       "PEACH.nc",
    "binnedRoger":       "ROGER.nc",
    "binnedSolomon":     "Solomon.nc",
}

_HEADERS = {"User-Agent": "Mozilla/5.0"}
##########################################################################


class DownloaderSprayGliders(Downloader):
    """class DownloaderSprayGliders: download Spray Gliders Level-3 NetCDF
    files from spraydata.ucsd.edu and store them locally in their original,
    unmodified form.

    No schema mapping or data manipulation is performed here. The raw
    NetCDF files are saved exactly as served by the remote host, ready
    for subsequent conversion by ConverterSprayGliders.

    For each ERDDAP dataset ID in self.fnames, the dataset's file listing
    is read to find its single .nc file. The file is saved under the fixed
    local name from self.fnames, whatever the server calls it.

    A file is downloaded only if the local copy is missing, its size differs
    from the server's, or its mtime is older than the server's Last
    modified time. After a download, the local mtime is set to the server's
    Last modified time. overwrite=True forces a re-download.

    Datasets whose listing or download fails are collected in self.failed
    (dataset ID -> reason); the other datasets are still processed.

    Typical usage
    -------------
    >>> config = {'db': 'SprayGliders', 'db_type': 'PHY'}
    >>> d = DownloaderSprayGliders(config=config)
    >>> d.spray_download()
    >>> d.failed
    {}
    """

    # ------------------------------------------------------------------ #
    # Constructors/Destructors                                             #
    # ------------------------------------------------------------------ #

    def __init__(
        self,
        config: dict = None,
        fnames: dict = None,
        base_url: str = SPRAY_BASE_URL,
        overwrite: bool = False,
        datasets_url: str = SPRAY_DATASETS_URL,
    ):
        """Constructor.

        Arguments
        ---------
        config      : configuration dictionary. Must contain at least
                      'db' (='SprayGliders') and 'db_type' ('PHY' or 'BGC').
                      Any key not supplied is read from datasets.yaml.
                      The resolved 'input_path' is used as the download
                      destination (set by the base Downloader).
        fnames      : dict mapping ERDDAP dataset ID to local filename.
                      Defaults to SPRAY_FILES.
        base_url    : base URL of the ERDDAP files/ service.
        overwrite   : if True, re-download files even if they are unchanged.
        datasets_url: URL of the allDatasets query listing binned datasets.
        """
        if config is None:
            config = {
                'db': 'SprayGliders',
                'db_type': 'PHY',
            }

        # base class resolves input_path from config + datasets.yaml and
        # creates the directory if needed
        super().__init__(config)

        self.fnames = fnames if fnames is not None else SPRAY_FILES
        self.base_url = base_url
        self.overwrite = overwrite
        self.datasets_url = datasets_url
        self.failed = {}

    # ------------------------------------------------------------------ #
    # Public interface                                                     #
    # ------------------------------------------------------------------ #

    def spray_download(self) -> list:
        """Download the Spray Gliders Level-3 files in self.fnames to
        self.input_path.

        A listing or download error for one dataset is recorded in
        self.failed and does not stop the others. A summary of failures is
        printed at the end.

        Returns
        -------
        list
            Paths of the local files that are up to date after this call
            (downloaded now or already unchanged).
        """
        self.failed = {}
        self.check_new_datasets()

        up_to_date = []
        for dataset_id, fname in self.fnames.items():
            local_path = Path(self.input_path) / fname
            try:
                entry = self._remote_nc_entry(dataset_id)
            except Exception as e:
                self._fail(dataset_id, f"listing failed: {e}")
                continue

            if not self._needs_download(local_path, entry):
                print(
                    f"{fname} is unchanged on the server ({dataset_id}/"
                    f"{entry['name']}), skipping. Use overwrite=True or the "
                    "'--overwrite' flag to force re-download."
                )
                up_to_date.append(local_path)
                continue

            url = f"{self.base_url}/{dataset_id}/{entry['name']}"
            print(f"Downloading {fname} from {url} ...")
            try:
                self._download_file(url, local_path)
            except Exception as e:
                self._fail(dataset_id, f"download of {url} failed: {e}")
                continue

            local_size = local_path.stat().st_size
            if local_size != entry['size']:
                # leave the file and its mtime as they are: the size
                # mismatch makes the next run download it again
                self._fail(
                    dataset_id,
                    f"downloaded {local_size} bytes, listing says {entry['size']}",
                )
                continue

            mtime_ns = entry['mtime_ms'] * 1_000_000
            os.utime(local_path, ns=(mtime_ns, mtime_ns))
            print(f"Saved to {local_path}")
            up_to_date.append(local_path)

        if self.failed:
            print(f"\n{len(self.failed)} Spray dataset(s) failed:")
            for dataset_id, reason in self.failed.items():
                print(f"  {dataset_id}: {reason}")

        return up_to_date

    def get_url(self, dataset_id: str) -> str:
        """Return the download URL of the single .nc file of dataset_id.

        Raises
        ------
        RuntimeError
            If the listing cannot be read or does not hold exactly one
            .nc file.
        """
        entry = self._remote_nc_entry(dataset_id)
        return f"{self.base_url}/{dataset_id}/{entry['name']}"

    def list_remote_files(self, dataset_id: str) -> list:
        """Return the file listing of dataset_id from ERDDAP files/.

        Returns
        -------
        list
            One dict per entry, with keys 'name' (str), 'mtime_ms' (int,
            epoch milliseconds) and 'size' (int, bytes). Subdirectories
            (names ending in '/') are included.
        """
        url = f"{self.base_url}/{dataset_id}/.csv"
        response = requests.get(url, timeout=30, headers=_HEADERS)
        response.raise_for_status()
        reader = csv.DictReader(io.StringIO(response.text))
        expected = {"Name", "Last modified", "Size"}
        if reader.fieldnames is None or not expected <= set(reader.fieldnames):
            raise RuntimeError(
                f"unexpected listing header {reader.fieldnames} at {url}"
            )
        entries = []
        for row in reader:
            name = row["Name"]
            if not name:
                continue
            if name.endswith("/"):
                entries.append({"name": name, "mtime_ms": None, "size": None})
                continue
            entries.append({
                "name": name,
                "mtime_ms": int(row["Last modified"]),
                "size": int(row["Size"]),
            })
        return entries

    def list_binned_datasets(self) -> list:
        """Return the IDs of all binned datasets on the server."""
        response = requests.get(self.datasets_url, timeout=30, headers=_HEADERS)
        response.raise_for_status()
        rows = list(csv.reader(io.StringIO(response.text)))
        # rows[0] is the header; ERDDAP returns an empty first data row
        return [r[0] for r in rows[1:] if r and r[0]]

    def check_new_datasets(self) -> list:
        """Warn about binned datasets on the server that are missing from
        SPRAY_FILES. They are not downloaded.

        Returns
        -------
        list
            The dataset IDs missing from SPRAY_FILES. Empty if the
            server's dataset list could not be read.
        """
        try:
            remote_ids = self.list_binned_datasets()
        except Exception as e:
            warnings.warn(f"Could not read the Spray dataset list: {e}")
            return []
        new_ids = [d for d in remote_ids if d not in SPRAY_FILES]
        if new_ids:
            warnings.warn(
                "Spray datasets on the server but not in SPRAY_FILES (not "
                f"downloaded): {', '.join(new_ids)}"
            )
        return new_ids

    # ------------------------------------------------------------------ #
    # Helpers                                                              #
    # ------------------------------------------------------------------ #

    def _remote_nc_entry(self, dataset_id: str) -> dict:
        """Return the listing entry of the single .nc file of dataset_id.

        Raises
        ------
        RuntimeError
            If the listing has zero or several .nc files.
        """
        entries = [
            e for e in self.list_remote_files(dataset_id)
            if not e["name"].endswith("/") and e["name"].endswith(".nc")
        ]
        if len(entries) != 1:
            names = [e["name"] for e in entries]
            raise RuntimeError(
                f"expected one .nc file in {dataset_id}, found {len(entries)}: {names}"
            )
        return entries[0]

    def _needs_download(self, local_path: Path, entry: dict) -> bool:
        """Return True if local_path must be (re-)downloaded."""
        if self.overwrite or not local_path.is_file():
            return True
        st = local_path.stat()
        if st.st_size != entry["size"]:
            return True
        local_mtime_ms = st.st_mtime_ns // 1_000_000
        return local_mtime_ms < entry["mtime_ms"]

    def _fail(self, dataset_id: str, reason: str) -> None:
        print(f"Error: {dataset_id}: {reason}")
        self.failed[dataset_id] = reason

##########################################################################

if __name__ == "__main__":
    DownloaderSprayGliders()
