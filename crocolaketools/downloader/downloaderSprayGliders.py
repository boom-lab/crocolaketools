#!/usr/bin/env python3

## @file downloaderSprayGliders.py
#
# Downloader for Spray Gliders Level-3 NetCDF files.
#
## @author mahi-anol
#
## @date Sat 21 Mar 2026

##########################################################################
import os
import requests

from crocolaketools.downloader.downloader import Downloader
##########################################################################

# Base URL for Spray Gliders Level-3 files hosted on spraydata.ucsd.edu
SPRAY_BASE_URL = "https://spraydata.ucsd.edu/erddap/files"

# Mapping from local filename to ERDDAP folder/file path.
# Each entry is (local_fname, remote_path) where remote_path is relative
# to SPRAY_BASE_URL. The local filename matches the issue #40 specification.
SPRAY_FILES = {
    "Calypso.nc":       "binnedCalypso/Calypso.nc",
    "CORC.nc":          "binnedCORC/CORC.nc",
    "CUGN_along.nc":    "binnedCUGNalong/CUGN_along.nc",
    "CUGN_line_56.nc":  "binnedCUGN56/CUGN_line_56.nc",
    "CUGN_line_66.nc":  "binnedCUGN66/CUGN_line_66.nc",
    "CUGN_line_80.nc":  "binnedCUGN80/CUGN_line_80.nc",
    "CUGN_line_90.nc":  "binnedCUGN90/CUGN_line_90.nc",
    "CUGN_line_93.nc":  "binnedCUGN93/CUGN_line_93.nc",
    "FLEAT.nc":         "binnedFLEAT/FLEAT.nc",
    "GoM.nc":           "binnedGoM/GoM.nc",
    "GulfStream.nc":    "binnedGS/GulfStream.nc",
    "Hawaii.nc":        "binnedHawaii/Hawaii.nc",
    "NASCar.nc":        "binnedNASCar/NASCar.nc",
    "NLIWI_IWISE.nc":   "binnedNLIWI_IWISE/NLIWI_IWISE.nc",
    "OKMC.nc":          "binnedOKMC/OKMC.nc",
    "PEACH.nc":         "binnedPEACH/PEACH.nc",
    "ROGER.nc":         "binnedRoger/ROGER.nc",
    "Solomon.nc":       "binnedSolomon/Solomon.nc",
}
##########################################################################


class DownloaderSprayGliders(Downloader):
    """class DownloaderSprayGliders: download Spray Gliders Level-3 NetCDF
    files from spraydata.ucsd.edu and store them locally in their original,
    unmodified form.

    No schema mapping or data manipulation is performed here. The raw
    NetCDF files are saved exactly as served by the remote host, ready
    for subsequent conversion by ConverterSprayGliders.

    The destination directory is resolved from the config dict /
    config.yaml, mirroring the pattern used by ConverterSprayGliders and
    DownloaderGLODAP.

    Files are skipped if they already exist locally and overwrite=False.
    get_url() is used to validate each file URL before downloading.

    Typical usage
    -------------
    >>> config = {'db': 'SprayGliders', 'db_type': 'PHY'}
    >>> d = DownloaderSprayGliders(config=config)
    >>> d.spray_download()
    """

    # ------------------------------------------------------------------ #
    # Constructors/Destructors                                             #
    # ------------------------------------------------------------------ #

    def __init__(
        self,
        config: dict = None,
        fnames: list = None,
        base_url: str = SPRAY_BASE_URL,
        overwrite: bool = False,
    ):
        """Constructor.

        Arguments
        ---------
        config   : configuration dictionary. Must contain at least
                   'db' (='SprayGliders') and 'db_type' ('PHY' or 'BGC').
                   Any key not supplied is read from config.yaml.
                   The resolved 'input_path' is used as the download
                   destination (set by the base Downloader).
        fnames   : dict mapping local filename to remote ERDDAP path.
                   Defaults to SPRAY_FILES.
        base_url : base URL for the Spray Gliders Level-3 files.
        overwrite: if False (default) and a file already exists on disk,
                   the download is skipped.
        """
        if config is None:
            config = {
                'db': 'SprayGliders',
                'db_type': 'PHY',
            }

        # base class resolves input_path from config + config.yaml and
        # creates the directory if needed
        super().__init__(config)

        self.fnames = fnames if fnames is not None else SPRAY_FILES
        self.base_url = base_url
        self.overwrite = overwrite

    # ------------------------------------------------------------------ #
    # Public interface                                                     #
    # ------------------------------------------------------------------ #

    def spray_download(self) -> list:
        """Download all Spray Gliders Level-3 files to self.input_path.

        For each filename in self.fnames, checks if the file already
        exists locally (skip if overwrite=False), validates the remote
        URL via get_url(), then downloads using the inherited
        _download_file() method.

        Files whose URLs are unreachable are skipped with a warning
        rather than raising an error, so a single broken URL does not
        abort the entire download.

        Returns
        -------
        list
            Absolute paths to all successfully downloaded (or pre-existing) files.
        """
        downloaded = []
        skipped = []

        for fname, remote_path in self.fnames.items():
            local_path = os.path.join(self.input_path, fname)

            if self._is_already_downloaded(local_path):
                print(
                    f"{fname} already present at {local_path}. "
                    "Use overwrite=True or use '--overwrite' flag to force re-download."
                )
                downloaded.append(local_path)
                continue

            url = f"{self.base_url}/{remote_path}"
            try:
                self._check_url_reachable(url)
            except RuntimeError as e:
                import warnings
                warnings.warn(
                    f"Skipping {fname}: {e}. "
                    "The file may have been removed/renamed on the server or the server may be temporarily down."
                )
                skipped.append(fname)
                continue

            print(f"Downloading {fname} from {url} ...")
            self._download_file(url, local_path)
            print(f"Saved to {local_path}")
            downloaded.append(local_path)

        if skipped:
            print(
                f"\nWarning: {len(skipped)} file(s) were skipped due to unreachable URLs: "
                f"{', '.join(skipped)}"
            )

        return downloaded

    def get_url(self, fname: str) -> str:
        """Return the URL for fname if it is reachable.

        Parameters
        ----------
        fname : filename to construct and validate the URL for.

        Returns
        -------
        str
            The full URL for the file.

        Raises
        ------
        RuntimeError
            If the URL is not reachable.
        """
        url = f"{self.base_url}/{self.fnames[fname]}"
        self._check_url_reachable(url)
        return url

    def _check_url_reachable(self, url: str) -> None:
        """Verify that a URL is reachable.

        Uses a streaming GET (not HEAD, because the ERDDAP server does
        not always respond to HEAD requests) and closes the connection
        immediately after checking the status code.

        Parameters
        ----------
        url : full URL to check.

        Raises
        ------
        RuntimeError
            If the URL is not reachable.
        """
        try:
            response = requests.get(
                url,
                timeout=5,
                stream=True,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            if response.ok:
                response.close()
                return
        except requests.RequestException:
            pass
        raise RuntimeError(f"URL not reachable: {url}")

##########################################################################

if __name__ == "__main__":
    DownloaderSprayGliders()