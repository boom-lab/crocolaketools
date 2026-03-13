#!/usr/bin/env python3

## @file downloaderGLODAP.py
#
# Downloader for the GLODAPv2 Merged Master File (CSV).
#
## @author mahi-anol
#
## @date Fri 12 Mar 2026

##########################################################################
import os
import requests
from tqdm import tqdm

from crocolaketools.downloader.downloader import Downloader
##########################################################################

# GLODAPv2.2023 master file constants
GLODAP_VERSION = "2.2023"
GLODAP_MASTER_FNAME = f"GLODAPv{GLODAP_VERSION}_Merged_Master_File.csv"

# Primary source: NOAA NCEI / OCADS
GLODAP_URL_NCEI = (
    "https://www.ncei.noaa.gov/data/oceans/ncei/ocads/data/0283442/"
    + GLODAP_MASTER_FNAME
)
# GEOMAR mirror (fallback)
GLODAP_URL_GEOMAR = (
    f"https://glodap.info/glodap_files/v{GLODAP_VERSION}/" + GLODAP_MASTER_FNAME
)
##########################################################################


class DownloaderGLODAP(Downloader):
    """class DownloaderGLODAP: download the GLODAPv2 Merged Master File
    (CSV) from NOAA NCEI (primary) or GEOMAR (fallback) and store it
    locally in its original, unmodified form.

    No schema mapping or data manipulation is performed here; the raw
    CSV is saved exactly as served by the remote host, ready for
    subsequent conversion by ConverterGLODAP.

    The destination directory is resolved from the config dict /
    config.yaml, mirroring the pattern used by ConverterGLODAP and
    DownloaderURLList.

    Typical usage
    -------------
    >>> config = {'db': 'GLODAP', 'db_type': 'PHY'}
    >>> d = DownloaderGLODAP(config=config)
    >>> d.glodap_download()
    """

    # ------------------------------------------------------------------ #
    # Constructors/Destructors                                             #
    # ------------------------------------------------------------------ #

    def __init__(
        self,
        config: dict = None,
        fname: str = GLODAP_MASTER_FNAME,
        url: str = GLODAP_URL_NCEI,
        fallback_url: str = GLODAP_URL_GEOMAR,
        overwrite: bool = False,
    ):
        """Constructor.

        Arguments
        ---------
        config       : configuration dictionary. Must contain at least
                       'db' (='GLODAP') and 'db_type' ('PHY' or 'BGC').
                       Any key not supplied is read from config.yaml.
                       The resolved 'input_path' is used as the download
                       destination (set by the base Downloader).
        fname        : filename to save on disk.
        url          : primary download URL (default: NOAA NCEI).
        fallback_url : mirror URL tried if the primary request fails
                       (default: GEOMAR).
        overwrite    : if False (default) and the file already exists on
                       disk, the download is skipped.
        """
        if config is None:
            config = {
                'db': 'GLODAP',
                'db_type': 'PHY',
            }

        # base class resolves input_path from config + config.yaml and
        # creates the directory if needed
        super().__init__(config)

        self.fname = fname
        self.url = url
        self.fallback_url = fallback_url
        self.overwrite = overwrite

    # ------------------------------------------------------------------ #
    # Public interface                                                     #
    # ------------------------------------------------------------------ #

    def glodap_download(self) -> str:
        """Download the GLODAP master CSV to self.input_path.

        Returns
        -------
        str
            Absolute path to the downloaded (or pre-existing) file.

        Raises
        ------
        RuntimeError
            If both the primary URL and the fallback URL fail.
        """
        local_path = os.path.join(self.input_path, self.fname)

        if self._is_already_downloaded(local_path):
            print(
                f"File already present at {local_path}. "
                "Use overwrite=True or use '--overwrite' flag to force re-download."
            )
            return local_path

        # Try primary URL, then fallback
        for attempt_url in (self.url, self.fallback_url):
            print(f"Attempting download from {attempt_url} ...")
            try:
                self._download_file(attempt_url, local_path)
                print(f"Saved to {local_path}")
                return local_path
            except requests.exceptions.RequestException as exc:
                print(f"Request failed ({attempt_url}): {exc}")

        raise RuntimeError(
            f"Download failed from both primary ({self.url}) "
            f"and fallback ({self.fallback_url}) URLs."
        )

    # ------------------------------------------------------------------ #
    # Private helpers                                                      #
    # ------------------------------------------------------------------ #

    def _is_already_downloaded(self, local_path: str) -> bool:
        """Return True if the file exists on disk and overwrite is False."""
        return (not self.overwrite) and os.path.isfile(local_path)

    @staticmethod
    def _download_file(url: str, local_path: str) -> None:
        """Stream *url* to *local_path* with a tqdm progress bar.

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


##########################################################################

if __name__ == "__main__":
    DownloaderGLODAP()
