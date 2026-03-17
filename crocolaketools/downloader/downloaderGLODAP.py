#!/usr/bin/env python3

## @file downloaderGLODAP.py
#
# Downloader for the GLODAPv2 Merged Master File (CSV).
#
## @author mahi-anol
#
## @date Fri 13 Mar 2026

##########################################################################
import os
import requests

from crocolaketools.downloader.downloader import Downloader
##########################################################################

# GLODAPv2.2023 master file constants
GLODAP_VERSION      = "2.2023"
GLODAP_MASTER_FNAME = f"GLODAPv{GLODAP_VERSION}_Merged_Master_File.csv"

# Primary source: NOAA NCEI / OCADS (plain CSV)
GLODAP_URL_NCEI = (
    "https://www.ncei.noaa.gov/data/oceans/ncei/ocads/data/0283442/"
    + GLODAP_MASTER_FNAME
)

# GEOMAR mirror (fallback) -- serves the CSV as a zip archive
GLODAP_URL_GEOMAR = (
    f"https://glodap.info/glodap_files/v{GLODAP_VERSION}/"
    + GLODAP_MASTER_FNAME
    + ".zip"
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

    The GEOMAR mirror serves the CSV wrapped in a zip archive. The
    downloader handles extraction automatically via the inherited
    unzip_file() method so that the resulting local file is always a
    plain CSV regardless of which source was used.

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
        overwrite: bool = False,
    ):
        """Constructor.

        Arguments
        ---------
        config    : configuration dictionary. Must contain at least
                    'db' (='GLODAP') and 'db_type' ('PHY' or 'BGC').
                    Any key not supplied is read from config.yaml.
                    The resolved 'input_path' is used as the download
                    destination (set by the base Downloader).
        fname     : filename to save on disk.
        overwrite : if False (default) and the file already exists on
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
        self.overwrite = overwrite

    # ------------------------------------------------------------------ #
    # Public interface                                                     #
    # ------------------------------------------------------------------ #

    def glodap_download(self) -> str:
        """Download the GLODAP master CSV to self.input_path.

        Selects a reachable URL via get_url(), downloads the file, and
        extracts it if the source is the GEOMAR zip mirror. The result
        is always a plain CSV at self.input_path/self.fname.

        Returns
        -------
        str
            Absolute path to the downloaded (or pre-existing) file.

        Raises
        ------
        RuntimeError
            If no reachable URL is found.
        """
        local_path = os.path.join(self.input_path, self.fname)

        if self._is_already_downloaded(local_path):
            print(
                f"File already present at {local_path}. "
                "Use overwrite=True to force re-download."
            )
            return local_path

        url = self.get_url()
        print(f"Downloading from {url} ...")

        if url == GLODAP_URL_GEOMAR:
            # GEOMAR serves a zip; download then extract via inherited method
            zip_path = local_path + ".zip"
            self._download_file(url, zip_path)
            self.unzip_file(zip_path)  # extracts and deletes the zip
        else:
            self._download_file(url, local_path)

        print(f"Saved to {local_path}")
        return local_path

    def get_url(self) -> str:
        """Return the first reachable download URL.

        Checks NCEI first, then GEOMAR. Raises RuntimeError if neither
        is reachable.

        Returns
        -------
        str
            The first URL that responds with a 2xx status.

        Raises
        ------
        RuntimeError
            If all candidate URLs are unreachable.
        """
        urls = [
            GLODAP_URL_NCEI,
            GLODAP_URL_GEOMAR,
        ]
        for url in urls:
            try:
                response = requests.head(url, timeout=5)
                if response.ok:
                    return url
            except requests.RequestException:
                pass
        raise RuntimeError(f"None of the URLs are reachable: {urls}")

##########################################################################

if __name__ == "__main__":
    DownloaderGLODAP()