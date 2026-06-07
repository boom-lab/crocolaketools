#!/usr/bin/env python3

## @file downloaderIOOS.py
#
# Parent class for IOOS source, inherited from downloaderERDDAP super class.
#
## @author Mahi Sarwar Anol <anol.mahi@gmail.com>
#
## @date Sunday 26 June, 2026

################################################################################################
from crocolaketools.downloader.downloaderERDDAP import DownloaderERDDAP

IOOS_SERVER_URL = "https://gliders.ioos.us/erddap"

class DownloaderIOOS(DownloaderERDDAP):

    SERVER_URL: str = IOOS_SERVER_URL
    PROTOCOL: str = "tabledap"
    RESPONSE_FORMAT: str = "parquet"

    def __init__(self, config: dict = None):
        if config is None:
            raise ValueError("No config argument provided to DownloaderIOOS.")
        super().__init__(config)

        self.delayed_only = config.get("delayed_only", True)
###############################################################################################