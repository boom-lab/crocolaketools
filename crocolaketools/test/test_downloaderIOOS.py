#!/usr/bin/env python3

## @file test_downloaderIOOS.py
#
#
## @author Mahi Sarwar Anol <anol.mahi@gmail.com>
#
## @date Thu 12 Jun 2026

##########################################################################
from unittest.mock import patch

import pytest

from crocolaketools.downloader.downloaderIOOS import IOOS_SERVER_URL, DownloaderIOOS
from crocolaketools.downloader.downloaderERDDAP import DownloaderERDDAP
##########################################################################

DUMMY_CONFIG = {'db': 'IOOS_GLIDERS', 'db_type': 'PHY'}


class TestDownloaderIOOSInit:
    """Tests for DownloaderIOOS.__init__"""

    def test_requires_config(self):
        """ValueError raised when no config is given."""
        with pytest.raises(ValueError):
            DownloaderIOOS(config=None)

    def test_delayed_only_default(self, mock_base_downloader):
        """delayed_only defaults to True."""
        d = DownloaderIOOS(config=dict(DUMMY_CONFIG))
        assert d.delayed_only is True

    def test_delayed_only_from_config(self, mock_base_downloader):
        """delayed_only takes the value from config."""
        d = DownloaderIOOS(config=dict(DUMMY_CONFIG, delayed_only=False))
        assert d.delayed_only is False

    def test_server_url(self, mock_base_downloader):
        """The erddapy client points at the IOOS server."""
        d = DownloaderIOOS(config=dict(DUMMY_CONFIG))
        assert d.server_url == IOOS_SERVER_URL
        assert d._erddap.server == IOOS_SERVER_URL

    def test_inherits_erddap(self):
        """DownloaderIOOS is a subclass of DownloaderERDDAP."""
        assert issubclass(DownloaderIOOS, DownloaderERDDAP)


##########################################################################
# Fixtures
##########################################################################

@pytest.fixture
def mock_base_downloader():
    """Patch the base Downloader.__init__ so tests don't need config.yaml."""
    with patch(
        "crocolaketools.downloader.downloaderERDDAP.Downloader.__init__",
        return_value=None,
    ):
        yield


##########################################################################

if __name__ == "__main__":
    pytest.main([__file__, "-v"])