#!/usr/bin/env python3

## @file test_downloaderOleanderXBT.py
#
#
## @author Mahi Sarwar Anol <anol.mahi@gmail.com>
#
## @date Mon 30 Mar 2026

##########################################################################
import os
import zipfile
from unittest.mock import MagicMock, patch, call

import pytest
import requests

from crocolaketools.downloader.downloaderOleanderXBT import (
    OLEANDER_BASE_URL,
    DownloaderURLList,
)
##########################################################################

DUMMY_URLS = [
    f"{OLEANDER_BASE_URL}/2022_xbt_nc.zip",
    f"{OLEANDER_BASE_URL}/2023_xbt_nc.zip",
]


class TestDownloaderURLListInit:
    """Tests for DownloaderURLList.__init__"""

    def test_defaults(self, mock_base_downloader):
        """Constructor stores correct defaults."""
        d = DownloaderURLList(urls=DUMMY_URLS)
        assert d.urls == DUMMY_URLS
        assert d.num_threads == 4
        assert d.overwrite is False
        assert d.dryrun is False

    def test_custom_args(self, mock_base_downloader):
        """Constructor stores custom arguments."""
        d = DownloaderURLList(
            urls=DUMMY_URLS,
            num_threads=8,
            overwrite=True,
            dryrun=True,
        )
        assert d.num_threads == 8
        assert d.overwrite is True
        assert d.dryrun is True

    def test_inherits_downloader(self):
        """DownloaderURLList is a subclass of Downloader."""
        from crocolaketools.downloader.downloader import Downloader
        assert issubclass(DownloaderURLList, Downloader)

    def test_default_config_is_oleanderxbt_phy(self, mock_base_downloader):
        """When no config given, base class called with OleanderXBT/PHY."""
        with patch(
            "crocolaketools.downloader.downloaderOleanderXBT.Downloader.__init__"
        ) as mock_super:
            mock_super.return_value = None
            DownloaderURLList(urls=[])
            called_config = mock_super.call_args[0][0]
            assert called_config['db'] == 'OleanderXBT'
            assert called_config['db_type'] == 'PHY'


class TestNcFilesExist:
    """Tests for DownloaderURLList._nc_files_exist"""

    def test_returns_true_when_nc_exists(self, tmp_path, mock_base_downloader):
        """Returns True when matching .nc files are present."""
        (tmp_path / "2022_data.nc").write_bytes(b"data")
        d = DownloaderURLList(urls=[])
        zip_path = str(tmp_path / "2022_xbt_nc.zip")
        assert d._nc_files_exist(zip_path) is True

    def test_returns_false_when_no_nc(self, tmp_path, mock_base_downloader):
        """Returns False when no matching .nc files are present."""
        d = DownloaderURLList(urls=[])
        zip_path = str(tmp_path / "2022_xbt_nc.zip")
        assert d._nc_files_exist(zip_path) is False

    def test_returns_false_when_dir_missing(self, tmp_path, mock_base_downloader):
        """Returns False when the extract directory does not exist."""
        d = DownloaderURLList(urls=[])
        zip_path = str(tmp_path / "nonexistent" / "2022_xbt_nc.zip")
        assert d._nc_files_exist(zip_path) is False


class TestGetAvailableYears:
    """Tests for DownloaderURLList.get_available_years"""

    def test_parses_years_from_html(self):
        """Extracts years correctly from ERDDAP directory listing HTML."""
        fake_html = (
            '<a href="2021_xbt_nc.zip">2021_xbt_nc.zip</a>'
            '<a href="2022_xbt_nc.zip">2022_xbt_nc.zip</a>'
            '<a href="2023_xbt_nc.zip">2023_xbt_nc.zip</a>'
        )
        mock_resp = MagicMock()
        mock_resp.text = fake_html
        mock_resp.raise_for_status = MagicMock()

        with patch("crocolaketools.downloader.downloaderOleanderXBT.requests.get",
                   return_value=mock_resp):
            years = DownloaderURLList.get_available_years()

        assert years == [2021, 2022, 2023]

    def test_returns_empty_on_connection_error(self):
        """Returns empty list when server is unreachable."""
        with patch("crocolaketools.downloader.downloaderOleanderXBT.requests.get",
                   side_effect=requests.RequestException("down")):
            years = DownloaderURLList.get_available_years()
        assert years == []


class TestBuildUrls:
    """Tests for DownloaderURLList.build_urls"""

    def test_builds_correct_urls(self):
        """Constructs correct zip URLs for given years."""
        urls = DownloaderURLList.build_urls([2021, 2022])
        assert urls == [
            f"{OLEANDER_BASE_URL}/2021_xbt_nc.zip",
            f"{OLEANDER_BASE_URL}/2022_xbt_nc.zip",
        ]


class TestDownload:
    """Tests for DownloaderURLList.download"""

    def test_skips_existing_nc_files(self, tmp_path, mock_base_downloader):
        """No download attempted when .nc files already exist and overwrite=False."""
        d = DownloaderURLList(urls=DUMMY_URLS, overwrite=False)
        d.base_dir = str(tmp_path) + "/"
        # Create existing .nc file for 2022
        (tmp_path / "2022_data.nc").write_bytes(b"data")

        with patch.object(DownloaderURLList, "download_parallel",
                          return_value=(0, 0)) as mock_parallel:
            d.download()
            # Only 2023 should be queued (2022 already has .nc files)
            args = mock_parallel.call_args[0][0]
            assert len(args) == 1
            assert "2023" in args[0][0]

    def test_downloads_all_when_nothing_present(self, tmp_path, mock_base_downloader):
        """All URLs are queued when no .nc files exist locally."""
        d = DownloaderURLList(urls=DUMMY_URLS)
        d.base_dir = str(tmp_path) + "/"

        with patch.object(DownloaderURLList, "download_parallel",
                          return_value=(2, 0)) as mock_parallel, \
             patch.object(DownloaderURLList, "unzip_file"):
            completed, failed = d.download()
            assert mock_parallel.call_args[0][0] == [
                (DUMMY_URLS[0], os.path.join(d.base_dir, "2022_xbt_nc.zip")),
                (DUMMY_URLS[1], os.path.join(d.base_dir, "2023_xbt_nc.zip")),
            ]

    def test_dryrun_skips_actual_download(self, tmp_path, mock_base_downloader):
        """Dryrun passes through to download_parallel without fetching files."""
        d = DownloaderURLList(urls=DUMMY_URLS, dryrun=True)
        d.base_dir = str(tmp_path) + "/"

        with patch.object(DownloaderURLList, "download_parallel",
                          return_value=(2, 0)) as mock_parallel:
            d.download()
            _, kwargs = mock_parallel.call_args
            assert kwargs.get("dryrun") is True

    def test_overwrite_downloads_even_if_nc_exists(self, tmp_path, mock_base_downloader):
        """All URLs are queued even if .nc files exist when overwrite=True."""
        d = DownloaderURLList(urls=DUMMY_URLS, overwrite=True)
        d.base_dir = str(tmp_path) + "/"
        (tmp_path / "2022_data.nc").write_bytes(b"data")

        with patch.object(DownloaderURLList, "download_parallel",
                          return_value=(2, 0)) as mock_parallel, \
             patch.object(DownloaderURLList, "unzip_file"):
            d.download()
            args = mock_parallel.call_args[0][0]
            assert len(args) == 2

    def test_unzip_called_for_each_downloaded_zip(self, tmp_path, mock_base_downloader):
        """unzip_file is called for each zip that was successfully downloaded."""
        d = DownloaderURLList(urls=[DUMMY_URLS[0]])
        d.base_dir = str(tmp_path) + "/"
        zip_path = os.path.join(d.base_dir, "2022_xbt_nc.zip")

        # Simulate zip file appearing after download
        with patch.object(DownloaderURLList, "download_parallel",
                          return_value=(1, 0)), \
             patch("os.path.isfile", return_value=True), \
             patch.object(DownloaderURLList, "unzip_file") as mock_unzip:
            d.download()
            mock_unzip.assert_called_once_with(zip_path)


##########################################################################
# Fixtures
##########################################################################

@pytest.fixture
def mock_base_downloader():
    """Patch Downloader.__init__ and configure_logging so tests don't need
    config.yaml or write log files to disk."""
    with patch(
        "crocolaketools.downloader.downloaderOleanderXBT.Downloader.__init__",
        return_value=None,
    ), patch(
        "crocolaketools.downloader.downloaderOleanderXBT.configure_logging",
    ):
        # input_path is set by Downloader.__init__ normally.
        # Since we mock it out, tests that need it set it manually: d.input_path = ...
        yield


##########################################################################

if __name__ == "__main__":
    pytest.main([__file__, "-v"])