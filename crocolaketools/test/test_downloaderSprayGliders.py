#!/usr/bin/env python3

## @file test_downloaderSprayGliders.py
#
#
## @author mahi-anol
#
## @date Sat 21 Mar 2026

##########################################################################
import os
from unittest.mock import MagicMock, patch

import pytest
import requests

from crocolaketools.downloader.downloaderSprayGliders import (
    SPRAY_BASE_URL,
    SPRAY_FILES,
    DownloaderSprayGliders,
)
##########################################################################

DUMMY_CONFIG = {'db': 'SprayGliders', 'db_type': 'PHY'}


class TestDownloaderSprayGlidersInit:
    """Tests for DownloaderSprayGliders.__init__"""

    def test_defaults(self, mock_base_downloader):
        """Constructor stores correct defaults when no args given."""
        d = DownloaderSprayGliders()
        assert d.fnames == SPRAY_FILES
        assert d.base_url == SPRAY_BASE_URL
        assert d.overwrite is False

    def test_custom_args(self, mock_base_downloader):
        """Constructor stores custom arguments."""
        custom_files = {"CORC.nc": "binnedCORC/CORC.nc", "GulfStream.nc": "binnedGS/GulfStream.nc"}
        custom_url = "https://example.com/spray"

        d = DownloaderSprayGliders(
            config=DUMMY_CONFIG,
            fnames=custom_files,
            base_url=custom_url,
            overwrite=True,
        )
        assert d.fnames == custom_files
        assert d.base_url == custom_url
        assert d.overwrite is True

    def test_inherits_downloader(self):
        """DownloaderSprayGliders is a subclass of Downloader."""
        from crocolaketools.downloader.downloader import Downloader
        assert issubclass(DownloaderSprayGliders, Downloader)

    def test_default_config_is_spraygliders_phy(self, mock_base_downloader):
        """When no config is given, base class is called with SprayGliders/PHY."""
        with patch(
            "crocolaketools.downloader.downloaderSprayGliders.Downloader.__init__"
        ) as mock_super:
            mock_super.return_value = None
            DownloaderSprayGliders()
            called_config = mock_super.call_args[0][0]
            assert called_config['db'] == 'SprayGliders'
            assert called_config['db_type'] == 'PHY'


class TestGetUrl:
    """Tests for DownloaderSprayGliders.get_url."""

    def test_returns_url_when_reachable(self, mock_base_downloader):
        """Returns full URL when the file is reachable."""
        d = DownloaderSprayGliders()
        mock_resp = MagicMock()
        mock_resp.ok = True

        with patch(
            "crocolaketools.downloader.downloaderSprayGliders.requests.get",
            return_value=mock_resp,
        ):
            url = d.get_url("CORC.nc")
        assert url == f"{SPRAY_BASE_URL}/{SPRAY_FILES['CORC.nc']}"

    def test_raises_when_url_unreachable(self, mock_base_downloader):
        """RuntimeError raised when the URL is not reachable."""
        d = DownloaderSprayGliders()

        with patch(
            "crocolaketools.downloader.downloaderSprayGliders.requests.get",
            side_effect=requests.RequestException("down"),
        ):
            with pytest.raises(RuntimeError, match="URL not reachable"):
                d.get_url("CORC.nc")


class TestIsAlreadyDownloaded:
    """Tests for Downloader._is_already_downloaded (inherited)."""

    def test_file_exists_no_overwrite(self, tmp_path, mock_base_downloader):
        """Returns True when file exists and overwrite=False."""
        existing = tmp_path / "CORC.nc"
        existing.write_bytes(b"data")
        d = DownloaderSprayGliders(overwrite=False)
        assert d._is_already_downloaded(str(existing)) is True

    def test_file_exists_with_overwrite(self, tmp_path, mock_base_downloader):
        """Returns False when file exists but overwrite=True."""
        existing = tmp_path / "CORC.nc"
        existing.write_bytes(b"data")
        d = DownloaderSprayGliders(overwrite=True)
        assert d._is_already_downloaded(str(existing)) is False

    def test_file_missing(self, tmp_path, mock_base_downloader):
        """Returns False when file does not exist."""
        d = DownloaderSprayGliders(overwrite=False)
        assert d._is_already_downloaded(str(tmp_path / "missing.nc")) is False


class TestSprayDownload:
    """Tests for DownloaderSprayGliders.spray_download."""

    def test_skips_existing_files(self, tmp_path, capsys, mock_base_downloader):
        """No HTTP request made when files exist and overwrite=False."""
        d = DownloaderSprayGliders(fnames={"CORC.nc": "binnedCORC/CORC.nc"}, overwrite=False)
        d.input_path = str(tmp_path) + "/"
        existing = os.path.join(d.input_path, "CORC.nc")
        with open(existing, "wb") as fh:
            fh.write(b"data")

        with patch.object(DownloaderSprayGliders, "_download_file") as mock_dl:
            result = d.spray_download()
            mock_dl.assert_not_called()

        assert result == [existing]
        captured = capsys.readouterr()
        assert "already present" in captured.out

    def test_downloads_missing_file(self, tmp_path, mock_base_downloader):
        """File is downloaded when not already present."""
        d = DownloaderSprayGliders(fnames={"CORC.nc": "binnedCORC/CORC.nc"})
        d.input_path = str(tmp_path) + "/"
        expected_path = os.path.join(d.input_path, "CORC.nc")
        expected_url = f"{SPRAY_BASE_URL}/binnedCORC/CORC.nc"

        with patch.object(
            DownloaderSprayGliders, "get_url",
            return_value=expected_url
        ), patch.object(DownloaderSprayGliders, "_download_file") as mock_dl:
            result = d.spray_download()
            mock_dl.assert_called_once_with(expected_url, expected_path)

        assert result == [expected_path]

    def test_returns_correct_paths(self, tmp_path, mock_base_downloader):
        """spray_download returns local_path for each file correctly."""
        fnames = {"CORC.nc": "binnedCORC/CORC.nc", "GulfStream.nc": "binnedGS/GulfStream.nc"}
        d = DownloaderSprayGliders(fnames=fnames)
        d.input_path = str(tmp_path) + "/"
        expected_paths = [os.path.join(d.input_path, f) for f in fnames]

        with patch.object(
            DownloaderSprayGliders, "get_url",
            side_effect=lambda f: f"{SPRAY_BASE_URL}/{fnames[f]}"
        ), patch.object(DownloaderSprayGliders, "_download_file"):
            result = d.spray_download()

        assert result == expected_paths

    def test_overwrite_triggers_redownload(self, tmp_path, mock_base_downloader):
        """Existing file is re-downloaded when overwrite=True."""
        d = DownloaderSprayGliders(fnames={"CORC.nc": "binnedCORC/CORC.nc"}, overwrite=True)
        d.input_path = str(tmp_path) + "/"
        existing = os.path.join(d.input_path, "CORC.nc")
        with open(existing, "wb") as fh:
            fh.write(b"old data")

        with patch.object(
            DownloaderSprayGliders, "get_url",
            return_value=f"{SPRAY_BASE_URL}/binnedCORC/CORC.nc"
        ), patch.object(DownloaderSprayGliders, "_download_file") as mock_dl:
            d.spray_download()
            mock_dl.assert_called_once()

    def test_raises_when_url_unreachable(self, tmp_path, mock_base_downloader):
        """RuntimeError propagated when get_url raises for a file."""
        d = DownloaderSprayGliders(fnames={"CORC.nc": "binnedCORC/CORC.nc"})
        d.input_path = str(tmp_path) + "/"

        with patch.object(
            DownloaderSprayGliders, "get_url",
            side_effect=RuntimeError("URL not reachable: ...")
        ):
            with pytest.raises(RuntimeError, match="URL not reachable"):
                d.spray_download()


##########################################################################
# Fixtures
##########################################################################

@pytest.fixture
def mock_base_downloader():
    """Patch Downloader.__init__ so tests don't need config.yaml or
    crocolakeloader.params to be installed."""
    with patch(
        "crocolaketools.downloader.downloaderSprayGliders.Downloader.__init__",
        return_value=None,
    ):
        yield


##########################################################################

if __name__ == "__main__":
    pytest.main([__file__, "-v"])