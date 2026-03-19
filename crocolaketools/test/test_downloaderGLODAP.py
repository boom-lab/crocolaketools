#!/usr/bin/env python3

## @file test_downloaderGLODAP.py
#
#
## @author mahi-anol
#
## @date Fri 13 Mar 2026

##########################################################################
import os
import zipfile
from unittest.mock import MagicMock, patch, call

import pytest
import requests

from crocolaketools.downloader.downloaderGLODAP import (
    GLODAP_MASTER_FNAME,
    GLODAP_URL_GEOMAR,
    GLODAP_URL_NCEI,
    DownloaderGLODAP,
)
##########################################################################

# Minimal config used across tests -- base class reads the rest from config.yaml
DUMMY_CONFIG = {'db': 'GLODAP', 'db_type': 'PHY'}


class TestDownloaderGLODAPInit:
    """Tests for DownloaderGLODAP.__init__"""

    def test_defaults(self, mock_base_downloader):
        """Constructor stores correct defaults when no args given."""
        d = DownloaderGLODAP()
        assert d.fname == GLODAP_MASTER_FNAME
        assert d.overwrite is False

    def test_custom_args(self, mock_base_downloader):
        """Constructor stores custom arguments."""
        custom_fname = "GLODAPv2.2022_Merged_Master_File.csv"

        d = DownloaderGLODAP(
            config=DUMMY_CONFIG,
            fname=custom_fname,
            overwrite=True,
        )
        assert d.fname == custom_fname
        assert d.overwrite is True

    def test_inherits_downloader(self):
        """DownloaderGLODAP is a subclass of Downloader."""
        from crocolaketools.downloader.downloader import Downloader
        assert issubclass(DownloaderGLODAP, Downloader)

    def test_default_config_is_glodap_phy(self, mock_base_downloader):
        """When no config is given, base class is called with GLODAP/PHY."""
        with patch(
            "crocolaketools.downloader.downloaderGLODAP.Downloader.__init__"
        ) as mock_super:
            mock_super.return_value = None
            DownloaderGLODAP()
            called_config = mock_super.call_args[0][0]
            assert called_config['db'] == 'GLODAP'
            assert called_config['db_type'] == 'PHY'


class TestIsAlreadyDownloaded:
    """Tests for Downloader._is_already_downloaded (inherited by DownloaderGLODAP)"""

    def test_file_exists_no_overwrite(self, tmp_path, mock_base_downloader):
        """Returns True when file exists and overwrite=False."""
        existing = tmp_path / "file.csv"
        existing.write_text("data")
        d = DownloaderGLODAP(overwrite=False)
        assert d._is_already_downloaded(str(existing)) is True

    def test_file_exists_with_overwrite(self, tmp_path, mock_base_downloader):
        """Returns False when file exists but overwrite=True."""
        existing = tmp_path / "file.csv"
        existing.write_text("data")
        d = DownloaderGLODAP(overwrite=True)
        assert d._is_already_downloaded(str(existing)) is False

    def test_file_missing(self, tmp_path, mock_base_downloader):
        """Returns False when file does not exist."""
        missing = str(tmp_path / "nonexistent.csv")
        d = DownloaderGLODAP(overwrite=False)
        assert d._is_already_downloaded(missing) is False


class TestDownloadFile:
    """Tests for Downloader._download_file (inherited by DownloaderGLODAP)."""

    def test_writes_content_to_disk(self, tmp_path):
        """File is written to disk with correct content."""
        dest = str(tmp_path / "out.csv")
        fake_content = b"cruise,station\n1,2\n"

        mock_response = MagicMock()
        mock_response.headers = {"content-length": str(len(fake_content))}
        mock_response.iter_content.return_value = [fake_content]
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_response.raise_for_status = MagicMock()

        with patch(
            "crocolaketools.downloader.downloader.requests.get",
            return_value=mock_response,
        ):
            DownloaderGLODAP._download_file("https://example.com/f.csv", dest)

        assert os.path.isfile(dest)
        with open(dest, "rb") as fh:
            assert fh.read() == fake_content

    def test_raises_on_http_error(self, tmp_path):
        """HTTPError from requests is propagated."""
        dest = str(tmp_path / "out.csv")

        mock_response = MagicMock()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_response.raise_for_status.side_effect = (
            requests.exceptions.HTTPError("404 Not Found")
        )

        with patch(
            "crocolaketools.downloader.downloader.requests.get",
            return_value=mock_response,
        ):
            with pytest.raises(requests.exceptions.HTTPError):
                DownloaderGLODAP._download_file("https://bad.url/f.csv", dest)


class TestUnzipFile:
    """Tests for Downloader.unzip_file (inherited by DownloaderGLODAP)."""

    def test_extracts_and_deletes_zip(self, tmp_path):
        """Contents are extracted and the zip is deleted."""
        csv_content = b"cruise,station\n1,2\n"
        zip_path = str(tmp_path / "test.zip")

        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("file.csv", csv_content)

        DownloaderGLODAP.unzip_file(zip_path)

        assert not os.path.exists(zip_path)
        extracted = tmp_path / "file.csv"
        assert extracted.exists()
        assert extracted.read_bytes() == csv_content

    def test_removes_macosx_folder(self, tmp_path):
        """__MACOSX metadata folder is removed after extraction."""
        zip_path = str(tmp_path / "test.zip")

        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("file.csv", b"data")
            zf.writestr("__MACOSX/._file.csv", b"macos metadata")

        DownloaderGLODAP.unzip_file(zip_path)

        macosx_path = tmp_path / "__MACOSX"
        assert not macosx_path.exists()


class TestGetUrl:
    """Tests for DownloaderGLODAP.get_url."""

    def test_returns_ncei_when_reachable(self, mock_base_downloader):
        """Returns NCEI URL when it responds with 2xx."""
        d = DownloaderGLODAP()
        mock_resp = MagicMock()
        mock_resp.ok = True

        with patch("crocolaketools.downloader.downloaderGLODAP.requests.head",
                   return_value=mock_resp):
            url = d.get_url()
        assert url == GLODAP_URL_NCEI

    def test_falls_back_to_geomar_when_ncei_unreachable(self, mock_base_downloader):
        """Falls back to GEOMAR URL when NCEI raises RequestException."""
        d = DownloaderGLODAP()

        def head_side_effect(url, timeout):
            if url == GLODAP_URL_NCEI:
                raise requests.RequestException("NCEI down")
            mock_resp = MagicMock()
            mock_resp.ok = True
            return mock_resp

        with patch("crocolaketools.downloader.downloaderGLODAP.requests.head",
                   side_effect=head_side_effect):
            url = d.get_url()
        assert url == GLODAP_URL_GEOMAR

    def test_raises_when_all_urls_unreachable(self, mock_base_downloader):
        """RuntimeError raised when all URLs are unreachable."""
        d = DownloaderGLODAP()

        with patch("crocolaketools.downloader.downloaderGLODAP.requests.head",
                   side_effect=requests.RequestException("all down")):
            with pytest.raises(RuntimeError, match="None of the URLs are reachable"):
                d.get_url()


class TestGLODAPDownload:
    """Tests for DownloaderGLODAP.glodap_download."""

    def test_skips_if_already_downloaded(self, tmp_path, capsys, mock_base_downloader):
        """No HTTP request made when file exists and overwrite=False."""
        d = DownloaderGLODAP(overwrite=False)
        d.input_path = str(tmp_path) + "/"
        existing = os.path.join(d.input_path, GLODAP_MASTER_FNAME)
        with open(existing, "w") as fh:
            fh.write("cruise,station\n")

        with patch.object(DownloaderGLODAP, "_download_file") as mock_dl:
            result = d.glodap_download()
            mock_dl.assert_not_called()

        assert result == existing
        captured = capsys.readouterr()
        assert "already present" in captured.out

    def test_downloads_from_ncei_url(self, tmp_path, mock_base_downloader):
        """File is downloaded directly when get_url() returns NCEI."""
        d = DownloaderGLODAP()
        d.input_path = str(tmp_path) + "/"
        expected_path = os.path.join(d.input_path, GLODAP_MASTER_FNAME)

        with patch.object(DownloaderGLODAP, "get_url", return_value=GLODAP_URL_NCEI), \
             patch.object(DownloaderGLODAP, "_download_file") as mock_dl:
            result = d.glodap_download()
            mock_dl.assert_called_once_with(GLODAP_URL_NCEI, expected_path)

        assert result == expected_path

    def test_downloads_and_unzips_from_geomar(self, tmp_path, mock_base_downloader):
        """Zip is downloaded and unzipped when get_url() returns GEOMAR."""
        d = DownloaderGLODAP()
        d.input_path = str(tmp_path) + "/"
        zip_path = os.path.join(d.input_path, GLODAP_MASTER_FNAME + ".zip")
        expected_path = os.path.join(d.input_path, GLODAP_MASTER_FNAME)
        with patch.object(DownloaderGLODAP, "get_url", return_value=GLODAP_URL_GEOMAR), \
             patch.object(DownloaderGLODAP, "_download_file") as mock_dl, \
             patch.object(DownloaderGLODAP, "unzip_file") as mock_unzip:
            result=d.glodap_download()
            mock_dl.assert_called_once_with(GLODAP_URL_GEOMAR, zip_path)
            mock_unzip.assert_called_once_with(zip_path)
        assert result == expected_path
    def test_overwrite_triggers_redownload(self, tmp_path, mock_base_downloader):
        """Existing file is re-downloaded when overwrite=True."""
        d = DownloaderGLODAP(overwrite=True)
        d.input_path = str(tmp_path) + "/"
        existing = os.path.join(d.input_path, GLODAP_MASTER_FNAME)
        with open(existing, "w") as fh:
            fh.write("old data")

        with patch.object(DownloaderGLODAP, "get_url", return_value=GLODAP_URL_NCEI), \
             patch.object(DownloaderGLODAP, "_download_file") as mock_dl:
            d.glodap_download()
            mock_dl.assert_called_once()


##########################################################################
# Fixtures
##########################################################################

@pytest.fixture
def mock_base_downloader():
    """Patch Downloader.__init__ so tests don't need config.yaml or
    crocolakeloader.params to be installed."""
    with patch(
        "crocolaketools.downloader.downloaderGLODAP.Downloader.__init__",
        return_value=None,
    ):
        yield


##########################################################################

if __name__ == "__main__":
    pytest.main([__file__, "-v"])