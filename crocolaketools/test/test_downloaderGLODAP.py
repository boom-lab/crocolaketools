#!/usr/bin/env python3

## @file test_downloaderGLODAP.py
#
#
## @author mahi-anol
#
## @date Fri 13 Mar 2026

##########################################################################
import os
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

# Minimal config used across tests — base class reads the rest from config.yaml
DUMMY_CONFIG = {'db': 'GLODAP', 'db_type': 'PHY'}


class TestDownloaderGLODAPInit:
    """Tests for DownloaderGLODAP.__init__"""

    def test_defaults(self, mock_base_downloader):
        """Constructor stores correct defaults when no args given."""
        d = DownloaderGLODAP()
        assert d.fname == GLODAP_MASTER_FNAME
        assert d.url == GLODAP_URL_NCEI
        assert d.fallback_url == GLODAP_URL_GEOMAR
        assert d.overwrite is False

    def test_custom_args(self, mock_base_downloader):
        """Constructor stores custom arguments."""
        custom_fname = "GLODAPv2.2022_Merged_Master_File.csv"
        custom_url = "https://example.com/glodap.csv"
        custom_fallback = "https://mirror.example.com/glodap.csv"

        d = DownloaderGLODAP(
            config=DUMMY_CONFIG,
            fname=custom_fname,
            url=custom_url,
            fallback_url=custom_fallback,
            overwrite=True,
        )
        assert d.fname == custom_fname
        assert d.url == custom_url
        assert d.fallback_url == custom_fallback
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
    """Tests for DownloaderGLODAP._is_already_downloaded"""

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
    """Tests for DownloaderGLODAP._download_file (static method)."""

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
            "crocolaketools.downloader.downloaderGLODAP.requests.get",
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
            "crocolaketools.downloader.downloaderGLODAP.requests.get",
            return_value=mock_response,
        ):
            with pytest.raises(requests.exceptions.HTTPError):
                DownloaderGLODAP._download_file("https://bad.url/f.csv", dest)


class TestGlodapDownload:
    """Tests for DownloaderGLODAP.glodap_download."""

    def test_skips_if_already_downloaded(self, tmp_path, capsys, mock_base_downloader):
        """No HTTP request made when file exists and overwrite=False."""
        d = DownloaderGLODAP(overwrite=False)
        # inject resolved input_path as base class would
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

    def test_downloads_from_primary_url(self, tmp_path, mock_base_downloader):
        """File is downloaded from primary URL when not already present."""
        d = DownloaderGLODAP()
        d.input_path = str(tmp_path) + "/"
        expected_path = os.path.join(d.input_path, GLODAP_MASTER_FNAME)

        with patch.object(DownloaderGLODAP, "_download_file") as mock_dl:
            result = d.glodap_download()
            mock_dl.assert_called_once_with(GLODAP_URL_NCEI, expected_path)

        assert result == expected_path

    def test_falls_back_to_mirror_on_primary_failure(self, tmp_path, mock_base_downloader):
        """Fallback URL is tried when primary URL raises RequestException."""
        d = DownloaderGLODAP()
        d.input_path = str(tmp_path) + "/"
        expected_path = os.path.join(d.input_path, GLODAP_MASTER_FNAME)

        def fail_primary(url, local_path):
            if url == GLODAP_URL_NCEI:
                raise requests.exceptions.ConnectionError("primary down")

        with patch.object(
            DownloaderGLODAP, "_download_file", side_effect=fail_primary
        ) as mock_dl:
            result = d.glodap_download()

        assert result == expected_path
        assert mock_dl.call_count == 2
        calls = mock_dl.call_args_list
        assert calls[0] == call(GLODAP_URL_NCEI, expected_path)
        assert calls[1] == call(GLODAP_URL_GEOMAR, expected_path)

    def test_raises_when_both_urls_fail(self, tmp_path, mock_base_downloader):
        """RuntimeError raised when both primary and fallback URLs fail."""
        d = DownloaderGLODAP()
        d.input_path = str(tmp_path) + "/"

        with patch.object(
            DownloaderGLODAP,
            "_download_file",
            side_effect=requests.exceptions.ConnectionError("all down"),
        ):
            with pytest.raises(RuntimeError, match="Download failed"):
                d.glodap_download()

    def test_overwrite_triggers_redownload(self, tmp_path, mock_base_downloader):
        """Existing file is re-downloaded when overwrite=True."""
        d = DownloaderGLODAP(overwrite=True)
        d.input_path = str(tmp_path) + "/"
        existing = os.path.join(d.input_path, GLODAP_MASTER_FNAME)
        with open(existing, "w") as fh:
            fh.write("old data")

        with patch.object(DownloaderGLODAP, "_download_file") as mock_dl:
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
