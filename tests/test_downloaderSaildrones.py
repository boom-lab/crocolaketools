#!/usr/bin/env python3

## @file test_downloaderSaildrones.py
#
#
# @author Alieldin Alaa <alieldinalaa04@gmail.com>
#
## @date Thu 19 Mar 2026

##########################################################################
import os
from unittest.mock import MagicMock, patch
import pytest

from crocolaketools.downloader.downloaderSaildrones import DownloaderSaildrones, SAILDRONES_SERVER, SAILDRONES_URLS
from crocolaketools.downloader.downloader import Downloader
##########################################################################

# Mock the base init to bypass config/yaml loading during testing
@pytest.fixture(autouse=True)
def mock_base_init():
    with patch.object(Downloader, '__init__', lambda self, config: None):
        yield

class TestDownloaderSaildronesInit:
    """Tests for DownloaderSaildrones class inheritance and init method"""

    def test_inherits_downloader(self):
        """DownloaderSaildrones is a subclass of Downloader."""
        assert issubclass(DownloaderSaildrones, Downloader)

    def test_init_parameters(self):
        dl = DownloaderSaildrones(overwrite=True)
        assert dl.overwrite is True


class TestSaildronesDownloadMethod:
    """Testing saildrones downloading logic sequence"""

    @patch("crocolaketools.downloader.downloaderSaildrones.requests.head")
    @patch.object(DownloaderSaildrones, "_download_file")
    def test_saildrones_download_success(self, mock_download, mock_head, tmp_path):
        """Test full download when files don't exist locally."""
        mock_head.return_value.ok = True
        
        dl = DownloaderSaildrones()
        dl.input_path = str(tmp_path)
        dl.overwrite = False
        
        # Test only the first url
        with patch("crocolaketools.downloader.downloaderSaildrones.SAILDRONES_URLS", [SAILDRONES_URLS[0]]):
            dl.saildrones_download()
            
        mock_download.assert_called_once()
        mock_head.assert_called_once_with(SAILDRONES_SERVER, timeout=10)

    @patch("crocolaketools.downloader.downloaderSaildrones.requests.head")
    @patch.object(DownloaderSaildrones, "_download_file")
    def test_saildrones_download_skip_existing(self, mock_download, mock_head, tmp_path):
        """Test skip of existing files when overwrite is False."""
        mock_head.return_value.ok = True
        
        dl = DownloaderSaildrones()
        dl.input_path = str(tmp_path)
        dl.overwrite = False
        
        # Create a mock file
        test_file = tmp_path / os.path.basename(SAILDRONES_URLS[0])
        test_file.touch()
        
        with patch("crocolaketools.downloader.downloaderSaildrones.SAILDRONES_URLS", [SAILDRONES_URLS[0]]):
            dl.saildrones_download()
            
        mock_download.assert_not_called()

    @patch("crocolaketools.downloader.downloaderSaildrones.requests.head")
    @patch.object(DownloaderSaildrones, "_download_file")
    def test_saildrones_download_overwrite(self, mock_download, mock_head, tmp_path):
        """Test overwriting existing files when overwrite is True."""
        mock_head.return_value.ok = True
        
        dl = DownloaderSaildrones()
        dl.input_path = str(tmp_path)
        dl.overwrite = True
        
        # Create a mock file
        test_file = tmp_path / os.path.basename(SAILDRONES_URLS[0])
        test_file.touch()
        
        with patch("crocolaketools.downloader.downloaderSaildrones.SAILDRONES_URLS", [SAILDRONES_URLS[0]]):
            dl.saildrones_download()
            
        mock_download.assert_called_once()

    @patch("crocolaketools.downloader.downloaderSaildrones.requests.head")
    def test_saildrones_download_server_down(self, mock_head, tmp_path):
        """Verify error is raised if ERDDAP server is unreachable."""
        import requests
        mock_head.side_effect = requests.RequestException("Connection dropped")
        
        dl = DownloaderSaildrones()
        dl.input_path = str(tmp_path)
        
        with pytest.raises(RuntimeError):
            dl.saildrones_download()

##########################################################################

if __name__ == "__main__":
    pytest.main([__file__, "-v"])