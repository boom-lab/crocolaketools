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
import pandas as pd
from datetime import datetime, timezone

from crocolaketools.downloader.downloader_saildrones import DownloaderSaildrones
##########################################################################


class TestDownloaderSaildronesInit:
    """Tests for DownloaderSaildrones class inheritance and init method"""

    def test_inherits_downloader(self):
        """DownloaderSaildrones is a subclass of Downloader."""
        from crocolaketools.downloader.downloader import Downloader
        assert issubclass(DownloaderSaildrones, Downloader)


class TestSaildronesTools:
    """Tests for the isolated saildrones tools fetching logic"""

    @patch("crocolaketools.downloader.downloader_saildrones.pd.read_csv")
    @patch("crocolaketools.downloader.downloader_saildrones.requests.get")
    def test_get_dataset_ids(self, mock_erddap, mock_read_csv):
        """Dataset string prefix filtering correctly excludes non-matching items"""
        mock_erddap.return_value.text = "mocked_csv_data"
        mock_read_csv.return_value = pd.DataFrame({
            "Dataset ID": ["sd1005_2017", "sd1006_2017", "glider01", "junk_string"]
        })
        
        dl = DownloaderSaildrones()
        ids = dl.get_dataset_ids(id_prefix="sd")
        assert ids == ["sd1005_2017", "sd1006_2017"]

    @patch("crocolaketools.downloader.downloader_saildrones.pd.read_csv")
    def test_get_time_url_modified(self, mock_read_csv):
        """Verify the correct time parsing from info.csv endpoints pulling NC_GLOBAL attrs"""
        mock_read_csv.return_value = pd.DataFrame({
            "Variable Name": ["NC_GLOBAL"],
            "Attribute Name": ["date_modified"],
            "Value": ["2026-03-18T12:00:00Z"]
        })
        dl = DownloaderSaildrones()
        dt = dl.get_time_url("sd1005", "dummy_server")
        assert dt.year == 2026
        assert dt.month == 3
        assert dt.day == 18

    @patch("crocolaketools.downloader.downloader_saildrones.pd.read_csv")
    def test_get_time_url_fallback(self, mock_read_csv):
        """Verify it falls back to date_created if date_modified is absent"""
        mock_read_csv.return_value = pd.DataFrame({
            "Variable Name": ["NC_GLOBAL", "NC_GLOBAL"],
            "Attribute Name": ["random_attr", "date_created"],
            "Value": ["foo", "2018-05-06T00:00:00Z"]
        })
        dl = DownloaderSaildrones()
        dt = dl.get_time_url("sd1005_2017", "dummy_server")
        assert dt.year == 2018


class TestSaildronesErddapDownload:
    """Tests for the actual downloading and modification logic sequence"""

    @patch("crocolaketools.downloader.downloader_saildrones.requests.get")
    @patch("crocolaketools.downloader.downloader_saildrones.shutil.copyfileobj")
    @patch.object(DownloaderSaildrones, "get_dataset_ids")
    def test_saildrones_erddap_fresh_download(self, mock_get_ids, mock_copy, mock_get, tmp_path):
        """Requests block correctly executes and triggers file copy given no local file present"""
        mock_get_ids.return_value = ["sd_test_data"]
        
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_get.return_value = mock_resp

        dl = DownloaderSaildrones()
        dl.saildrones_erddap(save_to=str(tmp_path), dryrun=False, verbose=False)
        
        mock_get.assert_called_once()
        mock_copy.assert_called_once()

    @patch.object(DownloaderSaildrones, "get_time_url")
    @patch.object(DownloaderSaildrones, "get_dataset_ids")
    @patch("crocolaketools.downloader.downloader_saildrones.requests.get")
    def test_saildrones_erddap_checktime_skip(self, mock_get, mock_get_ids, mock_get_time, tmp_path):
        """No download request dispatched if local file modifies date exceeds remote date (skip)"""
        mock_get_ids.return_value = ["sd_test_data"]
        
        # Simulate local file that already exists natively on the runner
        test_file = tmp_path / "sd_test_data.nc"
        test_file.touch()
        
        # Mock remote file as heavily outdated
        mock_get_time.return_value = datetime(1990, 1, 1, tzinfo=timezone.utc)
        
        dl = DownloaderSaildrones()
        dl.saildrones_erddap(save_to=str(tmp_path), checktime=True, verbose=False)
        mock_get.assert_not_called()

    @patch.object(DownloaderSaildrones, "get_time_url")
    @patch.object(DownloaderSaildrones, "get_dataset_ids")
    @patch("crocolaketools.downloader.downloader_saildrones.requests.get")
    @patch("crocolaketools.downloader.downloader_saildrones.shutil.copyfileobj")
    def test_saildrones_erddap_checktime_download_newer(self, mock_copy, mock_get, mock_get_ids, mock_get_time, tmp_path):
        """Download request IS dispatched if remote file modification date exceeds local file date"""
        mock_get_ids.return_value = ["sd_test_data"]
        
        # Simulate local file that already exists natively on the runner
        test_file = tmp_path / "sd_test_data.nc"
        test_file.touch()
        
        # Mock remote file as far in the future
        mock_get_time.return_value = datetime(2099, 1, 1, tzinfo=timezone.utc)
        
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_get.return_value = mock_resp

        dl = DownloaderSaildrones()
        dl.saildrones_erddap(save_to=str(tmp_path), checktime=True, verbose=False)
        mock_get.assert_called_once()
        mock_copy.assert_called_once()

    @patch.object(DownloaderSaildrones, "get_dataset_ids")
    @patch("crocolaketools.downloader.downloader_saildrones.requests.get")
    def test_saildrones_erddap_overwrite_skip(self, mock_get, mock_get_ids, tmp_path):
        """Skip download context when checktime is False and overwrite is False"""
        mock_get_ids.return_value = ["sd_test_data"]
        
        # Simulate local file that already exists natively on the runner
        test_file = tmp_path / "sd_test_data.nc"
        test_file.touch()
        
        dl = DownloaderSaildrones()
        dl.saildrones_erddap(save_to=str(tmp_path), checktime=False, overwrite=False, verbose=False)
        mock_get.assert_not_called()

    @patch("crocolaketools.downloader.downloader_saildrones.shutil.copyfileobj")
    @patch.object(DownloaderSaildrones, "get_dataset_ids")
    @patch("crocolaketools.downloader.downloader_saildrones.requests.get")
    def test_saildrones_erddap_overwrite_force(self, mock_get, mock_get_ids, mock_copy, tmp_path):
        """Execute download when checktime is False and overwrite is True"""
        mock_get_ids.return_value = ["sd_test_data"]
        
        # Simulate local file that already exists natively on the runner
        test_file = tmp_path / "sd_test_data.nc"
        test_file.touch()
        
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_get.return_value = mock_resp

        dl = DownloaderSaildrones()
        dl.saildrones_erddap(save_to=str(tmp_path), checktime=False, overwrite=True, verbose=False)
        mock_get.assert_called_once()
        mock_copy.assert_called_once()

    @patch("crocolaketools.downloader.downloader_saildrones.requests.get")
    @patch.object(DownloaderSaildrones, "get_dataset_ids")
    def test_saildrones_erddap_dryrun(self, mock_get_ids, mock_get, tmp_path):
        """Ensure dryrun skips all download blocks and purely prints behavior"""
        mock_get_ids.return_value = ["sd_test_data"]
        
        dl = DownloaderSaildrones()
        dl.saildrones_erddap(save_to=str(tmp_path), dryrun=True, verbose=False)
        mock_get.assert_not_called()

class TestSaildronesDownloadMethod:
    """Testing wrapper call structure inside logic controller DownloaderSaildrones"""

    @patch.object(DownloaderSaildrones, "saildrones_erddap")
    def test_saildrones_download_parameter_mapping(self, mock_erddap):
        """Validates parameter propagation directly back into tool chain"""
        downloader = DownloaderSaildrones()
        
        downloader.saildrones_download(
            outdir_nc="./dummy", 
            search_for="TPOS", 
            id_prefix="sd",
            dryrun_flag=True
        )
        
        mock_erddap.assert_called_once_with(
            save_to="./dummy",
            dryrun=True,
            verbose=True,
            checktime=True,
            search_for="TPOS",
            id_prefix="sd"
        )


##########################################################################

if __name__ == "__main__":
    pytest.main([__file__, "-v"])