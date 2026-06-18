#!/usr/bin/env python3

## @file test_downloaderIOOSGliders.py
#
#
## @author Mahi Sarwar Anol <anol.mahi@gmail.com>
#
## @date Thu 14 Jun 2026

##########################################################################
import os
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from crocolaketools.downloader.downloaderIOOSGliders import (
    GLIDER_VARIABLES,
    DownloaderIOOSGliders,
)
from crocolaketools.downloader.downloaderIOOS import DownloaderIOOS
##########################################################################

DUMMY_CONFIG = {'db': 'IOOS_GLIDERS', 'db_type': 'PHY'}


class TestDownloaderIOOSGlidersInit:
    """Tests for DownloaderIOOSGliders.__init__"""

    def test_defaults(self, mock_base_downloader):
        """sync defaults to False and delayed_only to True."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG))
        assert d.sync is False
        assert d.delayed_only is True

    def test_sync_from_config(self, mock_base_downloader):
        """sync takes the value from config."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG, sync=True))
        assert d.sync is True

    def test_inherits_ioos(self):
        """DownloaderIOOSGliders is a subclass of DownloaderIOOS."""
        assert issubclass(DownloaderIOOSGliders, DownloaderIOOS)

    def test_default_config(self, mock_base_downloader):
        """No config falls back to IOOS_GLIDERS/PHY."""
        with patch(
            "crocolaketools.downloader.downloaderIOOSGliders.DownloaderIOOS.__init__"
        ) as mock_super:
            mock_super.return_value = None
            DownloaderIOOSGliders()
            cfg = mock_super.call_args[0][0]
            assert cfg['db'] == 'IOOS_GLIDERS'
            assert cfg['db_type'] == 'PHY'


class TestFilterDatasets:
    """Tests for DownloaderIOOSGliders._filter_datasets"""

    DATASETS = [
        "bios_anna-20220101T0000-delayed",
        "bios_anna-20220101T0000",
        "ru29-20210601T1200-delayed",
    ]

    def test_delayed_only(self, mock_base_downloader):
        """Only the -delayed ids are kept when delayed_only is True."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG, delayed_only=True))
        assert d._filter_datasets(self.DATASETS) == [
            "bios_anna-20220101T0000-delayed",
            "ru29-20210601T1200-delayed",
        ]

    def test_keep_all(self, mock_base_downloader):
        """All ids are kept when delayed_only is False."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG, delayed_only=False))
        assert d._filter_datasets(self.DATASETS) == self.DATASETS


class TestLocalPath:
    """Tests for DownloaderIOOSGliders._local_path"""

    def test_path(self, tmp_path, mock_base_downloader):
        """Local path is input_path plus dataset id plus .parquet."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG))
        d.input_path = str(tmp_path) + "/"
        ds = "ru29-20210601T1200-delayed"
        assert d._local_path(ds) == os.path.join(d.input_path, ds + ".parquet")


class TestGetDatasetUrl:
    """Tests for DownloaderIOOSGliders.get_dataset_url"""

    def test_uses_glider_variables(self, mock_base_downloader):
        """The glider variable list is set on the erddapy client."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG))
        d.response_format = "parquet"
        d._erddap = MagicMock()
        d.get_dataset_url("ru29-delayed")
        assert d._erddap.variables == GLIDER_VARIABLES
        assert d._erddap._dataset_id == "ru29-delayed"

    def test_no_time_constraints(self, mock_base_downloader):
        """No constraints are passed when start and end are missing."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG))
        d.response_format = "parquet"
        d._erddap = MagicMock()
        d.get_dataset_url("ru29-delayed")
        assert d._erddap.get_download_url.call_args[1]["constraints"] == {}

    def test_time_constraints(self, mock_base_downloader):
        """Start and end are turned into time constraints."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG))
        d.response_format = "parquet"
        d._erddap = MagicMock()
        d.get_dataset_url(
            "ru29-delayed",
            time_start=datetime(2021, 6, 1, 12, 0, 0),
            time_end=datetime(2021, 6, 2, 12, 0, 0),
        )
        assert d._erddap.get_download_url.call_args[1]["constraints"] == {
            "time>=": "2021-06-01T12:00:00Z",
            "time<=": "2021-06-02T12:00:00Z",
        }


class TestBuildDownloadQueue:
    """Tests for DownloaderIOOSGliders._build_download_queue"""

    def test_overwrite_queues_all(self, tmp_path, mock_base_downloader):
        """overwrite queues every dataset."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG, sync=True))
        d.input_path = str(tmp_path) + "/"
        d.overwrite = True
        ids = ["a-delayed", "b-delayed"]
        to_dl, current, no_ts = d._build_download_queue(ids)
        assert to_dl == ids

    def test_skip_existing_no_sync(self, tmp_path, mock_base_downloader):
        """Existing files are skipped when sync is False."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG, sync=False))
        d.input_path = str(tmp_path) + "/"
        d.overwrite = False
        (tmp_path / "a-delayed.parquet").write_bytes(b"data")
        to_dl, current, no_ts = d._build_download_queue(["a-delayed", "b-delayed"])
        assert to_dl == ["b-delayed"]
        assert current == 1

    def test_sync_server_newer(self, tmp_path, mock_base_downloader):
        """sync queues a dataset when the server copy is newer."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG, sync=True))
        d.input_path = str(tmp_path) + "/"
        d.overwrite = False
        (tmp_path / "a-delayed.parquet").write_bytes(b"data")
        with patch.object(DownloaderIOOSGliders, "get_server_timestamp",
                          return_value=datetime(2030, 1, 1, tzinfo=timezone.utc)), \
             patch.object(DownloaderIOOSGliders, "_local_timestamp",
                          return_value=datetime(2020, 1, 1, tzinfo=timezone.utc)):
            to_dl, current, no_ts = d._build_download_queue(["a-delayed"])
        assert to_dl == ["a-delayed"]

    def test_sync_up_to_date(self, tmp_path, mock_base_downloader):
        """sync skips a dataset when the local copy is current."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG, sync=True))
        d.input_path = str(tmp_path) + "/"
        d.overwrite = False
        (tmp_path / "a-delayed.parquet").write_bytes(b"data")
        with patch.object(DownloaderIOOSGliders, "get_server_timestamp",
                          return_value=datetime(2020, 1, 1, tzinfo=timezone.utc)), \
             patch.object(DownloaderIOOSGliders, "_local_timestamp",
                          return_value=datetime(2030, 1, 1, tzinfo=timezone.utc)):
            to_dl, current, no_ts = d._build_download_queue(["a-delayed"])
        assert to_dl == []
        assert current == 1

    def test_sync_no_server_timestamp(self, tmp_path, mock_base_downloader):
        """sync skips a dataset with no server timestamp."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG, sync=True))
        d.input_path = str(tmp_path) + "/"
        d.overwrite = False
        (tmp_path / "a-delayed.parquet").write_bytes(b"data")
        with patch.object(DownloaderIOOSGliders, "get_server_timestamp",
                          return_value=None):
            to_dl, current, no_ts = d._build_download_queue(["a-delayed"])
        assert to_dl == []
        assert no_ts == 1


class TestDownload:
    """Tests for DownloaderIOOSGliders.download"""

    def test_no_datasets(self, tmp_path, mock_base_downloader):
        """download returns (0, 0) when there are no datasets."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG))
        d.input_path = str(tmp_path) + "/"
        d.dryrun = False
        with patch.object(DownloaderIOOSGliders, "list_dataset_ids", return_value=[]):
            assert d.download() == (0, 0)

    def test_dryrun(self, tmp_path, mock_base_downloader):
        """dryrun returns the number queued without downloading."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG))
        d.input_path = str(tmp_path) + "/"
        d.dryrun = True
        ids = ["a-delayed", "b-delayed"]
        with patch.object(DownloaderIOOSGliders, "list_dataset_ids", return_value=ids), \
             patch.object(DownloaderIOOSGliders, "_build_download_queue",
                          return_value=(ids, 0, 0)), \
             patch.object(DownloaderIOOSGliders, "_download_one") as mock_one:
            completed, failed = d.download()
        mock_one.assert_not_called()
        assert completed == 2

    def test_download_counts(self, tmp_path, mock_base_downloader):
        """Completed and failed counts come from _download_one."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG))
        d.input_path = str(tmp_path) + "/"
        d.dryrun = False
        ids = ["a-delayed", "b-delayed", "c-delayed"]

        def one(dataset_id, local_path):
            return dataset_id != "b-delayed"

        with patch.object(DownloaderIOOSGliders, "list_dataset_ids", return_value=ids), \
             patch.object(DownloaderIOOSGliders, "_build_download_queue",
                          return_value=(ids, 0, 0)), \
             patch.object(DownloaderIOOSGliders, "_download_one", side_effect=one):
            completed, failed = d.download()
        assert completed == 2
        assert failed == 1


##########################################################################
# Fixtures
##########################################################################

@pytest.fixture
def mock_base_downloader():
    """Patch the base Downloader.__init__ and configure_logging so tests don't
    need config.yaml or write a log file."""
    with patch(
        "crocolaketools.downloader.downloaderERDDAP.Downloader.__init__",
        return_value=None,
    ), patch(
        "crocolaketools.downloader.downloaderIOOSGliders.configure_logging",
    ):
        yield


##########################################################################

if __name__ == "__main__":
    pytest.main([__file__, "-v"])