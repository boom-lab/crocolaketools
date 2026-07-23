#!/usr/bin/env python3

## @file test_downloaderIOOSGliders.py
#
#
## @author Mahi Sarwar Anol <anol.mahi@gmail.com>
#
## @date Thu 30 Jun 2026

##########################################################################
import os
import threading
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from crocolaketools.downloader.downloaderIOOSGliders import (
    GLIDER_VARIABLES,
    IOOS_GLIDERS_SERVER_URL,
    DownloaderIOOSGliders,
)
from crocolaketools.downloader.downloaderERDDAP import DownloaderERDDAP
##########################################################################

DUMMY_CONFIG = {'db': 'IOOS_GLIDERS', 'db_type': 'PHY'}


class TestDownloaderIOOSGlidersInit:
    """Tests for DownloaderIOOSGliders.__init__"""

    def test_requires_no_config_uses_defaults(self, mock_base_downloader):
        """No config falls back to IOOS_GLIDERS/PHY."""
        with patch(
            "crocolaketools.downloader.downloaderIOOSGliders.DownloaderERDDAP.__init__"
        ) as mock_super:
            mock_super.return_value = None
            DownloaderIOOSGliders()
            cfg = mock_super.call_args[0][0]
            assert cfg['db'] == 'IOOS_GLIDERS'
            assert cfg['db_type'] == 'PHY'

    def test_sync_defaults_false(self, mock_base_downloader):
        """sync defaults to False."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG))
        assert d.sync is False

    def test_delayed_only_defaults_true(self, mock_base_downloader):
        """delayed_only defaults to True."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG))
        assert d.delayed_only is True

    def test_sync_from_config(self, mock_base_downloader):
        """sync takes the value from config."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG, sync=True))
        assert d.sync is True

    def test_delayed_only_from_config(self, mock_base_downloader):
        """delayed_only takes the value from config."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG, delayed_only=False))
        assert d.delayed_only is False

    def test_inherits_erddap(self):
        """DownloaderIOOSGliders is a subclass of DownloaderERDDAP."""
        assert issubclass(DownloaderIOOSGliders, DownloaderERDDAP)

    def test_server_url(self, mock_base_downloader):
        """CLASS-level SERVER_URL points at the IOOS Glider DAC."""
        assert DownloaderIOOSGliders.SERVER_URL == IOOS_GLIDERS_SERVER_URL

    def test_gated_parallel_download_defaults_true(self, mock_base_downloader):
        """gated_parallel_download defaults to True (first-byte gate active)."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG))
        assert d.gated_parallel_download is True

    def test_gated_parallel_download_from_config(self, mock_base_downloader):
        """gated_parallel_download takes the value from config."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG, gated_parallel_download=False))
        assert d.gated_parallel_download is False


class TestFilterDatasets:
    """Tests for DownloaderIOOSGliders._filter_datasets"""

    DATASETS = [
        "bios_anna-20220101T0000-delayed",
        "bios_anna-20220101T0000",
        "ru29-20210601T1200-delayed",
    ]

    def test_delayed_only(self, mock_base_downloader):
        """Only -delayed ids are kept when delayed_only is True."""
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
        """Local path is input_path / dataset_id.parquet."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG))
        d.input_path = str(tmp_path) + "/"
        ds = "ru29-20210601T1200-delayed"
        assert d._local_path(ds) == os.path.join(d.input_path, ds + ".parquet")


class TestGetDatasetUrl:
    """Tests for DownloaderIOOSGliders.get_dataset_url"""

    def test_filters_to_available_variables(self, mock_base_downloader):
        """Only variables present in the dataset are requested."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG))
        d.response_format = "parquet"
        d._erddap = MagicMock()

        # simulate a PHY-only dataset
        available = {"latitude", "longitude", "pressure", "time",
                     "temperature", "salinity", "trajectory", "profile_id"}
        with patch.object(DownloaderIOOSGliders, "get_dataset_variables",
                          return_value=available):
            d.get_dataset_url("ru29-delayed")

        requested = d._erddap.variables
        assert all(v in available for v in requested)
        # BGC variables not in this dataset must be absent
        assert "dissolved_oxygen" not in requested
        assert "chlorophyll_a" not in requested

    def test_fallback_when_info_fails(self, mock_base_downloader):
        """Falls back to full GLIDER_VARIABLES when info endpoint returns empty set."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG))
        d.response_format = "parquet"
        d._erddap = MagicMock()

        with patch.object(DownloaderIOOSGliders, "get_dataset_variables",
                          return_value=set()):
            d.get_dataset_url("ru29-delayed")

        assert d._erddap.variables == GLIDER_VARIABLES

    def test_no_time_constraints(self, mock_base_downloader):
        """No constraints are passed when start and end are missing."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG))
        d.response_format = "parquet"
        d._erddap = MagicMock()
        with patch.object(DownloaderIOOSGliders, "get_dataset_variables",
                          return_value=set()):
            d.get_dataset_url("ru29-delayed")
        assert d._erddap.get_download_url.call_args[1]["constraints"] == {}

    def test_time_constraints(self, mock_base_downloader):
        """Start and end are turned into time constraints."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG))
        d.response_format = "parquet"
        d._erddap = MagicMock()
        with patch.object(DownloaderIOOSGliders, "get_dataset_variables",
                          return_value=set()):
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
    """Tests for _build_download_queue (now in DownloaderERDDAP, exercised here)."""

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


class TestGatedParallelDownload:
    """Tests that gated_parallel_download controls which gate class is used."""

    def test_gate_class_selected_by_flag(self, mock_base_downloader, tmp_path):
        """_download_chunks_parallel constructs FirstByteGate when the flag is
        True and NoOpGate when False, for the same chunk job."""
        from crocolaketools.downloader import downloaderERDDAP as mod

        for flag, expected_name in ((True, "FirstByteGate"), (False, "NoOpGate")):
            d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG, gated_parallel_download=flag))
            d.num_threads = 1
            d.input_path = str(tmp_path) + "/"
            local_path = str(tmp_path / "a-delayed.parquet")

            calls = []
            real_first_byte_gate = mod.FirstByteGate
            real_noop_gate = mod.NoOpGate

            def _track_first_byte(*a, **kw):
                calls.append("FirstByteGate")
                return real_first_byte_gate(*a, **kw)

            def _track_noop(*a, **kw):
                calls.append("NoOpGate")
                return real_noop_gate(*a, **kw)

            with patch.object(d, "_safe_get_url", return_value="http://example.com/data"), \
                 patch.object(d, "_download_file_with_retry"), \
                 patch.object(d, "_merge_chunks", return_value=True), \
                 patch.object(mod, "FirstByteGate", side_effect=_track_first_byte), \
                 patch.object(mod, "NoOpGate", side_effect=_track_noop):
                d._download_chunks_parallel(
                    "a-delayed", [(0, 1)], str(tmp_path / "chunks"), local_path
                )

            assert calls == [expected_name], (
                f"gated_parallel_download={flag} should construct {expected_name}, got {calls}"
            )

    def test_noop_gate_never_blocks(self):
        """NoOpGate.wait() returns immediately and make_callback() is harmless to call."""
        from crocolaketools.downloader.downloaderERDDAP import NoOpGate

        gate = NoOpGate()
        gate.wait()  # must not raise or block
        cb = gate.make_callback()
        cb()  # must not raise
        cb()  # calling twice must also not raise (no "already released" semantics)

    def test_first_byte_gate_blocks_until_released(self):
        """FirstByteGate.wait() blocks until the callback fires (sanity check
        that the two gate classes have matching but distinct behavior)."""
        from crocolaketools.downloader.downloaderERDDAP import FirstByteGate

        gate = FirstByteGate()
        cb = gate.make_callback()
        released = threading.Event()

        def waiter():
            gate.wait()
            released.set()

        t = threading.Thread(target=waiter)
        t.start()
        t.join(timeout=0.2)
        assert not released.is_set(), "gate.wait() should still be blocking"

        cb()
        t.join(timeout=1)
        assert released.is_set(), "gate.wait() should unblock after callback fires"




class TestConstraints:
    """Tests for config.yaml constraint support in DownloaderERDDAP."""

    def test_no_constraints_by_default(self, mock_base_downloader):
        """time_start, time_end, extra_constraints are all None/empty by default."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG))
        assert d.time_start is None
        assert d.time_end is None
        assert d.extra_constraints == {}

    def test_time_constraints_read_from_config(self, mock_base_downloader):
        """time>= and time<= are parsed out of the constraints block."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG, constraints={
            "time>=": "2015-01-01T00:00:00Z",
            "time<=": "2026-01-01T00:00:00Z",
        }))
        assert d.time_start == "2015-01-01T00:00:00Z"
        assert d.time_end   == "2026-01-01T00:00:00Z"
        assert d.extra_constraints == {}

    def test_spatial_constraints_in_extra(self, mock_base_downloader):
        """Spatial constraints land in extra_constraints."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG, constraints={
            "latitude>=":  -60.0,
            "latitude<=":   60.0,
            "longitude>=": -180.0,
            "longitude<=":  180.0,
        }))
        assert d.extra_constraints == {
            "latitude>=":  -60.0,
            "latitude<=":   60.0,
            "longitude>=": -180.0,
            "longitude<=":  180.0,
        }

    def test_mixed_constraints_split_correctly(self, mock_base_downloader):
        """time>=/time<= go to time_start/time_end; the rest go to extra_constraints."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG, constraints={
            "time>=":      "2020-01-01T00:00:00Z",
            "latitude>=":  30.0,
            "longitude<=": 10.0,
        }))
        assert d.time_start == "2020-01-01T00:00:00Z"
        assert d.time_end is None
        assert d.extra_constraints == {"latitude>=": 30.0, "longitude<=": 10.0}

    def test_extra_constraints_merged_into_url(self, mock_base_downloader):
        """extra_constraints are merged into the URL constraints dict."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG, constraints={
            "latitude>=": -60.0,
            "latitude<=":  60.0,
        }))
        d.response_format = "parquet"
        d._erddap = MagicMock()
        with patch.object(DownloaderIOOSGliders, "get_dataset_variables",
                          return_value=set()):
            d.get_dataset_url("ru29-delayed")

        called_constraints = d._erddap.get_download_url.call_args[1]["constraints"]
        assert called_constraints["latitude>="] == -60.0
        assert called_constraints["latitude<="] ==  60.0

    def test_dataset_outside_time_window_skipped(self, tmp_path, mock_base_downloader):
        """_download_one returns True (skip, not failure) when dataset is outside window."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG, constraints={
            "time>=": "2025-01-01T00:00:00Z",
            "time<=": "2026-01-01T00:00:00Z",
        }))
        d.input_path = str(tmp_path) + "/"

        # dataset range is entirely before the requested window
        with patch.object(DownloaderIOOSGliders, "_get_dataset_time_range",
                          return_value=(
                              datetime(2010, 1, 1, tzinfo=timezone.utc),
                              datetime(2012, 1, 1, tzinfo=timezone.utc),
                          )), \
             patch.object(DownloaderIOOSGliders, "_download_chunks_parallel") as mock_dl:
            result = d._download_one("old-dataset-delayed", str(tmp_path / "out.parquet"))

        assert result is True          # skipped, not failed
        mock_dl.assert_not_called()    # nothing was downloaded

    def test_dataset_partially_in_window_clamped(self, tmp_path, mock_base_downloader):
        """_download_one clamps t_start/t_end to the requested window."""
        d = DownloaderIOOSGliders(config=dict(DUMMY_CONFIG, constraints={
            "time>=": "2021-06-01T00:00:00Z",
        }))
        d.input_path = str(tmp_path) + "/"

        captured = {}

        def fake_chunks(dataset_id, windows, tmp_dir, local_path):
            captured["windows"] = windows
            return True, False

        with patch.object(DownloaderIOOSGliders, "_get_dataset_time_range",
                          return_value=(
                              datetime(2021, 1, 1, tzinfo=timezone.utc),
                              datetime(2021, 12, 31, tzinfo=timezone.utc),
                          )), \
             patch.object(DownloaderIOOSGliders, "_download_chunks_parallel",
                          side_effect=fake_chunks):
            d._download_one("partial-delayed", str(tmp_path / "out.parquet"))

        # first window must start at the clamped time_start, not the dataset start
        assert captured["windows"][0][0] == datetime(2021, 6, 1, tzinfo=timezone.utc)


##########################################################################
# Fixtures
##########################################################################

@pytest.fixture
def mock_base_downloader():
    """Patch Downloader.__init__ and configure_logging so tests don't
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