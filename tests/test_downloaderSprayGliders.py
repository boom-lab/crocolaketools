#!/usr/bin/env python3

## @file test_downloaderSprayGliders.py
#
#
## @author mahi-anol
#
## @date Sat 21 Mar 2026

##########################################################################
import os
import warnings
from unittest.mock import MagicMock, patch

import pytest
import requests

from crocolaketools.downloader.downloaderSprayGliders import (
    SPRAY_BASE_URL,
    SPRAY_DATASETS_URL,
    SPRAY_FILES,
    DownloaderSprayGliders,
)
##########################################################################

DUMMY_CONFIG = {'db': 'SprayGliders', 'db_type': 'PHY'}
MODULE = "crocolaketools.downloader.downloaderSprayGliders"

# server Last modified times, epoch milliseconds
T_OLD = 1_700_000_000_000
T_NEW = 1_793_816_318_874

ALL_DATASETS_CSV = "datasetID,title\n,\n" + "".join(
    f"{d},title of {d}\n" for d in SPRAY_FILES
)


def listing_csv(*entries):
    """ERDDAP files/ listing; entries are (name, mtime_ms, size)."""
    rows = ["Name,Last modified,Size,Description"]
    for name, mtime, size in entries:
        rows.append(f"{name},{'' if mtime is None else mtime},{'' if size is None else size},")
    return "\n".join(rows) + "\n"


class FakeServer:
    """Stand-in for requests.get: serves ERDDAP listings from a dict."""

    def __init__(self, listings, datasets_csv=ALL_DATASETS_CSV):
        # listings: dataset_id -> CSV text, or an Exception to raise
        self.listings = listings
        self.datasets_csv = datasets_csv

    def get(self, url, **kwargs):
        if url == SPRAY_DATASETS_URL:
            body = self.datasets_csv
        else:
            assert url.startswith(SPRAY_BASE_URL + "/") and url.endswith("/.csv"), url
            dataset_id = url[len(SPRAY_BASE_URL) + 1:-len("/.csv")]
            body = self.listings.get(dataset_id)
            if body is None:
                body = requests.HTTPError(f"404 for {url}")
        if isinstance(body, Exception):
            raise body
        resp = MagicMock()
        resp.text = body
        resp.raise_for_status.return_value = None
        return resp


def fake_download(sizes):
    """Replacement for _download_file that writes sizes[url] bytes."""
    calls = []

    def _dl(url, local_path):
        calls.append((url, local_path))
        with open(local_path, "wb") as fh:
            fh.write(b"x" * sizes[url])

    return _dl, calls


def make_downloader(tmp_path, fnames, overwrite=False):
    d = DownloaderSprayGliders(fnames=fnames, overwrite=overwrite)
    d.input_path = tmp_path
    return d


def write_local(path, size, mtime_ms):
    path.write_bytes(b"x" * size)
    os.utime(path, ns=(mtime_ms * 1_000_000, mtime_ms * 1_000_000))


##########################################################################


class TestDownloaderSprayGlidersInit:
    """Tests for DownloaderSprayGliders.__init__"""

    def test_defaults(self, mock_base_downloader):
        d = DownloaderSprayGliders()
        assert d.fnames == SPRAY_FILES
        assert d.base_url == SPRAY_BASE_URL
        assert d.overwrite is False
        assert d.failed == {}

    def test_custom_args(self, mock_base_downloader):
        custom_files = {"binnedCORC": "CORC.nc"}
        d = DownloaderSprayGliders(
            config=DUMMY_CONFIG,
            fnames=custom_files,
            base_url="https://example.com/spray",
            overwrite=True,
        )
        assert d.fnames == custom_files
        assert d.base_url == "https://example.com/spray"
        assert d.overwrite is True

    def test_inherits_downloader(self):
        from crocolaketools.downloader.downloader import Downloader
        assert issubclass(DownloaderSprayGliders, Downloader)

    def test_default_config_is_spraygliders_phy(self, mock_base_downloader):
        with patch(f"{MODULE}.Downloader.__init__") as mock_super:
            mock_super.return_value = None
            DownloaderSprayGliders()
            called_config = mock_super.call_args[0][0]
            assert called_config['db'] == 'SprayGliders'
            assert called_config['db_type'] == 'PHY'


class TestSprayFiles:
    """SPRAY_FILES keeps the local filenames ConverterSprayGliders uses."""

    def test_keyed_by_dataset_id(self):
        assert all(k.startswith("binned") for k in SPRAY_FILES)

    def test_local_names(self):
        assert SPRAY_FILES["binnedFLEAT"] == "FLEAT.nc"
        assert SPRAY_FILES["binnedCalypso"] == "Calypso.nc"
        assert SPRAY_FILES["binnedOKMC"] == "OKMC.nc"
        assert SPRAY_FILES["binnedMabSwot"] == "mab_swot.nc"
        assert len(SPRAY_FILES) == 19
        assert len(set(SPRAY_FILES.values())) == 19


class TestListing:
    """Tests for list_remote_files, get_url and the .nc selection."""

    def test_renamed_file_resolved(self, tmp_path, mock_base_downloader):
        server = FakeServer({
            "binnedFLEAT": listing_csv(("fleat_v1.1.nc", T_NEW, 10)),
        })
        d = make_downloader(tmp_path, {"binnedFLEAT": "FLEAT.nc"})
        with patch(f"{MODULE}.requests.get", side_effect=server.get):
            assert d.get_url("binnedFLEAT") == f"{SPRAY_BASE_URL}/binnedFLEAT/fleat_v1.1.nc"

    def test_subdirectories_ignored(self, tmp_path, mock_base_downloader):
        server = FakeServer({
            "binnedCORC": listing_csv(("old/", None, None), ("CORC.nc", T_NEW, 10)),
        })
        d = make_downloader(tmp_path, {"binnedCORC": "CORC.nc"})
        with patch(f"{MODULE}.requests.get", side_effect=server.get):
            assert d.get_url("binnedCORC") == f"{SPRAY_BASE_URL}/binnedCORC/CORC.nc"

    def test_zero_nc_raises(self, tmp_path, mock_base_downloader):
        server = FakeServer({
            "binnedCORC": listing_csv(("old/", None, None), ("README.txt", T_NEW, 5)),
        })
        d = make_downloader(tmp_path, {"binnedCORC": "CORC.nc"})
        with patch(f"{MODULE}.requests.get", side_effect=server.get):
            with pytest.raises(RuntimeError, match="expected one .nc file"):
                d.get_url("binnedCORC")

    def test_several_nc_raises(self, tmp_path, mock_base_downloader):
        server = FakeServer({
            "binnedCORC": listing_csv(("CORC.nc", T_NEW, 10), ("corc_v2.nc", T_NEW, 12)),
        })
        d = make_downloader(tmp_path, {"binnedCORC": "CORC.nc"})
        with patch(f"{MODULE}.requests.get", side_effect=server.get):
            with pytest.raises(RuntimeError, match="found 2"):
                d.get_url("binnedCORC")


class TestSprayDownload:
    """Tests for DownloaderSprayGliders.spray_download."""

    def run(self, d, server, sizes):
        dl, calls = fake_download(sizes)
        with patch(f"{MODULE}.requests.get", side_effect=server.get), \
                patch.object(DownloaderSprayGliders, "_download_file", side_effect=dl):
            result = d.spray_download()
        return result, calls

    def test_renamed_file_saved_under_local_name(self, tmp_path, mock_base_downloader):
        url = f"{SPRAY_BASE_URL}/binnedFLEAT/fleat_v1.1.nc"
        server = FakeServer({"binnedFLEAT": listing_csv(("fleat_v1.1.nc", T_NEW, 10))})
        d = make_downloader(tmp_path, {"binnedFLEAT": "FLEAT.nc"})

        result, calls = self.run(d, server, {url: 10})

        local = tmp_path / "FLEAT.nc"
        assert calls == [(url, local)]
        assert result == [local]
        assert d.failed == {}
        assert local.stat().st_size == 10
        assert local.stat().st_mtime_ns == T_NEW * 1_000_000

    def test_zero_nc_marks_failed(self, tmp_path, mock_base_downloader):
        server = FakeServer({"binnedCORC": listing_csv(("old/", None, None))})
        d = make_downloader(tmp_path, {"binnedCORC": "CORC.nc"})
        result, calls = self.run(d, server, {})
        assert calls == []
        assert result == []
        assert "binnedCORC" in d.failed

    def test_several_nc_marks_failed(self, tmp_path, mock_base_downloader):
        server = FakeServer({
            "binnedCORC": listing_csv(("CORC.nc", T_NEW, 10), ("corc_v2.nc", T_NEW, 12)),
        })
        d = make_downloader(tmp_path, {"binnedCORC": "CORC.nc"})
        result, calls = self.run(d, server, {})
        assert calls == []
        assert "binnedCORC" in d.failed

    def test_unchanged_file_skipped(self, tmp_path, capsys, mock_base_downloader):
        server = FakeServer({"binnedCORC": listing_csv(("CORC.nc", T_NEW, 10))})
        d = make_downloader(tmp_path, {"binnedCORC": "CORC.nc"})
        local = tmp_path / "CORC.nc"
        write_local(local, 10, T_NEW)

        result, calls = self.run(d, server, {})

        assert calls == []
        assert result == [local]
        assert d.failed == {}
        assert "unchanged" in capsys.readouterr().out

    def test_changed_size_redownloaded(self, tmp_path, mock_base_downloader):
        url = f"{SPRAY_BASE_URL}/binnedCORC/CORC.nc"
        server = FakeServer({"binnedCORC": listing_csv(("CORC.nc", T_NEW, 12))})
        d = make_downloader(tmp_path, {"binnedCORC": "CORC.nc"})
        local = tmp_path / "CORC.nc"
        write_local(local, 10, T_NEW)

        result, calls = self.run(d, server, {url: 12})

        assert calls == [(url, local)]
        assert local.stat().st_size == 12
        assert d.failed == {}

    def test_older_mtime_redownloaded(self, tmp_path, mock_base_downloader):
        url = f"{SPRAY_BASE_URL}/binnedCORC/CORC.nc"
        server = FakeServer({"binnedCORC": listing_csv(("CORC.nc", T_NEW, 10))})
        d = make_downloader(tmp_path, {"binnedCORC": "CORC.nc"})
        local = tmp_path / "CORC.nc"
        write_local(local, 10, T_OLD)

        result, calls = self.run(d, server, {url: 10})

        assert calls == [(url, local)]
        assert local.stat().st_mtime_ns == T_NEW * 1_000_000

    def test_newer_local_mtime_skipped(self, tmp_path, mock_base_downloader):
        server = FakeServer({"binnedCORC": listing_csv(("CORC.nc", T_OLD, 10))})
        d = make_downloader(tmp_path, {"binnedCORC": "CORC.nc"})
        write_local(tmp_path / "CORC.nc", 10, T_NEW)
        _, calls = self.run(d, server, {})
        assert calls == []

    def test_overwrite_forces_redownload(self, tmp_path, mock_base_downloader):
        url = f"{SPRAY_BASE_URL}/binnedCORC/CORC.nc"
        server = FakeServer({"binnedCORC": listing_csv(("CORC.nc", T_NEW, 10))})
        d = make_downloader(tmp_path, {"binnedCORC": "CORC.nc"}, overwrite=True)
        local = tmp_path / "CORC.nc"
        write_local(local, 10, T_NEW)

        _, calls = self.run(d, server, {url: 10})

        assert calls == [(url, local)]

    def test_size_mismatch_after_download_marks_failed(self, tmp_path, mock_base_downloader):
        url = f"{SPRAY_BASE_URL}/binnedCORC/CORC.nc"
        server = FakeServer({"binnedCORC": listing_csv(("CORC.nc", T_NEW, 10))})
        d = make_downloader(tmp_path, {"binnedCORC": "CORC.nc"})
        result, _ = self.run(d, server, {url: 7})
        assert result == []
        assert "binnedCORC" in d.failed

    def test_one_failure_does_not_stop_others(self, tmp_path, capsys, mock_base_downloader):
        url_gs = f"{SPRAY_BASE_URL}/binnedGS/GulfStream.nc"
        url_fl = f"{SPRAY_BASE_URL}/binnedFLEAT/fleat_v1.1.nc"
        server = FakeServer({
            "binnedCORC": requests.ConnectionError("listing down"),
            "binnedGS": listing_csv(("GulfStream.nc", T_NEW, 10)),
            "binnedFLEAT": listing_csv(("fleat_v1.1.nc", T_NEW, 10)),
        })
        fnames = {"binnedCORC": "CORC.nc", "binnedGS": "GulfStream.nc", "binnedFLEAT": "FLEAT.nc"}
        d = make_downloader(tmp_path, fnames)

        def dl(url, local_path):
            if url == url_gs:
                raise requests.HTTPError("500")
            local_path.write_bytes(b"x" * 10)

        with patch(f"{MODULE}.requests.get", side_effect=server.get), \
                patch.object(DownloaderSprayGliders, "_download_file", side_effect=dl):
            result = d.spray_download()

        assert result == [tmp_path / "FLEAT.nc"]
        assert set(d.failed) == {"binnedCORC", "binnedGS"}
        out = capsys.readouterr().out
        assert "2 Spray dataset(s) failed" in out

    def test_new_dataset_warning(self, tmp_path, mock_base_downloader):
        datasets_csv = ALL_DATASETS_CSV + "binnedNewThing,A new dataset\n"
        server = FakeServer(
            {"binnedCORC": listing_csv(("CORC.nc", T_NEW, 10))},
            datasets_csv=datasets_csv,
        )
        d = make_downloader(tmp_path, {"binnedCORC": "CORC.nc"})
        write_local(tmp_path / "CORC.nc", 10, T_NEW)

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            _, calls = self.run(d, server, {})

        msgs = [str(w.message) for w in caught]
        assert any("binnedNewThing" in m for m in msgs)
        assert calls == []
        assert d.failed == {}

    def test_no_warning_when_datasets_match(self, tmp_path, mock_base_downloader):
        server = FakeServer({"binnedCORC": listing_csv(("CORC.nc", T_NEW, 10))})
        d = make_downloader(tmp_path, {"binnedCORC": "CORC.nc"})
        write_local(tmp_path / "CORC.nc", 10, T_NEW)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.run(d, server, {})
        assert not any("not in SPRAY_FILES" in str(w.message) for w in caught)

    def test_empty_first_row_skipped(self, tmp_path, mock_base_downloader):
        d = make_downloader(tmp_path, {})
        with patch(f"{MODULE}.requests.get", side_effect=FakeServer({}).get):
            ids = d.list_binned_datasets()
        assert ids == list(SPRAY_FILES)


class TestCli:
    """Tests for scripts/download_spraygliders.py."""

    def test_select_files_by_name_or_id(self):
        from scripts.download_spraygliders import select_files
        selected, unknown = select_files(["FLEAT.nc", "binnedMabSwot", "nope.nc"])
        assert selected == {"binnedFLEAT": "FLEAT.nc", "binnedMabSwot": "mab_swot.nc"}
        assert unknown == ["nope.nc"]

    def test_exit_nonzero_on_failure(self, tmp_path, mock_base_downloader):
        from scripts import download_spraygliders as cli
        server = FakeServer({"binnedCORC": listing_csv(("old/", None, None))})

        def init(self, *args, **kwargs):
            self.input_path = tmp_path

        with patch(f"{MODULE}.Downloader.__init__", init), \
                patch(f"{MODULE}.requests.get", side_effect=server.get), \
                patch("sys.argv", ["download_spraygliders", "--files", "CORC.nc"]):
            with pytest.raises(SystemExit) as exc:
                cli.main()
        assert exc.value.code == 1

    def test_exit_zero_on_success(self, tmp_path, mock_base_downloader):
        from scripts import download_spraygliders as cli
        server = FakeServer({"binnedCORC": listing_csv(("CORC.nc", T_NEW, 10))})
        write_local(tmp_path / "CORC.nc", 10, T_NEW)

        def init(self, *args, **kwargs):
            self.input_path = tmp_path

        with patch(f"{MODULE}.Downloader.__init__", init), \
                patch(f"{MODULE}.requests.get", side_effect=server.get), \
                patch("sys.argv", ["download_spraygliders", "--files", "binnedCORC"]):
            cli.main()  # no SystemExit


##########################################################################
# Fixtures
##########################################################################

@pytest.fixture
def mock_base_downloader():
    """Patch Downloader.__init__ so tests don't need datasets.yaml or
    crocolakeloader.params to be installed."""
    with patch(f"{MODULE}.Downloader.__init__", return_value=None):
        yield


##########################################################################

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
