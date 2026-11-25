#!/usr/bin/env python3

## @file test_converterSprayGliders.py
#
# Tests for ConverterSprayGliders.prepare_data on a synthetic file with an
# all-NaT chunk, and for the tmp_path cleanup in scripts/spray2parquet.py.
#
##########################################################################
import threading
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
import xarray as xr

from crocolaketools.converter.converterSprayGliders import ConverterSprayGliders
import scripts.spray2parquet as s2p
##########################################################################

TIME_ATTRS = {
    "calendar": "gregorian",
    "standard_name": "time",
    "units": "seconds since 1970-01-01T00:00:00Z",
}
N_PROFILE = 6
CHUNK_PROFILE = 2
# profiles 2 and 3 form chunk 1 and have no time
NAT_PROFILES = [2, 3]


def write_synthetic_spray(path):
    """Write a small Spray-like Level-3 file. With chunk_profile=2, chunk 1
    (profiles 2-3) has time and time_uv all missing."""
    t0 = 1.6e9
    time = t0 + 3600.0 * np.arange(N_PROFILE)
    time_uv = time - 1800.0
    time[NAT_PROFILES] = np.nan
    time_uv[NAT_PROFILES] = np.nan

    rng = np.random.default_rng(0)
    ds = xr.Dataset(
        data_vars={
            "temp": (("depth", "profile"), rng.normal(15, 1, (2, N_PROFILE))),
            "sal": (("depth", "profile"), rng.normal(35, 0.1, (2, N_PROFILE))),
            "mission": ("profile", np.array([0, 0, 0, 1, 1, 1], dtype="int32")),
            "mission_name": ("trajectory", np.array([b"m0", b"m1"], dtype="S8")),
        },
        coords={
            "depth": ("depth", [10.0, 20.0]),
            "profile": ("profile", np.arange(N_PROFILE, dtype="int32")),
            "lat": ("profile", np.linspace(25, 26, N_PROFILE)),
            "lon": ("profile", np.linspace(-90, -89, N_PROFILE)),
            "time": ("profile", time, TIME_ATTRS),
            "time_uv": ("profile", time_uv, TIME_ATTRS),
        },
        attrs={"title": "synthetic Spray", "date_created": "2026-01-01T00:00:00Z"},
    )
    encoding = {v: {"_FillValue": np.nan} for v in ("time", "time_uv", "lat", "lon", "temp", "sal")}
    ds.to_netcdf(path, engine="netcdf4", encoding=encoding)


def make_converter(input_dir, tmp_dir):
    """Real ConverterSprayGliders (tests/config/datasets.yaml) pointed at
    input_dir and tmp_dir."""
    converter = ConverterSprayGliders(db_type="phy")
    converter.input_path = input_dir
    converter.tmp_path = tmp_dir
    return converter


@pytest.fixture
def spray_dirs(tmp_path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    write_synthetic_spray(input_dir / "Synth.nc")
    return input_dir, tmp_path / "tmp_spray"


##########################################################################


class TestPrepareDataAllNaT:

    def test_source_has_all_nat_chunk(self, spray_dirs):
        input_dir, _ = spray_dirs
        with xr.open_dataset(input_dir / "Synth.nc") as ds:
            chunk = ds.isel(profile=slice(2, 4))
            assert chunk["time"].isnull().all()
            assert chunk["time_uv"].isnull().all()
            assert np.issubdtype(chunk["time"].dtype, np.datetime64)

    def test_prepare_data_writes_all_nat_chunk(self, spray_dirs):
        input_dir, tmp_dir = spray_dirs
        converter = make_converter(input_dir, tmp_dir)

        converter.prepare_data(
            flist=["Synth.nc"], lock=threading.Lock(), chunk_profile=CHUNK_PROFILE
        )

        chunk_files = sorted(p.name for p in tmp_dir.iterdir())
        assert chunk_files == [f"Synth_chunk_{j}.nc" for j in range(3)]

        # read back as read_to_df does (default decoding) and compare with
        # the source opened normally
        with xr.open_dataset(input_dir / "Synth.nc", engine="h5netcdf") as src:
            for j in range(3):
                expected = src.isel(profile=slice(CHUNK_PROFILE * j, CHUNK_PROFILE * (j + 1))).load()
                with xr.open_dataset(tmp_dir / f"Synth_chunk_{j}.nc", engine="h5netcdf") as got:
                    xr.testing.assert_identical(got.load(), expected)
                    for v in ("time", "time_uv"):
                        assert np.issubdtype(got[v].dtype, np.datetime64)
                        assert got[v].encoding["units"] == TIME_ATTRS["units"]


##########################################################################


class TestConvertDbTypeCleanup:
    """scripts/spray2parquet.py _convert_db_type removes the tmp_path it
    created when a step fails."""

    def run(self, converter):
        with patch.object(s2p, "ConverterSprayGliders", return_value=converter), \
                patch.object(s2p, "Client"), \
                patch.object(s2p, "Lock", side_effect=threading.Lock):
            return s2p._convert_db_type(
                "PHY", MagicMock(), None, None, None, use_config_file=True
            )

    def test_convert_failure_removes_tmp_path(self, spray_dirs):
        input_dir, tmp_dir = spray_dirs
        converter = make_converter(input_dir, tmp_dir)
        converter.chunk_profile = CHUNK_PROFILE
        converter.convert = MagicMock(side_effect=RuntimeError("convert broke"))

        with pytest.raises(RuntimeError, match="convert broke"):
            self.run(converter)

        assert not tmp_dir.exists()
        assert input_dir.is_dir()

    def test_prepare_data_failure_removes_tmp_path(self, spray_dirs):
        input_dir, tmp_dir = spray_dirs
        converter = make_converter(input_dir, tmp_dir)
        converter.store_chunks = MagicMock(side_effect=OSError("disk full"))

        with pytest.raises(OSError, match="disk full"):
            self.run(converter)

        assert not tmp_dir.exists()

    def test_existing_tmp_path_kept(self, spray_dirs):
        input_dir, tmp_dir = spray_dirs
        tmp_dir.mkdir()
        other_run_file = tmp_dir / "CORC_chunk_0.nc"
        other_run_file.write_bytes(b"another run")
        converter = make_converter(input_dir, tmp_dir)
        converter.cleanup = MagicMock()

        with pytest.raises(ValueError, match="already exists"):
            self.run(converter)

        converter.cleanup.assert_not_called()
        assert other_run_file.read_bytes() == b"another run"

    def test_success_still_cleans_up(self, spray_dirs):
        input_dir, tmp_dir = spray_dirs
        converter = make_converter(input_dir, tmp_dir)
        converter.chunk_profile = CHUNK_PROFILE
        converter.convert = MagicMock()

        self.run(converter)

        converter.convert.assert_called_once()
        assert not tmp_dir.exists()


class TestCleanup:

    def test_cleanup_keeps_empty_parent(self, spray_dirs, tmp_path):
        """cleanup() removes tmp_path only, not a parent left empty."""
        input_dir, _ = spray_dirs
        parent = tmp_path / "only_tmp_here"
        tmp_dir = parent / "tmp_spray"
        converter = make_converter(input_dir, tmp_dir)
        converter.prepare_data(
            flist=["Synth.nc"], lock=threading.Lock(), chunk_profile=CHUNK_PROFILE
        )

        converter.cleanup()

        assert not tmp_dir.exists()
        assert parent.is_dir()

    def test_cleanup_error_keeps_original_exception(self, spray_dirs, capsys):
        """If cleanup() fails after a failed step, the step's exception is
        raised and the cleanup error is printed as a warning."""
        input_dir, tmp_dir = spray_dirs
        converter = make_converter(input_dir, tmp_dir)
        converter.chunk_profile = CHUNK_PROFILE
        converter.convert = MagicMock(side_effect=RuntimeError("convert broke"))
        converter.cleanup = MagicMock(side_effect=OSError("cannot remove"))

        with pytest.raises(RuntimeError, match="convert broke") as exc:
            TestConvertDbTypeCleanup().run(converter)

        assert exc.value.__context__ is None
        converter.cleanup.assert_called_once()
        out = capsys.readouterr().out
        assert "could not remove temporary path" in out
        assert "cannot remove" in out


##########################################################################

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
