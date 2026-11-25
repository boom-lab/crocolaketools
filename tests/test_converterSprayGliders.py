#!/usr/bin/env python3

## @file test_converterSprayGliders.py
#
# Tests for ConverterSprayGliders.prepare_data on a synthetic file with an
# all-NaT chunk.
#
##########################################################################
import threading

import numpy as np
import pytest
import xarray as xr

from crocolaketools.converter.converterSprayGliders import ConverterSprayGliders
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

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
