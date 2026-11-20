#!/usr/bin/env python3

## @file test_parse_yaml_argo_gdac.py
#
# Tests for crocolaketools/utils/parse_yaml_argo_gdac.sh, which the Argo GDAC
# slurm jobs call to get the mirror and parquet paths from datasets.yaml.
#
## @date Fri 20 Nov 2026

##########################################################################
import os
import subprocess
from pathlib import Path

import pytest

import crocolaketools

SCRIPT = Path(crocolaketools.__file__).parent / "utils" / "parse_yaml_argo_gdac.sh"
TEST_CONFIG_DIR = Path(__file__).parent / "config"
##########################################################################


def _write_config_dir(path, phy_fields):
    """Write a minimal config directory with an ARGO-GDAC_PHY entry."""
    path.mkdir(parents=True, exist_ok=True)
    lines = ["ARGO-GDAC_PHY:", "  db: ARGO-GDAC", "  db_type: PHY"]
    lines += [f"  {k}: {v}" for k, v in phy_fields.items()]
    (path / "datasets.yaml").write_text("\n".join(lines) + "\n")
    (path / "dask_cluster.yaml").write_text(
        "ARGO-GDAC_PHY:\n  n_workers: 1\n  threads_per_worker: 1\n"
    )
    return path


def _run(config_dir, *variants):
    env = dict(os.environ, CROCOLAKE_CONFIG_DIR=str(config_dir))
    return subprocess.run(
        ["bash", str(SCRIPT), *variants],
        env=env, capture_output=True, text=True,
    )


def _parse(stdout):
    """'PHY|in|out' lines -> {variant: (in, out)}"""
    rows = [line.split("|") for line in stdout.strip().splitlines()]
    return {var: (Path(in_path), Path(out_path)) for var, in_path, out_path in rows}


class TestParseYamlArgoGdac:

    def test_absolute_paths_are_used_as_they_are(self, tmp_path):
        """Absolute paths must not be prefixed with the config directory."""
        site = _write_config_dir(tmp_path / "site", {
            "input_path": "/srv/mirror/GDAC/dac/",
            "outdir_pq": "/srv/db/1011_PHY_ARGO-CLOUD/current/",
        })
        result = _run(site, "PHY")
        assert result.returncode == 0, result.stderr
        assert _parse(result.stdout) == {
            "PHY": (Path("/srv/mirror/GDAC/dac"),
                    Path("/srv/db/1011_PHY_ARGO-CLOUD/current")),
        }

    def test_relative_paths_resolve_against_the_config_dir(self, tmp_path):
        site = _write_config_dir(tmp_path / "site", {
            "input_path": "../mirror/GDAC/dac/",
            "outdir_pq": "out/1011_PHY",
        })
        result = _run(site, "PHY")
        assert result.returncode == 0, result.stderr
        assert _parse(result.stdout) == {
            "PHY": (tmp_path / "mirror" / "GDAC" / "dac",
                    tmp_path / "site" / "out" / "1011_PHY"),
        }

    def test_current_symlink_is_not_resolved(self, tmp_path):
        """The jobs write into `current`, not into the snapshot it points at."""
        snapshot = tmp_path / "db" / "snapshots" / "2026-11-20"
        snapshot.mkdir(parents=True)
        (tmp_path / "db" / "current").symlink_to(snapshot)
        site = _write_config_dir(tmp_path / "site", {
            "input_path": "/srv/mirror/GDAC/dac/",
            "outdir_pq": str(tmp_path / "db" / "current") + "/",
        })
        result = _run(site, "PHY")
        assert result.returncode == 0, result.stderr
        assert _parse(result.stdout)["PHY"][1] == tmp_path / "db" / "current"

    @pytest.mark.parametrize("missing", ["input_path", "outdir_pq"])
    def test_missing_field_is_an_error(self, tmp_path, missing):
        fields = {"input_path": "/srv/mirror/GDAC/dac/",
                  "outdir_pq": "/srv/db/1011_PHY_ARGO-CLOUD/current/"}
        del fields[missing]
        site = _write_config_dir(tmp_path / "site", fields)
        result = _run(site, "PHY")
        assert result.returncode != 0
        assert f"ARGO-GDAC_PHY.{missing} is not set" in result.stderr
        assert "null" not in result.stdout

    def test_test_config_resolves_to_the_fixtures(self):
        """The committed test config (relative paths) keeps working."""
        result = _run(TEST_CONFIG_DIR, "PHY")
        assert result.returncode == 0, result.stderr
        in_path, _ = _parse(result.stdout)["PHY"]
        assert in_path == (TEST_CONFIG_DIR.parent / "fixtures" / "demo_ARGO_GDAC"
                           / "GDAC" / "dac").resolve()
