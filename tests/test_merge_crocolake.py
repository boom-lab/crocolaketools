"""Tests for scripts/merge_crocolake.py.

crocolakeloader and a dask cluster are not available in CI, so both are
replaced with stand-ins. The stand-in Loader concatenates db_rootpath with a
string, as crocolakeloader/loader.py does, so a pathlib.Path reaching it
raises the same TypeError seen in production with v1.1.1.
"""

import importlib.util
import logging
import os
import shutil
import sys
import types
from pathlib import Path
from unittest import mock

import pytest

from crocolaketools.config import config_paths as cfgp

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "scripts" / "merge_crocolake.py"
TEST_CONFIG_DIR = REPO / "tests" / "config"


class _FakeLoader:
    instances = []

    def __init__(self, db_type, db_rootpath):
        print("Looking for data in " + db_rootpath)
        self.db_type = db_type
        self.db_rootpath = db_rootpath
        self.global_schema = None
        self.ddf = mock.MagicMock()
        _FakeLoader.instances.append(self)

    def get_dataframe(self):
        return self.ddf

    def add_units_to_schema(self):
        pass


@pytest.fixture
def merge_module(monkeypatch):
    loader_pkg = types.ModuleType("crocolakeloader")
    loader_mod = types.ModuleType("crocolakeloader.loader")
    loader_mod.Loader = _FakeLoader
    loader_pkg.loader = loader_mod
    monkeypatch.setitem(sys.modules, "crocolakeloader", loader_pkg)
    monkeypatch.setitem(sys.modules, "crocolakeloader.loader", loader_mod)

    spec = importlib.util.spec_from_file_location("merge_crocolake_under_test", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    monkeypatch.setattr(module, "Client", mock.MagicMock())
    # basicConfig would attach a FileHandler to the root logger for the rest
    # of the session; the log path is tested through get_log_file instead
    monkeypatch.setattr(module, "configure_logging", mock.MagicMock())
    # main() writes CROCOLAKE_CONFIG_DIR into os.environ; restore it afterwards
    monkeypatch.setenv(cfgp.CONFIG_DIR_ENV_VAR, str(TEST_CONFIG_DIR))
    _FakeLoader.instances.clear()
    return module


def _write_config_dir(path, cluster_keys=True):
    """Config dir whose CROCOLAKE paths are relative and end in a slash."""
    path.mkdir(parents=True)
    (path / "datasets.yaml").write_text(
        "CROCOLAKE_PHY:\n"
        "  db: CROCOLAKE\n"
        "  db_type: PHY\n"
        "  outdir_pq: ./lake/CROCOLAKE_PHY/\n"
        "  fname_pq: PHY_CROCOLAKE\n"
        "  ln_path: ./lake/CROCOLAKE_PHY_LN/\n"
        "CROCOLAKE_BGC:\n"
        "  db: CROCOLAKE\n"
        "  db_type: BGC\n"
        "  outdir_pq: ./lake/CROCOLAKE_BGC/\n"
        "  fname_pq: BGC_CROCOLAKE\n"
        "  ln_path: ./lake/CROCOLAKE_BGC_LN/\n"
    )
    cluster = (TEST_CONFIG_DIR / "dask_cluster.yaml").read_text()
    if not cluster_keys:
        cluster = cluster.split("\nCROCOLAKE:")[0] + "\n"
        assert "\nCROCOLAKE:" not in cluster
    (path / "dask_cluster.yaml").write_text(cluster)
    return path


@pytest.mark.parametrize("db_type", ["PHY", "BGC"])
def test_merge_passes_string_rootpath_to_loader(merge_module, tmp_path, db_type):
    """get_config_paths_field() returns a Path; the loader needs a str."""
    croco_path = tmp_path / "current"
    outdir = tmp_path / "merged"

    merge_module.merge_crocolake(db_type, croco_path, outdir, "croco")

    (loader,) = _FakeLoader.instances
    assert isinstance(loader.db_rootpath, str)
    assert loader.db_rootpath == str(croco_path)
    assert loader.db_type == db_type
    merge_module.Client.assert_called_once_with(
        **cfgp.get_config_cluster_db_dict("CROCOLAKE")
    )


@pytest.mark.parametrize("db_type", ["PHY", "BGC"])
def test_main_config_reaches_loader_with_configured_paths(
        merge_module, monkeypatch, tmp_path, db_type):
    """The --config branch, as run in production, with real config resolution."""
    config_dir = _write_config_dir(tmp_path / "cfg")
    run = mock.MagicMock()
    monkeypatch.setattr(merge_module.subprocess, "run", run)
    monkeypatch.setattr(sys, "argv", [
        "merge_crocolake.py", "-d", db_type.lower(), "--config",
        "--config-dir", str(config_dir),
    ])

    merge_module.main()

    run.assert_called_once()
    assert run.call_args.args[0][-1] == db_type
    (loader,) = _FakeLoader.instances
    assert isinstance(loader.db_rootpath, str)
    # the loader joins sub-paths with os.path.join, so the dropped trailing
    # slash does not change what it finds
    assert loader.db_rootpath == str(config_dir / "lake" / f"CROCOLAKE_{db_type}_LN")

    outdir = config_dir / "lake" / f"CROCOLAKE_{db_type}"
    assert outdir.is_dir()
    to_parquet = loader.ddf.repartition.return_value.to_parquet
    to_parquet.assert_called_once()
    assert Path(to_parquet.call_args.args[0]) == outdir
    name_function = to_parquet.call_args.kwargs["name_function"]
    assert name_function(3) == f"{db_type}_CROCOLAKE_0003.parquet"

    merge_module.configure_logging.assert_called_once_with(
        str(config_dir / "lake" / f"{db_type}_CROCOLAKE.log")
    )
    merge_module.Client.assert_called_once_with(
        **cfgp.get_config_cluster_db_dict("CROCOLAKE")
    )


@pytest.mark.parametrize("argv", [
    ["-d", "PHY", "-o", "out"],
    ["-d", "PHY", "-i", "lake"],
    ["-d", "PHY"],
])
def test_main_without_config_requires_input_and_output(
        merge_module, monkeypatch, capsys, argv):
    """A missing -i must not reach the loader as the string 'None'."""
    monkeypatch.setattr(sys, "argv", ["merge_crocolake.py"] + argv)

    with pytest.raises(SystemExit) as excinfo:
        merge_module.main()

    assert excinfo.value.code == 2
    assert "-i and -o are required" in capsys.readouterr().err
    assert not _FakeLoader.instances


def test_cluster_falls_back_to_legacy_key(merge_module, monkeypatch, tmp_path, caplog):
    """Config dirs without a CROCOLAKE cluster key keep working."""
    config_dir = _write_config_dir(tmp_path / "cfg", cluster_keys=False)
    monkeypatch.setenv(cfgp.CONFIG_DIR_ENV_VAR, str(config_dir))

    with caplog.at_level(logging.WARNING):
        cluster = merge_module.get_cluster_config()

    assert cluster == cfgp.get_config_cluster_db_dict("SPRAY_GLIDERS")
    assert "CROCOLAKE not found" in caplog.text


def test_log_file_is_next_to_outdir(merge_module, tmp_path):
    """to_parquet(overwrite=True) empties outdir, so the log goes beside it."""
    outdir = tmp_path / "a" / "CROCOLAKE_PHY"

    log_file = merge_module.get_log_file(str(outdir) + os.sep, "PHY_CROCOLAKE")

    assert log_file == str(tmp_path / "a" / "PHY_CROCOLAKE.log")
    assert (tmp_path / "a").is_dir()
