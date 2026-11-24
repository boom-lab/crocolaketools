"""Tests for scripts/merge_crocolake.py.

crocolakeloader and a dask cluster are not available in CI, so both are
replaced with stand-ins. The stand-in Loader concatenates db_rootpath with a
string, as crocolakeloader/loader.py does, so a pathlib.Path reaching it
raises the same TypeError seen in production with v1.1.1.
"""

import importlib.util
import sys
import types
from pathlib import Path
from unittest import mock

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "merge_crocolake.py"


class _FakeLoader:
    instances = []

    def __init__(self, db_type, db_rootpath):
        print("Looking for data in " + db_rootpath)
        self.db_type = db_type
        self.db_rootpath = db_rootpath
        self.global_schema = None
        _FakeLoader.instances.append(self)

    def get_dataframe(self):
        return mock.MagicMock()

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
    monkeypatch.setattr(module.cfgp, "get_config_cluster_db_dict", lambda _: {})
    _FakeLoader.instances.clear()
    return module


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
