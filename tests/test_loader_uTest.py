#!/usr/bin/env python3

## @file test_loader.py
#
# Tests for crocolaketools.loader.Loader, run on a miniature CrocoLake built
# from the golden PHY outputs committed under tests/golden/.
#
##########################################################################
import shutil
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from crocolaketools.loader.loader import Loader
##########################################################################

GOLDEN_DIR = Path(__file__).parent / "golden"

# Loader database name -> golden directory holding its expected parquet output.
# Each directory name contains the db codename the Loader globs for
# (crocolaketools.db_names.databases_codenames).
PHY_DBS = {
    "ARGO": "ARGO-QC_PHY",
    "GLODAP": "GLODAP_PHY",
    "SprayGliders": "SPRAY_PHY",
}

@pytest.fixture
def crocolake_phy(tmp_path):
    """Lay out one folder per database, as merge_crocolake expects."""
    for golden_name in PHY_DBS.values():
        shutil.copytree(GOLDEN_DIR / golden_name / "expected", tmp_path / golden_name)
    return tmp_path

def expected_rows():
    return sum(
        pq.read_metadata(GOLDEN_DIR / g / "expected" / "_metadata").num_rows
        for g in PHY_DBS.values()
    )

#------------------------------------------------------------------------------#
## Constructor

def test_loader_finds_databases(crocolake_phy):
    loader = Loader(db_type="PHY", db_rootpath=str(crocolake_phy))
    assert sorted(loader.db_list) == sorted(PHY_DBS)
    assert loader.db_type == "PHY"

def test_loader_db_string(crocolake_phy):
    loader = Loader(db_list="GLODAP", db_rootpath=str(crocolake_phy))
    assert loader.db_list == ["GLODAP"]

def test_loader_wrong_db_list():
    with pytest.raises(ValueError):
        Loader(db_list=["NOT_A_DATABASE"])

def test_loader_wrong_db_type():
    with pytest.raises(ValueError):
        Loader(db_type="XYZ")

def test_loader_wrong_rootpath(tmp_path):
    with pytest.raises(ValueError):
        Loader(db_rootpath=str(tmp_path / "non_existing_dir"))

def test_loader_wrong_variables(crocolake_phy):
    with pytest.raises(ValueError):
        Loader(db_rootpath=str(crocolake_phy), selected_variables=["NOT_A_VARIABLE"])

#------------------------------------------------------------------------------#
## Methods

def test_loader_get_dataframe(crocolake_phy):
    loader = Loader(db_type="PHY", db_rootpath=str(crocolake_phy))
    df = loader.get_dataframe().compute()
    assert len(df) == expected_rows()
    assert list(df.columns) == loader.selected_variables
    assert set(df["DB_NAME"].unique()) == set(PHY_DBS)

def test_loader_selected_variables(crocolake_phy):
    variables = ["LATITUDE", "LONGITUDE", "TEMP"]
    loader = Loader(db_rootpath=str(crocolake_phy), selected_variables=variables)
    df = loader.get_dataframe().compute()
    assert list(df.columns) == variables
    assert len(df) == expected_rows()

def test_loader_add_units_to_schema(crocolake_phy):
    loader = Loader(db_type="PHY", db_rootpath=str(crocolake_phy))
    loader.add_units_to_schema()
    for field in loader.global_schema:
        assert field.metadata is not None and b"units" in field.metadata, field.name

##########################################################################

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
