#!/usr/bin/env python3

## @file test_loader.py
#
# Tests for crocolaketools.loader.Loader, in two categories:
#
#   unit        -- run on a miniature CrocoLake built from the golden PHY
#                  outputs committed under tests/golden/ (offline, fast)
#   integration -- run on a full CrocoLake on disk, found through the
#                  CROCOLAKE_ROOTPATH environment variable (skipped if unset)
#                  EX: CROCOLAKE_ROOTPATH=/path/to/crocolake pytest -m integration tests/test_loader.py
#
# Run only one category with e.g. `pytest -m unit tests/test_loader.py`.
#
## @author Enrico Milanese <enrico.milanese@whoi.edu> 
#
## @date Tue 19 Nov 2024
#
##########################################################################
import os
import shutil
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from crocolaketools import db_names, db_params
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

##########################################################################
# Unit tests (golden data)
##########################################################################

@pytest.mark.unit
class TestLoaderUnit:

#------------------------------------------------------------------------------#
## Constructor

    def test_loader_finds_databases(self, crocolake_phy):
        loader = Loader(db_type="PHY", db_rootpath=str(crocolake_phy))
        assert sorted(loader.db_list) == sorted(PHY_DBS)
        assert loader.db_type == "PHY"

    def test_loader_db_string(self, crocolake_phy):
        loader = Loader(db_list="GLODAP", db_rootpath=str(crocolake_phy))
        assert loader.db_list == ["GLODAP"]

    def test_loader_wrong_db_list(self):
        with pytest.raises(ValueError):
            Loader(db_list=["NOT_A_DATABASE"])

    def test_loader_wrong_db_type(self):
        with pytest.raises(ValueError):
            Loader(db_type="XYZ")

    def test_loader_wrong_rootpath(self, tmp_path):
        with pytest.raises(ValueError):
            Loader(db_rootpath=str(tmp_path / "non_existing_dir"))

    def test_loader_wrong_variables(self, crocolake_phy):
        with pytest.raises(ValueError):
            Loader(db_rootpath=str(crocolake_phy), selected_variables=["NOT_A_VARIABLE"])

#------------------------------------------------------------------------------#
## Methods

    def test_loader_get_dataframe(self, crocolake_phy):
        loader = Loader(db_type="PHY", db_rootpath=str(crocolake_phy))
        df = loader.get_dataframe().compute()
        assert len(df) == expected_rows()
        assert list(df.columns) == loader.selected_variables
        assert set(df["DB_NAME"].unique()) == set(PHY_DBS)

    def test_loader_selected_variables(self, crocolake_phy):
        variables = ["LATITUDE", "LONGITUDE", "TEMP"]
        loader = Loader(db_rootpath=str(crocolake_phy), selected_variables=variables)
        df = loader.get_dataframe().compute()
        assert list(df.columns) == variables
        assert len(df) == expected_rows()

    def test_loader_add_units_to_schema(self, crocolake_phy):
        loader = Loader(db_type="PHY", db_rootpath=str(crocolake_phy))
        loader.add_units_to_schema()
        for field in loader.global_schema:
            assert field.metadata is not None and b"units" in field.metadata, field.name

##########################################################################
# Integration tests (full CrocoLake on disk)
##########################################################################

@pytest.fixture
def crocolake_rootpath(monkeypatch):
    """Run from the root of a full CrocoLake, the Loader's default db_rootpath
    ("./"). Skips the test if CROCOLAKE_ROOTPATH is not set."""
    rootpath = os.environ.get("CROCOLAKE_ROOTPATH")
    if not rootpath:
        pytest.skip("CROCOLAKE_ROOTPATH not set: no full CrocoLake to test against")
    monkeypatch.chdir(rootpath)
    return rootpath

@pytest.mark.integration
@pytest.mark.usefixtures("crocolake_rootpath")
class TestLoaderIntegration:

#------------------------------------------------------------------------------#
## Set of tests for the Loader class constructor
    def test_loader(self):
        "Test the Loader constructor with no arguments"
        loader = Loader()
        assert loader.db_list == db_names.databases
        assert loader.db_type == "PHY"
        assert loader.selected_variables == db_params.params["CROCOLAKE_PHY_QC"]

    def test_loader_db_list(self):
        "Test the Loader constructor with a list of databases"
        loader = Loader(db_list=["ARGO","SprayGliders"])
        assert loader.db_list == ["ARGO","SprayGliders"]
        assert loader.db_type == "PHY"
        assert loader.selected_variables == db_params.params["CROCOLAKE_PHY_QC"]

    def test_loader_wrong_db_list(self):
        "Test the Loader constructor with a list of databases"
        with pytest.raises(ValueError):
            loader = Loader(db_list=["a","b","c"])

    def test_loader_db_string(self):
        "Test the Loader constructor with a single database as a string"
        loader = Loader(db_list="ARGO")
        assert loader.db_list == ["ARGO"]
        assert loader.db_type == "PHY"
        assert loader.selected_variables == db_params.params["CROCOLAKE_PHY_QC"]

    def test_loader_wrong_db_string(self):
        "Test the Loader constructor with a list of databases"
        with pytest.raises(ValueError):
            loader = Loader(db_list="a")

    def test_loader_type(self):
        "Test the Loader constructor with a database type"
        with pytest.warns(UserWarning):
            loader = Loader(db_type="BGC")
        assert loader.db_list == ['ARGO', 'GLODAP']
        assert loader.db_type == "BGC"
        assert loader.selected_variables == db_params.params["CROCOLAKE_BGC_QC"]

    def test_loader_wrong_rootpath(self):
        "Test the Loader constructor with non-existing database rootpath"
        with pytest.raises(ValueError):
            loader = Loader(db_rootpath="non_existing_dir")

    def test_loader_variables_phy(self):
        "Test the Loader constructor with selected variables"
        loader = Loader(selected_variables=["PSAL","TEMP","LATITUDE"])
        assert loader.db_list == db_names.databases
        assert loader.db_type == "PHY"
        assert loader.selected_variables == ["PSAL","TEMP","LATITUDE"]

    def test_loader_variables_bgc(self):
        "Test the Loader constructor with selected variables"
        with pytest.warns(UserWarning):
            loader = Loader(db_type="BGC",selected_variables=["PSAL","TEMP","LATITUDE","BBP","DOXY"])
        assert loader.db_list == ['ARGO', 'GLODAP']
        assert loader.db_type == "BGC"
        assert loader.selected_variables == ["PSAL","TEMP","LATITUDE","BBP","DOXY"]

    def test_loader_wrong_variables(self):
        "Test the Loader constructor with selected variables not present in database"
        with pytest.raises(ValueError):
            loader = Loader(selected_variables=["a","b","c","3","varname"])

    def test_loader_variables_empty(self):
        "Test the Loader constructor with empty selected variables"
        with pytest.raises(ValueError):
            loader = Loader(selected_variables=[])

#------------------------------------------------------------------------------#
## Set of tests for the Loader class methods
    def test_loader_get_dataframe_phy(self):
        "Test the Loader get_dataframe method for PHY database"
        # not all databases have the variable "DB_NAME", so I am testing for all
        # but that one
        selected_variables = [name for name in db_params.params["CROCOLAKE_PHY_QC"] if name != "DB_NAME"]
        loader = Loader(
            selected_variables=selected_variables,
            db_type="PHY"
        )
        df = loader.get_dataframe()
        assert df is not None

    def test_loader_get_dataframe_bgc(self):
        "Test the Loader get_dataframe method for BGC database"
        # not all databases have the variable "DB_NAME", so I am testing for all
        # but that one
        selected_variables = [name for name in db_params.params["CROCOLAKE_BGC_QC"] if name != "DB_NAME"]
        with pytest.warns(UserWarning):
            loader = Loader(
                selected_variables=selected_variables,
                db_type="BGC",
            )
        df = loader.get_dataframe()
        assert df is not None

    def test_loader_get_dataframe_bgc_print(self):
        "Test the Loader get_dataframe method for BGC database"
        # not all databases have the variable "DB_NAME", so I am testing for all
        # but that one
        selected_variables = ["LATITUDE","LONGITUDE","PRES","TEMP","PSAL","DOXY","BBP","SILICATE"]
        with pytest.warns(UserWarning):
            loader = Loader(
                selected_variables=selected_variables,
                db_type="BGC",
            )
        filters = [
            ("LATITUDE","<",40),
            ("LATITUDE",">",20),
            ("LONGITUDE","<",0),
            ("LONGITUDE",">",-20),
        ]
        loader.set_filters(filters)
        df = loader.get_dataframe()
        print(df.head())
        print(df.tail())
        print(df.compute())
        print(df.groupby("LATITUDE").LONGITUDE.std().compute())
        assert df is not None

    def test_loader_add_and_validate_filters_and(self):
        "Test the Loader set_filters and __validate_filters method with an "and" filter"
        loader = Loader()
        filters = [
            ("LATITUDE","<",70),
            ("LATITUDE",">", 0),
            ("LONGITUDE","<", 0),
            ("LONGITUDE",">", -40),
            ("PRES","<=",300),
            ("TEMP",">",10),
        ]
        loader.set_filters(filters)
        assert loader.filters == filters

    def test_loader_add_and_validate_filters_or(self):
        "Test the Loader set_filters and __validate_filters method with an "or" filter"
        loader = Loader()
        filters = [
            [("LATITUDE","<",70),
             ("LATITUDE",">", 0),
             ("LONGITUDE","<", 0),
             ("LONGITUDE",">", -40),
             ("PRES","<=",300),
             ("TEMP",">",10),
             ],
            [("LATITUDE","<", 0),
             ("LATITUDE",">",-70),
             ("LONGITUDE","<", 0),
             ("LONGITUDE",">", -40),
             ("PRES","<=",300),
             ("TEMP",">",10),
             ]
        ]
        loader.set_filters(filters)
        assert loader.filters == filters

    def test_loader_add_and_validate_filters_and_remove(self):
        "Test the Loader set_filters and __validate_filters method with an "and" filter and remove filter of invalid variable names"
        loader = Loader()
        filters = [
            ("LATITUDE","<",70),
            ("LATITUDE",">", 0),
            ("LONGITUDE","<", 0),
            ("LONGITUDE",">", -40),
            ("PRES","<=",300),
            ("TEMP",">",10),
            ("TEMPERATURE",">",10),
            ("VAR_NAME",">",10),
        ]
        with pytest.warns(UserWarning):
            loader.set_filters(filters)
        assert loader.filters ==  [
            ("LATITUDE","<",70),
            ("LATITUDE",">", 0),
            ("LONGITUDE","<", 0),
            ("LONGITUDE",">", -40),
            ("PRES","<=",300),
            ("TEMP",">",10),
        ]

    def test_loader_add_and_validate_filters_or_remove(self):
        "Test the Loader set_filters and __validate_filters method with an "or" filter and remove filter of invalid variable names"
        loader = Loader()
        filters = [
            [("LATITUDE","<",70),
             ("LATITUDE",">", 0),
             ("LONGITUDE","<", 0),
             ("LONGITUDE",">", -40),
             ("PRES","<=",300),
             ("TEMP",">",10),
             ("TEMPERATURE",">",10),
             ],
            [("LATITUDE","<", 0),
             ("LATITUDE",">",-70),
             ("LONGITUDE","<", 0),
             ("LONGITUDE",">", -40),
             ("PRES","<=",300),
             ("TEMP",">",10),
             ("VAR_NAME",">",10),
             ]
        ]
        with pytest.warns(UserWarning):
            loader.set_filters(filters)
        assert loader.filters ==  [
            [("LATITUDE","<",70),
             ("LATITUDE",">", 0),
             ("LONGITUDE","<", 0),
             ("LONGITUDE",">", -40),
             ("PRES","<=",300),
             ("TEMP",">",10),
             ],
            [("LATITUDE","<", 0),
             ("LATITUDE",">",-70),
             ("LONGITUDE","<", 0),
             ("LONGITUDE",">", -40),
             ("PRES","<=",300),
             ("TEMP",">",10),
             ]
        ]

##########################################################################

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
