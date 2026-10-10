#!/usr/bin/env python3

## @file test_loader.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Tue 19 Nov 2024
#
##########################################################################
import os
import pytest
import warnings

from crocolaketools.loader.loader import Loader
from crocolaketools.utils import params
##########################################################################

class TestLoader:

#------------------------------------------------------------------------------#
## Set of tests for the Loader class constructor
    def test_loader(self):
        "Test the Loader constructor with no arguments"
        loader = Loader()
        assert loader.db_list == params.databases
        assert loader.db_type == "PHY"
        assert loader.selected_variables == params.params["CROCOLAKE_PHY"]

    def test_loader_db_list(self):
        "Test the Loader constructor with a list of databases"
        loader = Loader(db_list=["ARGO","SprayGliders"])
        assert loader.db_list == ["ARGO","SprayGliders"]
        assert loader.db_type == "PHY"
        assert loader.selected_variables == params.params["CROCOLAKE_PHY"]

    def test_loader_wrong_db_list(self):
        "Test the Loader constructor with a list of databases"
        with pytest.raises(ValueError):
            loader = Loader(db_list=["a","b","c"])

    def test_loader_db_string(self):
        "Test the Loader constructor with a single database as a string"
        loader = Loader(db_list="ARGO")
        assert loader.db_list == ["ARGO"]
        assert loader.db_type == "PHY"
        assert loader.selected_variables == params.params["CROCOLAKE_PHY"]

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
        assert loader.selected_variables == params.params["CROCOLAKE_BGC"]

    def test_loader_wrong_rootpath(self):
        "Test the Loader constructor with non-existing database rootpath"
        with pytest.raises(ValueError):
            loader = Loader(db_rootpath="non_existing_dir")

    def test_loader_variables_phy(self):
        "Test the Loader constructor with selected variables"
        loader = Loader(selected_variables=["PSAL","TEMP","LATITUDE"])
        assert loader.db_list == params.databases
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
        selected_variables = [name for name in params.params["CROCOLAKE_PHY"] if name != "DB_NAME"]
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
        selected_variables = [name for name in params.params["CROCOLAKE_BGC"] if name != "DB_NAME"]
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