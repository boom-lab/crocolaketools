#!/usr/bin/env python3

## @file test_converter.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Fri 22 Nov 2024
#
##########################################################################
import glob
import os
import random
from pprint import pprint
import shutil
terminal_width = shutil.get_terminal_size().columns

import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import xarray as xr

from crocolaketools.converter.converterSprayGliders import ConverterSprayGliders
from crocolaketools.converter.converterArgoQC import ConverterArgoQC
from crocolaketools.converter.converterGLODAP import ConverterGLODAP
from crocolaketools.converter.converterCPR import ConverterCPR
from crocolaketools.converter.converterSaildrones import ConverterSaildrones
from crocolakeloader import params

##########################################################################

# FILL HERE YOUR DATABASE ROOTPATHS
argo_phy_path = ''
outdir_phy_pqt =  ''
argo_bgc_path = ''
outdir_bgc_pqt =  ''
spray_path = ''
outdir_spray_pqt = ''
cpr_path = ''
outdir_cpr_pqt = ''
saildrones_path = ''
outdir_saildrones_pqt = ''

# if not os.path.exists(outdir_phy_pqt):
#     raise ValueError("PHY output directory does not exist.")
# if not os.path.exists(outdir_bgc_pqt):
#     raise ValueError("BGC output directory does not exist.")
# if not os.path.exists(argo_phy_path):
#     raise ValueError("PHY input directory does not exist.")
# if not os.path.exists(argo_bgc_path):
#     raise ValueError("BGC input directory does not exist.")
# if not os.path.exists(spray_path):
#     raise ValueError("SprayGliders input directory does not exist.")
# if not os.path.exists(outdir_spray_pqt):
#     raise ValueError("SprayGliders output directory does not exist.")

class TestConverter:

#------------------------------------------------------------------------------#
## Set of tests for the Converter class constructor

    def test_converter_argoqc_read_pq_dtypes_phy(self):
        """
        Test that the data types of the columns in the ARGO QC dataframe are as expected
        """
        converterPHY = ConverterArgoQC(
            db = "ARGO",
            db_type="PHY",
            input_path = argo_phy_path,
            outdir_pq = outdir_phy_pqt,
            outdir_schema = './schemas/ArgoQC/',
            fname_pq = 'test_1002_PHY_ARGO-QC-DEV'
        )
        ddf = converterPHY.read_pq()
        print(ddf.head())
        assert not ddf.head().empty

        assert ddf.dtypes["PLATFORM_NUMBER"] == "int64[pyarrow]"#pd.Int64Dtype()
        isinstance(ddf.dtypes["DATA_MODE"], pd.CategoricalDtype)
        assert ddf.dtypes["LATITUDE"] == "float64[pyarrow]"#pd.Float64Dtype()
        assert ddf.dtypes["LONGITUDE"] == "float64[pyarrow]"#pd.Float64Dtype()
        assert ddf.dtypes["JULD"] == "timestamp[ns][pyarrow]"#np.dtype('datetime64[ns]')
        assert ddf.dtypes["PRES"] == "float32[pyarrow]"#pd.Float32Dtype()
        assert ddf.dtypes["PRES_QC"] == "uint8[pyarrow]"#pd.UInt8Dtype()
        assert ddf.dtypes["PRES_ADJUSTED_ERROR"] == "float32[pyarrow]"#pd.Float32Dtype()
        assert ddf.dtypes["TEMP"] == "float32[pyarrow]"#pd.Float32Dtype()
        assert ddf.dtypes["TEMP_QC"] == "uint8[pyarrow]"#pd.UInt8Dtype()
        assert ddf.dtypes["TEMP_ADJUSTED_ERROR"] == "float32[pyarrow]"#pd.Float32Dtype()
        assert ddf.dtypes["PSAL"] == "float32[pyarrow]"#pd.Float32Dtype()
        assert ddf.dtypes["PSAL_QC"] == "uint8[pyarrow]"#pd.UInt8Dtype()
        assert ddf.dtypes["PSAL_ADJUSTED_ERROR"] == "float32[pyarrow]"#pd.Float32Dtype()

    def test_converter_argoqc_filters_bgc(self):
        """
        Test that the qc filters are properly generated for the ARGO QC dataframe 
        """
        converterBGC = ConverterArgoQC(
            db = "ARGO",
            db_type="BGC",
            input_path = argo_bgc_path,
            outdir_pq = outdir_bgc_pqt,
            outdir_schema = './schemas/ArgoQC/',
            fname_pq = 'test_1002_BGC_ARGO-QC-DEV'
        )
        filters, param_basenames = converterBGC.generate_qc_schema_filters()

        # filters must be a list of lists of two tuples with three items each
        assert isinstance(filters, list)
        for f in filters:
            assert isinstance(f, list)
            assert len(f) == 2
            for item in f:
                assert isinstance(item, tuple)
                assert len(item) == 3

        pprint("filters:")
        pprint(filters)

    def test_converter_argoqc_read_pq_dtypes_bgc(self):
        """
        Test that the data types of the columns in the ARGO QC dataframe are as expected
        """
        converterBGC = ConverterArgoQC(
            db = "ARGO",
            db_type="BGC",
            input_path = argo_bgc_path,
            outdir_pq = outdir_bgc_pqt,
            outdir_schema = './schemas/ArgoQC/',
            fname_pq = 'test_1002_BGC_ARGO-QC-DEV'
        )
        ddf = converterBGC.read_pq()
        print(ddf.head())
        assert not ddf.head().empty

        for var in params.params["CROCOLAKE_BGC_QC"]:
            if var in ddf.columns:
                print(var)
                if var in ["PLATFORM_NUMBER"]:
                    assert ddf.dtypes[var] == "int64[pyarrow]"
                elif var == "JULD":
                    assert ddf.dtypes[var] == "timestamp[ns][pyarrow]"
                elif var in ["LATITUDE","LONGITUDE"]:
                    assert ddf.dtypes[var] == "float64[pyarrow]"
                elif "DATA_MODE" in var:
                    assert isinstance(ddf.dtypes[var], pd.CategoricalDtype)
                elif "QC" in var:
                    assert ddf.dtypes[var] == "uint8[pyarrow]"
                    if (var[:-2]+"ADJUSTED_QC" in ddf.columns):
                        print(var[:-2]+"ADJUSTED_QC")
                        assert ddf.dtypes[var[:-2]+"ADJUSTED_QC"] == "uint8[pyarrow]"
                else:
                    assert ddf.dtypes[var] == "float32[pyarrow]"
            elif ("ERROR" in var) and (var[:-5]+"ADJUSTED_ERROR" in ddf.columns):
                print(var[:-5]+"ADJUSTED_ERROR")
                assert ddf.dtypes[var[:-5]+"ADJUSTED_ERROR"] == "float32[pyarrow]"
            else:
                print(f"Variable {var} not in dataframe.")


    def test_converter_argoqc_update_cols_phy(self):
        """
        Test that the data types of the columns in the ARGO QC dataframe are as expected
        """
        converterPHY = ConverterArgoQC(
            db = "ARGO",
            db_type="PHY",
            input_path = argo_phy_path,
            outdir_pq = outdir_phy_pqt,
            outdir_schema = './schemas/ArgoQC/',
            fname_pq = 'test_1002_PHY_ARGO-QC-DEV'
        )
        ddf = converterPHY.read_pq()
        ddf = converterPHY.update_cols(ddf)

        assert ddf.dtypes["PLATFORM_NUMBER"] == "int64[pyarrow]"#pd.Int64Dtype()
        assert isinstance(ddf.dtypes["DATA_MODE"], pd.CategoricalDtype)#pd.StringDtype("pyarrow") # == "string[pyarrow]"
        assert ddf.dtypes["LATITUDE"] == "float64[pyarrow]"#pd.Float64Dtype()
        assert ddf.dtypes["LONGITUDE"] == "float64[pyarrow]"#pd.Float64Dtype()
        assert ddf.dtypes["JULD"] == "timestamp[ns][pyarrow]"#np.dtype("datetime64[ns]")
        assert ddf.dtypes["PRES"] == "float32[pyarrow]"#pd.Float32Dtype()
        assert ddf.dtypes["PRES_QC"] == "uint8[pyarrow]"#pd.UInt8Dtype()
        assert ddf.dtypes["PRES_ERROR"] == "float32[pyarrow]"#pd.Float32Dtype()
        assert ddf.dtypes["TEMP"] == "float32[pyarrow]"#pd.Float32Dtype()
        assert ddf.dtypes["TEMP_QC"] == "uint8[pyarrow]"#pd.UInt8Dtype()
        assert ddf.dtypes["TEMP_ERROR"] == "float32[pyarrow]"#pd.Float32Dtype()
        assert ddf.dtypes["PSAL"] == "float32[pyarrow]"#pd.Float32Dtype()
        assert ddf.dtypes["PSAL_QC"] == "uint8[pyarrow]"#pd.UInt8Dtype()
        assert ddf.dtypes["PSAL_ERROR"] == "float32[pyarrow]"#pd.Float32Dtype()
        assert isinstance(ddf.dtypes["DB_NAME"], pd.CategoricalDtype)

        assert "PRES_ADJUSTED" not in ddf.columns
        assert "TEMP_ADJUSTED" not in ddf.columns
        assert "PSAL_ADJUSTED" not in ddf.columns
        assert "PRES_ERROR_ADJUSTED" not in ddf.columns
        assert "TEMP_ERROR_ADJUSTED" not in ddf.columns
        assert "PSAL_ERROR_ADJUSTED" not in ddf.columns

    def test_converter_argoqc_update_cols_phy_dummy(self):
        """
        Test that the original data is correctly re-casted in the QC format
        """

        # the resulting QCed df should have temperature values of 20 or 25
        dummy_data = {
            "PLATFORM_NUMBER": [1000001, 1000002, 1000003, 1000004, 1000005, 1000006],
            "LATITUDE": [35.0, 36.0, 37.0, 38.0, 39.0, 40.0],
            "LONGITUDE": [-70.0, -71.0, -72.0, -73.0, -74.0, -75.0],
            "JULD": pd.to_datetime(
                ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04", "2021-01-05", "2021-01-06"]
            ),
            "TEMP": [20.0, 20.0, 11.0, 11.0, 11.0, 11.0],
            "TEMP_QC": [1, 2, 3, 1, 1, 1],
            "TEMP_ADJUSTED": [pd.NA, pd.NA, pd.NA, 25.0, 25.0, 16.0],
            "TEMP_ADJUSTED_QC": [pd.NA, pd.NA, pd.NA, 1, 2, 3],
            "TEMP_ADJUSTED_ERROR": [pd.NA, pd.NA, pd.NA, 0.02, 0.02, 0.02],
            "PSAL": [2.0, 2.0, 1.0, 1.0, 1.0, 1.0],
            "PSAL_QC": [1, 2, 3, 1, 1, 1],
            "PSAL_ADJUSTED": [pd.NA, pd.NA, pd.NA, 5.0, 5.0, 6.0],
            "PSAL_ADJUSTED_QC": [pd.NA, pd.NA, pd.NA, 1, 2, 3],
            "PSAL_ADJUSTED_ERROR": [pd.NA, pd.NA, pd.NA, 0.03, 0.03, 0.03],
            "PRES": [200.0, 200.0, 110.0, 110.0, 110.0, 110.0],
            "PRES_QC": [1, 2, 3, 1, 1, 1],
            "PRES_ADJUSTED": [pd.NA, pd.NA, pd.NA, 250.0, 250.0, 260.0],
            "PRES_ADJUSTED_QC": [pd.NA, pd.NA, pd.NA, 1, 2, 3],
            "PRES_ADJUSTED_ERROR": [pd.NA, pd.NA, pd.NA, 0.05, 0.05, 0.05],
            "DATA_MODE": ["R", "R", "R", "D", "D", "D"],
        }

        dummy_df = pd.DataFrame(dummy_data).convert_dtypes(dtype_backend='pyarrow')
        for param in dummy_df.columns:
            if param not in ["JULD", "DATA_MODE", "PLATFORM_NUMBER"] and "QC" not in param:
                dummy_df[ param ] = dummy_df[ param ].astype("float32[pyarrow]")
            elif "QC" in param:
                dummy_df[ param ] = dummy_df[ param ].astype("int64[pyarrow]")
        print("Dummy data:")
        print(f"memory usage: {dummy_df.memory_usage().sum()} bytes")
        with pd.option_context('display.max_columns', None, 'display.width', terminal_width):
            print(dummy_df)

        sol_data = {
            "PLATFORM_NUMBER": [1000001, 1000002, 1000004, 1000005],
            "LATITUDE": [35.0, 36.0, 38.0, 39.0],
            "LONGITUDE": [-70.0, -71.0, -73.0, -74.0],
            "JULD": pd.to_datetime(
                ["2021-01-01", "2021-01-02", "2021-01-04", "2021-01-05"]
            ),
            "TEMP": [20.0, 20.0, 25.0, 25.0],
            "TEMP_QC": [1, 2, 1, 2],
            "PSAL": [2.0, 2.0, 5.0, 5.0],
            "PSAL_QC": [1, 2, 1, 2],
            "PRES": [200.0, 200.0, 250.0, 250.0],
            "PRES_QC": [1, 2, 1, 2],
            "DATA_MODE": ["R", "R", "D", "D"],
            "PRES_ERROR": [pd.NA, pd.NA, 0.05, 0.05],
            "TEMP_ERROR": [pd.NA, pd.NA, 0.02, 0.02],
            "PSAL_ERROR": [pd.NA, pd.NA, 0.03, 0.03],
            "DB_NAME": ["ARGO", "ARGO", "ARGO", "ARGO"]
        }
        sol_df = pd.DataFrame(sol_data).convert_dtypes(dtype_backend='pyarrow')
        for param in sol_df.columns:
            if param not in ["JULD", "DATA_MODE", "PLATFORM_NUMBER", "DB_NAME"] and "QC" not in param:
                sol_df[ param ] = sol_df[ param ].astype("float32[pyarrow]")
            elif "ADJUSTED_QC" in param:
                sol_df[ param ] = sol_df[ param ].astype("int64[pyarrow]")
            elif "QC" in param:
                sol_df[ param ] = sol_df[ param ].astype("uint8[pyarrow]")
            elif param == "DB_NAME":
                categories = pd.Series(params.databases, dtype='string[pyarrow]')
                sol_df[ param ] = sol_df[ param ].astype(pd.CategoricalDtype(categories=categories, ordered=False))
            elif "DATA_MODE" in param:
                categories = pd.Series(["R", "A", "D"], dtype='string[pyarrow]')
                sol_df[ param ] = sol_df[ param ].astype(pd.CategoricalDtype(categories=categories, ordered=False))
        print("Solution data:")
        print(f"memory usage: {sol_df.memory_usage().sum()} bytes")
        with pd.option_context('display.max_columns', None, 'display.width', terminal_width):
            print(sol_df)

        converterPHY = ConverterArgoQC(
            db = "ARGO",
            db_type="PHY",
            input_path = argo_phy_path,
            outdir_pq = outdir_phy_pqt,
            outdir_schema = './schemas/ArgoQC/',
            fname_pq = 'test_1002_PHY_ARGO-QC-DEV'
        )
        filters, param_basenames = converterPHY.generate_qc_schema_filters()
        print("param_basenames:")
        print(param_basenames)
        converterPHY.param_basenames = param_basenames
        ddf = converterPHY.update_cols(dd.from_pandas(dummy_df))
        #ddf = converterPHY.keep_best_values(ddf)

        ddf = ddf.compute()
        print("Resulting data:")
        print(f"memory usage: {ddf.memory_usage().sum()} bytes")
        with pd.option_context('display.max_columns', None, 'display.width', terminal_width):
            print(ddf)

        pd.testing.assert_frame_equal(ddf, sol_df)

    def test_converter_argoqc_keep_best_values_phy(self):
        """
        Test that the original data is correctly re-casted in the QC format
        """

        # the resulting QCed df should have temperature values of 20 or 25
        dummy_data = {
            "PLATFORM_NUMBER": [1000001, 1000002, 1000003, 1000004, 1000005, 1000006],
            "LATITUDE": [35.1, 36.1, 37.1, 38.1, 39.1, 40.1],
            "LONGITUDE": [-70.1, -71.1, -72.1, -73.1, -74.1, -75.1],
            "JULD": pd.to_datetime(
                ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04", "2021-01-05", "2021-01-06"]
            ),
            "TEMP": [20.1, 20.1, 11.1, 11.1, 11.1, 11.1],
            "TEMP_QC": [1, 2, 3, 1, 1, 1],
            "TEMP_ADJUSTED": [pd.NA, pd.NA, pd.NA, 25.1, 25.1, 16.1],
            "TEMP_ADJUSTED_QC": [pd.NA, pd.NA, pd.NA, 1, 2, 3],
            "TEMP_ADJUSTED_ERROR": [pd.NA, pd.NA, pd.NA, 0.02, 0.02, 0.02],
            "PSAL": [2.1, 2.1, 1.1, 1.1, 1.1, 1.1],
            "PSAL_QC": [1, 2, 3, 1, 1, 1],
            "PSAL_ADJUSTED": [pd.NA, pd.NA, pd.NA, 5.1, 5.1, 6.1],
            "PSAL_ADJUSTED_QC": [pd.NA, pd.NA, pd.NA, 1, 2, 3],
            "PSAL_ADJUSTED_ERROR": [pd.NA, pd.NA, pd.NA, 0.03, 0.03, 0.03],
            "PRES": [2.1, 2.1, 110.1, 110.1, 110.1, 110.1],
            "PRES_QC": [1, 2, 3, 1, 1, 1],
            "PRES_ADJUSTED": [pd.NA, pd.NA, pd.NA, 25.1, 25.1, 26.1],
            "PRES_ADJUSTED_QC": [pd.NA, pd.NA, pd.NA, 1, 2, 3],
            "PRES_ADJUSTED_ERROR": [pd.NA, pd.NA, pd.NA, 0.05, 0.05, 0.05],
            "DATA_MODE": ["R", "R", "R", "D", "D", "D"],
        }

        dummy_df = pd.DataFrame(dummy_data).convert_dtypes(dtype_backend='pyarrow')
        for param in dummy_df.columns:
            if param not in ["JULD", "DATA_MODE", "PLATFORM_NUMBER"] and "QC" not in param:
                dummy_df[ param ] = dummy_df[ param ].astype("float32[pyarrow]")
            elif "QC" in param:
                dummy_df[ param ] = dummy_df[ param ].astype("int64[pyarrow]")
        print("Dummy data:")
        print(f"memory usage: {dummy_df.memory_usage().sum()} bytes")
        with pd.option_context('display.max_columns', None, 'display.width', terminal_width):
            print(dummy_df)

        sol_data = {
            "PLATFORM_NUMBER": [1000001, 1000002, 1000003, 1000004, 1000005, 1000006],
            "LATITUDE": [35.1, 36.1, 37.1, 38.1, 39.1, 40.1],
            "LONGITUDE": [-70.1, -71.1, -72.1, -73.1, -74.1, -75.1],
            "JULD": pd.to_datetime(
                ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04", "2021-01-05", "2021-01-06"]
            ),
            "TEMP": [20.1, 20.1, pd.NA, 25.1, 25.1, pd.NA],
            "TEMP_QC": [1, 2, pd.NA, 1, 2, pd.NA],
            "TEMP_ADJUSTED": [pd.NA, pd.NA, pd.NA, 25.1, 25.1, 16.1],
            "TEMP_ADJUSTED_QC": [pd.NA, pd.NA, pd.NA, 1, 2, 3],
            "TEMP_ADJUSTED_ERROR": [pd.NA, pd.NA, pd.NA, 0.02, 0.02, 0.02],
            "PSAL": [2.1, 2.1, pd.NA, 5.1, 5.1, pd.NA],
            "PSAL_QC": [1, 2, pd.NA, 1, 2, pd.NA],
            "PSAL_ADJUSTED": [pd.NA, pd.NA, pd.NA, 5.1, 5.1, 6.1],
            "PSAL_ADJUSTED_QC": [pd.NA, pd.NA, pd.NA, 1, 2, 3],
            "PSAL_ADJUSTED_ERROR": [pd.NA, pd.NA, pd.NA, 0.03, 0.03, 0.03],
            "PRES": [2.1, 2.1, pd.NA, 25.1, 25.1, pd.NA],
            "PRES_QC": [1, 2, pd.NA, 1, 2, pd.NA],
            "PRES_ADJUSTED": [pd.NA, pd.NA, pd.NA, 25.1, 25.1, 26.1],
            "PRES_ADJUSTED_QC": [pd.NA, pd.NA, pd.NA, 1, 2, 3],
            "PRES_ADJUSTED_ERROR": [pd.NA, pd.NA, pd.NA, 0.05, 0.05, 0.05],
            "DATA_MODE": ["R", "R", "R", "D", "D", "D"],
            "PRES_ERROR": [pd.NA, pd.NA, pd.NA, 0.05, 0.05, pd.NA],
            "TEMP_ERROR": [pd.NA, pd.NA, pd.NA, 0.02, 0.02, pd.NA],
            "PSAL_ERROR": [pd.NA, pd.NA, pd.NA, 0.03, 0.03, pd.NA],
        }
        sol_df = pd.DataFrame(sol_data).convert_dtypes(dtype_backend='pyarrow')
        for param in sol_df.columns:
            if param not in ["JULD", "DATA_MODE", "PLATFORM_NUMBER"] and "QC" not in param:
                sol_df[ param ] = sol_df[ param ].astype("float32[pyarrow]")
            elif "ADJUSTED_QC" in param:
                sol_df[ param ] = sol_df[ param ].astype("int64[pyarrow]")
            elif "QC" in param:
                sol_df[ param ] = sol_df[ param ].astype("uint8[pyarrow]")
        print("Solution data:")
        print(f"memory usage: {sol_df.memory_usage().sum()} bytes")
        with pd.option_context('display.max_columns', None, 'display.width', terminal_width):
            print(sol_df)

        converterPHY = ConverterArgoQC(
            db = "ARGO",
            db_type="PHY",
            input_path = argo_phy_path,
            outdir_pq = outdir_phy_pqt,
            outdir_schema = './schemas/ArgoQC/',
            fname_pq = 'test_1002_PHY_ARGO-QC-DEV'
        )
        filters, param_basenames = converterPHY.generate_qc_schema_filters()
        print("param_basenames:")
        print(param_basenames)
        converterPHY.param_basenames = param_basenames

        # test pandas dataframe
        df = dummy_df
        for param in param_basenames:
            df = converterPHY.keep_best_values(df, param, "DATA_MODE")

        #ddf = ddf.compute()
        print("Resulting data:")
        print(f"memory usage: {df.memory_usage().sum()} bytes")
        with pd.option_context('display.max_columns', None, 'display.width', terminal_width):
            print(df)

        assert df.equals(sol_df)

        # test dask dataframe
        ddf = dd.from_pandas(dummy_df, npartitions=1)
        for param in param_basenames:
            ddf = dd.map_partitions(converterPHY.keep_best_values, ddf, param, "DATA_MODE")
        ddf = ddf.compute()

        print("Resulting data:")
        print(f"memory usage: {ddf.memory_usage().sum()} bytes")
        with pd.option_context('display.max_columns', None, 'display.width', terminal_width):
            print(ddf)

        assert ddf.equals(sol_df)

    def test_converter_argoqc_keep_pos_juld_best_values_phy(self):
        """
        Test that the original data is correctly re-casted in the QC format
        """

        # the resulting QCed df should have temperature values of 20 or 25
        dummy_data = {
            "PLATFORM_NUMBER": [1000001, 1000002, 1000003, 1000004, 1000005, 1000006],
            "LATITUDE": [35.1, 36.1, 37.1, -99.99, 39.1, 40.1],
            "LONGITUDE": [-70.1, -71.1, -72.1, -999.99, -74.1, -75.1],
            "POSITION_QC": [1, 2, 2, 9, 1, 1],
            "JULD": pd.to_datetime(
                ["2021-01-01", "1920-12-31", "2021-01-03", "2021-01-04", "2021-01-05", "2021-01-06"]
            ),
            "JULD_QC": [1, 3, 1, 1, 1, 1],
            "TEMP": [20.1, 20.1, 20.1, 20.1, 20.1, 20.1],
            "TEMP_QC": [1, 2, 2, 1, 1, 1],
            "TEMP_ERROR": [0.02, 0.02, 0.02, 0.02, 0.02, 0.02],
            "PSAL": [2.1, 2.1, 2.1, 2.1, 2.1, 2.1],
            "PSAL_QC": [1, 2, 2, 1, 1, 1],
            "PSAL_ERROR": [0.03, 0.03, 0.03, 0.03, 0.03, 0.03],
            "PRES": [2.1, 2.1, 2.1, 2.1, 2.1, 2.1],
            "PRES_QC": [1, 2, 2, 1, 1, 1],
            "PRES_ERROR": [0.05, 0.05, 0.05, 0.05, 0.05, 0.05],
            "DATA_MODE": ["R", "R", "R", "D", "D", "D"],
        }

        dummy_df = pd.DataFrame(dummy_data).convert_dtypes(dtype_backend='pyarrow')
        for param in dummy_df.columns:
            if param not in ["JULD", "DATA_MODE", "PLATFORM_NUMBER"] and "QC" not in param:
                dummy_df[ param ] = dummy_df[ param ].astype("float32[pyarrow]")
            elif "QC" in param:
                dummy_df[ param ] = dummy_df[ param ].astype("uint8[pyarrow]")
        print("Dummy data:")
        print(f"memory usage: {dummy_df.memory_usage().sum()} bytes")
        with pd.option_context('display.max_columns', None, 'display.width', terminal_width):
            print(dummy_df)

        sol_data = {
            "PLATFORM_NUMBER": [1000001, 1000002, 1000003, 1000004, 1000005, 1000006],
            "LATITUDE": [35.1, 36.1, 37.1, -99.99, 39.1, 40.1],
            "LONGITUDE": [-70.1, -71.1, -72.1, -999.99, -74.1, -75.1],
            "POSITION_QC": [1, 2, 2, 9, 1, 1],
            "JULD": pd.to_datetime(
                ["2021-01-01", "1920-12-31", "2021-01-03", "2021-01-04", "2021-01-05", "2021-01-06"]
            ),
            "JULD_QC": [1, 3, 1, 1, 1, 1],
            "TEMP": [20.1, pd.NA, 20.1, pd.NA, 20.1, 20.1],
            "TEMP_QC": [1, 2, 2, 1, 1, 1],
            "TEMP_ERROR": [0.02, 0.02, 0.02, 0.02, 0.02, 0.02],
            "PSAL": [2.1, pd.NA, 2.1, pd.NA, 2.1, 2.1],
            "PSAL_QC": [1, 2, 2, 1, 1, 1],
            "PSAL_ERROR": [0.03, 0.03, 0.03, 0.03, 0.03, 0.03],
            "PRES": [2.1, pd.NA, 2.1, pd.NA, 2.1, 2.1],
            "PRES_QC": [1, 2, 2, 1, 1, 1],
            "PRES_ERROR": [0.05, 0.05, 0.05, 0.05, 0.05, 0.05],
            "DATA_MODE": ["R", "R", "R", "D", "D", "D"],
        }
        sol_df = pd.DataFrame(sol_data).convert_dtypes(dtype_backend='pyarrow')
        for param in sol_df.columns:
            if param not in ["JULD", "DATA_MODE", "PLATFORM_NUMBER"] and "QC" not in param:
                sol_df[ param ] = sol_df[ param ].astype("float32[pyarrow]")
            elif "QC" in param:
                sol_df[ param ] = sol_df[ param ].astype("uint8[pyarrow]")
        print("Solution data:")
        print(f"memory usage: {sol_df.memory_usage().sum()} bytes")
        with pd.option_context('display.max_columns', None, 'display.width', terminal_width):
            print(sol_df)

        converterPHY = ConverterArgoQC(
            db = "ARGO",
            db_type="PHY",
            input_path = argo_phy_path,
            outdir_pq = outdir_phy_pqt,
            outdir_schema = './schemas/ArgoQC/',
            fname_pq = 'test_1002_PHY_ARGO-QC-DEV'
        )
        filters, param_basenames = converterPHY.generate_qc_schema_filters()
        print("param_basenames:")
        print(param_basenames)
        converterPHY.param_basenames = param_basenames

        # test pandas dataframe
        df = dummy_df
        df = converterPHY.keep_pos_juld_best_values(df, param_basenames)

        # check columns names
        print("Checking column names")
        pd.testing.assert_frame_equal(
            df,
            sol_df,
            check_dtype=False,
            check_index_type=False,
            check_column_type=False,
            check_frame_type=False,
            check_names=True,
            check_exact=False
        )

        # check df dtype
        print("Checking df dtype")
        pd.testing.assert_frame_equal(
            df,
            sol_df,
            check_dtype=True,
            check_index_type=False,
            check_column_type=False,
            check_frame_type=False,
            check_names=True,
            check_exact=False
        )

        # check columns dtypes
        print("Checking columns dtypes")
        pd.testing.assert_frame_equal(
            df,
            sol_df,
            check_dtype=True,
            check_index_type=False,
            check_column_type=True,
            check_frame_type=False,
            check_names=True,
            check_exact=False
        )

        print("Resulting data (pandas):")
        print(f"memory usage: {df.memory_usage().sum()} bytes")
        with pd.option_context('display.max_columns', None, 'display.width', terminal_width):
            print(df)

        assert df.equals(sol_df)

        # test dask dataframe
        ddf = dd.from_pandas(dummy_df, npartitions=1)
        ddf = dd.map_partitions(converterPHY.keep_pos_juld_best_values, ddf, param_basenames)
        ddf = ddf.compute()

        print("Resulting data (dask):")
        print(f"memory usage: {ddf.memory_usage().sum()} bytes")
        with pd.option_context('display.max_columns', None, 'display.width', terminal_width):
            print(ddf)

        assert ddf.equals(sol_df)

    def test_converter_argoqc_remove_all_NAs(self):
        """
        Test that the rows where all measurements are NA are removed
        """

        dummy_data = {
            "PLATFORM_NUMBER": [1000001, 1000002, 1000003, 1000004, 1000005, 1000006],
            "LATITUDE": [35.1, 36.1, 37.1, 38.1, 39.1, 40.1],
            "LONGITUDE": [-70.1, -71.1, -72.1, -73.1, -74.1, -75.1],
            "JULD": pd.to_datetime(
                ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04", "2021-01-05", "2021-01-06"]
            ),
            "TEMP": [20.1, 20.1, pd.NA, 25.1, 25.1, pd.NA],
            "TEMP_QC": [1, 2, 9, 1, 2, 9],
            "TEMP_ERROR": [pd.NA, pd.NA, pd.NA, 0.02, 0.02, 0.02],
            "PSAL": [2.1, 2.1, pd.NA, 5.1, 5.1, pd.NA],
            "PSAL_QC": [1, 2, 9, 1, 2, 9],
            "PSAL_ERROR": [pd.NA, pd.NA, pd.NA, 0.03, 0.03, 0.03],
            "PRES": [200.1, 200.1, pd.NA, 250.1, 250.1, pd.NA],
            "PRES_QC": [1, 2, 9, 1, 2, 9],
            "PRES_ERROR": [pd.NA, pd.NA, pd.NA, 0.05, 0.05, 0.05],
            "DATA_MODE": ["R", "R", "R", "D", "D", "D"],
            "DB_NAME": ["ARGO", "ARGO", "ARGO", "ARGO", "ARGO", "ARGO"]
        }

        dummy_df = pd.DataFrame(dummy_data).convert_dtypes(dtype_backend='pyarrow')
        print("Dummy data:")
        print(f"memory usage: {dummy_df.memory_usage().sum()} bytes")
        with pd.option_context('display.max_columns', None):
            print(dummy_df)

        sol_data = {
            "PLATFORM_NUMBER": [1000001, 1000002, 1000004, 1000005],
            "LATITUDE": [35.1, 36.1, 38.1, 39.1],
            "LONGITUDE": [-70.1, -71.1, -73.1, -74.1],
            "JULD": pd.to_datetime(
                ["2021-01-01", "2021-01-02", "2021-01-04", "2021-01-05"]
            ),
            "TEMP": [20.1, 20.1, 25.1, 25.1],
            "TEMP_QC": [1, 2, 1, 2],
            "TEMP_ERROR": [pd.NA, pd.NA, 0.02, 0.02],
            "PSAL": [2.1, 2.1, 5.1, 5.1],
            "PSAL_QC": [1, 2, 1, 2],
            "PSAL_ERROR": [pd.NA, pd.NA, 0.03, 0.03],
            "PRES": [200.1, 200.1, 250.1, 250.1],
            "PRES_QC": [1, 2, 1, 2],
            "PRES_ERROR": [pd.NA, pd.NA, 0.05, 0.05],
            "DATA_MODE": ["R", "R", "D", "D"],
            "DB_NAME": ["ARGO", "ARGO", "ARGO", "ARGO"]
        }
        sol_df = pd.DataFrame(sol_data).convert_dtypes(dtype_backend='pyarrow')
        print("Solution data:")
        print(f"memory usage: {sol_df.memory_usage().sum()} bytes")
        with pd.option_context('display.max_columns', None):
            print(sol_df)

        converterPHY = ConverterArgoQC(
            db = "ARGO",
            db_type="PHY",
            input_path = argo_phy_path,
            outdir_pq = outdir_phy_pqt,
            outdir_schema = './schemas/ArgoQC/',
            fname_pq = 'test_1002_PHY_ARGO-QC-DEV'
        )
        filters, param_basenames = converterPHY.generate_qc_schema_filters()
        print("param_basenames:")
        print(param_basenames)

        # testing pandas df
        df = converterPHY.remove_all_NAs(dummy_df, param_basenames)
        print("Resulting df:")
        print(f"memory usage: {df.memory_usage().sum()} bytes")
        with pd.option_context('display.max_columns', None):
            print(df)

        assert df.equals(sol_df)

        # testing dask df
        ddf = dd.map_partitions(converterPHY.remove_all_NAs, dd.from_pandas(dummy_df), param_basenames)
        ddf = ddf.compute().convert_dtypes(dtype_backend='pyarrow')
        print("Resulting ddf:")
        print(f"memory usage: {df.memory_usage().sum()} bytes")
        with pd.option_context('display.max_columns', None):
            print(df)

        assert ddf.equals(sol_df)

    def test_converter_argoqc_update_cols_bgc(self):
        """
        Test that the data types of the columns in the ARGO QC dataframe are as expected
        """
        converterBGC = ConverterArgoQC(
            db = "ARGO",
            db_type="BGC",
            input_path = argo_bgc_path,
            outdir_pq = outdir_bgc_pqt,
            outdir_schema = './schemas/ArgoQC/',
            fname_pq = 'test_1002_BGC_ARGO-QC-DEV'
        )

        fname = random.choice(glob.glob(argo_bgc_path + '/*.parquet'))
        ddf = converterBGC.read_pq(filename=fname)
        ddf = converterBGC.update_cols(ddf)

        for var in params.params["CROCOLAKE_BGC_QC"]:
            if var in ddf.columns:
                print(var)
                if var == "PLATFORM_NUMBER":
                    assert ddf.dtypes[var] == "int64[pyarrow]"
                elif var == "JULD":
                    assert ddf.dtypes[var] == "timestamp[ns][pyarrow]"
                elif var in ["LATITUDE","LONGITUDE"]:
                    assert ddf.dtypes[var] == "float64[pyarrow]"
                elif ("DATA_MODE" in var) or (var=="DB_NAME"):
                    assert isinstance(ddf.dtypes[var], pd.CategoricalDtype)
                elif "QC" in var:
                    assert ddf.dtypes[var] == "uint8[pyarrow]"
                    assert var[:-2]+"ADJUSTED_QC" not in ddf.columns
                else:
                    assert ddf.dtypes[var] == "float32[pyarrow]"
                if "ERROR" in var:
                    assert ddf.dtypes[var] == "float32[pyarrow]"
            elif "ERROR" in var:
                assert var[:-5]+"ADJUSTED_ERROR" not in ddf.columns
            else:
                print(f"Variable {var} not in dataframe.")

    def test_converter_argoqc_convert_phy(self):
        """Test that no error is raised during execution of convert() function
        and that a parquet output is generated. This does not test the content
        of the parquet output.
        """

        pq_files = glob.glob(argo_phy_path + '/*.parquet')
        assert len(pq_files) > 0
        random_file = random.choice(pq_files)

        converterPHY = ConverterArgoQC(
            db = "ARGO",
            db_type="PHY",
            input_path = argo_phy_path,
            outdir_pq = outdir_phy_pqt,
            outdir_schema = './schemas/ArgoQC/',
            fname_pq = 'test_1002_PHY_ARGO-QC-DEV',
        )

        ddf = converterPHY.read_to_df(random_file)
        assert not ddf.head().empty

        converterPHY.convert(random_file)

    def test_converter_argoqc_convert_bgc(self):
        """Test that no error is raised during execution of convert() function
        and that a parquet output is generated. This does not test the content
        of the parquet output.
        """
        pq_files = glob.glob(argo_bgc_path + '/*.parquet')
        assert len(pq_files) > 0
        random_file = random.choice(pq_files)

        converterBGC = ConverterArgoQC(
            db = "ARGO",
            db_type="BGC",
            input_path = argo_bgc_path,
            outdir_pq = outdir_bgc_pqt,
            outdir_schema = './schemas/ArgoQC/',
            fname_pq = 'test_1002_BGC_ARGO-QC-DEV',
        )

        ddf = converterBGC.read_to_df(random_file)
        assert not ddf.head().empty

        converterBGC.convert(random_file)

    def test_converter_add_derived_variables(self):
        """Test that add_derived_variables() executes"""
        data = {
            'LATITUDE': [35.00, 36.25],
            'LONGITUDE': [-70.00, -70.00],
            'PSAL': [1.1, 1.1],
            'PRES': [2.1, 1.9],
            'TEMP': [20.2, 22.9]
        }
        pdf = pd.DataFrame(data)
        ddf = dd.from_pandas(pdf, npartitions=2)

        # create converter simply to access function to test

        converterPHY = ConverterArgoQC(
            db = "ARGO",
            db_type="PHY",
            input_path = argo_phy_path,
            outdir_pq = outdir_phy_pqt,
            outdir_schema = './schemas/ArgoQC/',
            fname_pq = 'test_1002_PHY_ARGO-QC-DEV',
        )

        with pytest.warns(UserWarning):
            ddf = converterPHY.add_derived_variables(ddf)

        for var in ["ABS_SAL_COMPUTED","CONSERVATIVE_TEMP_COMPUTED","SIGMA1_COMPUTED"]:
            assert var in ddf.columns
            assert ddf.dtypes[var] == "float32[pyarrow]"

        print(ddf.compute())

    def test_converter_spraygliders_standardize_data(self):
        """
        Test SprayGliders data standardization works as expected
        """
        spray_path = spray_path
        outdir_spray_pqt = outdir_spray_path
        converterSG = ConverterSprayGliders(
            db = "SprayGliders",
            db_type="PHY",
            input_path = spray_path,
            outdir_pq = outdir_spray_pqt,
            outdir_schema = './schemas/SprayGliders/',
            fname_pq = 'test_1200_PHY_SPRAY-DEV'
        )
        with pytest.warns(UserWarning):
            ddf = converterSG.read_to_df(filename="NASCar.nc")

        assert ddf.dtypes["PLATFORM_NUMBER"] == "string[pyarrow]"
        assert ddf.dtypes["LATITUDE"] == "float64[pyarrow]"#pd.Float64Dtype()
        assert ddf.dtypes["LONGITUDE"] == "float64[pyarrow]"#pd.Float64Dtype()
        assert ddf.dtypes["JULD"] == "timestamp[ns][pyarrow]"#np.dtype("datetime64[ns]")
        assert ddf.dtypes["PRES"] == "float32[pyarrow]"#pd.Float32Dtype()
        assert ddf.dtypes["PRES_QC"] == "uint8[pyarrow]"#pd.UInt8Dtype()
        assert ddf.dtypes["PRES_ERROR"] == "float32[pyarrow]"#pd.Float32Dtype()
        assert ddf.dtypes["TEMP"] == "float32[pyarrow]"#pd.Float32Dtype()
        assert ddf.dtypes["TEMP_QC"] == "uint8[pyarrow]"#pd.UInt8Dtype()
        assert ddf.dtypes["TEMP_ERROR"] == "float32[pyarrow]"#pd.Float32Dtype()
        assert ddf.dtypes["PSAL"] == "float32[pyarrow]"#pd.Float32Dtype()
        assert ddf.dtypes["PSAL_QC"] == "uint8[pyarrow]"#pd.UInt8Dtype()
        assert ddf.dtypes["PSAL_ERROR"] == "float32[pyarrow]"#pd.Float32Dtype()
        assert ddf.dtypes["DB_NAME"] == "string[pyarrow]"

    def test_converter_spraygliders_convert_single_file(self):
        """Test that SprayGliders conversion executes; this test does not use
        convert() but its internal steps to check the dataframe is never empty
        """
        spray_path = spray_path
        outdir_spray_pqt = outdir_spray_path
        converterSG = ConverterSprayGliders(
            db = "SprayGliders",
            db_type="PHY",
            input_path = spray_path,
            outdir_pq = outdir_spray_pqt,
            outdir_schema = './schemas/SprayGliders/',
            fname_pq = 'test_1200_PHY_SPRAY-DEV'
        )
        with pytest.warns(UserWarning):
            ddf = dd.from_pandas(
                converterSG.read_to_df(filename="NASCar.nc")
            )
        assert not ddf.head().empty

        ddf = ddf.repartition(partition_size="300MB")
        assert not ddf.head().empty

        converterSG.to_parquet(ddf)

    def test_converter_spraygliders_convert_steps_multiple_files(self):
        """Test that SprayGliders conversion executes; this test does not use
        convert() but its internal steps to check the dataframe is never empty
        """
        client = Client(
            threads_per_worker=2,
            n_workers=1,
            memory_limit='auto'
        )

        spray_path = spray_path
        outdir_spray_pqt = outdir_spray_path
        converterSG = ConverterSprayGliders(
            db = "SprayGliders",
            db_type="PHY",
            input_path = spray_path,
            outdir_pq = outdir_spray_pqt,
            outdir_schema = './schemas/SprayGliders/',
            fname_pq = 'test_1200_PHY_SPRAY-DEV'
        )

        from dask.distributed import Lock
        lock=Lock()
        ddf = dd.from_map(
            converterSG.read_to_df,
            ["NASCar.nc", "Hawaii.nc"],
            lock=lock
        )
        assert not ddf.head().empty
        print(ddf.head())
        print(ddf.dtypes)

        ddf = ddf.repartition(partition_size="300MB")
        assert not ddf.head().empty

        converterSG.to_parquet(ddf)

        client.shutdown()

    def test_converter_spraygliders_convert_multiple_files(self):
        """Test that SprayGliders conversion executes; this test uses convert()
        """
        client = Client(
            threads_per_worker=2,
            n_workers=1,
            memory_limit='auto'
        )

        spray_path = spray_path
        outdir_spray_pqt = outdir_spray_path
        converterSG = ConverterSprayGliders(
            db = "SprayGliders",
            db_type="PHY",
            input_path = spray_path,
            outdir_pq = outdir_spray_pqt,
            outdir_schema = './schemas/SprayGliders/',
            fname_pq = 'test_1200_PHY_SPRAY-DEV'
        )

        converterSG.convert(["NASCar.nc", "Hawaii.nc"])

        client.shutdown()

    def test_converter_spraygliders_prepare_tmp(self):
        """Test that SprayGliders conversion executes; this test does not use
        convert() but its internal steps to check the dataframe is never empty
        """
        client = Client(
            threads_per_worker=2,
            n_workers=1,
            memory_limit='100GB',
            dashboard_address=':8787',
        )

        spray_path = "/vortexfs1/share/boom/users/enrico.milanese/originalDatabases/SprayGliders/"
        outdir_spray_pqt = "./tmp_pqt/" # this should not be used in this test actually
        tmp_nc_path = "./tmp_nc_chunks/"

        if os.path.isdir(tmp_nc_path):
            raise ValueError("tmp_nc_path already exists, please remove it before running the test.")

        spray_files = glob.glob(os.path.join(spray_path, '*.nc'))
        spray_names = [os.path.basename(f) for f in spray_files]
        print("spray_names:")
        print(spray_names)

        converterSG = ConverterSprayGliders(
            db = "SprayGliders",
            db_type="PHY",
            input_path = spray_path,
            outdir_pq = outdir_spray_pqt,
            outdir_schema = './schemas/SprayGliders/',
            fname_pq = 'test_1200_PHY_SPRAY-DEV',
            tmp_path = tmp_nc_path
        )

        from dask.distributed import Lock
        lock=Lock()

        # select three random files to test
        flist = random.sample(spray_names, k=3)
        print(f"Testing with {len(flist)} of {len(spray_names)} files")
        print("flist:")
        print(flist)

        converterSG.prepare_data(flist=flist,lock=lock)

        not_empty_dir = bool(os.listdir(tmp_nc_path))
        assert not_empty_dir == True

        for file in glob.glob(tmp_nc_path+"/*.nc"):
            try:
                ds = xr.open_dataset(file, engine="h5netcdf", chunks=None, cache=True)
            except Exception as e:
                assert False, f"Failed to open file {file}: {e}"
        assert True

        client.shutdown()

    def test_converter_spraygliders_read_to_ddf_phy(self):
        """Test that SprayGliders conversion executes; this test does not use
        convert() but its internal steps to check the dataframe is never empty
        """
        client = Client(
            threads_per_worker=20,
            n_workers=1,
            memory_limit='100GB',
            dashboard_address=':1419',
        )
        print("Dashboard address:")
        print(client.dashboard_link)

        spray_path = "/vortexfs1/share/boom/users/enrico.milanese/crocolaketools-public-fork/crocolaketools/test/tmp_nc_chunks/"
        outdir_spray_pqt = "./tmp_pqt/" # this should not be used in this test actually
        tmp_nc_path = "./tmp_nc_chunks/"

        spray_files = glob.glob(os.path.join(spray_path, '*.nc'))
        spray_names = [os.path.basename(f) for f in spray_files]
        print("spray_names:")
        print(spray_names)

        converterSG = ConverterSprayGliders(
            db = "SprayGliders",
            db_type="PHY",
            input_path = spray_path,
            outdir_pq = outdir_spray_pqt,
            outdir_schema = './schemas/SprayGliders/',
            fname_pq = 'test_1200_PHY_SPRAY-DEV'
        )

        from dask.distributed import Lock
        lock=Lock()

        flist = spray_names
        print(f"Testing with {len(flist)} of {len(spray_names)} files")
        print("flist:")
        print(flist)

        converterSG.convert(
            filenames=flist
        )

        client.shutdown()

        return

    def test_converter_cpr_read_to_df(self):
        """
        Test that the CPR CSV file is correctly read into a pandas DataFrame.
        """
        converter = ConverterCPR(
            db="CPR",
            db_type="BGC",
            input_path=cpr_path,
            outdir_pq=outdir_cpr_pqt,
            outdir_schema="./schemas/CPR/",
            fname_pq="test_cpr"
        )

        df = converter.read_to_df(filename="765141_v5_cpr-plankton-abundance.csv")

        # Check that the DataFrame is not empty
        assert not df.empty

        # Check that required columns are present (after renaming)
        required_columns = ["PLATFORM_NUMBER", "LATITUDE", "LONGITUDE", "JULD"]
        for col in required_columns:
            assert col in df.columns

    def test_converter_cpr_standardize_data(self):
        """
        Test that the CPR DataFrame is correctly standardized.
        """
        converter = ConverterCPR(
            db="CPR",
            db_type="BGC",
            input_path=cpr_path,
            outdir_pq=outdir_cpr_pqt,
            outdir_schema="./schemas/CPR/",
            fname_pq="test_cpr"
        )

        sample_data = {
            "SampleId": ["100DA-13", "100DA-14"],
            "Latitude": [48.66, 49.66],
            "Longitude": [-25.1733, -26.1733],
            "MidPoint_Date_UTC": ["1972-10-24T06:05Z", "1972-10-25T06:05Z"],
            "Year": [1972, 1972],
            "Month": [10, 10],
            "Day": [24, 25],
            "Hour": [6, 6]
        }
        df = pd.DataFrame(sample_data)

        # Standardize the DataFrame
        standardized_df = converter.standardize_data(df)

        # Check that the DataFrame is not empty
        assert not standardized_df.empty

        # Check that columns are renamed correctly
        assert "PLATFORM_NUMBER" in standardized_df.columns
        assert "LATITUDE" in standardized_df.columns
        assert "LONGITUDE" in standardized_df.columns
        assert "JULD" in standardized_df.columns

        # Check that the date column is converted to datetime
        assert str(standardized_df["JULD"].dtype) == "timestamp[ns][pyarrow]"

    def test_converter_cpr_convert(self):
        """
        Test that the CPR CSV file is correctly converted to Parquet format.
        """
        # Ensure the output directory exists
        os.makedirs(outdir_cpr_pqt, exist_ok=True)

        converter = ConverterCPR(
            db="CPR",
            db_type="BGC",
            input_path=cpr_path,
            outdir_pq=outdir_cpr_pqt,
            outdir_schema="./schemas/CPR/",
            fname_pq="test_cpr"
        )

        # Convert a sample CPR CSV file
        converter.convert(filenames="765141_v5_cpr-plankton-abundance.csv")  # Pass the filename here

        # Check that the output Parquet file exists
        output_files = glob.glob(os.path.join(outdir_cpr_pqt, "test_cpr_BGC*.parquet"))
        assert len(output_files) > 0, "No output Parquet files found"

        # Read the first Parquet file and check its contents
        df = pd.read_parquet(output_files[0])
        assert not df.empty
        assert "PLATFORM_NUMBER" in df.columns
        assert "LATITUDE" in df.columns
        assert "LONGITUDE" in df.columns
        assert "JULD" in df.columns


    def test_converter_saildrones_read_to_df(self):
        """
        Test that the Saildrone NetCDF file is correctly read into a pandas DataFrame.
        """
        # Get first available NetCDF file in the directory
        SD_files = glob.glob(os.path.join(saildrones_path, '*.nc'))
        if not SD_files:
            pytest.skip("No NetCDF files found in input directory")
        
        test_file = os.path.basename(SD_files[0])

        converter = ConverterSaildrones(
            config={
                "db": "Saildrones",
                "db_type": "BGC",
                "input_path": saildrones_path,
                "outdir_pq": outdir_saildrones_pqt,
                "outdir_schema": "./schemas/Saildrones/",
                "fname_pq": "test_saildrones"
            }
        )

        df = converter.read_to_df(filename=test_file)

        # Check that the DataFrame is not empty
        assert not df.empty

        # Check that required columns are present
        required_columns = ["PLATFORM_NUMBER", "LATITUDE", "LONGITUDE", "JULD", "PRES", "TEMP", "PSAL", "CHLA", "CDOM", "BBP700"]
        for col in required_columns:
            assert col in df.columns

    def test_converter_saildrones_standardize_data(self):
        """
        Test that the Saildrone DataFrame is correctly standardized.
        """
        converter = ConverterSaildrones(
            config={
                "db": "Saildrones",
                "db_type": "BGC",
                "input_path": saildrones_path,
                "outdir_pq": outdir_saildrones_pqt,
                "outdir_schema": "./schemas/Saildrones/",
                "fname_pq": "test_saildrones"
            }
        )

        # Get a sample NetCDF file from the input directory
        SD_files = glob.glob(os.path.join(saildrones_path, '*.nc'))
        if not SD_files:
            pytest.skip("No NetCDF files found in input directory")
        
        test_file = os.path.basename(SD_files[0])
        
        # Read the DataFrame using read_to_df
        df = converter.read_to_df(filename=test_file)

        # Check data types of key columns
        assert df.dtypes["PLATFORM_NUMBER"] == "string[pyarrow]"
        assert df.dtypes["LATITUDE"] == "float64[pyarrow]"
        assert df.dtypes["LONGITUDE"] == "float64[pyarrow]"
        assert df.dtypes["JULD"] == "timestamp[ns][pyarrow]"
        assert df.dtypes["PRES"] == "float32[pyarrow]"
        assert df.dtypes["TEMP"] == "float32[pyarrow]"
        assert df.dtypes["PSAL"] == "float32[pyarrow]"
        assert df.dtypes["DOXY"] == "float32[pyarrow]"
        assert df.dtypes["CHLA"] == "float32[pyarrow]"
        assert df.dtypes["BBP700"] == "float32[pyarrow]"
        assert df.dtypes["CDOM"] == "float32[pyarrow]"
        assert df.dtypes["DB_NAME"] == "string[pyarrow]"

    def test_converter_saildrones_convert(self):
        """
        Test that the Saildrone NetCDF file is correctly converted to Parquet format.
        """
        # Get first available NetCDF file in the directory
        SD_files = glob.glob(os.path.join(saildrones_path, '*.nc'))
        if not SD_files:
            pytest.skip("No NetCDF files found in input directory")
        
        test_file = os.path.basename(SD_files[0])

        # Ensure the output directory exists
        os.makedirs(outdir_saildrones_pqt, exist_ok=True)

        converter = ConverterSaildrones(
            config={
                "db": "Saildrones",
                "db_type": "BGC",
                "input_path": saildrones_path,
                "outdir_pq": outdir_saildrones_pqt,
                "outdir_schema": "./schemas/Saildrones/",
                "fname_pq": "test_saildrones"
            }
        )

        # Convert a sample Saildrone NetCDF file
        converter.convert(filenames=test_file)

        # Check that the output Parquet file exists
        output_files = glob.glob(os.path.join(outdir_saildrones_pqt, "test_saildrones_BGC*.parquet"))
        assert len(output_files) > 0, "No output Parquet files found"

        # Read the first Parquet file and check its contents
        df = pd.read_parquet(output_files[0])
        assert not df.empty
        assert "PLATFORM_NUMBER" in df.columns
        assert "LATITUDE" in df.columns
        assert "LONGITUDE" in df.columns
        assert "JULD" in df.columns
        assert "PRES" in df.columns
        assert "TEMP" in df.columns
        assert "PSAL" in df.columns
        assert "DOXY" in df.columns
        assert "CHLA" in df.columns
        assert "BBP700" in df.columns
        assert "CDOM" in df.columns
        assert "PRES_QC" in df.columns
        assert "TEMP_QC" in df.columns
        assert "PSAL_QC" in df.columns
        assert "DOXY_QC" in df.columns
        assert "BBP700_QC" in df.columns
        assert "CHLA_QC" in df.columns
        assert "CDOM_QC" in df.columns

    def test_converter_wrap_longitude(self):
        import numpy as np

        for j in range(2):
            if j==0:
                lon_list = [-70.00, 70.00, -181.10,  181.10, 0.,  180]
                solution = [-70.00, 70.00,  178.90, -178.90, 0., -180]
            else:
                lon_list = [359,  360]
                solution = [179, -180]
            data = {
                'LATITUDE': list(np.random.rand(len(lon_list))*180-90),
                'LONGITUDE': lon_list,
                'PSAL': list(np.random.rand(len(lon_list))+1),
                'PRES': list(np.random.rand(len(lon_list))*1000),
                'TEMP': list(np.random.rand(len(lon_list))*5+15),
            }
            pdf = pd.DataFrame(data)
            ddf = dd.from_pandas(pdf, npartitions=2)
            print("input data:")
            print(pdf)

            sol_df = pdf.copy()
            sol_df["LONGITUDE"] = solution

            # we need an instance of ConverterGLODAP to access the _wrap_longitude
            # method
            config = {
                'db': 'GLODAP',
                'db_type': 'PHY',
                'input_path': "./",
                'outdir_pq': "./",
                'outdir_schema': "./",
                'fname_pq': "test",
                'add_derived_vars': True,
                'overwrite': False,
            }
            ConverterPHY = ConverterGLODAP(config)

            if j==0:
                pdf = ConverterPHY._wrap_longitude(pdf)
                ddf = ConverterPHY._wrap_longitude(ddf).compute()
            else:
                pdf = ConverterPHY._wrap_longitude(
                    pdf,
                    shift_range=True,
                )
                ddf = ConverterPHY._wrap_longitude(ddf,shift_range=True).compute()

            print("solution:")
            print(sol_df)
            print("pdf[LONGITUDE]:")
            print(pdf["LONGITUDE"])
            print("ddf[LONGITUDE]:")
            print(ddf["LONGITUDE"])
            pd.testing.assert_frame_equal(pdf, sol_df)
            pd.testing.assert_frame_equal(ddf, sol_df)