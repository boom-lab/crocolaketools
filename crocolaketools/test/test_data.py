#!/usr/bin/env python3

import copy
import datetime
import os
import glob
import importlib.resources
import logging
from pathlib import Path
import random

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest
import yaml
import xarray as xr

from crocolaketools import db_params
from crocolaketools.utils.logger_configurator import configure_logging

####################################################################################################
class TestData:
#------------------------------------------------------------------------------#
# Set of tests that verify that data in the original dataset is in the parquet
# conversion. Note that because of the size of the data, the integrity tests
# are often performed on a subset of the whole database.

#------------------------------------------------------------------------------#
    def _get_scalar_from_ds(self, selected_scalar):
        """Get a scalar value from a dataset, it handles the case where the a
        value is of np datetime64 type

        Arguments:
        selected_scalar -- scalar from xarray data array for given variable and
                           indices

        Returns:
        value -- the value of the selected data, as a scalar or numpy datetime
                 depending on the original dtype

        """

        if np.issubdtype(selected_scalar.dtype, np.datetime64):
            return selected_scalar.values
        elif isinstance(selected_scalar.item(), bytes):
            try:
                return np.datetime64(
                    datetime.datetime.strptime(
                        selected_scalar.item().decode(), 
                        "%Y%m%d%H%M%S"
                    )
                )
            except ValueError: # handle 'byte' data that isn't datetime format
                return selected_scalar.item().decode()
        else:
            return selected_scalar.item()


#------------------------------------------------------------------------------#
    def _check_profiles(self,db_name,db_type,db_name_config=None):

        """Pick a random profile given PLATFORM_NUMBER and CYCLE_NUMBER, check that
        there is no duplicate row and that it sorted by increasing PRES values"""

        if db_name_config is None:
            db_name_config = db_name

        config_path = importlib.resources.files("crocolaketools.config").joinpath("config.yaml")
        config = yaml.safe_load(open(config_path))
        config = config[db_name_config + "_" + db_type]

        pq_path = str(os.path.abspath(Path(config["outdir_pq"])))
        print("parquet dataset path:", pq_path)

        ddf_plat_nb = dd.read_parquet(
            pq_path+"/",
            columns=["PLATFORM_NUMBER"]
        )
        frac = 20/len(ddf_plat_nb.drop_duplicates())
        sample_frac = np.min([1,frac]) # max 20 entries

        # get random platform numbers
        platform_numbers = (
            ddf_plat_nb["PLATFORM_NUMBER"]
            .drop_duplicates()
            .sample(frac=sample_frac, random_state=25)
            .compute()
        )

        logging.info(platform_numbers)

        for pn in platform_numbers:
            ddf_prof = dd.read_parquet(
                pq_path,
                columns=["CYCLE_NUMBER"],
                filters=[ ("PLATFORM_NUMBER", "==", pn)]
            )
            profs = (
                ddf_prof["CYCLE_NUMBER"]
                .drop_duplicates()
                .compute()
            )

            # test (at most) 10 random profiles
            for p in random.choices(profs.to_list(), k=np.min([10,len(profs.to_list())])):
                logging.info(f"PLATFORM_NUMBER = {pn}")
                logging.info(f"CYCLE_NUMBER = {p}")
                df = dd.read_parquet(
                    pq_path,
                    filters=[
                        ("PLATFORM_NUMBER", "==", pn),
                        ("CYCLE_NUMBER", "==", p)
                    ]
                )

                df = df.dropna(subset=["PRES"])
                if len(df) == 0:
                    # data for this profile are NaNs
                    continue

                # test that ddf has no duplicates for (pressure,time,lat,lon) values
                df = df[["PRES","JULD","LATITUDE","LONGITUDE"]].compute()
                # note that Argo's cycle number might have multiple profiles, so
                # the above still work, but it would not be unique by pressure
                # (this would be the case if we used N_PROF but CYCLE_NUMBER
                # seems to be the standard)
                logging.info(f"len(df): {len(df)}")
                logging.info(f"len(df.drop_duplicates): {len(df.drop_duplicates())}")

                if db_name != "GLODAP":
                    assert len(df) == len(df.drop_duplicates())

                # test that ddf is sorted by PRES
                def check_sorted(df):
                    """Check that df is sorted by PRES"""
                    return df["PRES"].is_monotonic_increasing
                condition = df.groupby(["JULD","LATITUDE","LONGITUDE"]).apply(check_sorted)

                assert condition.all()


#------------------------------------------------------------------------------#
    def _check_variables_nc(self,db_name,db_type,db_name_config=None,nc_pattern=None):

        """Pick a random variable in a random original netCDF file and find it
        in the parquet version, check that they are equal

        Arguments:
        db_name   --  database name as in params.py
        db_type   --  phy or bgc
        db_name_config  -- database name as in config.yaml if different from db_name
        nc_pattern      -- specific file name patterns for original netCDF files

        """

        if db_name_config is None:
            db_name_config = db_name
        if nc_pattern is None:
            nc_pattern = "*.nc"

        config_path = importlib.resources.files("crocolaketools.config").joinpath("config.yaml")
        config = yaml.safe_load(open(config_path))
        config = config[db_name_config + "_" + db_type]

        nc_path = config["input_path"]
        pq_path = config["outdir_pq"]

        # get list of original nc files
        if os.path.isdir(nc_path):
            nc_path = os.path.join(nc_path, "**", nc_pattern)
        nc_files = glob.glob(nc_path, recursive=True)

        logging.info(f"nc_path: {nc_path}")
        logging.info(f"Files found: {nc_files}")

        params_db2crocolake = db_params.params[db_name + "2CROCOLAKE"]
        # remove PLATFORM_NUMBER from params_db2crocolake because it needs to be dealt with separately
        # (in general it is not unique given lat, lon, profile)

        multi = ["CYCLE_NUMBER", "PLATFORM_NUMBER", "DATA_MODE", "DIRECTION",
                 "JULD_QC", "LATITUDE", "LONGITUDE", "POSITION_QC", "JULD"]

        # PLATFORM_NUMBER needs to be handled differently for spray gliders
        # because it's not 1:1 conversion but there is some extra step (not
        # implemented yet)
        if db_name == "SprayGliders":
            params_in_crocolake = [k for k, v in params_db2crocolake.items()
                                   if v not in ["PLATFORM_NUMBER"]]
        else:
            params_in_crocolake = params_db2crocolake.keys()

        params_crocolake2db = db_params.params[ "CROCOLAKE2" + db_name]
        lat_name = params_crocolake2db["LATITUDE"]
        lon_name = params_crocolake2db["LONGITUDE"]

        for j in range(1000):
            nc_file = random.choice( nc_files )

            if db_name == "Argo":
                ds = xr.open_dataset(nc_file, engine="argo")
            elif db_name in ["OleanderXBT", "Saildrones"]:
                ds = xr.open_dataset(nc_file, engine="netcdf4")
            else:
                ds = xr.open_dataset(nc_file, engine="h5netcdf")

            #only test variables that are preserved in crocolake
            ds_vars = list(ds.data_vars)
            logging.info(f"ds_vars:{ds_vars}")
            logging.info(f"params_crocolake:{params_in_crocolake}")

            variables = list(
                set(list(ds.data_vars)) & set(params_in_crocolake)
            )

            if db_name == "Saildrones":
                # Exclude coordinate variables ('latitude', 'longitude', 'time') for Saildrones
                # since they do not have corresponding depth information, which is required 
                # for uniquely identifying each row in the dataset.
                excluded_vars = {lat_name, lon_name, "time"}
                variables = [v for v in variables if v not in excluded_vars]

            logging.info(f"variables:{variables}")
            random_var = random.choice(variables)
            var_data = ds[random_var]
            indices = {dim: random.randint(0, size - 1) for dim, size in var_data.sizes.items()}
            if len(ds[random_var].dims) > 0:
                nc_value = self._get_scalar_from_ds(var_data.isel(**indices))
            else:
                nc_value = self._get_scalar_from_ds(var_data)

            if len(ds[random_var].dims) > 0:
                shared_indices_lat = {dim: idx for dim, idx in indices.items() if dim in ds[lat_name].dims}
                shared_indices_lon = {dim: idx for dim, idx in indices.items() if dim in ds[lon_name].dims}
            else: # scalar has no indices, we just need to assign
                shared_indices_lat = {dim: random.randint(0, size - 1) for dim, size in ds[lat_name].sizes.items()}
                shared_indices_lon = shared_indices_lat

            nc_lat = self._get_scalar_from_ds(ds[lat_name].isel(**shared_indices_lat))
            nc_lon = self._get_scalar_from_ds(ds[lon_name].isel(**shared_indices_lon))

            # Some Spray Gliders data have nan for lat and lon, the target
            # variable seems to be nan too in that case; it doesn't hurt to keep
            # them in the parquet database for now, but we could filter them out
            # if np.isnan(nc_lat) or np.isnan(nc_lon):
            #     continue

            logging.info(f"nc_file: {nc_file}")
            logging.info(f"random_var: {random_var}")
            logging.info(f"indices: {indices}")
            logging.info(f"coordinates:")
            for k,v in indices.items():
                logging.info(f"{k} : {self._get_scalar_from_ds(ds[k][v])}")
            logging.info(f"nc_value: {nc_value}")

            # get coords and variable names in crocolake
            var_pq = params_db2crocolake[random_var]
            cols_pq = [var_pq]
            indices_pq = {}
            for k, v in indices.items():
                if k in params_db2crocolake:
                    indices_pq[ params_db2crocolake[k] ] = self._get_scalar_from_ds(ds[k][v])
                else:
                    if db_name == "OleanderXBT":
                        nc_depth = self._get_scalar_from_ds(ds["depth"].isel(**indices))
                        indices_pq[ "DEPTH" ] = nc_depth
                    elif db_name == "Saildrones":
                        depth_map = db_params.params["Saildrones_depth_map"]
                        nc_depth = depth_map[random_var]
                        nc_juld = self._get_scalar_from_ds(ds["time"].isel(**indices))
                        indices_pq[ "DEPTH" ] = np.float32(nc_depth)
                        indices_pq[ "JULD" ] = nc_juld
            indices_pq[ "LATITUDE" ] = nc_lat
            indices_pq[ "LONGITUDE" ] = nc_lon
            if db_name_config != "ARGO-GDAC":
                indices_pq[ "LONGITUDE" ] = (indices_pq[ "LONGITUDE" ] - 180) % 360 - 180
            cols_pq.extend(indices_pq.keys())

            logging.info(f"var_pq: {var_pq}")
            logging.info(f"indices_pq: {indices_pq}")
            pq_filters = [(column, "==", value) for column, value in indices_pq.items()]
            logging.info(f"filters: {pq_filters}")

            ddf = dd.read_parquet(
                pq_path,
                columns=[var_pq],
                filters=pq_filters,
            )
            ddf = ddf.compute() # this should be one row or an empty dataframe
                                # (if the was missing and the whole row it ended
                                # up into contained missing data that was thus
                                # discarded)
            if ddf.shape[0] == 0:
                # if the original data ended in a row with all observations as
                # pd.NAs the row was dropped as it did not contain relevant info
                logging.info("pq_value was pd.NA and discarded")

                if isinstance(nc_value, (float, int)) and nc_value < -1e20:
                    # some missing data might be stored as extremely large negative num
                    # (e.g, -9.999900276792041e+20). that should be treated as missing
                    nc_value = np.nan
                    
                assert pd.isna(nc_value)
                continue

            # otherwise ddf has multiple rows only for the variables in multi or
            # if random_var was constant for a given file (float), but all rows
            # should be identical
            if var_pq in multi or len(ds[random_var].dims)==0:
                ddf = ddf.drop_duplicates()

            # otherwise has at most one row
            assert ddf.shape[0] == 1


            pq_value = ddf[var_pq].values[0] # this should be a scalar or a pd.NA
            logging.info(f"pq_value: {pq_value}")

            if pd.isna(pq_value):
                # check that also original source is NaN or pd.NA
                assert pd.isna(nc_value)

            elif np.isscalar(pq_value) or isinstance(pq_value, pd.Timestamp):
                # CrocoLake measured variables are float32, but original dataset
                # might have float64 precision
                if np.issubdtype(type(pq_value), np.integer):
                    pq_value = np.int32(pq_value)
                    nc_value = np.int32(nc_value)
                elif np.issubdtype(type(pq_value), np.floating):
                    pq_value = np.float32(pq_value)
                    nc_value = np.float32(nc_value)
                assert pq_value == nc_value

            else:
                assert False, "value in CrocoLake is not a scalar nor a pd.NA"

#------------------------------------------------------------------------------#
    def _check_variables_csv(self, db_name, db_type):
        """Compare a sample of valid SPOTS CSV observations with Parquet."""
        config_path = importlib.resources.files("crocolaketools.config").joinpath(
            "config.yaml"
        )
        with open(config_path) as config_file:
            config = yaml.safe_load(config_file)[db_name + "_" + db_type]
        csv_path = os.path.abspath(os.path.join(
            importlib.resources.files("crocolaketools.config"),
            config["input_path"],
            "spots.csv",
        ))
        source = pd.read_csv(csv_path)
        source["CTDPRS"] = source["CTDPRS"].astype("float32")
        source["TIME"] = source["TIME"].fillna(0)
        source["JULD"] = pd.to_datetime(
            source["DATE"].astype("Int64").astype(str)
            + source["TIME"].astype("Int64").astype(str).str.zfill(4),
            format="%Y%m%d%H%M",
            errors="coerce",
        )
        profile_columns = ["TimeSeriesSite", "CRUISE", "STNNBR", "CASTNO"]
        source["_profile_key"] = (
            source[profile_columns]
            .fillna("")
            .astype("string")
            .agg("|".join, axis=1)
        )
        profiles = (
            source[profile_columns + ["_profile_key"]]
            .drop_duplicates()
            .sort_values(profile_columns)
        )
        profiles["profile_nb"] = (
            profiles.groupby("TimeSeriesSite").cumcount() + 1
        ).astype("int32")
        source["profile_nb"] = source["_profile_key"].map(
            profiles.set_index("_profile_key")["profile_nb"]
        )
        source = source[source["SALNTY_FLAG_W"].eq(2)].head(20)
        source = source.drop(columns="_profile_key")

        rename = db_params.params["SPOTS2CROCOLAKE"]
        parquet = dd.read_parquet(
            os.path.abspath(os.path.join(
                importlib.resources.files("crocolaketools.config"),
                config["outdir_pq"],
            ))
        ).compute()
        for source_name, parquet_name in rename.items():
            if source_name not in source or parquet_name not in parquet:
                continue
            if parquet[parquet_name].notna().sum() == 0:
                continue
            if source_name.endswith("_FLAG_W"):
                value_name = source_name[:-7]
                value_output = rename.get(value_name)
                if value_output in parquet and parquet[value_output].notna().sum() == 0:
                    continue
            for _, row in source.iterrows():
                matches = parquet[
                    parquet["PLATFORM_NUMBER"].eq(row["TimeSeriesSite"])
                    & parquet["CYCLE_NUMBER"].eq(row["profile_nb"])
                    & parquet["JULD"].eq(row["JULD"])
                    & np.isclose(parquet["LATITUDE"], row["LATITUDE"])
                    & np.isclose(parquet["LONGITUDE"], row["LONGITUDE"])
                    & np.isclose(parquet["PRES"], row["CTDPRS"])
                ]
                assert not matches.empty
                expected = row[source_name]
                flag_name = source_name + "_FLAG_W"
                if flag_name in source and row[flag_name] != 2:
                    expected = np.nan
                if source_name.endswith("_FLAG_W"):
                    value = row[source_name[:-7]]
                    if pd.isna(value) or value == -999:
                        expected = np.nan
                actual = matches.iloc[0][parquet_name]
                if pd.isna(expected) or expected == -999:
                    assert pd.isna(actual)
                else:
                    assert not pd.isna(actual), (
                        f"Missing converted value for {source_name} "
                        f"({parquet_name}) at source row {row.name}"
                    )
                    if isinstance(actual, str) or isinstance(expected, str):
                        assert actual == expected
                    else:
                        assert np.float32(actual) == np.float32(expected)

#------------------------------------------------------------------------------#
    def test_data_integrity_spraygliders_phy(self):
        self._check_variables_nc(
            db_type="PHY",
            db_name="SprayGliders"
        )

#------------------------------------------------------------------------------#
    def test_data_integrity_spraygliders_bgc(self):

        self._check_variables_nc(
            db_type="BGC",
            db_name="SprayGliders"
        )
         
#------------------------------------------------------------------------------#
    def test_data_integrity_saildrones_phy(self):
        self._check_variables_nc(
            db_type="PHY",
            db_name="Saildrones"
        )

#------------------------------------------------------------------------------#
    def test_data_integrity_saildrones_bgc(self):

        self._check_variables_nc(
            db_type="BGC",
            db_name="Saildrones"
        )
     
#------------------------------------------------------------------------------#
    def test_data_integrity_oleanderXBT_phy(self):
        self._check_variables_nc(
            db_type="PHY",
            db_name="OleanderXBT"
        )

#------------------------------------------------------------------------------#
    def test_data_integrity_argogdac_phy(self):
        self._check_variables_nc(
            db_type="PHY",
            db_name="Argo",
            db_name_config="ARGO-GDAC",
            nc_pattern="*_prof.nc"
        )

#------------------------------------------------------------------------------#
    def test_data_integrity_argogdac_bgc(self):
        self._check_variables_nc(
            db_type="BGC",
            db_name="Argo",
            db_name_config="ARGO-GDAC",
            nc_pattern="*_Sprof.nc"
        )

#------------------------------------------------------------------------------#
    def test_data_integrity_spots_phy(self):
        self._check_variables_csv("SPOTS", "PHY")

    def test_data_integrity_spots_bgc(self):
        self._check_variables_csv("SPOTS", "BGC")

#------------------------------------------------------------------------------#
    def test_profiles_spraygliders_phy(self):
        self._check_profiles(
            db_type="PHY",
            db_name="SprayGliders",
        )

#------------------------------------------------------------------------------#
    def test_profiles_spraygliders_bgc(self):
        self._check_profiles(
            db_type="BGC",
            db_name="SprayGliders",
        )

#------------------------------------------------------------------------------#
    def test_profiles_saildrones_phy(self):
        self._check_profiles(
            db_type="PHY",
            db_name="Saildrones",
        )

#------------------------------------------------------------------------------#
    def test_profiles_saildrones_bgc(self):
        self._check_profiles(
            db_type="BGC",
            db_name="Saildrones",
        )

#------------------------------------------------------------------------------#
    def test_profiles_oleanderXBT_phy(self):
        self._check_profiles(
            db_type="PHY",
            db_name="OleanderXBT"
        )

#------------------------------------------------------------------------------#
    def test_profiles_argogdac_phy(self):
        self._check_profiles(
            db_type="PHY",
            db_name="Argo",
            db_name_config="ARGO-GDAC",
        )

#------------------------------------------------------------------------------#
    def test_profiles_argogdac_bgc(self):
        self._check_profiles(
            db_type="BGC",
            db_name="Argo",
            db_name_config="ARGO-GDAC",
        )

#------------------------------------------------------------------------------#
    def test_profiles_argoqc_phy(self):
        self._check_profiles(
            db_type="PHY",
            db_name="Argo",
            db_name_config="ARGO",
        )

#------------------------------------------------------------------------------#
    def test_profiles_argoqc_bgc(self):
        self._check_profiles(
            db_type="BGC",
            db_name="Argo",
            db_name_config="ARGO",
        )

#------------------------------------------------------------------------------#
    def test_profiles_glodap_phy(self):
        self._check_profiles(
            db_type="PHY",
            db_name="GLODAP",
        )

#------------------------------------------------------------------------------#
    def test_profiles_glodap_bgc(self):
        self._check_profiles(
            db_type="BGC",
            db_name="GLODAP",
        )
