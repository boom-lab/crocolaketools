#!/usr/bin/env python3

## @file converter.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Fri 04 Oct 2024

##########################################################################
import os
import yaml
import warnings
import dask.dataframe as dd
from dask.distributed import Lock
import gsw
import importlib.resources
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import shutil
import xarray as xr
from crocolakeloader import params
from crocolaketools.converter import unit_converter
##########################################################################


class Converter:

    """class Converter: methods to generate parquet schemas for different
    databases and different versions
    """

    # ------------------------------------------------------------------ #
    # Constructors/Destructors                                           #
    # ------------------------------------------------------------------ #

    def __init__(self, config=None):
        """Constructor

        Arguments:

        config -- configuration dictionary, it must contains at least db and
                  db_type; other values as below; if any value is not specified,
                  defaults in config.yaml are used; vice versa, if a values is
                  specified, the corresponging entry in config.yaml is
                  overwritten with the user-specified value

        db            -- database name to generate schema for
        db_type       -- type of database desired (PHY or BGC parameters)
        input_path    -- path to file(s) to be converted
        outdir_pq     -- path to directory to output converted database
        outdir_schema -- path to directory to output schema(s)
        fname_pq      -- name of the parquet file to be generated
        add_derived_vars -- flag to add derived variables to the database
        overwrite     -- flag to overwrite existing parquet files
        tmp_path      -- path to temporary directory to store intermediate files
        """

        if config is not None:
            db = config['db']
            db_type = config['db_type'].upper()

            config_path = importlib.resources.files("crocolaketools.config").joinpath("config.yaml")
            base_path = importlib.resources.files("crocolaketools.config")
            config_disk = yaml.safe_load(open(config_path))
            config_disk = config_disk[db + "_" + db_type]

            config_user_keys = list(config.keys())
            config_disk_keys = list(config_disk.keys())

            read_keys = [k for k in config_disk_keys if k not in config_user_keys]
            if len(read_keys)>0:
                for k in ["db","db_type"]:
                    if not config[k] == config_disk[k]:
                        warnings.warn(f"User-specified and config file are not matching at key {k} (got {config[k]} and {config_disk[k]}), the user-specified value {config[k]} is used")
            for k in read_keys:
                config[k] = config_disk[k]

            print("Converter configuration:")
            print(config)

            input_path = os.path.abspath(os.path.join(base_path, config["input_path"]))
            outdir_pq = os.path.abspath(os.path.join(base_path, config["outdir_pq"]))
            outdir_schema = os.path.abspath(os.path.join(base_path, config["outdir_schema"]))
            fname_pq = config["fname_pq"]
            add_derived_vars = config["add_derived_vars"]
            overwrite = config["overwrite"]
            if config["tmp_path"] is None:
                tmp_path = None
            else:
                tmp_path = os.path.abspath(os.path.join(base_path, config["tmp_path"]))

        else:
            raise ValueError("No config argument provided.")

        if isinstance(db,str):
            if db in params.databases:
                self.db = db
                print("Setting up converter for " + self.db + " database.")
            else:
                raise ValueError("Database db must be one of " + str(params.databases))
        elif db is not None:
            raise ValueError("Database db not a string.")
        else:
            print("No database provided, only the reference schema is generated.")

        if isinstance(db_type,str):
            if db_type in ["PHY","BGC"]:
                self.db_type = db_type
                print("Using " + self.db_type + " parameters.")
            else:
                raise ValueError("Database db must be one of " + str(["PHY","BGC"]))
        elif db is not None:
            raise ValueError("Database type db_type not a string.")

        if input_path is None:
            raise ValueError("No input file path provided.")
        if input_path[-1] != "/":
            input_path = input_path + "/"
        if len(os.listdir(input_path))==0:
            raise ValueError(f"Input folder {input_path} is empty. If you are using config.yaml, is the relative path correct?")
        self.input_path = input_path
        print("Original files read from " + self.input_path)

        if outdir_schema is None:
            self.outdir_schema = "./schemas/"
        else:
            self.outdir_schema = outdir_schema
        print("Schema(s) will be stored at " + self.outdir_schema)

        if outdir_pq is None:
            self.outdir_pq = "./parquet/"
        else:
            self.outdir_pq = outdir_pq
            if self.outdir_pq[-1] != "/":
                self.outdir_pq = self.outdir_pq + "/"
        print("Parquet database will be stored at " + self.outdir_pq)

        if fname_pq is None:
            self.fname_pq = self.db+"_"+self.db_type+".parquet"
        else:
            if fname_pq[-8:] == ".parquet": # removing parquet extension if present
                self.fname_pq = fname_pq[:-8]
            else:
                self.fname_pq = fname_pq
            if self.db_type not in self.fname_pq:
                self.fname_pq = self.fname_pq+"_"+self.db_type
        print("Parquet database will be called " + self.fname_pq)

        self.generate_reference_schema()

        self.add_derived_vars = add_derived_vars
        if self.add_derived_vars:
            print("Derived variables will be added.")

        # Generate temporary folder variable
        if tmp_path is None:
            self.tmp_path = "./tmp/"
        else:
            if tmp_path[-1] != "/":
                tmp_path = tmp_path + "/"
            self.tmp_path = tmp_path

        print("Temporary files will be stored at " + self.tmp_path)

        self.overwrite = overwrite

        self.tmp_paths_to_remove = None

        self.generate_dtypes_maps()

        # This should be false unless you're using from_delayed to generate the
        # dask dataframe
        self.call_guess_schema = False

        # Initialize unit conversion mapping - override in subclasses
        self.cols_to_convert = {"skip": "skip"}

    # ------------------------------------------------------------------ #
    # Methods                                                            #
    # ------------------------------------------------------------------ #

#------------------------------------------------------------------------------#
## Generate maps
    def generate_dtypes_maps(self):
        """Generate dictionaries containing maps to convert names of pyarrow
        dtypes between pandas and pyarrow backends"""

        self.pa2pd_dtype_map = {
            pa.int8(): "int8[pyarrow]",
            pa.int16(): "int16[pyarrow]",
            pa.int32(): "int32[pyarrow]",
            pa.int64(): "int64[pyarrow]",
            pa.uint8(): "uint8[pyarrow]",
            pa.uint16(): "uint16[pyarrow]",
            pa.uint32(): "uint32[pyarrow]",
            pa.uint64(): "uint64[pyarrow]",
            pa.bool_(): "bool[pyarrow]",
            pa.float32(): "float32[pyarrow]",
            pa.float64(): "float64[pyarrow]",
            pa.string(): "string[pyarrow]",
            pa.timestamp("ns"): pd.ArrowDtype(pa.timestamp("ns")),
        }

        # multiple names map to the same dtype in pandas, and they might not be
        # the same used when going from pyarrow to pandas
        self.pd2pa_dtype_map = {v: k for k, v in self.pa2pd_dtype_map.items()}
        self.pd2pa_dtype_map["timestamp[ns][pyarrow]"] = pa.timestamp("ns")
        self.pd2pa_dtype_map["float[pyarrow]"] = pa.float32()
        self.pd2pa_dtype_map["double[pyarrow]"] = pa.float64()
        self.pd2pa_dtype_map[pd.StringDtype("pyarrow")] = pa.string()


#------------------------------------------------------------------------------#
## Convert file
    def convert(self, filenames=None, filepath=None):
        """Convert filename to parquet. This executes all the steps needed from
        reading to converting to storing, and might not work for non-simple
        workflows. You can still refer to it to build your own workflow.
        """

        if filenames is None:
            if filepath is None:
                guess_path = self.input_path
                warnings.warn("Filename(s) not provided, guessing from input path: " + guess_path)
            else:
                guess_path = filepath
                warnings.warn("Filename(s) not provided, guessing from provided file path: " + guess_path)
            filenames = os.listdir(guess_path)
        print("List of files to convert: ", filenames)

        # adapt for single filename input
        if isinstance(filenames,str):
            filenames = [filenames]

        lock = Lock()
        # if len(filenames) > 1:
        #     print("reading reference files")
        ddf = self.read_to_ddf(
            flist=filenames,
            lock=lock
        )

        if not isinstance(ddf,dd.DataFrame):
            raise TypeError("ddf must be a dask dataframe, not: ", type(df))

        if self.add_derived_vars:
            print("adding derived variables")
            ddf = self.add_derived_variables(ddf)

        ddf = self.convert_units(ddf)

        ddf = self.reorder_columns(ddf)

        ddf = ddf.drop_duplicates()

        ddf = self.sort_rows(ddf)

        print("repartitioning dask dataframe")
        ddf = ddf.repartition(partition_size="300MB")

        print("save to parquet")
        self.to_parquet(ddf)

        return

#------------------------------------------------------------------------------#
## Re-order columns
    def reorder_columns(self,ddf):
        """Re-order columns to have DB_NAME, JULD, LATITUDE, LONGITUDE,
        PLATFORM_NUMBER, CYCLE_NUMBER first

        Argument:
        ddf -- dask dataframe to re-order

        Returns:
        ddf -- re-ordered dask dataframe
        """

        cols = ddf.columns.to_list()
        first_cols = [
            "DB_NAME",
            "JULD",
            "LATITUDE",
            "LONGITUDE",
            "PLATFORM_NUMBER",
            "CYCLE_NUMBER"
        ]
        for col in first_cols:
            cols.remove(col)
        cols = first_cols + cols
        ddf = ddf[cols]

        return ddf

#------------------------------------------------------------------------------#
## Read file to convert into a pandas dataframe
    def read_to_df(self, filename=None, lock=None):

        """currently implemented on db by db basis"""
        return NotImplementedError

#------------------------------------------------------------------------------#
## Store file to parquet version
    def guess_schema(self,ddf):
        """Guess schema from dask dataframe. It seems that dataframes build from
        delayed objects do not trigger computations the same ways as those build
        from from_map, and they do not know the schema that is generated during
        standardizing. The dataframe though has the right dtypes and names, so
        we generate it here.
        This is a workaround that should be made more robust in the future.

        Argument:
        ddf -- dask dataframe to get schema from

        Returns:
        schema -- pyarrow schema of the dask dataframe
        """

        ddf_dtypes = ddf.dtypes
        ddf_schema = []
        for key in ddf_dtypes.keys():
            f = pa.field(
                key, self.pd2pa_dtype_map[ddf_dtypes[key]]
            )
            ddf_schema.append(f)
        ddf_schema = pa.schema(ddf_schema)

        return ddf_schema

#------------------------------------------------------------------------------#
## Store file to parquet version
    def to_parquet(self,df):

        if self.call_guess_schema:
            schema_pq = self.guess_schema(df)
        else:
            schema_pq = self.schema_pq

        name_function = lambda x: f"{self.fname_pq}_{x:03d}.parquet"

        print(f"{self.fname_pq}.parquet")

        print("Saving " + self.db + ", " + self.db_type + " version, to " + self.outdir_pq)

        os.makedirs(self.outdir_pq, exist_ok=True)

        append = False
        overwrite = True
        if len(os.listdir(self.outdir_pq))>0:
            if self.overwrite:
                print("Folder exists and contains files. All content is being removed before and new files created.")
            else:
                raise ValueError("Folder exists and contains files. Overwrite is set to False, but no append is possible. Please remove the folder or set overwrite to True.")

        df.to_parquet(
            self.outdir_pq,
            engine="pyarrow",
            name_function=name_function,
            append=append,
            overwrite=overwrite,
            write_metadata_file = True,
            write_index=False,
            schema=schema_pq
        )

#------------------------------------------------------------------------------#
## Generating schema
    def generate_reference_schema(self, vars_schema=None):

        if vars_schema is None or vars_schema == "QC":
            vars_schema = "_QC"
        elif not vars_schema == "_ALL":
            raise ValueError("vars_schema must be 'QC' or '_ALL'.")

        param = params.params["CROCOLAKE_" + self.db_type + vars_schema].copy()

        self.fields = []
        for p in param:

            if p in ["N_PROF","N_LEVELS","CYCLE_NUMBER"]:
                f = pa.field( p, pa.int32() )

            elif "_QC" in p:
                f = pa.field( p, pa.uint8() )

            elif p in ["LATITUDE","LONGITUDE"]:
                f = pa.field( p, pa.float64() )

            elif p in ['JULD','DATE_UPDATE']:
                f = pa.field( p, pa.from_numpy_dtype(np.dtype("datetime64[ns]") ) )

            elif "DATA_MODE" in p or p=="DB_NAME":
                f = pa.field( p, pa.string() )  #this should become a dictionary
                                                #at some point

            elif p=="PLATFORM_NUMBER":
                f = pa.field( p, pa.string() )

            else:
                f = pa.field( p, pa.float32() )

            self.fields.append(f)

        self.reference_schema = pa.schema( self.fields )
        self.reference_schema_name = "CROCOLAKE_"+ self.db_type  + vars_schema + "_schema.metadata"

#------------------------------------------------------------------------------#
## Generating schema
    def generate_schema(self,param_names):
        """Generate consistent pyarrow and pandas schema. The pandas schema is
        used to standardize the dataframes, while the pyarrow schema is used to
        store the parquet files. If your data is already in pyarrow format, you
        still need to call this function to generate the pyarrow schema for
        storage later, but you probably won"t need to use the pandas schema

        Argument:
        param_names -- list of parameter names in the dataframe

        Generates:

        schema_pq -- pyarrow schema with all the variables in both param_names
                     and the reference schema
        schema_pd -- pandas schema equivalent to schema_pq
        """

        self.schema_pq = self.reference_schema

        # set of parameters in standard db and in input db
        todrop = [item for item in self.schema_pq.names if item not in param_names]

        if len(todrop) == len(self.schema_pq.names):
            raise Exception("All fields removed from schema. Are you sure you renamed the dataframe's columns to CROCOLAKE's standard names?")

        for p in todrop:
            idx = self.schema_pq.get_field_index(p)

            if idx == -1:
                idcs = self.schema_pq.get_all_field_indices(p)
                if len(idcs) > 1:
                    raise ValueError(p + " found multiple times in schema.")
                raise ValueError(p + " not found in schema; are you sure you renamed the dataframe's columns to CROCOLAKE's standard names?")
            else:
                self.schema_pq = self.schema_pq.remove(idx)

        self.schema_pd = self.__translate_pq_to_pd(self.schema_pq)

        return

#------------------------------------------------------------------------------#
## Trim schema if db type is PHY
    def trim_schema(self,df):

        """Remove BGC parameters and keep PHY only

        Argument:

        db_type -- currently only works with "PHY", as the input dataset has
                   already all and only the BGC parameters, and PHY is a subset
                   of them

        Returns:
        df -- trimmed dataset with PHY parameters only
        """

        if self.db_type != "PHY":
            raise ValueError("Database type can only be PHY to trim schema.")

        db_phy_name = self.db + self.db_type
        param = params.params[db_phy_name].copy()
        schema_phy_pq = self.schema_pq

        columns_to_drop = []
        for p in param:
            if p not in schema_phy_pq.names:
                columns_to_drop.append(p)
                idx = schema_phy_pq.get_field_index(p)
                schema_phy_pq = schema_phy_pq.remove(idx)

        # update schemas so that standardize and store_to_parquet use the
        # correct schemas
        self.schema_pq = schema_phy_pq
        self.schema_pd = self.__translate_pq_to_pd(self.schema_pq)

        df.drop(columns=columns_to_drop, inplace=True)

        return df

#------------------------------------------------------------------------------#
## Convert parquet schema to pandas
    def standardize_data(self,data):
        """Standardize pandas dataframe to schema consistent across databases

        Argument:
        data -- pandas or dask dataframe or xarray dataset

        Returns:
        data -- homogenized pandas dataframe
        """

        print("Renaming columns")
        rename_map = params.params[self.db + "2CROCOLAKE"]

        if isinstance(data,(pd.DataFrame,dd.DataFrame)):
            data = data.rename(columns=rename_map)
            data_vars = data.columns.to_list()
        elif isinstance(data,xr.Dataset):
            data = data.rename(rename_map)
            data_vars = data.data_vars.keys()

        # drop columns that are not of interest for CrocoLake
        todrop = [c for c in data_vars if c not in params.params["CROCOLAKE_" + self.db_type + "_QC"]]

        if isinstance(data,(pd.DataFrame,dd.DataFrame)):
            data = data.drop(columns=todrop) #inplace defaults to False
        elif isinstance(data,xr.Dataset):
            data = data.drop_vars(todrop)
            data = data.to_dataframe()
            data = data.reset_index()
        # data is always a pandas dataframe now

        # add <NA> for columns in params.params["CROCOLAKE_" + self.db_type +
        # "_QC"] but not in data; this is needed when different files for the
        # same original database do not have the same variables (e.g. some Spray
        # Gliders do not have doxy and others do)
        toadd = [
            c for c in params.params["CROCOLAKE_" + self.db_type + "_QC"]
            if (c not in data.columns
                and c in list(params.params[self.db + "2CROCOLAKE"].values())
                and not any(item in c for item in ["QC", "ERROR", "DB_NAME"]))
        ]
        for col in toadd:
            data[col] = pd.NA

        # add <NA> for missing error and QC columns
        for col in data.columns:
            col_error = col+"_ERROR"
            col_qc = col+"_QC"
            if (col_error in params.params["CROCOLAKE_" + self.db_type + "_QC"]) and (col_error not in data.columns):
                data[col_error] = pd.NA
                data[col_error] = data[col_error].astype("float32[pyarrow]")
            if (col_qc in params.params["CROCOLAKE_" + self.db_type + "_QC"]) and (col_qc not in data.columns):
                data[col_qc] = pd.NA
                data[col_qc] = data[col_qc].astype("uint8[pyarrow]")

        # add database name
        data["DB_NAME"] = self.db
        data["DB_NAME"] = data["DB_NAME"].astype("string[pyarrow]")

        # wrap LONGITUDE in -180,+180 range
        data = self._wrap_longitude(data)

        self.generate_schema(data.columns.to_list())

        data = data.astype(self.schema_pd)
        if isinstance(data,dd.DataFrame):
            data = data.persist()

        return data

#------------------------------------------------------------------------------#
## wrap LONGITUDE in -180,+180 range
    def _wrap_longitude(self,df, shift_range=False, shift_value=None, ignore_range=False):
        """Enforce uniformity for longitude measurements to be in [-180,180) range

        Arguments:
        df -- pandas or dask dataframe

        Returns:
        df -- pandas or dask dataframe with longitude column in the range
              [-180, 180)
        """

        if isinstance(df,pd.DataFrame):
            ddf = dd.from_pandas(df, npartitions=1)
            flag_pd = True
            flag_dd = False
        elif isinstance(df,dd.DataFrame):
            ddf = df
            flag_pd = False
            flag_dd = True
        else:
            raise TypeError(
                "df is not a pandas or dask dataframe, I cannot"
                "wrap longitude values"
            )

        # if LONGITUDE is in [0,360) range, it is shifted to [-180,180) range if
        # flag is passed
        if (
                ddf["LONGITUDE"].min().compute() >= 0
                and ddf["LONGITUDE"].max().compute() >= 180
                and ddf["LONGITUDE"].max().compute() <= 360
        ):
            # it might be that this dataset uses LONGITUDE in [0,360) range
            # instead of [-180,180). The converter expects the latter range by
            # default, so the user should be warned
            if shift_range is False:
                if ignore_range is False:
                    raise ValueError(
                        "LONGITUDE values are in [0,360) range, while the"
                        "converter expects them in [-180,180) range. Either "
                        "convert LONGITUDE values to [-180,180) range with "
                        "the argument shift_range=True, or ignore at your "
                        "own risk with ignore_range=True."
                    )
            else:
                if shift_value is None:
                    shift_value = -180
                ddf["LONGITUDE"] = ddf["LONGITUDE"] + shift_value

        # note that the following only works if the wrapped LONGITUDE must be in [-180,180) range
        def modulo_longitude(df):
            # this turns 180 into -180
            #
            # not elegant but pyarrow backend does not support modulo operator
            df["LONGITUDE"] = df["LONGITUDE"].astype("float64")
            df["LONGITUDE"] = (df["LONGITUDE"] - 180) % 360 - 180
            df["LONGITUDE"] = df["LONGITUDE"].astype("float64[pyarrow]")
            return df

        ddf = ddf.map_partitions(
                modulo_longitude,
                meta=ddf
        )

        if flag_pd:
            df = ddf.compute()
        elif flag_dd:
            df = ddf
        else:
            raise TypeError("input dataframe is not a pandas or dask dataframe.")

        return df

#------------------------------------------------------------------------------#
## Convert parquet schema to pandas
    def __translate_pq_to_pd(self,schema_pq):
        """Convert parquet schema to pandas schema

        Generates:
        pd_dict -- schema for pandas dataframe
        """

        pd_types = []
        for d in schema_pq.types:
            try:
                pd_type = self.pa2pd_dtype_map[d]
            except KeyError:
                pd_type = d.to_pandas_dtype()
            pd_types.append( pd_type )
        pd_dict = dict(zip(schema_pq.names,pd_types))

        return pd_dict

#------------------------------------------------------------------------------#
## Compute derived variables
    def compute_derived_variables(self,df):
        """Row-computed derived variables

        Arguments:
        df  --  dataframe (needed to use this function with dd.map_partitions)

        Returns:
        df -- dataframe containign absolute salinity, conservative temperature,
              and potential density anomaly
        """

        # absolute salinity
        df['ABS_SAL_COMPUTED'] = gsw.conversions.SA_from_SP(
            df['PSAL'], # PSU
            df['PRES'], # dbar
            df['LONGITUDE'], # degrees east
            df['LATITUDE'] # degrees north
        ).astype("float32[pyarrow]") # PSU

        # conservative temperature
        df['CONSERVATIVE_TEMP_COMPUTED'] = gsw.conversions.CT_from_t(
            df['ABS_SAL_COMPUTED'], # PSU
            df['TEMP'], # degrees Celsius
            df['PRES']  # dbar
        ).astype("float32[pyarrow]") # degrees Celsius

        # potential density anomaly with reference pressure of 1000 dbar
        df['SIGMA1_COMPUTED'] = gsw.density.sigma1(
            df['ABS_SAL_COMPUTED'], # PSU
            df['CONSERVATIVE_TEMP_COMPUTED'] # degrees Celsius
        ).astype("float32[pyarrow]") # kg/m^3

        return df

#------------------------------------------------------------------------------#
## Add derived variables
    def add_derived_variables(self,ddf):
        """Add absolute salinity, conservative temperature, and potential
        density anomaly to dataframe

        Argument:
        ddf  --  dask dataframe containing practical salinity, temperature, and
                 pressure

        Returns:
        ddf  --  updated dask dataframe
        """

        # Add columns that will be created or dask might not find the metadata
        # when building the graph
        # Also add the columns to the schema for storing to parquet
        meta = {}
        meta = {col: ddf.dtypes[col] for col in ddf.columns}

        for col in ["ABS_SAL_COMPUTED","CONSERVATIVE_TEMP_COMPUTED","SIGMA1_COMPUTED"]:
            if col not in ddf.columns:
                meta[col] = "float32[pyarrow]"

                if not hasattr(self, 'schema_pq'):
                    warnings.warn("No schema found. You might encounter issues when storing to parquet.")
                else:
                    self.schema_pq = self.schema_pq.append(
                    pa.field(col, pa.float32() )
                )

        # Update the pandas schema
        if not hasattr(self, 'schema_pq'):
            warnings.warn("No schema found. You might encounter issues when storing to parquet.")
        else:
            self.schema_pd = self.__translate_pq_to_pd(self.schema_pq)

        ddf = ddf.map_partitions(
            self.compute_derived_variables,
            meta=meta
        )

        return ddf

#------------------------------------------------------------------------------#
## Compute derived variables
    def add_qc_flags(self,df,param_names,qc_value):
        """Add QC flags for param_names; flags are assigned to the whole column

        Arguments:
        df  --  dataframe (needed to use this function with dd.map_partitions)
        param_names -- list of parameter names to add QC flags for
        qc_value -- value(s) to set for QC flags; scalar if same values is
        assigned to all parameters, otherwise it must be a list of the same
        length as param_names and ordered as param_names

        Returns:
        df -- dataframe with QC flags added
        """

        for param in param_names:
            param_qc = param+"_QC"
            if param_qc not in self.reference_schema.names:
                raise ValueError(param_qc + " not found in reference schema.")
            if isinstance(qc_value,list):
                qc = qc_value[param_names.index(param)]
            elif isinstance(qc_value,int):
                qc = qc_value
            else:
                raise ValueError("QC value must be an integer or a list of integers.")

            df[param_qc] = df[param].apply(lambda x: qc if pd.notna(x) else pd.NA)
            df[param_qc] = df[param_qc].astype("uint8[pyarrow]")

            if param_qc not in self.schema_pq.names:
                self.schema_pq = self.schema_pq.append(
                    pa.field(param_qc, pa.uint8() )
                )

        self.schema_pd = self.__translate_pq_to_pd(self.schema_pq)

        return df

#------------------------------------------------------------------------------#
## Remove row if all measurements are NA
    def remove_all_NAs(self,df,cols_to_check):
        """Remove rows with all NA values

        Arguments:
        df  --  pandas dataframe
        cols_to_check -- list of columns to check for NA values

        Returns:
        df -- dataframe with rows removed
        """

        condition_na = df[ cols_to_check ].isna().all(axis="columns")
        df = df.loc[~condition_na]
        df.reset_index(drop=True, inplace=True)

        return df

#------------------------------------------------------------------------------#
## Sort rows
    def sort_rows(self,df):
        """Sort dataframe's rows hierarchically by PLATFORM_NUMBER,
        CYCLE_NUMBER, and PRES. This should ensure that profiles are sorted
        correctly

        Arguments:
        df  --  pandas dataframe

        Returns:
        df -- sorted dataframe

        """

        df = df.sort_values(
            by=["PLATFORM_NUMBER", "CYCLE_NUMBER", "PRES"],
            ascending=[True, True, True],
            ignore_index=True
        )

        return df

#------------------------------------------------------------------------------#
## Convert units
    def convert_units(self, ddf):
        """Apply unit conversions defined in self.cols_to_convert.
        
        Arguments:
        ddf -- dask dataframe
        
        Returns:
        ddf -- updated dask dataframe with converted units
        """

        for col, conversion_key in self.cols_to_convert.items():
            if conversion_key == "skip":
                continue
            if conversion_key not in unit_converter.conversion_map:
                raise ValueError(f"No conversion defined for '{conversion_key}'")
            if col not in ddf.columns: # column not found in df - skipping
                continue
            convert_fn = unit_converter.conversion_map[conversion_key]
            ddf = convert_fn(ddf, col)

        return ddf

#------------------------------------------------------------------------------#
## Update columns
    def update_cols(self):
        """Update columns to keep the best values for each row, add database
        name, and remove extra columns"""
        raise NotImplementedError("Subclasses must implement this method")

#------------------------------------------------------------------------------#
## Keep best values for each row
    def keep_best_values(self):
        """Keep the best observation available for each row"""
        raise NotImplementedError("Subclasses must implement this method")

##########################################################################
if __name__ == "__main__":
    Converter()
