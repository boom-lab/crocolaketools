#!/usr/bin/env python3

## @file Converter.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Fri 04 Oct 2024

##########################################################################
import os
import glob
import warnings
import dask
import dask.dataframe as dd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from crocolakeloader import params
##########################################################################

class Loader:

    """class Loader: methods to load different databases (generated with
    Converter) into one dataframe

    """

    # ------------------------------------------------------------------ #
    # Constructors/Destructors                                           #
    # ------------------------------------------------------------------ #

    def __init__(self, db_list=None, db_type=None, selected_variables=None, db_rootpath=None, qc_only=True):
        """Constructor

        Arguments:
        db_list            -- list of databases to consider
        db_type            -- type of database desired (PHY or BGC parameters)
        selected_variables -- list of columns to filter by
        db_rootpath            -- path to common root for all databases (this normally
                              should be None and left to the default set below)
        qc_only            -- if True, dictionary for only QC variables is used
                              (default is True)
        """

        if db_list is None:
            self.db_list = params.databases
        elif isinstance(db_list,list):
            self.db_list = db_list
        elif isinstance(db_list,str):
            self.db_list = [db_list]
        else:
            raise ValueError("Database list db_list must be a list or a string")
        if not all(name in params.databases for name in self.db_list):
            raise ValueError("Database list db_list can only take values in: " + str(params.databases))
        print(f"Reading data from {self.db_list} .")

        if db_type is None:
            self.db_type = "PHY"
        elif isinstance(db_type,str):
            if db_type.upper() in ["PHY","BGC"]:
                self.db_type = db_type.upper()
            else:
                raise ValueError("Database type db_type must be one of " + str(["PHY","BGC"]))
        print("Reading " + self.db_type + " parameters.")

        if db_rootpath is None:
            self.db_rootpath = "./"
        else:
            self.db_rootpath = db_rootpath
        print("Looking for data in " + self.db_rootpath)

        if qc_only:
            admitted_vars = params.params["TRITON_"+self.db_type+"_QC"]
        else:
            admitted_vars = params.params["TRITON_"+self.db_type+"_ALL"]
        if isinstance(selected_variables,str):
            selected_variables = [selected_variables]
        if selected_variables is None:
            self.selected_variables = admitted_vars
        elif isinstance(selected_variables, list) and len(selected_variables) > 0:
            if all(name in admitted_vars for name in selected_variables):
                self.selected_variables = selected_variables
            else:
                raise ValueError(f"Selected variables must be a list of variables contained in the database: {admitted_vars}")
        else:
            raise ValueError(f"Selected variables must be a list of variables contained in the database: {admitted_vars}")

        # Initialize paths and filters, update db_list
        self.__set_db_rootpaths()
        self.filters = None
        self.__update_db_list()

        self.dtype_mapping = {
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

    # ------------------------------------------------------------------ #
    # Methods                                                            #
    # ------------------------------------------------------------------ #

#------------------------------------------------------------------------------#
## Set list of databases to read
    def __set_db_rootpaths(self):
        """Generates a list containing all the path of the sub-databases (Argo,
        GLODAP, etc.) to read from
        """

        self.db_paths = {}
        db_codenames = params.databases_codenames

        for db in self.db_list:
            search_pattern = os.path.join(self.db_rootpath,"*"+db_codenames[db]+"*")
            paths = glob.glob(search_pattern)
            print(f"Searching for database {db} in {search_pattern}.")
            print(f"Found {paths}")
            if len(paths) > 1:
                raise ValueError(f"Found multiple version of database {db}: {paths}.")
            if len(paths)==0:
                warnings.warn(f"No database {db} found in {search_pattern}. Skipping it. It is possible that the database is present in another type (e.g. Spray Glider data exist in PHY but not in BGC type).")
                continue
            self.db_paths[db] = paths[0]

        print("Reading databases from the following paths:")
        print(self.db_paths)

        return

#------------------------------------------------------------------------------#
## Build global schema given all the databases to read
    def __build_global_schema(self):
        """Builds the global schema for the final database, by looping over each
        database's schema and for each database selecting all the columns that
        are present in the selected variables. Note that not all of the columns
        in global schema are necessarily present in each database.

        Returns:
        global_schema  --  list of columns to read from all the databases

        """

        global_schema = None
        for db in self.db_list:
            if db not in self.db_paths:
                warnings.warn(f"No database {db} found in {self.db_paths}. Skipping it. It is possible that the database is present in another type (e.g. Spray Glider data exist in PHY but not in BGC type).")
                continue
            search_pattern = os.path.join(self.db_paths[db]+"/_common_metadata")
            paths = glob.glob(search_pattern)
            # discarding databases that are not present (e.g. Spray Gliders in BGC path)
            if len(paths) > 1:
                raise ValueError(f"Found multiple version of database {db}: {paths}.")
            if len(paths)==0:
                warnings.warn(f"No database {db} found in {search_pattern}. Skipping it. It is possible that the database is present in another type (e.g. Spray Glider data exist in PHY but not in BGC type).")
                continue

            db_schema = pq.read_schema(paths[0])
            if global_schema is None:
                global_schema = db_schema
            else:
                for field in db_schema:
                    if field.name not in global_schema.names:
                        global_schema = global_schema.append(field)

        return global_schema

#------------------------------------------------------------------------------#
## Validate filters agains list of columns
    def __validate_filters(self,filters,valid_cols):
        """Remove filters that operate on undesired columns (e.g. because they
        don't exist in the database)

        Arguments:
        filters     --  a filter compatible with dask.dataframe.read_parquet()
        valid_cols  --  a list of column names

        Returns: valid_filters -- update filter that operates only on variables
                                  present in valid_cols and is compatible with
                                  dask.dataframe.read_parquet()

        """

        # filters for read_parquet can be either a list of two lists of tuples
        # or a list of tuples, so we normalize the filters to always be a list
        # of lists of tuples
        if isinstance(filters[0], tuple):
            filters = [filters]

        # #Filter out any non-existing columns from the normalized filters
        valid_filters = []
        discarded_filters = []
        for filtergroup in filters: # loop over each list inside the outer list
            valid_filter_group = []
            for f in filtergroup: # loop over each filter inside inner list
                print(f[0])
                if f[0] in valid_cols: # first entry of the tuple is the name of the column to filter
                    valid_filter_group.append(f)
                    print(f"Filter {f} added to valid filters.")
                else:
                    discarded_filters.append(f)
                    print(f"Filter {f} discarded.")
            if valid_filter_group:
                valid_filters.append(valid_filter_group)

        #Print warnings for discarded filters
        for f in discarded_filters:
            warnings.warn(f"Filter {f} discarded because column '{f[0]}' does not exist in the database.")

        #If there is only one inner list, return a list instead of a list of lists
        if len(valid_filters)==1:
            valid_filters = valid_filters[0]

        return valid_filters

#------------------------------------------------------------------------------#
## Update db_list
    def __update_db_list(self):
        """As not all databases exist in both PHY and BGC db type, this
        function updates the db_list variable to take this into account so that
        later reading from parquet does not lead to error related to this"""

        updated_db_list = []
        for db in self.db_list:
            if db not in self.db_paths:
                warnings.warn(f"No database {db} found in {self.db_paths}. Removing it from db_list. It is possible that the database is present in another type (e.g. Spray Glider data exist in PHY but not in BGC type).")
                continue
            search_pattern = os.path.join(self.db_paths[db]+"/_common_metadata")
            paths = glob.glob(search_pattern)
            # discarding databases that are not present (e.g. Spray Gliders in BGC path)
            if len(paths) > 1:
                raise ValueError(f"Found multiple version of database {db}: {paths}.")
            if len(paths)==0:
                warnings.warn(f"No database {db} found in {search_pattern}. Removing it from db_list. It is possible that the database is present in another type (e.g. Spray Glider data exist in PHY but not in BGC type).")
                continue
            updated_db_list.append(db)

        if len(updated_db_list)==0:
            raise ValueError("No database found in the specified path. Please check the path and the database list.")
        self.db_list = updated_db_list

#------------------------------------------------------------------------------#
## Read single database
    def __read_db_dask(self,db_name,target_schema=None):
        """Read parquet database applying all filters and adds its name if not
        present

        Argument:
        db_name  --  database name

        Returns:
        ddf  --  dask dataframe containing the filtered data

        """

        # read schema to get the list of columns
        db_schema = pq.read_schema(self.db_paths[db_name]+"/_common_metadata")

        # read in only the selected variables that are present in the database
        cols_to_read = [name for name in self.selected_variables if name in db_schema.names]

        # filter only the columns that exist in the database
        if len(self.filters)>0:
            filters_to_use = self.__validate_filters(self.filters,cols_to_read)
        else:
            filters_to_use = None

        ddf = dd.read_parquet(
            self.db_paths[db_name],
            engine="pyarrow",
            columns = cols_to_read,
            filters = filters_to_use,
            index = False,
            split_row_groups = False, # one partition per file, they should be already optimized
        )

        return ddf

#------------------------------------------------------------------------------#
## Read single database
    def __read_db_pq(self,db_name,target_schema=None):
        """Read parquet database applying all filters and adds its name if not
        present, using pyarrow.parquet

        Argument:
        db_name  --  database name

        Returns:
        ddf  --  dask dataframe containing the filtered data

        """

        db_path = self.db_paths[db_name]

        db_schema = pq.read_schema(self.db_paths[db_name]+"/_common_metadata")
        #local_schema = pq.read_schema(db_path+"/_common_metadata")

        cols_to_read = [name for name in self.selected_variables if name in db_schema.names]
        # cols_to_read = [name for name in self.selected_variables if name in local_schema.names]
        print(f"Reading columns {cols_to_read} from db {db_name}.")

        def read_db(local_path):
            ds = pq.ParquetDataset(
                local_path,
                schema=db_schema,
                filters=self.filters
            )
            df = ds.read(columns=cols_to_read).to_pandas()
            return df.convert_dtypes(dtype_backend="pyarrow")

        local_paths = glob.glob(db_path+"/*.parquet")
        ddf = dd.from_map(
            read_db,
            local_paths,
            )

        # add columns that are not present in the database for compatibility with other databases
        cols_to_add = [name for name in self.selected_variables if name not in db_schema.names]

        def assign_DB_NAME(df,col):
            df[col] = db_name
            return df

        def assign_NA(df,col):
            df[col] = pd.NA
            return df

        for col in cols_to_add:
            if col == "DB_NAME":
                print(f"Adding {col} to db {db_name}")
                ddf = ddf.map_partitions(
                    assign_DB_NAME, col
                )
                ddf[col] = ddf[col].astype("string[pyarrow]")
            else:
                print(f"Adding empty column {col} to db {db_name}")
                if target_schema is not None:
                    field_idx = target_schema.get_field_index(col)
                    pa_dtype = target_schema.types[field_idx]
                    pd_dtype = self.dtype_mapping[pa_dtype]
                    ddf = ddf.map_partitions(
                        assign_NA, col
                    )
                    ddf[col] = ddf[col].astype(pd_dtype)
                else:
                    ddf = ddf.map_partitions( lambda df: df.assign(col=pd.NA) )

        ddf = ddf[list(self.selected_variables)]
        return ddf

#------------------------------------------------------------------------------#
## Set filters
    def set_filters(self,filters,reset_variables=False):
        """Store filters to later apply to dask.dataframe.read_parquet

        Argument:
        filters -- filters in the appropriate format for
                   dask.dataframe.read_parquet (see
        https://docs.dask.org/en/stable/generated/dask.dataframe.read_parquet.html)
        """

        #validate filters
        if filters is None:
            self.filters = None
            return

        if reset_variables:
            def get_varname_in_filter(data):
                if isinstance(data, list):
                    return [get_varname_in_filter(item) for item in data]
                elif isinstance(data, tuple):
                    return data[0]
                else:
                    raise ValueError("Unsupported data type")

            def flatten(data):
                for item in data:
                    if isinstance(item, list):
                        yield from flatten(item)
                    else:
                        yield item

            variables_in_filters = list(
                set(
                    flatten(
                        get_varname_in_filter(filters)
                    )
                )
            )

            self.selected_variables = [
                var for var in self.selected_variables
                if var in variables_in_filters
            ]

        self.filters = filters
        # self.filters = self.__validate_filters(
        #     filters,
        #     self.selected_variables
        # )

#------------------------------------------------------------------------------#
## Get dataframes
    def get_dataframe(self, memory_check=False):
        """Return a dask dataframe containing all the relevant parameters from
        all the desired databases

        Argument:
        memory_check -- (optional) check memory usage and returns a computed
                        pandas dataframe if the usage is low

        Returns:
        ddf -- dask dataframe complete of all sub-databases; pandas dataframe if
               memory_check is performed and succesfull
        """

        # get global schema for the data load
        self.global_schema = self.__build_global_schema()

        ddf = []
        for db_name in self.db_list:
            ddf.append(
                    self.__read_db_pq(
                        db_name,
                        target_schema=self.global_schema
                    )
            )
        ddf = dd.concat(ddf)
        #ddf = ddf.repartition(partition_size="300MB")

        if memory_check:
            memory_usage = ddf.memory_usage(deep=True).compute()
            total_memory_usage = memory_usage.sum()*1e-6 #in MB
            if total_memory_usage < 256: #MB
                print("Retrieved dataset smaller than 256 MB, loading into memory and returning a pandas dataframe.")
                return ddf.compute()

        return ddf

##########################################################################
if __name__ == '__main__':
    Loader()
