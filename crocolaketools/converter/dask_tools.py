#!/usr/bin/env python3

## @file daskTools.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Tue 20 Aug 2024

##########################################################################
import dask
import dask.dataframe as dd
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import xarray as xr
import argopy
import numpy as np
# ignore pandas "educational" performance warnings
import warnings
warnings.simplefilter(action="ignore", category=pd.errors.PerformanceWarning)
#warnings.simplefilter(action="always", category=RuntimeWarning)
#warnings.simplefilter(action="error", category=RuntimeWarning)
from pprint import pprint
from dask.distributed import print
import crocolakeloader.params as params
##########################################################################

class daskTools():

    """class daskTools:
    methods to convert the argo database to parquet format
    and that use the dask module
    """

    # ------------------------------------------------------------------ #
    # Constructors/Destructors                                           #
    # ------------------------------------------------------------------ #

    def __init__(self, db_type=None, out_dir=None, flist=None, schema_path='../schemas', chunk=None):
        """Constructor

        Arguments:
        db_type     -- "PHY" for physical Argo, "BGC" for biogeochemical Argo
        out_dir     -- destination directory for the converted database
        flist       -- list of paths to Argo files to be converted
        schema_path -- path to ArgoPHY_schema.metadata and ArgoBGC_schema.metadata
        chunk       -- number of files processed at a time by dask
        """

        if db_type is None:
            print("No database type provided, remember to provide the variables you desire to read.")
        elif db_type not in ["PHY", "BGC"]:
            raise ValueError("db_type can only take values PHY or BGC.")
        
        self.db_type = db_type;

        if flist is None:
            print("No file lists provided, remember to assign one to self.flist before triggering the database conversion.")
        else:
            self.flist = flist

        if out_dir is None:
            self.out_dir = './ArgoParquet/'
        else:
            self.out_dir = out_dir

        if chunk is None:
            self.chunk = 2000
        else:
            self.chunk = chunk

        if schema_path is None:
            self.schema_path = '../schemas/Argo' + self.db_type + '_schema.metadata'
        else:
            self.schema_path = schema_path
        self.schema = pq.read_schema(self.schema_path)
        self.__translate_pq_to_pd()

        self.__assign_vars()
        self.VARS = sorted(self.VARS)

        self.failed_reads = [-1] * len(flist)

        pass

    # ------------------------------------------------------------------ #
    # Methods                                                            #
    # ------------------------------------------------------------------ #

#------------------------------------------------------------------------------#
## Delayed function to read an Argo profile into a dataframe with a prescribed
## schema
    @dask.delayed(nout=1)
    def read_argo(self,argo_file):
        """ Read Argo file into dataframe

        Arguments:
        argo_file -- path to file

        Returns:
        df -- dataframe

        Exceptions:
        if the Argo file cannot be read, the file name is printed to screen
        and returns an empty dataframe for compatibility when dask gathers
        returns from multiple workers
        """

        okflag = -1

        try:
            ds = xr.open_dataset(argo_file, engine="argo") #loading into memory the profile

            # updating data modes for BGC argo floats data
            if 'PARAMETER_DATA_MODE' in list(ds.data_vars):
                if (ds['PARAMETER'].isel(N_CALIB=0) == ds['PARAMETER']).all():
                    ds = self.__assign_data_mode(ds)
                else:
                    raise ValueError("PARAMETER not independent of N_CALIB.")
            
            ds_vars = list(ds.data_vars)
            invars = list(set(self.VARS) & set(ds_vars))
            df = ds[invars].to_dataframe()
            df = df.reset_index() #flatten dataframe
            okflag = 1

        except Exception as e:
            print("The following exception occurred:", e)
            okflag = 0
            # create empty dataframe
            df = pd.DataFrame({c: pd.Series(dtype=t) for c, t in self.pd_dict.items()})

        if okflag == -1:
            print('No comms from ' + str(argo_file))
        elif okflag == 0:
            print('Failed on ' + str(argo_file))
        elif okflag == 1:
            print('Processing    ' + str(argo_file))

        #ensures that all data frames have all the columns and in the same order; it creates NaNs where not present
        df = df.reindex( columns=self.VARS )

        # enforcing dtypes otherwise to_parquet() gives error when appending
        df = df.astype(self.pd_dict)

        return df

#------------------------------------------------------------------------------#
## Performs conversion
    def convert_to_parquet(self, flist=None, out_dir=None, chunk=None):
        """Performs conversion by building compute graph and triggering
        operations

        Arguments:
        flist    -- list of paths to files to convert
        out_dir   -- output directory for the parquet database
        chunk    -- number of files processed at a time
        """

        if flist is None:
            flist = self.flist
        if out_dir is None:
            out_dir = self.out_dir
        if chunk is None:
            chunk = self.chunk

        for j in range( int(np.ceil(len(flist)/chunk)) ):
            initchunk = j*chunk
            endchunk = (j+1)*chunk
            if endchunk > len(flist):
                endchunk = len(flist)

            df = [ self.read_argo(file) for file in flist[initchunk:endchunk] ]

            df = dd.from_delayed(df) # creating unique df from list of df

            df = df.repartition(partition_size="300MB")

            name_function = lambda x: f"Argo{self.db_type}_{x}.parquet"

            # to_parquet() triggers execution of lazy functions
            append_db = False
            if j>0:
                append_db = True # append to pre-existing partition
            overwrite_db = not append_db

            df.to_parquet(
                out_dir,
                engine="pyarrow",
                name_function = name_function,
                append = append_db,
                write_metadata_file = True,
                write_index=False,
                schema = self.schema,
                overwrite = overwrite_db
            )

            print()

        print("stored.")

#------------------------------------------------------------------------------#
## Finalize compute of failed files
    def failed_files(self):
        """ Trigger dask compute of failed files"""

        okflags = dask.compute(*self.failed_reads) #needs unpacked list of delayed objects

        # find the files that failed on reads or never communicated if read was
        # succesfull
        self.failed_list = [file for idx, file in enumerate(self.flist) if okflags[idx]!=1]

        failed_percentage = len(self.failed_list)/len(self.flist)*100

        print("The following files failed on read and were not converted (" + str(failed_percentage) + "%):")
        pprint(self.failed_list)

#------------------------------------------------------------------------------#
## Convert parquet schema to pandas
    def __translate_pq_to_pd(self):
        """Convert parquet schema to pandas schema

        Generates:
        pd_dict -- schema for pandas dataframe
        """

        pd_types = []

        for d in self.schema.types:
            pd_types.append( self.__pa2pd(d) ) #conversion
            pd_dict = dict(zip(self.schema.names,pd_types))

        self.pd_dict = pd_dict

#------------------------------------------------------------------------------#
## Convert pyarrow datatypes to pandas datatype
    def __pa2pd(self,pa_dtype):

        """Convert pyarrow datatypes to pandas datatype, forcing pyarrow integers
        to null integers, as by default pyarrow's to_pandas_dtype() converts
        them to numpy integers, which cannot handle NaNs.

        Arguments:
        pa_dtype -- parquet datatype

        returns:
        pd_dtype -- pandas datatype
        """

        if pa.types.is_integer(pa_dtype):
            bit_width = pa_dtype.bit_width
            if bit_width == 8:
                return pd.Int8Dtype()
            elif bit_width == 16:
                return pd.Int16Dtype()
            elif bit_width == 32:
                return pd.Int32Dtype()
            elif bit_width == 64:
                return pd.Int64Dtype()
            else:
                raise ValueError(f"Unsupported integer bit width: {bit_width}")

        elif pa.types.is_string(pa_dtype):
            return pd.StringDtype()

        else:
            return pa_dtype.to_pandas_dtype()

#------------------------------------------------------------------------------#
## Select PHY or BGC variables
    def __assign_vars(self):
        """ Select variables in target Argo database"""

        if self.db_type in ["PHY","BGC"]:
            self.VARS = params.params["Argo"+self.db_type].copy()
        else:
            raise ValueError("List of variables to read from Argo files not provided.")

        return

#------------------------------------------------------------------------------#
## Assign data mode to each parameter
    def __assign_data_mode(self,ds):
        """Spread 'PARAM_DATA_MODE' value across as many <PARAM>_DATA_MODE
        variables as N_PARAM in the dataset

        Arguments:
        ds    -- xarray Argo dataset

        Returns:
        ds    -- xarray Argo dataset with <PARAM>_DATA_MODE variables
        """

        nparam = ds.sizes["N_PARAM"]
        nprof = ds.sizes["N_PROF"]
        nlevels = ds.sizes["N_LEVELS"]
        for v in self.VARS:
            if "_DATA_MODE" in v:
                ds[v] = xr.DataArray(
                    np.full( (nprof,nlevels), "", dtype=str ),
                    dims=["N_PROF","N_LEVELS"]
                )

        parameter = ds["PARAMETER"].isel(N_CALIB=0)
        skipped_params = []
        for p in range(nparam):
            for j in range(nprof):

                param_name = str(parameter.isel(N_PARAM=p,N_PROF=j).values).strip()
                if len(param_name) < 1:
                    continue

                param_data_mode_name = param_name + "_DATA_MODE"
                if param_data_mode_name not in self.VARS:
                    if param_data_mode_name not in skipped_params:
                        skipped_params.append(param_data_mode_name)
                    continue

                data_mode = str( ds["PARAMETER_DATA_MODE"].isel(N_PARAM=p,N_PROF=j).values ).strip()
                if len(data_mode) > 1:
                    raise ValueError("Data mode should be a one-character long string.")

                for k in range(nlevels):
                    ds[param_data_mode_name][j,k] = data_mode

        if len(skipped_params)>0:
            print("The following parameters were not found in the target variables to be converted, its <PARAM>_DATA_MODE has not been created: ")
            print(skipped_params)

        return ds

##########################################################################

if __name__ == '__main__':
    test = daskTools()
