#!/usr/bin/env python3

## @file generate_schemas.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Fri 04 Oct 2024

##########################################################################
import copy
from datetime import datetime
import numpy as np
import pandas as pd
import pathlib
from pathlib import Path
from pprint import pprint
import pyarrow as pa
import pyarrow.parquet as pq
import xarray as xr
import crocolakeloader.params as params
##########################################################################

class generateSchema():

    """class generateSchema: methods to generate parquet schemas for different
    databases and different versions
    """

    # ------------------------------------------------------------------ #
    # Constructors/Destructors                                           #
    # ------------------------------------------------------------------ #

    def __init__(self, outdir=None, db=None):
        """Constructor

        Arguments:
        outdir     -- path to directory to output schema
        db         -- database name to generate schema for
        """

        if outdir is None:
            self.outdir = './schemas/'
        else:
            self.outdir = outdir
        print("Schema(s) will be stored at " + self.outdir)

        if isinstance(db,str):
            db = db.upper()
        if db not in ["PHY","BGC",None]:
            raise ValueError("db must be PHY, BGC or None.")
        elif db is None:
            self.db = ["PHY","BGC"]
        else:
            self.db = [db]

        for db in self.db:
            print("Generating " + db + " schema.")
            self.generate_schema(db)
            self.save_schema()

    # ------------------------------------------------------------------ #
    # Methods                                                            #
    # ------------------------------------------------------------------ #

#------------------------------------------------------------------------------#
## Save schema to file
    def save_schema(self):

        if self.schema is None:
            raise ValueError("schema is None -- did you generate it?")

        Path(self.outdir).mkdir(parents = True, exist_ok = True)

        self.schema_fname = self.outdir + self.schema_name

        pq.write_metadata(
            self.schema,
            self.schema_fname
        )

        print("Schema(s) stored at " + self.schema_fname)

#------------------------------------------------------------------------------#
## Generate schema
    def generate_schema(self,db):

        params_schema = params.params["Argo"+db].copy()

        fields = []
        for p in params_schema:

            if p in ['PLATFORM_NUMBER','N_PROF','N_LEVELS','CYCLE_NUMBER']:
                f = pa.field( p, pa.int64() )

            elif  '_QC' in p:
                f = pa.field( p, pa.uint8() )

            elif p in ['LATITUDE','LONGITUDE']:
                f = pa.field( p, pa.float64() )

            elif p in ['JULD','DATE_UPDATE']:
                f = pa.field( p, pa.from_numpy_dtype(np.dtype('datetime64[ns]') ) )

            elif (p=='DIRECTION') or ('DATA_MODE' in p):
                f = pa.field( p, pa.string() )

            else:
                f = pa.field( p, pa.float32() )

            fields.append(f)

        self.schema = pa.schema( fields )
        self.schema_name = "Argo" + db + "_schema.metadata"

##########################################################################

if __name__ == "__main__":
    test = generateSchema(outdir="./test/")
