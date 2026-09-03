#!/usr/bin/env python3

## @file converterCPR.py
#
#
## @author David Nady <davidnady4yad@gmail.com>
#
# @date Fri 21 Mar 2025

##########################################################################
import os
import warnings
import logging
import pandas as pd
from crocolaketools import db_params
from crocolaketools.converter.converter import Converter
##########################################################################

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ConverterCPR(Converter):

    """class ConverterCPR: methods to generate parquet schemas for
    Continuous Plankton Recorder (CPR) database

    """

    # ------------------------------------------------------------------ #
    # Constructors/Destructors                                           #
    # ------------------------------------------------------------------ #

    def __init__(self, db=None, db_type=None, input_path=None, outdir_pq=None, outdir_schema=None, fname_pq=None, add_derived_vars=False, overwrite=False, tmp_path=None):
        if not db == "CPR":
            raise ValueError("Database must be CPR.")
        Converter.__init__(self, db, db_type, input_path, outdir_pq, outdir_schema, fname_pq, add_derived_vars, overwrite, tmp_path)

    # ------------------------------------------------------------------ #
    # Methods                                                            #
    # ------------------------------------------------------------------ #

    def read_to_df(self, filename=None, lock=None):
        """Read file into a pandas dataframe

        Argument:
        filename -- file name, including relative path

        Returns
        df -- pandas dataframe
        """

        if filename is None:
            raise ValueError("No filename provided for CPR database.")

        input_fname = self.input_path / filename
        logger.info(f"Reading CPR file: {input_fname}")

        try:
            # Read the CSV file
            df = pd.read_csv(input_fname, delimiter=",", header=0, low_memory=False)
        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {input_fname}")
        except pd.errors.EmptyDataError:
            raise ValueError(f"File is empty: {input_fname}")
        except pd.errors.ParserError:
            raise ValueError(f"Unable to parse file: {input_fname}")

        return self.standardize_data(df)

    def standardize_data(self, df):
        """Standardize pandas dataframe to schema consistent across databases

        Argument:
        df -- pandas dataframe

        Returns:
        df -- homogenized dataframe
        """

        # Rename columns using CPR2CROCOLAKE mapping
        df = df.rename(columns=db_params.params["CPR2CROCOLAKE"])

        # Convert CPR date column to datetime
        logger.info("Converting CPR date column to datetime")
        if 'JULD' in df.columns:
            df['JULD'] = pd.to_datetime(df['JULD'])
        elif all(col in df.columns for col in ['Year', 'Month', 'Day', 'Hour']):
            df['JULD'] = pd.to_datetime(df[['Year', 'Month', 'Day', 'Hour']])
        else:
            warnings.warn("No valid date columns found. Unable to construct 'JULD'.")

        # Ensure data types match the Crocolake schema
        if 'LATITUDE' in df.columns:
            df['LATITUDE'] = df['LATITUDE'].astype('float32')
        if 'LONGITUDE' in df.columns:
            df['LONGITUDE'] = df['LONGITUDE'].astype('float32')
        if 'PLATFORM_NUMBER' in df.columns:
            df['PLATFORM_NUMBER'] = df['PLATFORM_NUMBER'].astype('string')

        df = super().standardize_data(df)

        return df

##########################################################################
if __name__ == "__main__":
    ConverterCPR()
