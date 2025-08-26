#!/usr/bin/env python3

## @file downloader.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#         Updated by David Nady <davidnady4yad@gmail.com>
#
## @date Tue 11 Feb 2025

##########################################################################
# imports
##########################################################################
import os
import yaml
import warnings
import importlib.resources
from crocolakeloader import params


class Downloader:

    """class Downloader: common facilities to configure downloads for different
    databases, mirroring the Converter's path handling.
    """

    # ------------------------------------------------------------------ #
    # Constructors/Destructors                                           #
    # ------------------------------------------------------------------ #

    def __init__(self, config=None):
        """Constructor

        Arguments:

        config -- configuration dictionary. Must contain at least 'db' and
                  'db_type'. If any value is not specified, defaults in
                  config.yaml are used; user-provided values override them.

        Relevant fields used by Downloader implementations:
        db            -- database name (e.g., 'Oleander')
        db_type       -- 'PHY' or 'BGC'
        input_path    -- destination path where original files are stored
        """

        if config is None:
            raise ValueError("No config argument provided to Downloader.")

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

        print("Downloader configuration:")
        print(config)

        # Basic validation and assignments
        if isinstance(db,str):
            if db in params.databases:
                self.db = db
                print("Setting up downloader for " + self.db + " database.")
            else:
                raise ValueError("Database db must be one of " + str(params.databases))
        elif db is not None:
            raise ValueError("Database db not a string.")
        else:
            raise ValueError("No database provided.")

        if isinstance(db_type,str):
            if db_type in ["PHY","BGC"]:
                self.db_type = db_type
                print("Using " + self.db_type + " parameters.")
            else:
                raise ValueError("Database db_type must be one of " + str(["PHY","BGC"]))
        elif db is not None:
            raise ValueError("Database type db_type not a string.")

        input_path = os.path.abspath(os.path.join(base_path, config["input_path"]))
        if input_path[-1] != "/":
            input_path = input_path + "/"
        # Ensure destination exists for downloads
        os.makedirs(input_path, exist_ok=True)
        self.input_path = input_path
        print("Original files will be stored at " + self.input_path)

    # ------------------------------------------------------------------ #
    # Methods                                                            #
    # ------------------------------------------------------------------ #

##########################################################################
if __name__ == "__main__":
    Downloader()
