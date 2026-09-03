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
import importlib.resources
##########################################################################

def get_config_path():
    config_path = importlib.resources.files(
        "crocolaketools.config"
    )
    return config_path

def get_config_paths_file():
    config_paths = get_config_path().joinpath("config.yaml")
    return config_paths

def get_config_paths_db_dict(db_name):
    config_paths = get_config_paths_file()
    config_db = yaml.safe_load(open(config_paths))[db_name]
    return config_db

def get_config_paths_field(db_name, field):
    config_db = get_config_paths_db_dict(db_name)
    config_field = config_db[field]
    return get_config_path() / config_field

def get_config_cluster_file():
    config_path = get_config_path().joinpath("config_cluster.yaml")
    return config_path

def get_config_cluster_db_dict(db_name, config_file=None):
    """Read db_name's cluster settings.

    config_file -- path to config_cluster.yaml
                   (default: crocolaketools/config/config_cluster.yaml)
    """
    config_paths = config_file if config_file is not None else get_config_cluster_file()
    config_db = yaml.safe_load(open(config_paths))[db_name]
    return config_db

def get_config_cluster_field(db_name, field):
    config_db = get_config_cluster_db_dict(db_name)
    config_field = config_db[field]
    return get_config_path() / config_field

