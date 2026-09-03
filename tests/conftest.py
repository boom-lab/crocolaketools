#!/usr/bin/env python3

"""Shared pytest fixtures for crocolaketools test suite.

This module provides common fixtures used across all test categories.
"""

import os
from pathlib import Path
from typing import Dict, Any

import pytest
import yaml
from dask.distributed import Client

import crocolaketools.config.config_paths as cfgp

TEST_CONFIG_CLUSTER_FILE = Path(__file__).parent / "config_cluster_tests.yaml"

# ============================================================================
# Dask Fixtures
# ============================================================================

@pytest.fixture
def dask_client(request):
    """Client built from the named key's settings in
    tests/config_cluster_tests.yaml (small, CI-safe settings -- not
    production's crocolaketools/config/config_cluster.yaml).

    Indirect fixture: parametrize with the config_cluster_tests.yaml key to use,
    e.g. @pytest.mark.parametrize("dask_client", ["TESTS"], indirect=True)
    """
    config_cluster = cfgp.get_config_cluster_db_dict(request.param, config_file=TEST_CONFIG_CLUSTER_FILE)
    client = Client(**config_cluster)
    yield client
    client.close()

# ============================================================================
# Path Fixtures
# ============================================================================

def pytest_addoption(parser):
    """Add custom command-line flag for golden tests."""
    parser.addoption(
        "--update-golden",
        action="store_true",
        default=False,
        help="Update the golden reference files with current test outputs",
    )

@pytest.fixture
def update_golden(request):
    """Fixture to check if the --update-golden flag is present."""
    return request.config.getoption("--update-golden")
