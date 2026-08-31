#!/usr/bin/env python3

"""Registry of golden-test targets for tests/test_golden.py.

Add an entry to registry when a new converter is created.

Each entry names a (converter, db_type) pair already exercised by
tests/fixtures/, the config_cluster.yaml key to use for its dask_client, and
which output columns are derived via gsw (compared with
numpy.testing.assert_allclose(atol=1e-10, rtol=1e-10)) rather than exact match.

The three gsw-computed columns (ABS_SAL_COMPUTED/CONSERVATIVE_TEMP_COMPUTED/
SIGMA1_COMPUTED, added by Converter.compute_derived_variables) and PRES (when
computed from DEPTH with gsw) are checked with tolerance; golden tests should be
run with add_derived_vars = True in config.yaml when possible (exception eg:
OleanderXBT has no salinity measurement and derived variable cannot be
estimated)

"""

from typing import NamedTuple, List, Optional, Type

from crocolaketools.converter.converterArgoQC import ConverterArgoQC
from crocolaketools.converter.converterGLODAP import ConverterGLODAP
from crocolaketools.converter.converterSprayGliders import ConverterSprayGliders
from crocolaketools.converter.converterSaildrones import ConverterSaildrones
from crocolaketools.converter.converterOleanderXBT import ConverterOleanderXBT

GSW_COLUMNS = ["ABS_SAL_COMPUTED", "CONSERVATIVE_TEMP_COMPUTED", "SIGMA1_COMPUTED"]


class GoldenTarget(NamedTuple):
    name: str                  # tests/golden/<name>/ directory
    converter_cls: Type
    db_type: str                # "PHY" or "BGC"
    cluster_key: str            # key into config_cluster.yaml
    tolerant_columns: List[str] # compared with atol=1e-10, rtol=1e-10; all others exact
    # SprayGliders only: it has no convert() override, so the base class's
    # generic convert() would try to read straight from tmp_path -- it needs
    # prepare_data() run first to chunk input_path's files into tmp_path, then
    # convert(filenames=<chunk files>). Set to the chunk_profile to use when
    # this two-phase flow is required; leave None everywhere else.
    chunk_profile: Optional[int] = None


GOLDEN_REGISTRY = [
    GoldenTarget("ARGO-QC_PHY", ConverterArgoQC, "PHY", "ARGO-QC_PHY", GSW_COLUMNS),
    GoldenTarget("ARGO-QC_BGC", ConverterArgoQC, "BGC", "ARGO-QC_BGC", GSW_COLUMNS),
    GoldenTarget("GLODAP_PHY", ConverterGLODAP, "PHY", "GLODAP", GSW_COLUMNS),
    GoldenTarget("GLODAP_BGC", ConverterGLODAP, "BGC", "GLODAP", GSW_COLUMNS),
    GoldenTarget("SPRAY_PHY", ConverterSprayGliders, "PHY", "SPRAY_GLIDERS", ["PRES"] + GSW_COLUMNS, chunk_profile=20),
    GoldenTarget("SPRAY_BGC", ConverterSprayGliders, "BGC", "SPRAY_GLIDERS", ["PRES"] + GSW_COLUMNS, chunk_profile=20),
    GoldenTarget("Saildrones_PHY", ConverterSaildrones, "PHY", "SAILDRONES", ["PRES"] + GSW_COLUMNS),
    GoldenTarget("Saildrones_BGC", ConverterSaildrones, "BGC", "SAILDRONES", ["PRES"] + GSW_COLUMNS),
    GoldenTarget("OleanderXBT_PHY", ConverterOleanderXBT, "PHY", "OLEANDER_XBT", ["PRES"]),
]


class DataTarget(NamedTuple):
    name: str                  # tests/fixtures/parquet/<name>/ directory
    converter_cls: Type
    db_type: str                # "PHY" or "BGC"

DATA_REGISTRY = [
    DataTarget("ARGO-QC_PHY", ConverterArgoQC, "PHY"),
    DataTarget("ARGO-QC_BGC", ConverterArgoQC, "BGC"),
    DataTarget("GLODAP_PHY", ConverterGLODAP, "PHY"),
    DataTarget("GLODAP_BGC", ConverterGLODAP, "BGC"),
    DataTarget("SPRAY_PHY", ConverterSprayGliders, "PHY"),
    DataTarget("SPRAY_BGC", ConverterSprayGliders, "BGC"),
    DataTarget("Saildrones_PHY", ConverterSaildrones, "PHY"),
    DataTarget("Saildrones_BGC", ConverterSaildrones, "BGC"),
    DataTarget("OleanderXBT_PHY", ConverterOleanderXBT, "PHY"),
]
