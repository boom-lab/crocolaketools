#!/usr/bin/env python3

"""Golden tests for converters, Argo schemas, and the Argo GDAC conversion
stage.

1. test_golden -- for each target in tests/golden_registry.py, runs the
   converter against its tests/fixtures/ input and compares the output
   parquet dataset against a checked-in "golden" copy in
   tests/golden/<target>/expected/, plus a human-readable
   tests/golden/<target>/expected_summary.json companion (row count,
   per-column null count/min/max) meant to be a human-readable diff to
   accompany binary golden data updates.

2. test_golden_schema -- checks the Argo PHY/BGC parquet schemas
   (crocolaketools.converter.generate_schema). Most likely source of changes if
   test fails: (a) unintentional change to db_params.py's Argo variable list or
   generate_schema.py's dtype rules; (b) intentional change as in (a), but not
   followed by committing the new schemas.

3. test_golden_argo_gdac -- the raw GDAC netCDF -> intermediate PQT parquet
   conversion stage (crocolaketools.converter.dask_tools.daskTools, the
   tool ConverterArgoGDAC wraps). This is stage 1 of the Argo pipeline;
   stage 2 (PQT -> final CrocoLake schema) is covered by ARGO-QC_PHY/BGC in
   test_golden.

Regenerate golden files (after an intentional change) with:
    pytest tests/test_golden.py --update-golden

"""

import glob
import json
import os
import shutil
from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest
from dask.distributed import Lock

from crocolaketools.converter.dask_tools import daskTools
from crocolaketools.converter.generate_schema import generateSchema
from tests.golden_registry import GOLDEN_REGISTRY

GOLDEN_DIR = Path(__file__).parent / "golden"
REPO_SCHEMAS_DIR = Path(__file__).parent.parent / "schemas"
GDAC_DAC_DIR = Path(__file__).parent / "fixtures" / "demo_ARGO_GDAC" / "GDAC" / "dac"


def _summarize(df: pd.DataFrame) -> dict:
    """Row count + per-column null count/min/max : this stays small and legible
    in a git diff."""
    summary = {"n_rows": len(df), "columns": {}}
    for col in df.columns:
        s = df[col]
        col_summary = {"dtype": str(s.dtype), "n_null": int(s.isna().sum())}
        non_null = s.dropna()
        if len(non_null) > 0 and pd.api.types.is_numeric_dtype(s):
            col_summary["min"] = float(non_null.min())
            col_summary["max"] = float(non_null.max())
        summary["columns"][col] = col_summary
    return summary


def _sorted_by_all_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Row order isn't guaranteed to be stable across runs (dask partition
    scheduling can vary with the number of workers/processes), so compare
    content, not position: sort every row by every column's value."""
    return df.sort_values(by=list(df.columns), kind="mergesort").reset_index(drop=True)


def _schema_columns(schema) -> dict:
    return {field.name: str(field.type) for field in schema}


def _schema_summary(schema) -> dict:
    columns = _schema_columns(schema)
    return {"n_columns": len(columns), "columns": columns}


# ============================================================================
# 1. Converter output -> final CrocoLake schema
# ============================================================================

@pytest.mark.parametrize(
    "golden_target,dask_client",
    [(target, target.cluster_key) for target in GOLDEN_REGISTRY],
    ids=[target.name for target in GOLDEN_REGISTRY],
    indirect=["dask_client"],
)
def test_golden(golden_target, dask_client, tmp_path, request):
    converter = golden_target.converter_cls(db_type=golden_target.db_type)

    # input_path/outdir_schema stay config.yaml-driven (real fixture data);
    # outdir_pq/tmp_path are always overridden here so each run is hermetic
    # and never touches tests/fixtures/parquet/. Converter reads both
    # attributes live at call time, so a post-construction override works.
    converter.outdir_pq = tmp_path / "parquet"
    converter.tmp_path = str(tmp_path / "tmp") + "/"

    if golden_target.chunk_profile is not None:
        # SprayGliders has no convert() override: the base class's generic
        # convert() reads straight from tmp_path, so the input files must be
        # chunked into tmp_path first (see golden_registry.GoldenTarget
        # docstring).
        input_files = [f for f in os.listdir(converter.input_path) if f.endswith(".nc")]
        converter.prepare_data(flist=input_files, lock=Lock(), chunk_profile=golden_target.chunk_profile)
        chunk_files = [os.path.basename(f) for f in glob.glob(os.path.join(converter.tmp_path, "*.nc"))]
        converter.convert(filenames=chunk_files)
    else:
        converter.convert()

    actual = dd.read_parquet(converter.outdir_pq).compute()

    target_dir = GOLDEN_DIR / golden_target.name
    expected_dir = target_dir / "expected"
    summary_path = target_dir / "expected_summary.json"

    if request.config.getoption("--update-golden"):
        if expected_dir.exists():
            shutil.rmtree(expected_dir)
        expected_dir.mkdir(parents=True, exist_ok=True)
        shutil.copytree(converter.outdir_pq, expected_dir, dirs_exist_ok=True)
        summary_path.write_text(json.dumps(_summarize(actual), indent=2, sort_keys=True) + "\n")
        pytest.skip(f"golden regenerated for {golden_target.name} : review the diff and commit")

    assert expected_dir.exists(), (
        f"No golden files for {golden_target.name} at {expected_dir}. "
        "Run `pytest tests/test_golden.py --update-golden` to seed them."
    )

    expected = dd.read_parquet(expected_dir).compute()

    assert set(actual.columns) == set(expected.columns), (
        f"{golden_target.name}: column set changed.\n"
        f"  only in actual:   {sorted(set(actual.columns) - set(expected.columns))}\n"
        f"  only in expected: {sorted(set(expected.columns) - set(actual.columns))}"
    )
    assert len(actual) == len(expected), (
        f"{golden_target.name}: row count changed ({len(actual)} vs expected {len(expected)}). "
        f"See {summary_path} for the last-known-good per-column summary."
    )

    actual = _sorted_by_all_columns(actual[list(expected.columns)])
    expected = _sorted_by_all_columns(expected)

    for col in expected.columns:
        if col in golden_target.tolerant_columns:
            np.testing.assert_allclose(
                actual[col].to_numpy(dtype="float64", na_value=np.nan),
                expected[col].to_numpy(dtype="float64", na_value=np.nan),
                atol=1e-10,
                rtol=1e-10,
                equal_nan=True,
                err_msg=f"{golden_target.name}: tolerant column {col!r} diverged",
            )
        else:
            pd.testing.assert_series_equal(
                actual[col],
                expected[col],
                check_names=False,
                check_exact=True,
                obj=f"{golden_target.name}: exact-match column {col!r}",
            )


# ============================================================================
# 2. Argo PHY/BGC parquet schemas
# ============================================================================

@pytest.mark.parametrize("db_type", ["PHY", "BGC"])
def test_golden_schema(db_type, tmp_path, request):
    generateSchema(outdir=str(tmp_path) + "/", db=db_type)
    fresh_columns = _schema_columns(pq.read_schema(tmp_path / f"Argo{db_type}_schema.metadata"))

    target_dir = GOLDEN_DIR / f"Argo{db_type}_schema"
    expected_path = target_dir / "expected.metadata"
    summary_path = target_dir / "expected_summary.json"

    if request.config.getoption("--update-golden"):
        target_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy(tmp_path / f"Argo{db_type}_schema.metadata", expected_path)
        summary_path.write_text(json.dumps(_schema_summary(pq.read_schema(expected_path)), indent=2, sort_keys=False) + "\n")
        pytest.skip(f"golden regenerated for Argo{db_type}_schema -- review the diff and commit")

    assert expected_path.exists(), (
        f"No golden schema for Argo{db_type} at {expected_path}. "
        "Run `pytest tests/test_golden.py --update-golden` to seed it."
    )
    expected_columns = _schema_columns(pq.read_schema(expected_path))

    assert fresh_columns == expected_columns, (
        f"Argo{db_type}: generateSchema() output no longer matches the golden baseline in "
        f"{summary_path}. If this is intentional (db_params.py's Argo{db_type} variable list "
        "changed on purpose), regenerate with --update-golden and explain the diff."
    )

    # The actual file ConverterArgoGDAC/daskTools reads in production must
    # also match -- otherwise db_params.py changed but the committed schema
    # file was never regenerated (the exact ArgoPHY_schema.metadata drift
    # documented in the deployment plan).
    committed_path = REPO_SCHEMAS_DIR / f"Argo{db_type}_schema.metadata"
    assert committed_path.exists(), f"{committed_path} does not exist."
    committed_columns = _schema_columns(pq.read_schema(committed_path))
    assert committed_columns == expected_columns, (
        f"schemas/Argo{db_type}_schema.metadata (the file ConverterArgoGDAC actually reads) "
        f"does not match the golden baseline in {summary_path}. It looks like db_params.py's "
        f"Argo{db_type} variable list changed but the committed schema file was never "
        "regenerated -- run generateSchema(outdir='schemas/', db=...) and commit the result."
    )


# ============================================================================
# 3. Argo GDAC: raw netCDF -> intermediate PQT parquet
# ============================================================================

@pytest.mark.parametrize(
    "db_type,suffix,dask_client",
    [("PHY", "_prof.nc", "TESTS"), ("BGC", "_Sprof.nc", "TESTS")],
    ids=["ARGO-GDAC_PHY", "ARGO-GDAC_BGC"],
    indirect=["dask_client"],
)
def test_golden_argo_gdac(db_type, suffix, dask_client, tmp_path, request):
    flist = sorted(str(p) for p in GDAC_DAC_DIR.glob(f"*/*/*{suffix}"))
    assert flist, f"no {suffix} files found under {GDAC_DAC_DIR}"

    tool = daskTools(
        db_type=db_type,
        out_dir=str(tmp_path) + "/",
        flist=flist,
        schema_path=str(REPO_SCHEMAS_DIR / f"Argo{db_type}_schema.metadata"),
        chunk=len(flist),
    )
    tool.convert_to_parquet()

    actual = dd.read_parquet(tmp_path).compute()

    target_dir = GOLDEN_DIR / f"ARGO-GDAC_{db_type}"
    expected_dir = target_dir / "expected"
    summary_path = target_dir / "expected_summary.json"

    if request.config.getoption("--update-golden"):
        if expected_dir.exists():
            shutil.rmtree(expected_dir)
        expected_dir.mkdir(parents=True, exist_ok=True)
        shutil.copytree(tmp_path, expected_dir, dirs_exist_ok=True)
        summary_path.write_text(json.dumps(_summarize(actual), indent=2, sort_keys=True) + "\n")
        pytest.skip(f"golden regenerated for ARGO-GDAC_{db_type} -- review the diff and commit")

    assert expected_dir.exists(), (
        f"No golden files for ARGO-GDAC_{db_type} at {expected_dir}. "
        "Run `pytest tests/test_golden.py --update-golden` to seed them."
    )
    expected = dd.read_parquet(expected_dir).compute()

    assert set(actual.columns) == set(expected.columns), (
        f"ARGO-GDAC_{db_type}: column set changed.\n"
        f"  only in actual:   {sorted(set(actual.columns) - set(expected.columns))}\n"
        f"  only in expected: {sorted(set(expected.columns) - set(actual.columns))}"
    )
    assert len(actual) == len(expected), (
        f"ARGO-GDAC_{db_type}: row count changed ({len(actual)} vs expected {len(expected)}). "
        f"See {summary_path} for the last-known-good per-column summary."
    )

    actual = _sorted_by_all_columns(actual[list(expected.columns)])
    expected = _sorted_by_all_columns(expected)

    # this stage is pure raw-passthrough (nothing gsw-derived happens until
    # ConverterArgoQC), so every column compares exactly (no tolerant set)
    for col in expected.columns:
        pd.testing.assert_series_equal(
            actual[col],
            expected[col],
            check_names=False,
            check_exact=True,
            obj=f"ARGO-GDAC_{db_type}: column {col!r}",
        )
