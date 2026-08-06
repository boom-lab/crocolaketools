# Developer Instructions for Requesting a New Converter

Use this document together with
[`CONVERTER_INSTRUCTIONS.md`](CONVERTER_INSTRUCTIONS.md) when handing a new
dataset-converter task to an AI agent.

The developer is responsible for supplying accurate dataset context. The agent
should implement and validate the work, but it should not be expected to infer
dataset-specific semantics that the developer has not checked.

The agent must always begin in plan mode. It should inspect the repository,
source data, and documentation, then submit a structured plan for developer
approval before editing code. If uncertainty arises, the agent must ask the
developer a clarifying question and must not proceed by silently making an
assumption.

## Required information to provide

Before handing over the task, provide the following:

- [ ] Canonical database name, exactly as it should appear in `db_names.py`.
- [ ] Source dataset name and a short description.
- [ ] Database type:
  - [ ] PHY only.
  - [ ] BGC only.
  - [ ] Both PHY and BGC.
- [ ] Absolute path to the source data.
- [ ] Source file names or file-patterns to convert.
- [ ] Absolute path to the dataset documentation.
- [ ] Paths to supplementary documentation, metadata tables, data dictionaries,
      QC tables, or method descriptions.
- [ ] A small representative sample or demo dataset path.
- [ ] Expected source file format:
  CSV, NetCDF, Parquet, ERDDAP response, or another format.
- [ ] Expected output directory names and Parquet basenames, if prescribed.
- [ ] Whether a downloader is required or the source data already exists
      locally.

## Documentation review required from the developer

The developer must inspect the dataset documentation before handing the task
to the agent, especially the quality-control process. Record the findings in
the request.

At minimum, verify and report:

- [ ] Meaning of every QC flag value.
- [ ] Which QC values are retained.
- [ ] Whether QC applies independently to each variable.
- [ ] Missing-value and fill-value conventions.
- [ ] Whether invalid values should become missing or rows should be removed.
- [ ] Units and any required conversions.
- [ ] Date and time formats, timezone, and missing-time behavior.
- [ ] Definitions of platform, station, cruise, cast, cycle, profile, and
      bottle/sample identifiers.
- [ ] Which fields uniquely identify one profile.
- [ ] Whether profile/cycle numbers reset per platform, cruise, or another
      grouping.
- [ ] Whether source coordinates can change within a cast.
- [ ] Whether the source contains derived variables that should be preserved
      or recomputed.

Do not delegate QC interpretation to the agent without first checking the
documentation. Include page numbers, table names, or section names wherever
possible.

## Context to include in the handoff

Provide any information that may affect implementation, including:

- Existing converters that are the closest precedents.
- Existing branches, commits, pull requests, or partial implementations.
- Known bugs or intentionally deferred work.
- Existing parameter mappings or schema entries.
- Required naming conventions.
- Expected Dask resource requirements.
- Whether output should overwrite existing data.
- Whether generated profile IDs must match another database or prior product.
- Known edge cases in the source data.
- Required tests or acceptance criteria.
- Commands that should be used for conversion, testing, and symlink generation.
- Any files already changed by the developer that the agent must preserve.

If a behavior is uncertain, state the uncertainty explicitly instead of
allowing the agent to guess silently.

## Recommended handoff format

Use a structured request like this:

```text
Add support for converting the <DATASET NAME> dataset.

Follow CONVERTER_INSTRUCTIONS.md and the existing <PRECEDENT 1> and
<PRECEDENT 2> converters.

Database information:
- Canonical DB name: <DB_NAME>
- Database types: <PHY, BGC, or both>
- Source format: <CSV/NetCDF/Parquet/etc.>
- Source data path: <ABSOLUTE DATA PATH>
- Source files/pattern: <FILE OR PATTERN>
- Demo data path: <ABSOLUTE DEMO PATH>
- Documentation path: <ABSOLUTE DOCUMENTATION PATH>
- Supplementary documentation: <PATHS OR NONE>

Documentation findings:
- QC values retained: <VALUES>
- QC behavior: <PER-VARIABLE OR OTHER>
- Missing/fill values: <DETAILS>
- Units/conversions: <DETAILS>
- Time format and timezone: <DETAILS>
- Profile identity: <COMPOSITE KEY>
- Profile-number reset scope: <PLATFORM/CRUISE/OTHER>
- Coordinate behavior within a profile: <DETAILS>

Implementation requirements:
- Add <SCRIPT NAME> in scripts/.
- Update db_names.py and db_params.py.
- Update config.yaml and config_cluster.yaml.
- Ensure generate_crocolake_symlinks.sh includes the outputs.
- Add converter tests and source-to-Parquet integrity tests.
- Preserve or account for these existing changes: <DETAILS>.

Validation requirements:
- Run: <CONVERSION COMMAND>
- Run targeted tests: <TEST COMMAND>
- Check output with Dask and verify <EXPECTED SCHEMA/ROW/PROFILE RESULTS>.
- Report any uncertainty or blocked validation explicitly.
```

## Example handoff

```text
Add support for the SPOTS dataset.

Follow CONVERTER_INSTRUCTIONS.md. Use GLODAP as the profile-ID and workflow
precedent and SprayGliders as the PHY/BGC script precedent.

Database information:
- Canonical DB name: SPOTS
- Database types: PHY and BGC
- Source format: CSV
- Source data path: /home/enrico/myWHOI/CrocoLake/crocolaketools/crocolaketools/demo/demo_SPOTS/
- Source file: spots.csv
- Documentation path: /home/enrico/myWHOI/databases/SPOTS/data/Dataset_description.pdf
- Supplementary documentation:
  /home/enrico/myWHOI/databases/SPOTS/data/Table1_ProductVariables_v2.csv
  /home/enrico/myWHOI/databases/SPOTS/supplement/Table S3 Flag schemes.xlsx
  /home/enrico/myWHOI/databases/SPOTS/supplement/Table S5 QC methods.xlsx

Documentation findings:
- Only WOCE QC flag 2 is retained.
- QC is applied independently to each measured variable.
- Invalid values use -999 or are removed after QC.
- DATE is yyyymmdd and TIME is UTC hhmm.
- CASTNO is a cast identifier but resets across cruises/stations.
- BTLNBR identifies a bottle and must not define a profile.
- Profile key: TimeSeriesSite, CRUISE, STNNBR, CASTNO.
- CYCLE_NUMBER resets for each TimeSeriesSite and starts at 1.
- Coordinates and times may vary slightly between samples in a moving cast.

Implementation requirements:
- Add scripts/spots2parquet.py.
- Complete ConverterSPOTS.
- Update db_names.py, db_params.py, config.yaml, and config_cluster.yaml.
- Verify that configured SPOTS PHY/BGC directories are symlinked into CrocoLake.
- Add converter tests and data-integrity tests.

Validation:
- Run spots2parquet --config.
- Check both Parquet outputs with Dask.
- Verify per-platform progressive CYCLE_NUMBER values, QC filtering, pressure
  ordering, and source-to-Parquet value preservation.
```

## Final developer checklist

- [ ] I inspected the dataset documentation myself.
- [ ] I specifically checked and documented QC rules.
- [ ] I supplied all data and documentation paths.
- [ ] I explained profile identity and numbering semantics.
- [ ] I identified the closest existing converter precedents.
- [ ] I stated expected PHY/BGC behavior.
- [ ] I included edge cases and known limitations.
- [ ] I provided concrete conversion and test commands.
- [ ] I identified existing changes the agent must not overwrite.
- [ ] I asked the agent to report uncertainty rather than guess.
- [ ] I expect the agent to start in plan mode and obtain approval before
      making code changes.
