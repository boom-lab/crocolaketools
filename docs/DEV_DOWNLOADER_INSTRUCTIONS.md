# Developer Instructions for Requesting a New Downloader

Use this document together with
[`AGENT_DOWNLOADER_INSTRUCTIONS.md`](AGENT_DOWNLOADER_INSTRUCTIONS.md) when
handing a new dataset-download task to an AI agent.

The developer is responsible for supplying accurate source and endpoint
context. The agent should inspect, implement, and validate the downloader, but
it should not be expected to infer source ownership, URL semantics, update
policy, or dataset selection rules that the developer has not checked.

The agent must always begin in plan mode. It should inspect the repository,
source documentation, and the remote endpoint, then submit a structured plan
for developer approval before editing code. The agent must ask the developer
whether the downloader is **direct-link** or **ERDDAP** before choosing a
template. If the answer is ERDDAP, the developer must also specify whether the
workflow enumerates a catalogue, downloads known dataset IDs, or supports
both. If uncertainty arises, the agent must ask a clarifying question rather
than silently making an assumption.

## Required information to provide

Before handing over the task, provide:

- [ ] Canonical database name, exactly as it should appear in `db_names.py`.
- [ ] Source dataset name and a short description.
- [ ] Database type:
  - [ ] PHY only.
  - [ ] BGC only.
  - [ ] Both PHY and BGC.
- [ ] Downloader type:
  - [ ] Direct-link, fixed URL list, generated URLs, or source listing.
  - [ ] ERDDAP.
  - [ ] Specialized GDAC/index workflow.
- [ ] For ERDDAP:
  - [ ] ERDDAP server URL.
  - [ ] Protocol and response format.
  - [ ] Catalogue enumeration, known dataset IDs, or both.
  - [ ] Dataset-ID filtering rules.
- [ ] Source endpoint or URL inventory.
- [ ] Whether authentication, special headers, or rate limits are required.
  Never include credentials or tokens in the request.
- [ ] Absolute path to any local source data, fixtures, or demo files.
- [ ] Absolute path to source documentation and endpoint documentation.
- [ ] Supplementary metadata, variable dictionaries, or update-history paths.
- [ ] Expected raw file format, archive format, and local filename convention.
- [ ] Expected input directory and whether one source item creates one local
      file or multiple files.
- [ ] Whether a downloader is replacing an existing script or adding a new
      workflow.

## Documentation review required from the developer

Inspect the source documentation and endpoint behavior before handing over the
task. Record:

- [ ] How files or ERDDAP datasets are discovered.
- [ ] Which URL patterns, directory paths, or dataset IDs are authoritative.
- [ ] Whether URLs are stable, versioned, redirected, or time-limited.
- [ ] Whether the server supports HEAD, GET streaming, or only one of them.
- [ ] Archive contents and the extracted completion marker.
- [ ] Whether source files can be revised in place.
- [ ] How update timestamps are exposed, if at all.
- [ ] ERDDAP variable names, required variables, and per-dataset variation.
- [ ] ERDDAP time variable, timezone, time coverage metadata, and constraints.
- [ ] Empty-query behavior and expected HTTP responses.
- [ ] Server limits, recommended request size, concurrency, and rate limits.
- [ ] Missing-file, unavailable-dataset, and partial-failure behavior.

Do not ask the agent to infer whether `sync=True` is meaningful. Confirm that
the endpoint exposes reliable metadata such as `date_modified`,
`date_created`, or `date_issued`, or state that the downloader must use
download-only behavior.

## Context to include in the handoff

Provide information that affects implementation:

- Closest existing downloader and CLI precedents.
- Existing branches, commits, pull requests, or partial implementations.
- Existing `db_names.py` or configuration changes.
- Required input path, output naming, and PHY/BGC behavior.
- Whether raw files must remain unmodified.
- Desired overwrite, dry-run, sync, retry, and concurrency behavior.
- Whether archives should be extracted and whether ZIPs should be deleted.
- Whether a fixed inventory, URL file, year range, or endpoint catalogue is
  the supported selection interface.
- For ERDDAP, requested variables, constraints, dataset filters, and time
  chunking expectations.
- Known endpoint quirks, unavailable files, and expected HTTP errors.
- Required tests and acceptance criteria.
- Commands for dry runs, smoke downloads, and targeted tests.
- Any files already changed by the developer that the agent must preserve.

If a behavior is intentionally out of scope, state that explicitly. For
example, distinguish “download raw ERDDAP Parquet files” from “convert the
downloaded files into CrocoLake Parquet.”

## Recommended handoff format

Use a structured request like this:

```text
Add download support for the <DATASET NAME> dataset.

Follow AGENT_DOWNLOADER_INSTRUCTIONS.md and use <PRECEDENT 1> and
<PRECEDENT 2> as implementation references.

Dataset information:
- Canonical DB name: <DB_NAME>
- Database types: <PHY, BGC, or both>
- Downloader type: <direct-link, ERDDAP, or specialized GDAC>
- Source description: <SHORT DESCRIPTION>
- Source documentation: <ABSOLUTE DOCUMENTATION PATH OR URL>
- Supplementary documentation: <PATHS OR NONE>
- Local demo/fixture path: <ABSOLUTE PATH OR NONE>

Source and selection:
- Base URL/server: <URL>
- Direct-link inventory or URL pattern: <DETAILS OR NONE>
- ERDDAP workflow: <catalogue, known IDs, both, or none>
- Dataset IDs/filter: <DETAILS OR NONE>
- Required variables: <LIST OR NONE>
- Time constraints: <DETAILS OR NONE>
- Archive format: <ZIP/TAR/NONE>
- Local filename convention: <DETAILS>
- Source update policy: <IMMUTABLE/REVISED/UNKNOWN>
- Server timestamp behavior: <DETAILS OR UNKNOWN>
- Authentication/rate limits: <DETAILS, WITHOUT SECRETS>

Implementation requirements:
- Create crocolaketools/downloader/downloader<DB>.py.
- Create scripts/download_<db>.py.
- Update config.yaml with the required downloader settings.
- Register the CLI entry point in setup.py.
- Add tests with mocked network/ERDDAP responses.
- Preserve these existing changes: <DETAILS>.
- Do not implement conversion behavior unless explicitly requested.

Behavior requirements:
- Existing files: <SKIP/OVERWRITE/SYNC POLICY>
- Dry run: <REQUIRED/NOT REQUIRED>
- Parallelism: <THREAD COUNT OR CONFIGURED>
- Archive extraction: <REQUIRED/NOT REQUIRED>
- Partial failures: <EXPECTED RESULT>
- ERDDAP chunking/gating: <DETAILS OR USE BASE DEFAULTS>

Validation requirements:
- Run: <DRY-RUN OR SMALL DOWNLOAD COMMAND>
- Run targeted tests: <TEST COMMAND>
- Verify: <EXPECTED FILES, NAMES, COUNTS, VARIABLES, OR TIMESTAMPS>
- Report any uncertainty, unavailable endpoint, or blocked validation explicitly.
```

## Direct-link handoff example

```text
Add download support for the ExampleSurvey dataset.

Follow AGENT_DOWNLOADER_INSTRUCTIONS.md. Use GLODAP for fallback URLs and
SprayGliders for a filename-to-remote-path mapping.

Dataset information:
- Canonical DB name: EXAMPLESURVEY
- Database types: PHY
- Downloader type: direct-link
- Source documentation: /home/enrico/data/ExampleSurvey/README.pdf
- Local demo path: /home/enrico/myWHOI/CrocoLake/crocolaketools/demo/example/

Source and selection:
- Base URL: https://data.example.org/releases/
- URL inventory: release_2025/*.zip listed in manifest.csv
- Archive format: ZIP
- Extracted files: one NetCDF per archive
- Local filename: preserve the extracted NetCDF filename
- Source update policy: versioned and immutable
- Authentication/rate limits: public endpoint, maximum 4 concurrent downloads

Implementation requirements:
- Add downloaderExampleSurvey.py and download_examplesurvey.py.
- Parse the manifest and support an optional local manifest path.
- Skip archives whose expected extracted NetCDF files already exist.
- Use inherited temporary-file downloads and ZIP extraction.
- Add EXAMPLESURVEY_PHY to config.yaml and register the console script.
- Add mocked tests for manifest parsing, URL construction, skip behavior,
  extraction, and one failed URL.

Validation:
- Run a dry run against the checked-in manifest.
- Download one archive into the demo path.
- Verify the archive is removed, the NetCDF exists, and no .tmp file remains.
```

## ERDDAP handoff example

```text
Add download support for the ExampleGliders dataset.

Follow AGENT_DOWNLOADER_INSTRUCTIONS.md. Use IOOS Gliders as the ERDDAP
catalogue/sync precedent.

Dataset information:
- Canonical DB name: EXAMPLEGLIDERS
- Database types: PHY and BGC
- Downloader type: ERDDAP
- Server URL: https://gliders.example.org/erddap
- Protocol: tabledap
- Response format: parquet
- ERDDAP workflow: catalogue enumeration
- Dataset filter: IDs ending in "-delayed"
- Source documentation: https://gliders.example.org/docs

ERDDAP behavior:
- Request variables: latitude, longitude, time, depth, temperature, salinity.
- Dataset variables differ; intersect requested variables with each info
  endpoint's available variables.
- Use time coverage metadata for chunking.
- `date_modified` is reliable for sync.
- Empty time windows return HTTP 404 and should be skipped.
- Use the base first-byte gate and retry behavior.
- Local filename: <dataset_id>.parquet with path-safe ID normalization.

Implementation requirements:
- Add downloaderExampleGliders.py and download_examplegliders.py.
- Add EXAMPLEGLIDERS_PHY and EXAMPLEGLIDERS_BGC downloader configuration.
- Add mocked tests for filtering, variable discovery, URL constraints,
  timestamp-based queueing, empty windows, and failed chunks.
- Register the CLI entry point in setup.py.
- Do not add converter changes.

Validation:
- Run the catalogue dry run.
- Run one known dataset with a narrow time range.
- Verify the merged Parquet file is time ordered and no temporary chunk
  directory remains.
```

## Specialized GDAC handoff

If the dataset uses Argo GDAC indexes and profile paths, say so explicitly.
Provide:

- the GDAC index directory;
- the PHY/BGC index names;
- geographic, date, sensor, and float filters;
- profile versus Sprof download requirements;
- GDAC root URL and update/check-time policy;
- local directory layout;
- whether the existing `argo_tools.py` workflow should be reused.

Do not ask the agent to implement a normal `Downloader` or
`DownloaderERDDAP` subclass unless the developer has confirmed that the GDAC
workflow is being replaced.

## Final developer checklist

- [ ] I inspected the source documentation and endpoint myself.
- [ ] I explicitly selected direct-link, ERDDAP, or specialized GDAC.
- [ ] For ERDDAP, I specified catalogue, known IDs, or hybrid behavior.
- [ ] I supplied the canonical database name and database type.
- [ ] I supplied source, documentation, fixture, and configuration paths.
- [ ] I documented URL discovery and local filename semantics.
- [ ] I checked archive extraction and completion-marker behavior.
- [ ] I checked source revisions and timestamp/sync semantics.
- [ ] I documented ERDDAP variables, filters, time ranges, and server limits.
- [ ] I stated overwrite, dry-run, retry, concurrency, and failure behavior.
- [ ] I identified the closest downloader and CLI precedents.
- [ ] I provided concrete smoke-download and test commands.
- [ ] I identified existing changes the agent must preserve.
- [ ] I asked the agent to report uncertainty rather than guess.
- [ ] I expect the agent to start in plan mode and obtain approval before
  making code changes.
