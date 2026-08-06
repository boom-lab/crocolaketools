---
name: downloader-builder
description: Agent specialized in adding a Downloader to CrocoLakeTools
---

# Instructions for Adding a CrocoLake Downloader

Use this guide when adding download support for a new observational dataset.
Follow the existing `Downloader`, `DownloaderERDDAP`, CLI, configuration, and
test patterns rather than creating a separate workflow.

## Start in plan mode

Always begin by inspecting the repository, the dataset documentation, and the
source endpoint. Before selecting an implementation, ask the developer:

> Is this dataset a **direct-link downloader** or an **ERDDAP downloader**?

Do not silently choose when both interpretations are plausible. If the answer
is ERDDAP, also ask whether the downloader should enumerate a catalogue,
download one known dataset, or support both. If the source is an Argo GDAC
index/profile workflow, identify it as a specialized workflow instead of
forcing it into either template in this guide.

If the source format, URL discovery, dataset naming, archive behavior,
incremental-update policy, or failure semantics are uncertain, ask a focused
clarifying question before editing code.

## 1. Inspect the source and repository first

Before editing:

1. Read the complete source documentation and representative endpoint
   responses.
2. Determine the raw file format, archive format, naming convention, update
   cadence, and whether files are immutable or revised in place.
3. Identify how datasets or files are discovered:
   - a fixed list of URLs;
   - a directory/index listing;
   - a generated URL pattern;
   - an ERDDAP `allDatasets` catalogue;
   - one known ERDDAP dataset ID.
4. Determine whether one source dataset maps to one local file, many local
   files, or a merged output.
5. Inspect the closest existing downloader and script before designing new
   methods.

Use these repository references directly:

- Shared download behavior:
  `crocolaketools/downloader/downloader.py`
- ERDDAP behavior:
  `crocolaketools/downloader/downloaderERDDAP.py`
- Direct-link examples:
  `downloaderGLODAP.py`, `downloaderSprayGliders.py`,
  `downloaderSaildrones.py`
- Dynamic file-list example:
  `downloaderOleanderXBT.py`
- ERDDAP catalogue example:
  `downloaderIOOSGliders.py`
- CLI examples:
  `scripts/download_glodap.py`, `scripts/download_spraygliders.py`,
  `scripts/download_saildrones.py`, `scripts/download_oleanderXBT.py`,
  `scripts/download_ioos_gliders.py`
- Configuration:
  `crocolaketools/config/config.yaml`
- Console registration:
  `setup.py`

## 2. Understand the shared `Downloader` contract

`Downloader.__init__(config)` requires a configuration dictionary containing
`db` and `db_type`. It loads the matching `<db>_<db_type>` section from
`config.yaml`, fills missing keys, validates the database name and `PHY`/`BGC`
type, resolves `input_path` relative to the packaged config directory, and
creates the destination directory.

Common options are:

- `overwrite`: download again even when the expected local file exists;
- `dryrun`: report work without writing files;
- `num_threads`: maximum parallel download workers.

Reuse the base methods:

- `_is_already_downloaded(local_path)` for idempotent reruns;
- `_download_file(url, local_path)` for streamed, temporary-file downloads;
- `download_parallel(url_path_pairs, ...)` for independent files;
- `unzip_file(zip_path)` for archive extraction and ZIP cleanup.

Do not duplicate streaming, progress, temporary-file, or archive logic unless
the source requires behavior that the base class cannot provide. Failed
downloads must not leave a final-looking partial file. Do not catch broad
exceptions merely to report success; surface failures or return an explicit
failure count consistent with the closest existing implementation.

## 3. Direct-link downloader style

Use this style when the source provides fixed URLs, a predictable URL pattern,
or a file/index listing that can be resolved without ERDDAP table queries.

### 3.1 Design the source inventory

Choose one of these patterns:

- a module-level list or mapping for a stable inventory, as in
  `DownloaderSaildrones` and `DownloaderSprayGliders`;
- a primary URL plus fallback URLs, as in `DownloaderGLODAP`;
- a directory listing or year/range resolver, as in
  `DownloaderOleanderXBT`;
- a user-supplied URL file when the source inventory changes frequently.

Document the relationship between remote names and local names. Do not use
unstable URL-derived names when the dataset has a documented canonical name.
If an archive extracts to a different filename, check the extracted output
rather than treating the archive itself as the completion marker.

### 3.2 Implement the downloader module

Create `crocolaketools/downloader/downloader<DB>.py` inheriting from
`Downloader`.

Match existing style:

- Doxygen-style file header;
- separator banners before methods;
- typed arguments where the surrounding module uses them;
- `Arguments` and `Returns` docstrings;
- logging and user-facing messages consistent with sibling downloaders.

Implement:

1. module-level URL constants or an explicit URL-builder;
2. a constructor with a config default containing the correct `db` and
   `db_type`;
3. source-specific options such as `fname`, URL file, year range, base URL,
   thread count, `dryrun`, and `overwrite`;
4. a public method named for the dataset, or `download()` when that is the
   established pattern;
5. local-path construction and completion checks;
6. URL reachability checks only when they provide useful source behavior;
7. archive extraction and cleanup when required.

For multiple independent files, build `(url, local_path)` pairs and use
`download_parallel()`. For source-specific extraction, perform it only after
the download future succeeds. If one missing URL should not abort the entire
inventory, record it explicitly and report skipped files at the end; do not
silently discard it.

For fallback URLs, check candidates in documented priority order and only
continue on an expected reachability failure. Preserve the original filename
and format after extraction unless the source workflow explicitly requires
normalization.

### 3.3 Direct-link checklist

- [ ] URL inventory or discovery behavior is documented.
- [ ] URL-to-local filename mapping is deterministic.
- [ ] Existing files are skipped unless `overwrite=True`.
- [ ] Archives are extracted only after successful downloads.
- [ ] Temporary archives and partial files are cleaned up.
- [ ] Missing or unreachable files are reported explicitly.
- [ ] Parallelism is bounded by `num_threads`.
- [ ] Dry-run behavior does not write files.

## 4. ERDDAP downloader style

Use this style when the source exposes an ERDDAP server and data should be
selected through dataset IDs, variables, constraints, or time ranges.

### 4.1 Configure the ERDDAP client

Create `crocolaketools/downloader/downloader<DB>.py` inheriting from
`DownloaderERDDAP`.

Define class defaults where appropriate:

```python
SERVER_URL = "https://example.org/erddap"
PROTOCOL = "tabledap"
RESPONSE_FORMAT = "parquet"
```

The base constructor reads `server_url`, `protocol`, and `response_format`
from config, falling back to these class values. It creates an `erddapy`
client and provides dataset listing, URL generation, timestamp lookup,
variable discovery, local timestamps, retries, time-window splitting,
parallel chunk downloads, and Parquet merging.

Do not bypass the base ERDDAP download machinery without a documented reason.
In particular, preserve temporary chunk cleanup and the no-partial-output
behavior of `_download_chunks_parallel()` and `_merge_chunks()`.

### 4.2 Choose the ERDDAP workflow

Ask the developer which workflow is intended:

- **Catalogue workflow:** call `list_dataset_ids()`, filter the returned IDs,
  and download each dataset.
- **Known-dataset workflow:** accept or configure one or more dataset IDs and
  call `get_dataset_url()` for each.
- **Hybrid workflow:** enumerate IDs but allow explicit filtering or IDs.

Override `_filter_datasets()` for source-specific rules such as delayed-mode
suffixes. Override `_local_path()` so every dataset ID maps to a safe,
deterministic local filename. Do not assume every server dataset ID is already
safe as a path component.

### 4.3 Build ERDDAP URLs

Override `get_dataset_url(dataset_id, time_start=None, time_end=None)` when
the source needs:

- a selected variable list;
- per-dataset variable discovery;
- required constraints;
- time constraints;
- a source-specific response format.

Reset `self._erddap._dataset_id`, `variables`, and `constraints` on every URL
construction. Do not allow constraints from a previous dataset to leak into
the next request. Format time constraints with the repository's
`ERDDAP_TS_FMT` convention unless the endpoint documents another format.

When variables differ between datasets, use `get_dataset_variables()` and
intersect the requested list with the available variables. Decide explicitly
whether a dataset with no required variables should be skipped, downloaded
with a reduced list, or treated as an error.

### 4.4 Implement incremental behavior

Use the base `_build_download_queue()` when its policy matches the source:

- `overwrite=True`: queue all datasets;
- `sync=False`: queue only missing local files;
- `sync=True`: compare server metadata timestamps to local file mtime.

If the server has no reliable timestamp, define and document the fallback.
Do not claim that `sync=True` detects updates if the endpoint does not expose
usable `date_modified`, `date_created`, or `date_issued` metadata.

The public `download()` method should:

1. discover or validate dataset IDs;
2. apply source-specific filtering;
3. build the download queue;
4. handle dry-run without writing files;
5. download each selected dataset;
6. return `(completed, failed)` or another documented result;
7. log skipped, current, failed, and completed counts separately.

### 4.5 ERDDAP chunking and failure behavior

The base ERDDAP implementation downloads time windows in parallel and merges
them in time order. Preserve these rules:

- 404 for an empty time window may be treated as an empty chunk;
- 413 causes the configured chunk schedule to retry with smaller windows;
- 429, 500, and 503 plus transient connection/read errors use bounded retries;
- `gated_parallel_download=True` limits concurrent ERDDAP heavy-build phases;
- failed chunks prevent a final merge;
- temporary chunk directories are removed after each dataset attempt.

Choose `num_threads` conservatively. Do not disable the first-byte gate merely
to increase concurrency without verifying the server's memory behavior.

### 4.6 ERDDAP checklist

- [ ] The developer selected catalogue, known-dataset, or hybrid behavior.
- [ ] Dataset filtering is explicit and tested.
- [ ] Local filenames are safe and deterministic.
- [ ] Variables are selected from documented source availability.
- [ ] Constraints are reset for every dataset request.
- [ ] Time ranges and time zones are handled explicitly.
- [ ] Sync semantics match available server metadata.
- [ ] Chunking, retry, gate, and merge behavior use the base contract.
- [ ] Empty windows, failed chunks, and partial results are reported.

## 5. Add the download CLI script

Create `scripts/download_<db>.py` following the closest existing script.

The script should:

- expose a Python wrapper around the downloader;
- define `main()` with `argparse`;
- provide `--config` to use `config.yaml`;
- expose source-specific selection options;
- expose `--overwrite`, `--dryrun`/`--dry-run`, and thread options when
  supported by the downloader;
- pass only explicitly supplied CLI overrides when config values should be
  preserved;
- print timestamped start/end messages when that is the local convention;
- return or print completed/failed results clearly.

Avoid duplicating URL construction in the script. Keep source behavior in the
downloader module so the Python API and CLI use the same implementation.

## 6. Update configuration and package registration

Add the dataset configuration to
`crocolaketools/config/config.yaml`. Usually add both `<DB>_PHY` and
`<DB>_BGC` entries when the downloader is shared, even if only one type is
currently meaningful. Include only keys used by the source:

- `db`;
- `db_type`;
- `input_path`;
- `server_url`, `protocol`, `response_format` for ERDDAP;
- `overwrite`, `dryrun`, `num_threads`, `sync`, and
  `gated_parallel_download` where applicable.

Use paths relative to the packaged config directory, matching existing entries.
Do not copy a converter-only configuration block without checking the
downloader's constructor contract.

Register the CLI in `setup.py`:

```python
'download_<db> = scripts.download_<db>:main',
```

If the repository adds a new package module, ensure `find_packages()` and
package-data behavior still include it.

## 7. Add tests

Add focused tests in the repository's existing test locations, following the
current fixtures and mocking conventions.

For direct-link downloaders, test:

- URL lists, URL builders, and fallback order;
- directory/index parsing and year/range filtering;
- local filename and archive extraction behavior;
- existing-file skipping and overwrite behavior;
- dry-run summaries;
- unreachable URLs and partial failure reporting;
- parallel pair construction without requiring real network access.

For ERDDAP downloaders, test:

- dataset filtering;
- safe local paths;
- variable intersection and missing-variable behavior;
- URL construction with and without time constraints;
- reset of variables and constraints between calls;
- sync queue decisions using mocked timestamps;
- retry and chunk-size behavior where the repository's test setup supports it;
- empty windows, failed chunks, temporary cleanup, and merge ordering.

Mock network requests and ERDDAP responses. Do not make the test suite depend
on a live external server.

## 8. Validate end to end

Run the smallest relevant checks first:

```bash
python -m py_compile crocolaketools/downloader/downloader<DB>.py \
    scripts/download_<db>.py
git diff --check
```

Then run targeted downloader tests. Exercise a dry run and, when practical,
one small real-data smoke test or a supplied demo fixture. Inspect:

- output filenames and extensions;
- output directory placement;
- archive cleanup;
- row/file counts;
- skipped versus downloaded items;
- ERDDAP variable and time constraints;
- sync behavior on a second run;
- absence of `.tmp` files and temporary chunk directories after completion;
- behavior when one source item fails.

Do not claim success solely because an output directory was created.

## 9. Final review checklist

- [ ] The developer was asked to choose direct-link or ERDDAP.
- [ ] ERDDAP catalogue versus known-dataset behavior was clarified when needed.
- [ ] Source documentation and representative endpoint responses were inspected.
- [ ] The closest downloader implementation was reused appropriately.
- [ ] Raw files remain unmodified unless transformation was explicitly requested.
- [ ] Existing files, overwrite, and dry-run behavior are deterministic.
- [ ] Partial downloads cannot masquerade as completed files.
- [ ] Direct-link archives and fallback URLs are handled explicitly.
- [ ] ERDDAP variables, constraints, timestamps, chunking, retries, and merges
  follow the base contract.
- [ ] The CLI wrapper is implemented and registered in `setup.py`.
- [ ] `config.yaml` contains the correct database/type and downloader settings.
- [ ] Tests cover source discovery, local paths, failures, and reruns.
- [ ] Syntax checks, targeted tests, and a smoke or dry-run validation passed.

## 10. Repository-specific pitfalls

- `Downloader` merges user config with the matching config-file section;
  verify `db` and `db_type` agree before relying on inherited defaults.
- Several older scripts use different option names and return shapes. Match the
  nearest current implementation, but preserve explicit failure reporting.
- A URL may respond to GET but not HEAD; use the source-appropriate reachability
  check, as `DownloaderSprayGliders` does.
- For archive downloads, the extracted file—not the ZIP—is normally the
  idempotency marker.
- ERDDAP builds can consume substantial server memory before the first byte.
  Keep the first-byte gate enabled unless the server has been tested without it.
- ERDDAP dataset variables are not uniform. Requesting a variable absent from
  one dataset can turn an otherwise valid request into HTTP 400.
- A failed ERDDAP chunk must not be merged into a successful-looking final
  file.
- Argo GDAC uses index files, profile selection, and specialized download
  helpers in `argo_tools.py`; it is not a normal direct-link or ERDDAP tabledap
  downloader.
