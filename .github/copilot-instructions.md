# Copilot Instructions for CrocoLakeTools

These instructions apply to all agents working in this repository. Follow
them together with the task-specific guides:

- `.github/agents/converter.agent.md`
- `.github/agents/downloader.agent.md`
- `docs/DEV_CONVERTER_INSTRUCTIONS.md`
- `docs/DEV_DOWNLOADER_INSTRUCTIONS.md`
- `CONTRIBUTE.md`

Use the most specific applicable guide for converter and downloader work.
Developer requirements take precedence when they are compatible with the
repository, safety, and correctness requirements below.

## Start in plan mode

Before editing:

1. Inspect the relevant files, tests, configuration, and documentation.
2. Search for existing helpers, precedents, and public contracts.
3. Identify ambiguities and ask focused clarifying questions.
4. Present a structured implementation plan for approval.

Do not silently choose behavior when multiple reasonable interpretations exist.
State uncertainty and blocked validation explicitly.

## Repository structure

- Python package: `crocolaketools/`
- Converters: `crocolaketools/converter/`
- Downloaders: `crocolaketools/downloader/`
- CLI scripts and entry points: `scripts/` and `setup.py`
- Runtime configuration: `crocolaketools/config/`
- Tests: `crocolaketools/test/`
- Developer and agent documentation: `docs/` and `.github/agents/`
- Demo and source data: `crocolaketools/demo/`

Before adding code, inspect the closest existing implementation and reuse
shared base classes, utilities, configuration conventions, and test helpers.
Keep CLI orchestration in `scripts/`; keep source-specific logic in package
modules.

## Environment and dependencies

- Do not load, activate, or create Python virtualenv environments.
- Use the Conda environment named `crocolaketools`, as defined by
  `environment.yml`.
- Prefer commands such as:

  ```bash
  conda run -n crocolaketools python -m pytest <target>
  conda run -n crocolaketools python -m py_compile <files>
  ```

- Treat `environment.yml` and the existing dependency files as the source of
  truth for dependencies.
- Do not install packages or introduce new tooling unless an existing command
  requires it or the task explicitly calls for it. Always ask for confirmation before installing.
- Do not modify dependency manifests for unrelated work.

## Python style and code quality

- Follow PEP 8 and the style of nearby code.
- Preserve existing naming, spacing, import organization, module headers,
  separator banners, logging conventions, and docstring conventions.
- Use clear names, focused functions, explicit control flow, and appropriate
  type hints where they improve safety.
- Match existing `Arguments`/`Returns` documentation in public methods.
- Add comments only for non-obvious reasoning or source-specific behavior.
- Avoid unnecessary casts, duplicated helpers, speculative abstractions, and
  unrelated refactors.
- Preserve public APIs, configuration keys, output schemas, and file naming
  behavior unless a deliberate change is required.
- Do not add broad exception handlers, silent fallbacks, or success-shaped
  results after failures. Surface errors consistently with repository
  patterns.

## Project-specific architecture

- Reuse `Converter` and `Downloader` base behavior instead of duplicating it.
- Use `DownloaderERDDAP` for ERDDAP catalogue/query workflows.
- Keep PHY/BGC distinctions and CrocoLake standardized names intact.
- Add new CLI commands to `setup.py` under `console_scripts`.
- Add corresponding configuration entries to packaged YAML files.
- Keep raw downloads unmodified unless transformation is explicitly requested.
- Treat Argo GDAC/index workflows as specialized; do not force them into a
  normal downloader or converter template.

## Dask, data, and resource safety

- Use partition-aware Dask operations and `map_partitions()` where pandas
  row-wise behavior cannot be inferred safely.
- Use explicit metadata when Dask cannot infer output columns or dtypes.
- Reserve `compute()` for genuinely global, bounded metadata operations.
- Avoid unnecessary `persist()` and uncontrolled data materialization.
- Preserve expected schemas, nullable dtypes, units, ordering, and identifier
  semantics.
- Open delayed datasets inside delayed functions and close them explicitly.
- Release locks in `finally` blocks.
- Clean temporary files and directories after success and failure.
- Use atomic temporary-file downloads so partial files are not mistaken for
  completed outputs.

## Tests and validation

Run the smallest relevant checks first:

```bash
conda run -n crocolaketools python -m py_compile <changed-python-files>
git diff --check
conda run -n crocolaketools python -m pytest <targeted-tests>
```

Tests should be deterministic and should not depend on live external servers.
Mock network, ERDDAP, filesystem, and time-dependent behavior where practical.
For data workflows, validate more than file creation:

- output paths, filenames, and archive cleanup;
- schemas, dtypes, units, and missing-value behavior;
- row counts, duplicate handling, and ordering;
- profile/platform identifiers and PHY/BGC placement;
- retries, skipped items, partial failures, and reruns;
- temporary-file and resource cleanup.

Do not claim completion based only on a successful import, created directory,
or one passing smoke command. Report tests not run and external validation
that was unavailable.

## Git and worktree safety

- Inspect `git status` and the relevant diff before changing files.
- Preserve unrelated user changes, including changes in files you need to
  touch; understand them before editing.
- Alwaysk for permission before committing anything.
- Never use destructive commands such as `git reset --hard` or
  `git checkout --` unless explicitly requested and approved.
- Keep changes surgical and avoid reformatting unrelated code.
- Do not commit secrets, credentials, tokens, local data, or generated output.
- Do not amend commits unless explicitly requested.
- Review the final diff for accidental files, debug output, and environment
  paths.

## Documentation and handoff

Update documentation when behavior, configuration, CLI usage, or repository
workflow changes. Prefer concise, durable guidance over task-specific notes.

When completing work, lead with the outcome and briefly state:

- what changed;
- the meaningful behavior or integration impact;
- any incomplete or blocked validation.

Do not hide uncertainty or claim checks that were not performed.
