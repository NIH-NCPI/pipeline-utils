
# pipeline-utils

Standalone, composable CLI tools for generating dbt files (`models.yml`, `sources.yml`, SQL models).

## Commands

All commands accept a data dictionary file, a list of files, or a directory (every `*_dd.*`/`*-dd.*` file in it) as input. All tools also accept `-p`/`--table-prefix`, prepended to every table/model name derived from a dd filename (e.g. dd `accesspolicy_dd.csv` + prefix `inc_access` -> table/model name `inc_access_accesspolicy`).

- `generate_models_yml -i <dds> -o <models.yml> [-p <table_prefix>] [-d <_column_descriptions.md>] [-f <dd_format>]`
  Builds/updates a dbt `models.yml`. Models and columns are matched by name and merged in place: unchanged entries keep their position, changed ones update, new ones are added, and ones no longer backed by a dd are dropped - so re-running produces a small, readable diff instead of rewriting the whole file.
- `generate_sources_yml -i <dds> -o <sources.yml> -s <source_name> --schema <schema> [-p <table_prefix>] [-d <_column_descriptions.md>]`
  Same merge behavior as `generate_models_yml`, but for a `sources.yml` source block.
- `generate_sql_models -i <dds> -o <output_dir> [-p <table_prefix>] [--from-clause-template "{{ source('x', '{table_name}') }}"] [--reference-dir <dir>]`
  Generates one `<table>.sql` per dd. That file is created once and never overwritten again - it's owned by data devs, who hand-edit it to harmonize the data. A `<table>_generated.sql` file is always refreshed with the current dd-derived SQL, so after re-running you can diff the two to see exactly what changed in the dd since the model was last harmonized:
  ```
  diff models/int/patients.sql models/int/patients_generated.sql
  ```

There's no notion of a "stage" baked into the code - int/exp/whatever are just different directories/output paths you choose per call. Use separate `just sql` invocations with different `DD_DIR`, `SQL_DIR`, `TABLE_PREFIX`, and `FROM_CLAUSE_TEMPLATE` values for multiple model sets.

## Pulling data dictionaries from a GitHub release

All three tools also accept `--release <repo_url> <asset_name>` (repeatable), which pulls a named GitHub release asset into memory and uses it as an additional dd source - combine freely with `-i` local files, or omit `-i` entirely to use release assets only. No DD file is ever written to disk for a `--release` source. Options `--release-tag` (default `latest`), `--release-token` (for private repos), and `--refresh` (force a fresh pull, bypassing the in-memory cache) apply to every `--release` entry in the call:

```bash
generate_models_yml \
    --release https://github.com/<owner>/<repo> patients_dd.csv \
    --release https://github.com/<owner>/<repo> labs_dd.csv \
    -o models/access/includes/__models.yml \
    -p inc_access
```

`generate_models_yml_from_release_asset <repo_url> <asset_name> -o <models.yml> [-t <tag>] [--token <token>] [--refresh] [-p <table_prefix>]` remains as a simpler, single-asset convenience command for `models.yml` specifically.

The pulled bytes are cached in memory per `(repository_url, asset_name, tag)`, so calling this again for the same version doesn't re-hit the network - pass `--refresh` to force a fresh pull (e.g. after re-publishing the same tag), or just point at a different tag to always fetch fresh.

From Python, `dbt_pipeline_utils.p_utils.dd_sources.pull_release_dd(repository_url, asset_name, tag=..., token=..., refresh=...)` returns an in-memory source object that can be passed directly (mixed freely with local paths) to `generate_models_yml`/`generate_sources_yml`/`generate_sql_models`.

## Tracking data dictionary versions

All four generators accept `--version-log <path>` (Python: `version_log=`), which appends one row per model/table to a CSV: `timestamp,model,source,version,asset,content_hash`. A row is only appended when that information differs from the last recorded row for that model, so re-running against unchanged data never grows the log - only real refreshes (a new release tag, a re-published asset, or an edited local CSV) do.

## Justfile

Run the individual generators with the root `justfile`. Override variables before the recipe name for each DD set:

```bash
just DD_DIR=data_dictionaries/access \
  MODELS_YML=models/access/__models.yml \
  MODELS_DOCS=models/access/_column_descriptions.md \
  TABLE_PREFIX=inc_access models

just DD_DIR=data_dictionaries/raw \
  SOURCES_YML=models/staging/sources.yml \
  SOURCE_NAME=raw_study SOURCE_SCHEMA=raw sources

just DD_DIR=data_dictionaries/access \
  SQL_DIR=models/access TABLE_PREFIX=inc_access \
  FROM_CLAUSE_TEMPLATE="{{ source('raw_study', '{table_name}') }}" sql
```

`just generate` runs all three recipes. `just help` lists the variables.

Local CSV/DD directories (`DD_DIR`) are the default input. To pull from a GitHub release instead of (or in addition to) `DD_DIR`, set `RELEASE_ASSETS` to one or more space-separated `repo_url|asset_name` entries:

```bash
just RELEASE_ASSETS="https://github.com/o/r|patients_dd.csv" models

just DD_DIR="" \
  RELEASE_ASSETS="https://github.com/o/r|patients_dd.csv https://github.com/o/r|labs_dd.csv" \
  RELEASE_TAG=v1.2.0 REFRESH=1 sql
```

Set `VERSION_LOG=path/to/dd_versions.csv` on any recipe to track what data dictionary version was used for each model, appended only when it changes.

For non-GitHub-release sources, a project-specific importer can still be supplied via `DDS_PULL_COMMAND` and run with `just DDS_PULL_COMMAND='...' pull-dd` to populate `DD_DIR` before generation.

## Shared dbt macros

This repo is itself importable as a dbt package, so macros in [`macros/`](macros/) (`grants/`, `ad_hoc/`, `pipeline/`) can be reused across pipeline repos instead of copy-pasted into each one. It has no models/seeds of its own - `dbt_project.yml` only declares `macro-paths`.

Add it to a pipeline repo's `packages.yml`:

```yaml
packages:
  - git: "https://github.com/NIH-NCPI/pipeline-utils.git"
    revision: main  # pin to a tag/commit for anything beyond local testing
  - package: dbt-labs/dbt_utils   # required directly by some pipeline macros (e.g. combined_union)
    version: [">=1.1.0", "<2.0.0"]
```

Then run `dbt deps` in the pipeline repo and call macros with the package's `name` from `dbt_project.yml` as the namespace, e.g. `{{ dbt_pipeline_utils.grant_devs_access(tag=..., users_role=...) }}`.

dbt packages don't resolve transitive dependencies - if a macro here uses `dbt_utils`, the pipeline repo importing this package must also declare `dbt-labs/dbt_utils` in its own `packages.yml`, as shown above.

## Development

Running the script locally or working on a branch?
Create and activate a virtual environment (recommended):
[Click here] for more on virtual environments.

** NOTE: SOME projects recommend pyenv. 
TODO: Give more info about pyenv

```
# Step 1: cd into the directory to store the venv

# Step 2: run this code. It will create the virtual env named utils_venv in the current directory.
python3 -m venv utils_venv

# Step 3: run this code. It will activate the utils_venv environment
source utils_venv/bin/activate # On Windows: venv\Scripts\activate

# You are ready for installations! 
# If you want to deactivate the venv run:
deactivate
```
Install the package
If working on a new feature it is possible to install a package version within the remote or local branch NOTE If testing changes to dbt_pipeline_utils in the dbt project don't forget to deploy a dbt project branch with the correct dbt_pipeline_utils version in the requirements.txt file! NOTE Any new env variables created, e.g. api keys, will need to be added to the dbt project deployment files.
# remote
pip install git+https://github.com/NIH-NCPI/pipeline-utils.git@{branch_name}

# local
pip install -e .

# Locutus should install using the following command.
pip install git+https://github.com/NIH-NCPI/pipeline_utils.git

# A re-install might be required while testing any changes to this repo, use this command to force the reinstall and ensure the latest version.
pip install --force-reinstall --no-cache-dir git+https://github.com/NIH-NCPI/pipeline-utils.git


# Developers
To add a new data dictionary format, add an entry to `DD_FORMATS` in `src/dbt_pipeline_utils/p_utils/common.py` mapping each expected column (`variable_name`, `description`, `data_type`, `enumerations`, `comment`, `src_variable_name`, `tests`, ...) to the corresponding column name in that format. Pass the format's key as `-f`/`--dd-format` to any of the CLI tools.
 - always update the readme options

Data dictionary files are matched by the `*_dd.*`/`*-dd.*` naming convention when a directory is given as input; the trailing `_dd`/`-dd` is stripped when deriving a default table name from the filename.