set shell := ["bash", "-cu"]
set export

# Local DD inputs remain the default. A pull target may populate DD_DIR before
# generation when a release importer is available.
DD_DIR := "data_dictionaries"  # Local DD file or directory input root for generators.
DD_FORMAT := "ftd_dd"  # DD column-mapping profile key used by all generators.
TABLE_PREFIX := ""  # Optional prefix prepended to derived table/model names.

# Set these to pull a dd asset directly from a GitHub release into memory
# instead of (or in addition to) DD_DIR - no CSV is written to disk. Repeat
# with RELEASE_ASSETS="repo1|asset1 repo2|asset2" for more than one.
RELEASE_ASSETS := ""  # Space-separated release inputs: "repo_url|asset_name" (repeat by spacing).
RELEASE_TAG := "latest"  # Release tag for all --release pulls.
RELEASE_TOKEN := ""  # GitHub token for private release access.
REFRESH := ""  # Set non-empty (for example 1) to force fresh pulls instead of cache.

# Presets understood by release-aware commands: kf, include/inc, common (or full URL).
RELEASE_REPO := ""  # Repo alias/url for extract-release-file (kf/include/inc/common or full URL).
RELEASE_ARTIFACT := "project-artifacts.zip"  # ZIP release asset name to extract from.
RELEASE_INTERNAL_PATH := ""  # Internal ZIP file path or directory prefix to copy.
RELEASE_OUTPUT_PATH := ""  # Destination file path (file mode) or directory path (dir mode).

VERSION_LOG := ""  # Optional CSV path to append DD source/version/hash change history.

MODELS_YML := "models.yml"  # Output path for generate_models_yml.
MODELS_DOCS := ""  # Optional markdown file for generated model column docs.

SOURCES_YML := "sources.yml"  # Output path for generate_sources_yml.
SOURCE_NAME := "raw_study"  # dbt source block name for tables in sources.yml.
SOURCE_SCHEMA := "raw"  # Database schema recorded in sources.yml source block.
SOURCES_DOCS := ""  # Optional markdown file for generated source column docs.

SQL_DIR := "models"  # Output directory for dev-owned <table>.sql model files.
SQL_REFERENCE_DIR := ""  # Optional output directory for refreshed *_generated.sql reference files.
FROM_CLAUSE_TEMPLATE := ""  # Optional SQL FROM template containing {table_name} placeholder.

# Set this to a project-specific local/GitHub release importer when needed.
DDS_PULL_COMMAND := ""  # Optional custom command used by pull-dd to populate DD_DIR.

default:
    @just --list

help:
    @just --list --unsorted
    @printf '%s\n' '' 'Common overrides:' \
        '  just DD_DIR=data_dictionaries/access TABLE_PREFIX=inc_access gen_models_ymls' \
        '  just MODELS_YML=models/access/__models.yml MODELS_DOCS=models/access/_column_descriptions.md gen_models_ymls' \
        '  just SOURCES_YML=models/staging/sources.yml SOURCE_NAME=raw SOURCE_SCHEMA=raw gen_sources_ymls' \
        "  FROM_CLAUSE_TEMPLATE=<dbt source/ref template containing {table_name}>" \
        '' \
        'Pulling from a GitHub release instead of local files:' \
        '  just RELEASE_ASSETS="https://github.com/o/r|patients_dd.csv" gen_models_ymls' \
        '  just DD_DIR="" RELEASE_ASSETS="https://github.com/o/r|patients_dd.csv https://github.com/o/r|labs_dd.csv" REFRESH=1 gen_models_ymls' \
        '  just RELEASE_REPO=kf RELEASE_INTERNAL_PATH=data_dictionaries/patients_dd.csv RELEASE_OUTPUT_PATH=/tmp/patients_dd.csv extract-release-file' \
        '' \
        'Tracking data versions: VERSION_LOG=dd_versions.csv'

# Build a dd-source args array (-i "$DD_DIR" plus one --release pair per
# "repo|asset" entry in RELEASE_ASSETS) shared by models/gen_sources_ymls/sql below.
_dd_source_args := '
    args=()
    if [[ -n "$DD_DIR" ]]; then args+=(-i "$DD_DIR"); fi
    for entry in $RELEASE_ASSETS; do
        repo="${entry%%|*}"
        asset="${entry#*|}"
        args+=(--release "$repo" "$asset")
    done
    if [[ -n "$RELEASE_TAG" ]]; then args+=(--release-tag "$RELEASE_TAG"); fi
    if [[ -n "$RELEASE_TOKEN" ]]; then args+=(--release-token "$RELEASE_TOKEN"); fi
    if [[ -n "$REFRESH" ]]; then args+=(--refresh); fi
'

gen_models_ymls:
    #!/usr/bin/env bash
    {{ _dd_source_args }}
    args+=(-o "$MODELS_YML" -f "$DD_FORMAT")
    if [[ -n "$TABLE_PREFIX" ]]; then args+=(-p "$TABLE_PREFIX"); fi
    if [[ -n "$MODELS_DOCS" ]]; then args+=(-d "$MODELS_DOCS"); fi
    if [[ -n "$VERSION_LOG" ]]; then args+=(--version-log "$VERSION_LOG"); fi
    generate_models_yml "${args[@]}"

gen_sources_ymls:
    #!/usr/bin/env bash
    {{ _dd_source_args }}
    args+=(-o "$SOURCES_YML" -s "$SOURCE_NAME" --schema "$SOURCE_SCHEMA" -f "$DD_FORMAT")
    if [[ -n "$TABLE_PREFIX" ]]; then args+=(-p "$TABLE_PREFIX"); fi
    if [[ -n "$SOURCES_DOCS" ]]; then args+=(-d "$SOURCES_DOCS"); fi
    if [[ -n "$VERSION_LOG" ]]; then args+=(--version-log "$VERSION_LOG"); fi
    generate_sources_yml "${args[@]}"

gen_sql:
    #!/usr/bin/env bash
    {{ _dd_source_args }}
    args+=(-o "$SQL_DIR" -f "$DD_FORMAT")
    if [[ -n "$TABLE_PREFIX" ]]; then args+=(-p "$TABLE_PREFIX"); fi
    if [[ -n "$FROM_CLAUSE_TEMPLATE" ]]; then args+=(--from-clause-template "$FROM_CLAUSE_TEMPLATE"); fi
    if [[ -n "$SQL_REFERENCE_DIR" ]]; then args+=(--reference-dir "$SQL_REFERENCE_DIR"); fi
    if [[ -n "$VERSION_LOG" ]]; then args+=(--version-log "$VERSION_LOG"); fi
    generate_sql_models "${args[@]}"

pull-dd:
    #!/usr/bin/env bash
    if [[ -z "$DDS_PULL_COMMAND" ]]; then
        echo "Set DDS_PULL_COMMAND to a command that downloads/releases DD files into DD_DIR=$DD_DIR." >&2
        exit 2
    fi
    eval "$DDS_PULL_COMMAND"

extract-release-file:
    #!/usr/bin/env bash
    if [[ -z "$RELEASE_REPO" ]]; then
        echo "Set RELEASE_REPO to a preset (kf/include/inc/common) or a full GitHub URL." >&2
        exit 2
    fi
    if [[ -z "$RELEASE_INTERNAL_PATH" ]]; then
        echo "Set RELEASE_INTERNAL_PATH to the file path inside $RELEASE_ARTIFACT." >&2
        exit 2
    fi
    if [[ -z "$RELEASE_OUTPUT_PATH" ]]; then
        echo "Set RELEASE_OUTPUT_PATH to the destination file path." >&2
        exit 2
    fi

    args=(
      "$RELEASE_REPO"
      "$RELEASE_ARTIFACT"
      "$RELEASE_INTERNAL_PATH"
      "$RELEASE_OUTPUT_PATH"
      --tag "$RELEASE_TAG"
    )
    if [[ -n "$RELEASE_TOKEN" ]]; then args+=(--token "$RELEASE_TOKEN"); fi
    if [[ -n "$REFRESH" ]]; then args+=(--refresh); fi

    extract_release_artifact_file "${args[@]}"

clean-dir:
    if [[ -d "$SQL_DIR" ]]; then
    find "$SQL_DIR" -type f -name '*_generated.sql' -delete
    fi