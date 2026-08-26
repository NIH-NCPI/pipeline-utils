set shell := ["bash", "-cu"]
set export

# Local DD inputs remain the default. A pull target may populate DD_DIR before
# generation when a release importer is available.
DD_DIR := "data_dictionaries"
DD_FORMAT := "ftd_dd"
TABLE_PREFIX := ""

# Set these to pull a dd asset directly from a GitHub release into memory
# instead of (or in addition to) DD_DIR - no CSV is written to disk. Repeat
# with RELEASE_ASSETS="repo1|asset1 repo2|asset2" for more than one.
RELEASE_ASSETS := ""
RELEASE_TAG := "latest"
RELEASE_TOKEN := ""
REFRESH := ""

VERSION_LOG := ""

MODELS_YML := "models.yml"
MODELS_DOCS := ""

SOURCES_YML := "sources.yml"
SOURCE_NAME := "raw_study"
SOURCE_SCHEMA := "raw"
SOURCES_DOCS := ""

SQL_DIR := "models"
SQL_REFERENCE_DIR := ""
FROM_CLAUSE_TEMPLATE := ""

# Set this to a project-specific local/GitHub release importer when needed.
DDS_PULL_COMMAND := ""

default:
    @just --list

help:
    @just --list --unsorted
    @printf '%s\n' '' 'Common overrides:' \
        '  just DD_DIR=data_dictionaries/access TABLE_PREFIX=inc_access models' \
        '  just MODELS_YML=models/access/__models.yml MODELS_DOCS=models/access/_column_descriptions.md models' \
        '  just SOURCES_YML=models/staging/sources.yml SOURCE_NAME=raw SOURCE_SCHEMA=raw sources' \
        "  FROM_CLAUSE_TEMPLATE=<dbt source/ref template containing {table_name}>" \
        '' \
        'Pulling from a GitHub release instead of local files:' \
        '  just RELEASE_ASSETS="https://github.com/o/r|patients_dd.csv" models' \
        '  just DD_DIR="" RELEASE_ASSETS="https://github.com/o/r|patients_dd.csv https://github.com/o/r|labs_dd.csv" REFRESH=1 models' \
        '' \
        'Tracking data versions: VERSION_LOG=dd_versions.csv'

# Build a dd-source args array (-i "$DD_DIR" plus one --release pair per
# "repo|asset" entry in RELEASE_ASSETS) shared by models/sources/sql below.
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

models:
    #!/usr/bin/env bash
    {{ _dd_source_args }}
    args+=(-o "$MODELS_YML" -f "$DD_FORMAT")
    if [[ -n "$TABLE_PREFIX" ]]; then args+=(-p "$TABLE_PREFIX"); fi
    if [[ -n "$MODELS_DOCS" ]]; then args+=(-d "$MODELS_DOCS"); fi
    if [[ -n "$VERSION_LOG" ]]; then args+=(--version-log "$VERSION_LOG"); fi
    generate_models_yml "${args[@]}"

sources:
    #!/usr/bin/env bash
    {{ _dd_source_args }}
    args+=(-o "$SOURCES_YML" -s "$SOURCE_NAME" --schema "$SOURCE_SCHEMA" -f "$DD_FORMAT")
    if [[ -n "$TABLE_PREFIX" ]]; then args+=(-p "$TABLE_PREFIX"); fi
    if [[ -n "$SOURCES_DOCS" ]]; then args+=(-d "$SOURCES_DOCS"); fi
    if [[ -n "$VERSION_LOG" ]]; then args+=(--version-log "$VERSION_LOG"); fi
    generate_sources_yml "${args[@]}"

sql:
    #!/usr/bin/env bash
    {{ _dd_source_args }}
    args+=(-o "$SQL_DIR" -f "$DD_FORMAT")
    if [[ -n "$TABLE_PREFIX" ]]; then args+=(-p "$TABLE_PREFIX"); fi
    if [[ -n "$FROM_CLAUSE_TEMPLATE" ]]; then args+=(--from-clause-template "$FROM_CLAUSE_TEMPLATE"); fi
    if [[ -n "$SQL_REFERENCE_DIR" ]]; then args+=(--reference-dir "$SQL_REFERENCE_DIR"); fi
    if [[ -n "$VERSION_LOG" ]]; then args+=(--version-log "$VERSION_LOG"); fi
    generate_sql_models "${args[@]}"

generate: models sources sql

pull-dd:
    #!/usr/bin/env bash
    if [[ -z "$DDS_PULL_COMMAND" ]]; then
        echo "Set DDS_PULL_COMMAND to a command that downloads/releases DD files into DD_DIR=$DD_DIR." >&2
        exit 2
    fi
    eval "$DDS_PULL_COMMAND"

clean-generated:
    if [[ -d "$SQL_DIR" ]]; then
    find "$SQL_DIR" -type f -name '*_generated.sql' -delete
    fi