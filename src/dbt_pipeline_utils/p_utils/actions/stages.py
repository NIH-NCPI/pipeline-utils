from pathlib import Path
from typing import Any, Optional

from dbt_pipeline_utils import logger
from dbt_pipeline_utils.p_utils.actions import (load_study_config, resolve_study_config_path)
from dbt_pipeline_utils.p_utils.configs import StudyConfig
from dbt_pipeline_utils.p_utils.factory_functions import PipelineObject
from dbt_pipeline_utils.p_utils.files import shorten_identifier,get_existing_yaml, normalize_name, write_file, read_file


def run_all(pipeline_objects):
    """Run every generation stage (equivalent to ``generate_pipeline``)."""
    any_obj = next(iter(pipeline_objects.values()))

    # Per-project stages
    any_obj.structure.generate_dbt_project_yaml()
    any_obj.structure.generate_models_yml_files(
        any_obj.int_config, any_obj.exp_config
    )
    any_obj.structure.generate_static_sql_models(
        config=any_obj.int_config, stage="stb"
    )
    any_obj.structure.generate_static_sql_models(
        config=any_obj.int_config, stage="int"
    )
    any_obj.structure.generate_static_sql_models(
        config=any_obj.int_config, stage="combined"
    )
    any_obj.structure.generate_static_sql_models(
        config=any_obj.exp_config, stage="exp"
    )
    any_obj.structure.copy_static_export_dir()
    any_obj.structure.copy_project_macros_dir()
    any_obj.structure.copy_profiles_yml()
    run_dag_generation(pipeline_objects)

    for table_name, obj in pipeline_objects.items():
        logger.info(f"Table:{table_name} - file generation STARTED")
        obj.structure.generate_stg_dds()
        obj.structure.generate_sources_yml_files()
        obj.structure.generate_study_sql_models()
        obj.structure.generate_run_script()
        logger.info(f"Table:{table_name} - file generation COMPLETE")


def run_dbt_project(pipeline_objects):
    """Generate ``dbt_project.yml``."""
    any_obj = next(iter(pipeline_objects.values()))
    any_obj.structure.generate_dbt_project_yaml()
    logger.info("dbt_project.yml generated")


def run_models_yml(pipeline_objects):
    """Generate ``__models.yml`` and column descriptions for int/exp stages."""
    any_obj = next(iter(pipeline_objects.values()))
    any_obj.structure.generate_models_yml_files(
        any_obj.int_config, any_obj.exp_config
    )
    logger.info("Models YAML files generated")


def run_static_models(pipeline_objects, stage=None):
    """Generate static SQL models for stb, access, combined, and/or exp.

    Parameters
    ----------
    stage : str | None
        One of ``"stb"``, ``"access"``, ``"combined"``, ``"exp"``. The legacy alias
        ``"int"`` is accepted and mapped to ``"access"``. When *None*, all
        four are generated.
    """
    any_obj = next(iter(pipeline_objects.values()))
    stages = [stage] if stage else ["stb", "access", "combined", "exp"]
    for s in stages:
        if s == "int":
            s = "access"
        config = any_obj.exp_config if s == "exp" else any_obj.int_config
        any_obj.structure.generate_static_sql_models(config=config, stage=s)
        logger.info(f"Static SQL models generated for stage: {s}")


def run_copy_files(pipeline_objects):
    """Copy static files (export dir, macros, ``profiles.yml``)."""
    any_obj = next(iter(pipeline_objects.values()))
    any_obj.structure.copy_static_export_dir()
    any_obj.structure.copy_project_macros_dir()
    any_obj.structure.copy_profiles_yml()
    logger.info("Static files copied")


def run_staging_dds(pipeline_objects):
    """Generate staging data dictionaries for each table."""
    for table_name, obj in pipeline_objects.items():
        logger.info(f"Table:{table_name} - generating staging DDs")
        obj.structure.generate_stg_dds()
        logger.info(f"Table:{table_name} - staging DDs COMPLETE")


def run_sources(pipeline_objects):
    """Generate ``sources.yml`` for each table."""
    for table_name, obj in pipeline_objects.items():
        logger.info(f"Table:{table_name} - generating sources.yml")
        obj.structure.generate_sources_yml_files()
        logger.info(f"Table:{table_name} - sources.yml COMPLETE")


def run_study_models(pipeline_objects):
    """Generate study-level SQL models for each table."""
    for table_name, obj in pipeline_objects.items():
        logger.info(f"Table:{table_name} - generating study SQL models")
        obj.structure.generate_study_sql_models()
        logger.info(f"Table:{table_name} - study SQL models COMPLETE")


def run_run_script(pipeline_objects):
    """Generate dbt run script once using any_obj."""
    any_obj = next(iter(pipeline_objects.values()))
    logger.info("Generating run script (single call on any_obj)")
    any_obj.structure.generate_run_script()
    logger.info("Run script generation COMPLETE")


def run_dag_generation(pipeline_objects, overrides: Optional[dict[str, Any]] = None):
    """Generate an Airflow DAG from the study config's optional ``dag`` section."""
    any_obj = next(iter(pipeline_objects.values()))
    logger.info("Generating Airflow DAG from study config")
    any_obj.structure.generate_dag()
    logger.info("DAG generation COMPLETE")


    

def generate_sources_standalone(
    *,
    dd_path,
    dd_format,
    data_files,
    study_id,
    schema,
    output_dir,
):
    """Generate ``sources.yml`` and column descriptions from a source data dictionary without a study config."""
    import re
    import pandas as pd
    from dbt_pipeline_utils.p_utils.common import DD_FORMATS

    dd_path = Path(dd_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # get dd cols and descriptions
    dd_df = read_file(dd_path)
    column_map = DD_FORMATS[dd_format]
    dd_df = dd_df.astype("string").where(pd.notna(dd_df), None)

    def _as_str(v):
        return None if v is None or pd.isna(v) else str(v)

    column_info = []
    for _, row in dd_df.iterrows():
        col_name = _as_str(row.get(column_map["variable_name"]))
        if col_name is None:
            continue
        col_name_code = normalize_name(col_name, extension="drop")
        description = _as_str(row.get(column_map["description"]))
        column_info.append((col_name, col_name_code, description))

    # build sources.yml
    filepath = output_dir / "sources.yml"
    existing = get_existing_yaml(filepath)
    sources = existing.setdefault("sources", [])

    study_block = next(
        (s for s in sources if s.get("name") == study_id), None
    )
    if study_block is None:
        study_block = {"name": study_id, "schema": schema, "tables": []}
        sources.append(study_block)

    tables_list = study_block.setdefault("tables", [])
    existing_names = {t["name"] for t in tables_list}

    all_doc_blocks = []

    for filename in data_files:
        table_name = normalize_name(filename, trailing=False, extension="drop")
        if table_name in existing_names:
            continue

        columns = []
        for col_name, col_name_code, description in column_info:
            doc_id = shorten_identifier(
                normalize_name(
                    [table_name, col_name_code],
                    trailing=False,
                    extension="drop",
                )
            )
            columns.append({
                "name": col_name,
                "description": f'{{{{ doc("{doc_id}") }}}}',
            })
            all_doc_blocks.append((
                doc_id,
                description.strip()
                if isinstance(description, str) and description.strip()
                else col_name,
            ))

        tables_list.append({
            "name": table_name,
            "description": f"Source table for {table_name}.",
            "columns": columns,
        })

    write_file(filepath, existing, mode="merge")

    # build column descriptions in column_descriptions.md
    # skipping any that already exist based on doc_id
    desc_path = output_dir / "column_descriptions.md"
    if not desc_path.exists():
        desc_path.touch()

    desc_text = desc_path.read_text().rstrip()
    existing_ids = set(re.findall(r"\{%\s*docs\s+([\w\d_]+)\s*%\}", desc_text))

    new_blocks = []
    for doc_id, description in all_doc_blocks:
        if doc_id in existing_ids:
            continue
        new_blocks.append(
            f"{{% docs {doc_id} %}}\n{description}\n{{% enddocs %}}"
        )
        existing_ids.add(doc_id)

    if new_blocks:
        content = (
            desc_text + "\n\n" + "\n\n".join(new_blocks)
            if desc_text
            else "\n\n".join(new_blocks)
        )
        write_file(desc_path, content, mode="overwrite")

    logger.info(f"sources.yml written to {filepath}")


def generate_staging_dds_standalone(
    *,
    dd_path,
    dd_format,
    table_name,
    output_dir,
    additions_path=None,
):
    """Generate a staging data dictionary without a study config.

    Transforms the source DD from *dd_format* into ``pipeline_format``.

    """
    import pandas as pd
    from dbt_pipeline_utils.p_utils.common import DD_FORMATS

    dd_path = Path(dd_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    output_filename = normalize_name(
        [table_name, "stg_dd.csv"], trailing=False, extension="keep"
    )
    output_path = output_dir / output_filename

    # Read input DD
    input_dd = read_file(dd_path)
    column_map = DD_FORMATS[dd_format]
    pipeline_map = DD_FORMATS["pipeline_format"]

    # Build normalised column name mapping
    dd_clean = input_dd.astype("string").where(pd.notna(input_dd), None)
    col_name_mapping = {}
    for _, row in dd_clean.iterrows():
        col_name = row.get(column_map["variable_name"])
        if col_name:
            col_name_mapping[col_name] = normalize_name(col_name, extension="drop")

    # Map src_variable_name
    input_dd[pipeline_map["src_variable_name"]] = input_dd.get(
        column_map["variable_name"], ""
    )

    # Map variable_name
    variable_name_key = column_map.get("variable_name", "variable_name")
    if variable_name_key in input_dd.columns:
        input_dd[pipeline_map["variable_name"]] = (
            input_dd[variable_name_key]
            .map(col_name_mapping)
            .fillna(input_dd[variable_name_key])
        )

    # Rename columns to pipeline format
    rename_map = {
        column_map[key]: pipeline_map[key]
        for key in pipeline_map
        if key in column_map
    }
    input_dd.rename(columns=rename_map, inplace=True)

    # Apply additions if provided
    if additions_path:
        additions_path = Path(additions_path)
        if additions_path.exists():
            additions = read_file(additions_path)
            input_dd = pd.concat([input_dd, additions])
        else:
            logger.debug(f"Additions file not found: {additions_path}")

    write_file(output_path, input_dd)
    logger.info(f"Staging DD written to {output_path}")
