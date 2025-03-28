import pandas as pd
import re
from dbt_pipeline_utils.scripts.helpers.general import *
from dbt_pipeline_utils.scripts.helpers.common import type_mapping
from dbt_pipeline_utils.scripts.helpers.ftd_doc_generation import *
from pathlib import Path


def generate_dbt_models_yml(
    data_dictionary, column_data, output_dir, study_id, ftd_model=None
):
    """Generates dbt models.yml file for each table in its respective directory, including src and staging models."""

    ftd_models = []

    for table_id, table_info in data_dictionary.items():
        src_models = []
        if ftd_model:
            table_models_dir = output_dir 
            id_tables = [f"{study_id}_ftd_{table_id}"]
        else:
            table_models_dir = output_dir / Path(f"{table_id}")
            id_tables = [f"{study_id}_src_{table_id}", f"{study_id}_stg_{table_id}"]

        for table_name in id_tables:

            columns_metadata = [
                {
                    "name": col_name,
                    "description": f'{{{{ doc("{table_name}_{col_name_code}") }}}}',
                    "data_type": col_data_type,
                }
                for col_name, col_name_code, _, col_data_type, _ in column_data.get(
                    table_name, []
                )
            ]

            model_entry = {
                "name": table_name,
                "description": table_info.get("description", f"Model for {table_name}."),
                "columns": columns_metadata
            }

            if ftd_model:
                ftd_models.append(model_entry)
            else:
                src_models.append(model_entry)

        if not ftd_model:
            models = {"version": 2, "models": src_models}
            filepath = table_models_dir / "__models.yml"
            write_file(filepath, models)

    if ftd_model:
        # Write all ftd models at once
        models = {
            "version": 2,
            "models": ftd_models
        }
        filepath = output_dir / "__models.yml"
        write_file(filepath, models)


def generate_column_descriptions(data_dictionary, column_data, output_dir, study_id, ftd_model=None):
    """Generates a separate column_descriptions.md for each table in its respective docs directory."""
    filepath = output_dir / "column_descriptions.md"

    # Read existing file content if it exists
    if not filepath.exists():
        filepath.touch()
    existing_data = filepath.read_text().rstrip()

    # Use regex to extract existing col_doc_ids
    existing_col_doc_ids = set(re.findall(r"\{%\s*docs\s+([\w\d_]+)\s*%\}", existing_data))

    new_descriptions = []

    for table_id, table_info in data_dictionary.items():
        print(f'printing table: {table_id}')
        if ftd_model:
            # Only process the FTD table key
            table_keys = [f"{study_id}_ftd_{table_id}"]
        else:
            # Process both src and stg table keys in a single loop
            table_keys = [f"{study_id}_src_{table_id}", f"{study_id}_stg_{table_id}"]

        for table_key in table_keys:
            print(f'printing table: {table_id} - key: {table_key}')

            table_description = table_info.get("description", f"Model for {table_key}.")
            table_desc_id = f"{table_key}_description"
            table_desc_block = f"{{% docs {table_desc_id} %}}\n{table_description}\n{{% enddocs %}}\n"

            # Add table description if not already present
            if table_desc_id not in existing_col_doc_ids:
                new_descriptions.append(table_desc_block)
                existing_col_doc_ids.add(table_desc_id)

            for col_name, col_name_code, col_description, _, _ in column_data.get(table_key, []):
                col_doc_id = f"{table_key}_{col_name_code}"
                col_desc_block = f"{{% docs {col_doc_id} %}}\n{col_description}\n{{% enddocs %}}\n"

                if col_doc_id not in existing_col_doc_ids:
                    new_descriptions.append(col_desc_block)
                    existing_col_doc_ids.add(col_doc_id)  # Ensure newly added col_doc_id is tracked

    new_data = "\n\n".join(new_descriptions).strip()

    if new_data:
        data = existing_data + "\n\n" + new_data if existing_data else new_data
        write_file(filepath, data)
        print(f"Edited: {filepath}")
    else:
        print(f"No updates needed: {filepath}")


def generate_model_descriptions(data_dictionary, output_dir, study_id):
    """Generates model_descriptions.md using the specified format."""
    model_descriptions = []

    # Group tables by prefix (e.g., "moo_src_", "moo_stg_")
    grouped_tables = {}
    for table_id, table_info in data_dictionary.items():
        prefix = table_id.split("_")[0]  # Assumes prefix is the first part of table_id
        grouped_tables.setdefault(prefix, []).append((table_id, table_info))

    for prefix, tables in grouped_tables.items():
        model_descriptions.append(f"### {prefix.capitalize()} Models\n")

        for table_id, table_info in tables:
            src_table_id = f"{study_id}_src_{table_id}"
            src_description = table_info.get(
                "description", f"Model for {src_table_id}."
            )
            model_descriptions.append(
                f"{{% docs {src_table_id} %}}\n{src_description}\n{{% enddocs %}}\n"
            )

            stg_table_id = f"{study_id}_stg_{table_id}"
            stg_description = table_info.get("description", f"Model for {stg_table_id}.")
            model_descriptions.append(f"{{% docs {stg_table_id} %}}\n{stg_description}\n{{% enddocs %}}\n")

            data = "\n".join(model_descriptions)

            filepath = output_dir / "model_descriptions.md"

            write_file(filepath, data)


def generate_src_sql_files(data_dictionary, output_dir, db_name, study_id):
    """Generates SQL files dynamically for each table in its respective directory."""

    for table_id in data_dictionary.keys():
        src_table_id = f"{study_id}_src_{table_id}"
        sql_content = f"""{{{{ config(materialized='table') }}}}

SELECT * FROM {db_name}.{study_id}_src_data.{table_id}
"""
        filepath = output_dir / Path(table_id) / f"{src_table_id}.sql"

        write_file(filepath, sql_content)


def generate_stg_sql_files(
    data_dictionary, column_data, output_dir, study_id, type_mapping
):
    """Generates staging SQL files dynamically for each table based on the data dictionary."""

    for table_id, table_info in data_dictionary.items():
        src_table = f"{study_id}_src_{table_id}"
        new_table = f"{study_id}_stg_{table_id}"
        filepath = output_dir / Path(table_id) / f"{new_table}.sql"

        column_definitions = []
        for col_name, column_name_code, _, col_data_type, _ in column_data.get(src_table, []):
            sql_type = type_mapping.get(col_data_type, "TEXT")
            column_definitions.append(f'"{col_name}"::{sql_type} AS "{column_name_code}"')

        sql_content = f"""{{{{ config(materialized='table') }}}}

WITH source AS (
    SELECT 
       {",\n       ".join(column_definitions)}
    FROM {{{{ ref('{src_table}') }}}}
)

SELECT 
    *,
    CONCAT(study_code, '-', participant_global_id) AS ftd_key
FROM source
"""

        # Write SQL file to the correct directory
        write_file(filepath, sql_content)

def generate_stg_dds(
    data_dictionary, src_dd_path, study_id
):
    """Generates staging SQL files dynamically for each table based on the data dictionary.
    open the src dd and apply minimal transformations"""

    for table_id, table_info in data_dictionary.items():
        src_table = f"{study_id}_src_{table_id}"
        filepath = src_dd_path / f"{table_id}_stg_dd.csv"

        ddict = table_info.get("table_details")
        ddict_full_path = src_dd_path / ddict
        stg_df = read_file(ddict_full_path)

        column_data = load_src_column_data(
            data_dictionary, src_dd_path, study_id, src_only=True
        )

        column_mapping = {
            col_name: column_name_code
            for col_name, column_name_code, _, _, _ in column_data.get(src_table, [])
        }

        stg_df["src_variable_name"] = stg_df["variable_name"]

        stg_df["variable_name"] = stg_df["variable_name"].map(column_mapping)

        t_path = src_dd_path / f"ftd_transformations/{table_id}_stg_additions_dd.csv"
        if t_path.exists():  
            transformations = read_file(t_path)
            stg_df = pd.concat([stg_df, transformations], ignore_index=True)
        else:
            pass

        write_file(filepath, stg_df)


def generate_dbt_project_yaml(data_dictionary, study_id, output_dir):
    study_info = {}

    for table_id in data_dictionary.keys():
        src_table_id = f"{study_id}_src_{table_id}"
        stg_table_id = f"{study_id}_stg_{table_id}"

        study_info[table_id] = {
            src_table_id: {
                "+schema": f"{study_id}_data",
                "+materialized": "table",
            },
            stg_table_id: {
                "+schema": f"{study_id}_data",
                "+materialized": "table",
            },
        }

    # Correct structure for dbt_project.yml
    dbt_config = {
        "name": study_id,
        "version": "1.0.0",
        "profile": "pgtest",
        "model-paths": ["models"],
        "macro-paths": ["macros"],
        "snapshot-paths": ["snapshots"],
        "clean-targets": ["target", "dbt_packages"],
        "models": {
            "+schema": f"{study_id}_data",
            "+materialized": "table",
            **study_info,
        },
    }

    filepath = output_dir / "dbt_project.yml"

    write_file(filepath, dbt_config)


def generate_model_docs(study_config, paths, db_name, type_mapping):
    """Main function to generate dbt model files, loading column data once."""

    data_dictionary = study_config.get("data_dictionary", {})

    study_id = study_config.get("study_id", "study")

    generate_dbt_project_yaml(data_dictionary, study_id, paths["dbtp_src_study_dir"])

    generate_stg_dds(data_dictionary, paths["dbtp_study_data_dir"], study_id)

    column_data = load_src_column_data(
        data_dictionary, paths["dbtp_study_data_dir"], study_id
    )

    generate_dbt_models_yml(
        data_dictionary, column_data, paths["dbtp_src_study_model_dir"], study_id
    )
    generate_column_descriptions(
        data_dictionary, column_data, paths["dbtp_src_study_model_docs_dir"], study_id
    )
    generate_model_descriptions(
        data_dictionary, paths["dbtp_src_study_model_docs_dir"], study_id
    )
    generate_src_sql_files(
        data_dictionary, paths["dbtp_src_study_model_dir"], db_name, study_id
    )
    generate_stg_sql_files(
        data_dictionary,
        column_data,
        paths["dbtp_src_study_model_dir"],
        study_id,
        type_mapping,
    )

def generate_ftd_model_docs(study_config, src_study_dir_path, ftd_study_dir_path, ftd_yml_path, dbtp_ftdc_dir,dbtp_ftdc_study_docs_dir, ftd_model_study_dir, utils_ftd_study_dir_path, utils_ftd_yml, ftd_study_yml_path):
    """Main function to generate dbt model files, loading column data once."""
    
    study_id = study_config.get("study_id", "study")

    generate_ftd_dds(utils_ftd_study_dir_path,
                    utils_ftd_yml,
                    src_study_dir_path,
                    ftd_study_dir_path,
                    ftd_yml_path,
                    study_id,
                    study_config)

    data_dictionary = study_config.get("data_dictionary", {})
    ftd_config = read_file(ftd_study_yml_path)

    ftd_dd = ftd_config.get("data_dictionary", {})

    column_data = load_ftd_column_data(
        data_dictionary,
        src_study_dir_path,
        ftd_dd,
        ftd_study_dir_path,
        study_id,
    )

    generate_dbt_models_yml(
        ftd_dd, column_data, dbtp_ftdc_study_docs_dir, study_id, ftd_model=True
    )
    generate_ftd_sql_files(
        data_dictionary, ftd_dd, column_data, ftd_model_study_dir, study_id, type_mapping
    )
    generate_ftd_dbt_project_yaml(ftd_dd, study_id, dbtp_ftdc_dir)
    generate_column_descriptions(
        ftd_dd,
        column_data,
        dbtp_ftdc_study_docs_dir,
        study_id,
        ftd_model=True,
    )
