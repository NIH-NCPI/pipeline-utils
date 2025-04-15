import pandas as pd
from dbt_pipeline_utils.scripts.helpers.general import *
from dbt_pipeline_utils.scripts.helpers.common import type_mapping
from pathlib import Path


def generate_ftd_dbt_project_yaml(data_dictionary, study_id, output_dir):

    filepath = output_dir / "dbt_project.yml"

    # Read existing file content if it exists
    if not filepath.exists():
        filepath.touch()
    existing_data = read_file(filepath)

    models = existing_data.get("models", {})

    import yaml


def generate_ftd_dbt_project_yaml(data_dictionary, project_id, study_id, output_dir):
    filepath = output_dir / "dbt_project.yml"

    # Read existing file content if it exists
    if filepath.exists():
        with open(filepath, "r") as f:
            existing_data = yaml.safe_load(f) or {}
    else:
        existing_data = {}

    # Ensure the correct structure
    models = existing_data.get("models", {})

    # Define table-specific schema and materialization
    study_info = {}

    for table_id in data_dictionary.keys():
        study_info[f"{study_id}_ftd_{table_id}"] = {
            "+schema": f"{study_id}_ftd_data",
            "+materialized": "table"
        }

    # Merge into the models structure
    if study_id not in models:
        models[study_id] = study_info
    else:
        models[study_id].update(study_info)

    dbt_config = {
        "name": "ftd",
        "version": "1.0.0",
        "profile": project_id,
        "model-paths": ["models"],
        "macro-paths": ["macros"],
        "clean-targets": ["target", "dbt_packages"],
        "models": models,
    }

    write_file(filepath, dbt_config)

    print(f"Generated: ftd dbt_project yaml")


def generate_ftd_sql_files(data_dictionary, ftd_dd, column_data, output_dir, study_id, type_mapping):

    src_table_columns = {
        table_id: {col[1] for col in column_data.get(f"{study_id}_stg_{table_id}", [])}
        for table_id in data_dictionary.keys()
    }

    for table_id in ftd_dd.keys():
        new_table = f"{study_id}_ftd_{table_id}"
        filepath = output_dir / f"{new_table}.sql" 

        column_definitions = []
        joins = []

        for col_name, _, _, col_data_type, src_var_name in column_data.get(new_table, []):
            sql_type = type_mapping.get(col_data_type, "text")

            alias = 'GEN_UNKNOWN'
            for src_id, cols in src_table_columns.items():
                if src_var_name in cols:
                    alias = src_id
                    break

            column_definitions.append(f'{alias}.{src_var_name}::{sql_type} AS "{col_name}"')

        base_table = list(src_table_columns.keys())[0]
        for src_id in src_table_columns.keys():
            if src_id != base_table:
                joins.append(f"JOIN {{{{ ref('{study_id}_stg_{src_id}') }}}} AS {src_id} USING (ftd_key)")

        
        sql_content = f"""{{{{ config(materialized='table', schema='{study_id}_data') }}}}

with source as (
    select 
       {",\n       ".join(column_definitions)}
    from {{{{ ref('{study_id}_stg_{base_table}') }}}} AS {base_table}
    {' '.join(joins)}
)

select 
    * 
from source
"""

        write_file(filepath, sql_content)

def generate_ftd_dds(
   utils_ftd_study_dir_path, trans_study_data_dir, ftd_study_dir_path, ftd_yml_path
):
    
    ftd_yml = read_file(ftd_yml_path)
    utils_ftd_dd = ftd_yml.get("data_dictionary", {})
    additions_temp_path = utils_ftd_study_dir_path / "additions_template.csv"

    if additions_temp_path.exists:
        temp = read_file(additions_temp_path)

    for table_id, table_info in utils_ftd_dd.items():
        filepath = ftd_study_dir_path / f"ftd_{table_id}_dd.csv"

        ddict = table_info.get("identifier")
        ddict_full_path = utils_ftd_study_dir_path / ddict
        if ddict_full_path.exists:
            utils_df = read_file(ddict_full_path)
        else:
            continue

        utils_df["src_variable_name"] = utils_df["variable_name"]

        trans_path = trans_study_data_dir / f"{table_id}_stg_additions_dd.csv"

        write_file(filepath, utils_df)
        write_file(trans_path, temp)
