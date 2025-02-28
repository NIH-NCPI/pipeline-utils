import yaml
import os
import csv
import pandas as pd
from general import *


def load_column_data(data_dictionary, study_id):
    """Loads column names, descriptions, and data types from CSV files and stores them in a dictionary."""
    column_data = {}

    for table_id, table_info in data_dictionary.items():
        raw_table_id = f"{study_id}_raw_{table_id}"
        ddict = table_info.get("table_details")
        df = read_file(ddict)

        columns = []
        for _, row in df.iterrows():
            col_name = row["variable_name"]
            col_name_code = col_name.lower().replace(" ", "_")
            col_description = row.get("variable_description", "UNKNOWN_VARIABLE_DESCRIPTION")
            col_data_type = row.get("data_type", "UNKNOWN").lower() 
            
            columns.append((col_name, col_name_code, col_description, col_data_type))

        column_data[raw_table_id] = columns

    return column_data


def generate_dbt_models_yml(data_dictionary, column_data, output_dir, study_id):
    """Generates dbt models.yml file for each table in its respective directory, including raw and staging models."""

    for table_id, table_info in data_dictionary.items():
        raw_table_id = f"{study_id}_raw_{table_id}"
        stg_table_id = f"{study_id}_stg_{table_id}"

        # Generate columns metadata
        raw_columns_metadata = [
            {
                "name": col_name,
                "description": f'{{{{ doc("{col_name_code}") }}}}',
                "data_type": col_data_type
            }
            for col_name, col_name_code, col_description, col_data_type in column_data.get(raw_table_id, [])
        ]

        stg_columns_metadata = [
            {
                "name": col_name_code,
                "description": f'{{{{ doc("{col_name_code}") }}}}',
                "data_type": col_data_type
            }
            for col_name, col_name_code, col_description, col_data_type in column_data.get(raw_table_id, [])
        ]
        # Define dbt models configuration
        models = {
            "version": 2,
            "models": [
                {
                    "name": raw_table_id,
                    "description": f'{{{{ doc("{raw_table_id}_description") }}}}',
                    "columns": raw_columns_metadata
                },
                {
                    "name": stg_table_id,
                    "description": f'{{{{ doc("{stg_table_id}_description") }}}}',
                    "columns": stg_columns_metadata
                }
            ]
        }

        # Define output directory and write the models.yml file
        table_models_dir = f"{output_dir}/{table_id}"
        filename = "__models.yml"

        write_file(table_models_dir, filename, models)

        print(f"Generated: {table_models_dir}/{filename}")


def generate_column_descriptions(data_dictionary, column_data, output_dir, study_id):
    """Generates a separate column_descriptions.md for each table in its respective docs directory."""

    for table_id, table_info in data_dictionary.items():
        raw_table_id = f"{study_id}_raw_{table_id}"
        descriptions = []

        # Table description
        table_description = table_info.get("description", f"Model for {raw_table_id}.")
        descriptions.append(f"{{% docs {raw_table_id}_description %}}\n{table_description}\n{{% enddocs %}}\n")

        # Column descriptions
        for col_name, col_name_code, col_description, _ in column_data.get(raw_table_id, []):
            descriptions.append(f"{{% docs {col_name_code} %}}\n{col_description}\n{{% enddocs %}}\n")

        data = "\n".join(descriptions)

        table_docs_dir = f"{output_dir}/{table_id}/docs"
        filename = "column_descriptions.md"

        write_file(table_docs_dir, filename, data)

def generate_model_descriptions(data_dictionary, output_dir, study_id):
    """Generates model_descriptions.md using the specified format."""
    model_descriptions = []

    # Group tables by prefix (e.g., "moo_raw_", "moo_stg_")
    grouped_tables = {}
    for table_id, table_info in data_dictionary.items():
        prefix = table_id.split("_")[0]  # Assumes prefix is the first part of table_id
        grouped_tables.setdefault(prefix, []).append((table_id, table_info))

    for prefix, tables in grouped_tables.items():
        model_descriptions.append(f"### {prefix.capitalize()} Models\n")

        for table_id, table_info in tables:
            raw_table_id = f"{study_id}_raw_{table_id}"
            raw_description = table_info.get("description", f"Model for {raw_table_id}.")
            model_descriptions.append(f"{{% docs {raw_table_id} %}}\n{raw_description}\n{{% enddocs %}}\n")

            stg_table_id = f"{study_id}_stg_{table_id}"
            stg_description = table_info.get("description", f"Model for {stg_table_id}.")
            model_descriptions.append(f"{{% docs {stg_table_id} %}}\n{stg_description}\n{{% enddocs %}}\n")

            data = "\n".join(model_descriptions)

            filename = "model_descriptions.md"

            write_file(output_dir, filename, data)

def generate_raw_sql_files(data_dictionary, output_dir, db_name, study_id):
    """Generates SQL files dynamically for each table in its respective directory."""

    for table_id in data_dictionary.keys():
        raw_table_id = f"{study_id}_raw_{table_id}"
        sql_filename = f"{raw_table_id}.sql"
        sql_content = f"""{{{{ config(materialized='table') }}}}

SELECT * FROM {db_name}.{study_id}_raw_data.{table_id}
"""
        table_sql_dir = f"{output_dir}/{table_id}"

        write_file(table_sql_dir, sql_filename, sql_content)


def generate_stg_sql_files(data_dictionary, column_data, output_dir, db_name, study_id, type_mapping):
    """Generates staging SQL files dynamically for each table based on the data dictionary."""

    for table_id, table_info in data_dictionary.items():
        raw_table_id = f"{study_id}_raw_{table_id}"
        stg_table_id = f"{study_id}_stg_{table_id}"

        # Generate column definitions
        column_definitions = []
        for col_name, column_name_code, _, col_data_type in column_data.get(raw_table_id, []):
            sql_type = type_mapping.get(col_data_type, "TEXT")
            column_definitions.append(f'"{col_name}" ::{sql_type} AS {column_name_code}')

        # Generate SQL content
        sql_content = f"""{{{{ config(materialized='table') }}}}

WITH source AS (
    SELECT 
       {",\n       ".join(column_definitions)} 
    FROM {{{{ ref('{raw_table_id}') }}}}
)

SELECT 
    *,
    ,CONCAT(study_code, '-', participant_global_id) AS ftd_key
FROM source;
"""

        # Define SQL directory and write file
        table_sql_dir = f"{output_dir}/{table_id}"
        filename = f"{stg_table_id}.sql"

        write_file(table_sql_dir, filename, sql_content)

        print(f"Generated: {table_sql_dir}/{filename}")


def generate_model_docs(study_config, models_dir, outer_docs_dir, db_name, type_mapping):
    """Main function to generate dbt model files, loading column data once."""

    data_dictionary = study_config.get("data_dictionary", {})

    study_id = study_config.get("study_id", "study")

    column_data = load_column_data(data_dictionary, study_id) 

    generate_dbt_models_yml(data_dictionary, column_data, models_dir, study_id)
    generate_column_descriptions(data_dictionary, column_data, models_dir, study_id)
    generate_model_descriptions(data_dictionary, outer_docs_dir, study_id)
    generate_raw_sql_files(data_dictionary, models_dir, db_name, study_id)
    generate_stg_sql_files(data_dictionary, column_data, models_dir, db_name, study_id, type_mapping)
