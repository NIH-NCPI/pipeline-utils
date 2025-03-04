import pandas as pd
from dbt_utils.scripts.helpers.general import *
from dbt_utils.scripts.helpers.common import FTD_CONFIG, type_mapping


def load_column_data(data_dictionary, study_id, ftd_model=False):
    """Loads column names, descriptions, and data types from CSV files and stores them in a dictionary."""
    column_data = {}

    for table_id, table_info in data_dictionary.items():
        if ftd_model:
            table_key = f"{study_id}_ftd_{table_id}"
            stg_table_key = f"{study_id}_stg_{table_id}"
        else:
            raw_table_key = f"{study_id}_raw_{table_id}"
            stg_table_key = f"{study_id}_stg_{table_id}"

        ddict = table_info.get("table_details")
        df = read_file(ddict, pkg_file=ftd_model)

        columns = [
            (
                row["variable_name"],
                row["variable_name"].lower().replace(" ", "_"),
                row.get("variable_description", "UNKNOWN_VARIABLE_DESCRIPTION"),
                row.get("data_type", "UNKNOWN").lower(),
            )
            for _, row in df.iterrows()
        ]

        if ftd_model:
            column_data[table_key] = columns
            column_data[stg_table_key] = columns

        else:
            column_data[raw_table_key] = columns
            column_data[stg_table_key] = columns  

    return column_data


def generate_dbt_models_yml(data_dictionary, column_data, output_dir, study_id, ftd_model=False):
    """Generates dbt models.yml file for each table in its respective directory, including raw and staging models."""
    
    ftd_models = []

    for table_id, table_info in data_dictionary.items():
        raw_models = []
        if ftd_model:
            table_models_dir = f"{output_dir}"  # Single directory
            id_tables = [f"{study_id}_ftd_{table_id}"]
        else:
            table_models_dir = f"{output_dir}/{table_id}"  # Separate directories
            id_tables = [
                f"{study_id}_raw_{table_id}",
                f"{study_id}_stg_{table_id}"
            ]

        for table_name in id_tables:
            
            columns_metadata = [
                {
                    "name": col_name,
                    "description": f'{{{{ doc("{col_name_code}") }}}}',
                    "data_type": col_data_type
                }
                for col_name, col_name_code, col_description, col_data_type in column_data.get(table_name, [])
            ]

            model_entry = {
                "name": table_name,
                "description": table_info.get("description", f"Model for {table_name}."),
                "columns": columns_metadata
            }

            if ftd_model:
                ftd_models.append(model_entry)
            else:
                raw_models.append(model_entry)
        
        if not ftd_model:
            models = {
                "version": 2,
                "models": raw_models
            }
            write_file(table_models_dir, "__models.yml", models)

    if ftd_model:
        # Write all ftd models at once
        models = {
            "version": 2,
            "models": ftd_models
        }
        write_file(output_dir, "__models.yml", models)


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


def generate_stg_sql_files(data_dictionary, column_data, output_dir, study_id, type_mapping, ftd_model=False):
    """Generates staging SQL files dynamically for each table based on the data dictionary."""

    for table_id, table_info in data_dictionary.items():
        # Define output directory and table names
        if ftd_model:
            table_sql_dir = f"{output_dir}"  # All FTD models in the same directory
            src_table = f"{study_id}_stg_{table_id}"
            new_table = f"{study_id}_ftd_{table_id}"
        else:
            table_sql_dir = f"{output_dir}/{table_id}"  # Separate directories per table
            src_table = f"{study_id}_raw_{table_id}"
            new_table = f"{study_id}_stg_{table_id}"



        # Generate column definitions
        column_definitions = []
        for col_name, column_name_code, _, col_data_type in column_data.get(src_table, []):
            sql_type = type_mapping.get(col_data_type, "TEXT")
            column_definitions.append(f'"{col_name}"::{sql_type} AS "{column_name_code}"')

            print(f"Table: {new_table}")
            print(f"Column Definitions: {column_definitions}")  # Debugging statement

        # Generate SQL content
        sql_content = f"""{{{{ config(materialized='table') }}}}

WITH source AS (
    SELECT 
       {",\n       ".join(column_definitions)}
    FROM {{{{ ref('{src_table}') }}}}
)

SELECT 
    *,
    CONCAT(study_code, '-', participant_global_id) AS ftd_key
FROM source;
"""

        filename = f"{new_table}.sql"
        print(f"Final SQL for {new_table}:\n{sql_content}\n")


        # Write SQL file to the correct directory
        write_file(table_sql_dir, filename, sql_content)



def generate_dbt_project_yaml(data_dictionary, study_id, output_dir):
    study_info = {}

    for table_id in data_dictionary.keys():
        raw_table_id = f"{study_id}_raw_{table_id}"
        stg_table_id = f"{study_id}_stg_{table_id}"
        
        study_info[table_id] = {
            raw_table_id: None,
            stg_table_id: None
        }

    dbt_config = {
        "name": study_id,
        "version": "1.0.0",
        "profile": "pgtest",
        "model-paths": ["models"],
        "macro-paths": ["macros"],
        "snapshot-paths": ["snapshots"],
        "clean-targets": ["target", "dbt_packages"],
        "models": {
            study_id: {
                "+schema": f"{study_id}_data",
                "+materialized": "table",
                **study_info 
            }
        }
    }

    filename = 'dbt_project.yml'

    write_file(output_dir, filename, dbt_config)   



def generate_ftd_dbt_project_yaml(data_dictionary, study_id, output_dir):

    study_info = {
        "+schema": f"{study_id}_ftd_data",
        "+materialized": "table"
    }

    for table_id in data_dictionary.keys():
        study_info[f"{study_id}_ftd_{table_id}"] = None

    dbt_config = {
        "name": "ftd",
        "version": "1.0.0",
        "profile": "pgtest",
        "model-paths": ["models"],
        "macro-paths": ["macros"],
        "clean-targets": ["target", "dbt_packages"],
        "models": {
            "ftd": {
                study_id: study_info
            }
        }
    }

    filename = 'dbt_project.yml'

    write_file(output_dir, filename, dbt_config)  

    
def generate_model_docs(study_config, sources_dir, models_dir, outer_docs_dir, db_name, type_mapping):
    """Main function to generate dbt model files, loading column data once."""

    data_dictionary = study_config.get("data_dictionary", {})

    study_id = study_config.get("study_id", "study")

    column_data = load_column_data(data_dictionary, study_id) 

    generate_dbt_models_yml(data_dictionary, column_data, models_dir, study_id)
    generate_column_descriptions(data_dictionary, column_data, models_dir, study_id)
    generate_model_descriptions(data_dictionary, outer_docs_dir, study_id)
    generate_raw_sql_files(data_dictionary, models_dir, db_name, study_id)
    generate_stg_sql_files(data_dictionary, column_data, models_dir, study_id, type_mapping)
    generate_dbt_project_yaml(data_dictionary, study_id, sources_dir)
 

def generate_ftd_model_docs(study_config, ftd_dir):
    """Main function to generate dbt model files, loading column data once."""

    ftd_config = read_file(FTD_CONFIG, pkg_file=True)

    data_dictionary = ftd_config.get("data_dictionary", {})

    study_id = study_config.get("study_id", "study")

    column_data = load_column_data(data_dictionary, study_id, ftd_model=True) 

    generate_dbt_models_yml(data_dictionary, column_data, ftd_dir, study_id, ftd_model=True)
    generate_stg_sql_files(data_dictionary, column_data, ftd_dir, study_id, type_mapping, ftd_model=True)
    generate_ftd_dbt_project_yaml(data_dictionary, study_id, ftd_dir)