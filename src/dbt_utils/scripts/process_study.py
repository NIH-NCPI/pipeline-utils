import argparse
import yaml
import pandas as pd
import sys
import os
import subprocess
from jinja2 import Template
import pdb
import json

from dbt_utils.scripts.helpers.generate_model_docs import generate_model_docs, generate_ftd_model_docs
from dbt_utils.scripts.helpers.generate_model_run_script import generate_dbt_run_script
from dbt_utils.scripts.helpers.general import *
from dbt_utils.scripts.helpers.common import *


def validate_study_config(study_config, study_yml_path):
    """
    Validates the study configuration (YAML) for correctness.
     - Must have a data_dictionary(DD) section
     - Must have a data_files(DF) section
     - DDs must have table details
     - DD table_details must have valid paths
     - DD and DF sections must contain data for the same tables
     - DFs must have filenames
     - DFs filenames must have valid paths
    """

    if "data_dictionary" not in study_config or not study_config["data_dictionary"]:
        raise ValueError("Error: The data_dictionary section must have at least one entry.")

    if "data_files" not in study_config or not study_config["data_files"]:
        raise ValueError("Error: The data_files section must have at least one entry.")
    

    data_dictionary = study_config["data_dictionary"]
    data_files = study_config["data_files"]

    table_ids = set()

    for table_id, table_info in data_dictionary.items():
        table_ids.add(table_id)

        if table_info is None:
            raise ValueError(f"Error: Missing details for table_id: {table_id}")

        filename = table_info["table_details"]
        full_file_path = study_yml_path / filename

        if not os.path.exists(full_file_path):
            raise ValueError(f"Error: Specified data dictionary file does not exist: {full_file_path}")

    # Ensure each table_id in data_files matches one in data_dictionary
    for table_id, data_info in data_files.items():
        if table_id not in table_ids:
            raise ValueError(f"Error: data_files contains table_id '{table_id}' which is missing in data_dictionary.")

        if data_info is None:
            raise ValueError(f"Error: Missing filenames in data_files for table_id: {table_id}")

        # Validate that all file paths exist
        for file in data_info["filename"]:
            full_file_path = study_yml_path / file
            if not os.path.exists(full_file_path):
                raise ValueError(f"Error: Data file {full_file_path} does not exist for table_id {table_id}.")

    print("Validation passed: Study configuration is valid.")

def extract_table_schema(dd_path, type_mapping):
    """Extracts column definitions from the data dictionary CSV."""
    df = read_file(dd_path)
    column_definitions = []
    for _, row in df.iterrows():

        column_name = row["variable_name"].strip()
        data_type = row["data_type"].strip().upper()

        sql_type = type_mapping.get(data_type, "TEXT")
        column_definitions.append(f'"{column_name}" {sql_type}')

    return column_definitions

def generate_new_table(schema, table_name, column_defs, db_name):

    # Define the template for the CREATE TABLE statement
    create_table_template = """
    CREATE SCHEMA IF NOT EXISTS {{schema}};

    CREATE TABLE IF NOT EXISTS {{db_name}}.{{schema}}.{{table_name}} (
        {% for column in columns %}
        {{ column }}{% if not loop.last %},{% endif %}
        {% endfor %}
    );
    """

    sql_query = Template(create_table_template).render(db_name=db_name,
                                                       columns=column_defs,
                                                       table_name=table_name,
                                                       schema=schema)

    try:
        subprocess.run(
            [
                "dbt",
                "clean"
            ],
            check=True,
        )

        subprocess.run(
            [
                "dbt",
                "deps"
            ],
            check=True,
        )
        print(f"Ran dbt clean and deps")

    except subprocess.CalledProcessError as e:
        print(f"Error executing DBT operation: {e}")
    except Exception as ex:
        print(f"An unexpected error occurred: {ex}")

    try:
        subprocess.run(
            [
                "dbt",
                "run-operation",
                "run_sql",
                "--args",
                json.dumps({"sql": sql_query}),
            ],
            check=True,
        )
        print(f"Executed SQL:\n{sql_query}")

    except subprocess.CalledProcessError as e:
        print(f"Error executing DBT operation: {e}")
    except Exception as ex:
        print(f"An unexpected error occurred: {ex}")


def copy_csv_into_new_table(schema, table_name, csv_file, db_host, db_user, db_name):

    # Will error if not a one liner with semi colon
    sql_query = f"""
    \\COPY {schema}.{table_name} FROM '{csv_file}' DELIMITER ',' CSV HEADER;
    """

    try:
        subprocess.run(
            ["psql", "-h", db_host, "-U", db_user, "-d", db_name],
            input=sql_query,
            text=True,
            check=True,
        )
        print(f"Executed SQL:\n{sql_query}")

    except subprocess.CalledProcessError as e:
        print(f"Error executing DBT operation: {e}")
    except Exception as ex:
        print(f"An unexpected error occurred: {ex}")

def main(study_id):
    
    # Set paths
    paths = get_paths(study_id)
    # If project dirs don't exist create them.
    for var, path in paths.items():
        if var.endswith("dir"):
            path.mkdir(parents=True, exist_ok=True)
            print(f'created {path}')
    validate_paths(paths)

    study_config = read_file(paths["study_yml_path"])

    print(f"Start validation of {study_id} config")
    validate_study_config(study_config, paths["dbtp_study_data_dir"])
    print("End validation of study config")

    schema = f"{study_config['study_id']}_raw_data"

    for table_id, table_info in study_config["data_dictionary"].items():
        dd_path = table_info["table_details"]
        full_dd_path = paths["dbtp_study_data_dir"] / dd_path
        column_definitions = extract_table_schema(full_dd_path, type_mapping)
        print(f"Start table creation {table_id}")
        # pdb.set_trace() 
        generate_new_table(schema, table_id, column_definitions, DB_NAME)
        print(f"End table creation {schema}.{table_id}")

    for table_id, data_info in study_config["data_files"].items():
        for csv_file in data_info["filename"]:
            full_csv_path = paths["dbtp_study_data_dir"] / csv_file
            copy_csv_into_new_table(schema, table_id, full_csv_path, DB_HOST, DB_USER, DB_NAME)
            print(f"Data from {csv_file} loaded into {schema}.{table_id}.")

        # TODO: DB_NAME and type mapping to common location/var
        generate_model_docs(study_config,
                            paths,
                            DB_NAME,
                            type_mapping
                            )
        
    generate_ftd_model_docs(study_config,
                            paths["dbtp_study_data_dir"],
                            paths["ftd_study_data_dir"],
                            paths["ftd_study_yml_path"],
                            paths["dbtp_ftdc_dir"],
                            paths["dbtp_ftdc_study_docs_dir"],
                            paths["dbtp_ftdc_study_dir"],
                            paths["utils_ftd_study_data_dir"],
                            paths["utils_ftd_study_yml_path"],
                            paths["ftd_study_yml_path"]
                            )
    
    ftd_config = read_file(paths["ftd_study_yml_path"])

    generate_dbt_run_script(study_config, ftd_config, scripts_dir=paths["dbtp_scripts_dir"])

    print(f"END SCRIPT")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize DBT transformation for study data.")
    
    parser.add_argument("-y", "--yaml", required=True, help="Path to the YAML study_configuration file")

    args = parser.parse_args()

    main(yaml_study_config=args.yaml)
