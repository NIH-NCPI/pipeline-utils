import argparse
import pandas as pd
import os
from jinja2 import Template
import subprocess
from helpers.common import *

def read_csv(filepath):

    # If it's a local file path
    if os.path.exists(filepath):
        print(f"Reading CSV from local file: {filepath}")
        df = pd.read_csv(filepath)
        print("CSV data loaded from local file.")
    else:
        raise Exception(f"Local file does not exist: {filepath}")

    return df


def get_column_data(imported_csv):
    """
    Using the imported csv, returns a string of column metadata
    that can be used in a PostgreSQL CREATE TABLE query.

    Args:
        imported_csv (str): Path to the CSV file or URL to the CSV file.

    Returns:
        column_md_str (str): Column data in formatted for postgreSQL
    """

    column_definitions = []
    original_names = []

    for column in imported_csv.columns:

        original_name = column.strip()

        original_names.append(f"\"{original_name}\"")

        dtype = imported_csv[column].dtype

        # Map pandas dtype to PostgreSQL types
        if dtype == 'object':
            sql_type = "TEXT"
        elif dtype == 'int64':
            sql_type = "INTEGER"
        elif dtype == 'float64':
            sql_type = "FLOAT"
        elif dtype == 'bool':
            sql_type = "BOOLEAN"
        elif "datetime" in str(dtype):
            sql_type = "TIMESTAMP"
        else:
            sql_type = "TEXT"  # Default to TEXT

        column_definitions.append(f"\"{original_name}\" {sql_type} ")

    return column_definitions, original_names


def check_db_status():
    # Print sanity check
    # TODO remove schema_name hardcode
    try:
        print(f"dbo_raw_data METADATA")
        subprocess.run(
            [
                "dbt",
                "run-operation",
                "print_database_stats",
                "--args",
                '{"schema_name":"dbo_raw_data"}',
            ],
            check=True,
        )

    except subprocess.CalledProcessError as e:
        print(f"Error executing DBT operation: {e}")
    except Exception as ex:
        print(f"An unexpected error occurred: {ex}")


def generate_new_table(imported_csv, table_name):
    """
    Generates and executes a CREATE TABLE SQL statement based on the imported CSV.

    Args:
        imported_csv (DataFrame or str): A DataFrame or path to a CSV file.

    Returns:
        None
    """

    # Get column metadata.
    column_defs, names = get_column_data(imported_csv)

    # Define the template for the CREATE TABLE statement
    create_table_template = """
    CREATE TABLE IF NOT EXISTS {{schema}}.{{table_name}} (
        {% for column in columns %}
        {{ column }}{% if not loop.last %},{% endif %}
        {% endfor %}
    );
    """

    # Render the SQL statement using Jinja2
    sql_query = Template(create_table_template).render(columns=column_defs,
                                                       table_name=table_name,
                                                       schema=RAW_SCHEMA)

    try:
        subprocess.run(
            [
                "dbt",
                "run-operation",
                "run_sql",
                "--args",
                f'{{"sql": "{sql_query.replace("\"", "\\\"")}"}}',
            ],
            check=True,
        )
        print(f"Executed SQL:\n{sql_query}")

    except subprocess.CalledProcessError as e:
        print(f"Error executing DBT operation: {e}")
    except Exception as ex:
        print(f"An unexpected error occurred: {ex}")

    check_db_status()


def copy_csv_into_new_table(imported_csv, filepath, table_name):

    # Get column metadata.
    column_defs, column_names = get_column_data(imported_csv)
    # Define the template for the INSERT INTO statement
    insert_template = r"""
    \COPY {{schema}}.{{table_name}} ({{ columns }}) FROM '{{filepath}}' DELIMITER ',' CSV HEADER
    """

    # Render the SQL insert statement for each row
    sql_query = Template(insert_template).render(
        schema=RAW_SCHEMA,
        table_name=table_name,
        columns=", ".join(column_names),
        filepath=filepath
    )

    try:
        process = subprocess.run(
            ["psql", "-h", DB_HOST, "-U", DB_USER, "-d", DB_NAME],
            input=sql_query,
            text=True,
            check=True,
        )

        print(f"Executed SQL:\n{sql_query}")

    except subprocess.CalledProcessError as e:
        print(f"Error executing DBT operation: {e}")
    except Exception as ex:
        print(f"An unexpected error occurred: {ex}")


def main(filepath, table_name):

    csv_df = read_csv(filepath)

    generate_new_table(csv_df, table_name)

    sql_query = copy_csv_into_new_table(csv_df, filepath, table_name)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Import new data and generate dbt files for the new model.")
    
    parser.add_argument('-f', '--filepath')

    parser.add_argument('-t', '--tablename')

    args = parser.parse_args()



    main(filepath=args.filepath, table_name=args.tablename)
