import psycopg2
import pandas as pd
import subprocess
from jinja2 import Template
import json
from abc import ABC, abstractmethod
from dbt_pipeline_utils.scripts.helpers.general import *
from dbt_pipeline_utils.scripts.helpers.common import *
from dbt_pipeline_utils.scripts.helpers.doc_generation import DocGeneration

class DatabaseBC(ABC):
    """Base class to define common methods for file processing."""

    def __init__(self, study_details, file_details, paths):
        self.study_details = study_details
        self.file_details = file_details
        self.paths = paths
        self.profiles_path = paths.get("profiles_path")
        self.profile = ""
        self.src_schema = ""
        self.src_data_csv =  ""

        # Dynamically assign values from file_details
        for key, value in self.file_details.items():
            setattr(self, key, value)

        for key, value in self.study_details.items():
            setattr(self, key, value)

        self.new_db_table_id = f"{self.table_name}" # Should be the same as the src sql doc generated

        # Make the profile_keys into attributes
        pipeline_db_vars = self.get_db_vars()
        for key, value in pipeline_db_vars.items():
            setattr(self, key, value)

    def get_db_vars(self):
        """Loads specific key-value pairs from a YAML file based on the profile type."""
        profile_keys = ["host", "user", "dbname"] # update if not, pipeline_db: postgres

        config = read_file(self.profiles_path)
        env_section = config.get(self.pipeline_db, {}).get("outputs", {}).get("dev", {})

        env_vars = {key: env_section.get(key) for key in profile_keys}

        return env_vars

    def generate_new_table(self, column_defs):
        """
        Define the template for the CREATE TABLE statement

        Default postgress pipeline db
        """

        create_table_template = """
        CREATE SCHEMA IF NOT EXISTS {{schema}};

        CREATE TABLE IF NOT EXISTS {{db_name}}.{{schema}}.{{table_name}} (
            {% for column in columns %}
            {{ column }}{% if not loop.last %},{% endif %}
            {% endfor %}
        );
        """

        sql_query = Template(create_table_template).render(db_name=self.dbname,
                                                        columns=column_defs,
                                                        table_name=self.new_db_table_id,
                                                        schema=self.src_schema)

        try:
            result = subprocess.run(
                [
                    "dbt",
                    "run-operation",
                    "run_sql",
                    "--profile",
                    f"{self.pipeline_db}",
                    "--args",
                    json.dumps({"sql": sql_query}),
                ],
                check=True,
            )
            if result.stderr and "ERROR" in result.stderr:
                logger.error("❌ PostgreSQL COPY failed with error:\n%s", result.stderr.strip())
            else:
                logger.info("✅ Executed SQL successfully:\n%s", sql_query.strip())
                if result.stdout:
                    logger.info("stdout:\n%s", result.stdout.strip())

        except subprocess.CalledProcessError as e:
            logger.error("❌ Subprocess failed with return code %s", e.returncode)
            if e.stderr:
                logger.error("stderr:\n%s", e.stderr.strip())
            if e.stdout:
                logger.info("stdout:\n%s", e.stdout.strip())

        except Exception as ex:
            logger.exception("❌ Unexpected error during import:")
        

    def import_data(self):

        # Will error if not a one liner with semi colon
        csv_file = self.paths["src_data_dir"] / Path(f"{self.src_data_csv}")

        sql_query = f"""
        \\COPY {self.src_schema}.{self.new_db_table_id} FROM '{csv_file}' DELIMITER ',' CSV HEADER;
        """

        try:
            result = subprocess.run(
                ["psql", "-h", self.host, "-U", self.user, "-d", self.dbname],
                input=sql_query,
                text=True,
                check=True,
            )
            if result.stderr and "ERROR" in result.stderr:
                logger.error("❌ PostgreSQL COPY failed with error:\n%s", result.stderr.strip())
            else:
                logger.info("✅ Executed SQL successfully:\n%s", sql_query.strip())
                if result.stdout:
                    logger.info("stdout:\n%s", result.stdout.strip())

        except subprocess.CalledProcessError as e:
            logger.error("❌ Subprocess failed with return code %s", e.returncode)
            if e.stderr:
                logger.error("stderr:\n%s", e.stderr.strip())
            if e.stdout:
                logger.info("stdout:\n%s", e.stdout.strip())

        except Exception as ex:
            logger.exception("❌ Unexpected error during import:")

    def extract_table_schema(self):
        """Extracts column definitions from the data dictionary CSV."""

        full_file_path = self.paths['src_data_dir']  / Path(f'{self.src_data_csv}')
        dd = read_file(full_file_path)
        logger.info(f"{self.identifier}")
        # Use extract_columns to get structured column data
        column_data_list = extract_columns(dd, self.format)

        column_definitions = []
        for variable_name, formatted_name, _, data_type, _ in column_data_list:
            sql_type = type_mapping.get(data_type, "TEXT")
            column_definitions.append(f'"{variable_name}" {sql_type}')

        return column_definitions, self.src_data_csv
