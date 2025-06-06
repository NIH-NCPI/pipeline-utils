import subprocess
from jinja2 import Template
import json
from abc import ABC, abstractmethod
from dbt_pipeline_utils.scripts.helpers.general import *
from dbt_pipeline_utils.scripts.helpers.common import *
from dbt_pipeline_utils.scripts.helpers.pipeline_docs_generation.generate_common_docs import DocGeneration
from dbt_pipeline_utils.scripts.helpers.pipeline_docs_generation.generate_ftd_docs import FTDDocGenClass
from dbt_pipeline_utils.scripts.helpers.pipeline_docs_generation.generate_tgt_docs import TgtDocGenClass
from dbt_pipeline_utils.scripts.helpers.pipeline_docs_generation.generate_model_run_script import RunScriptClass

class DatabaseBC(ABC, DocGeneration, FTDDocGenClass, TgtDocGenClass, RunScriptClass):
    """Base class to define common methods for file processing."""

    def __init__(self, study_config, ftd_config, table_name, table_info, paths):
        self.study_config = study_config
        self.ftd_config = ftd_config
        self.table_name = table_name
        self.table_info = table_info
        self.paths = paths
        self.profiles_path = paths.get("profiles_path_root")
        self.profile = ""
        self.src_schema = ""
        self.src_data_csv =  ""

        study_details = {
            "study_id": self.study_config["study_id"],
            "project_id": self.study_config["project_id"],
            "pipeline_db": self.study_config["pipeline_db"],
            "data_dictionary" : self.study_config.get("data_dictionary", {}),
            "data_files" : self.study_config.get("data_files", {}),
            "ftd_dd": self.ftd_config.get("data_dictionary", {})
        }

        # # Dynamically assign values from file_details
        # for key, value in self.file_details.items():
        #     setattr(self, key, value)

        for key, value in study_details.items():
            setattr(self, key, value)

        self.new_table_name = Path(self.get_src_table_key(self.table_name)).stem


        # Make the profile_keys into attributes
        pipeline_db_vars = self.get_db_vars()
        for key, value in pipeline_db_vars.items():
            setattr(self, key, value)

    def get_src_table_key(self, table_id):
        '''
        The filename that is stored with column data. 
        '''
        datafile_info = self.data_files.get(table_id, {})
        return Path(datafile_info.get("identifier")).stem


    def get_db_vars(self):
        """Loads specific key-value pairs from a YAML file based on the profile type."""
        profile_keys = ["host", "user", "dbname", "schema"] # update if not, pipeline_db: postgres

        config = read_file(self.profiles_path)
        env_section = config.get(self.pipeline_db, {}).get("outputs", {}).get("dev", {})

        env_vars = {key: env_section.get(key) for key in profile_keys}

        return env_vars

    def generate_new_table(self):
        """
        Define the template for the CREATE TABLE statement

        Default postgress pipeline db
        """
        column_defs, src_table_id = self.extract_table_schema()
        logger.debug(f"Start pipeline db, src table creation {src_table_id}")

        create_table_template = """
        CREATE SCHEMA IF NOT EXISTS {{schema}};

        CREATE TABLE IF NOT EXISTS {{schema}}.{{table_name}} (
            {% for column in columns %}
            {{ column }}{% if not loop.last %},{% endif %}
            {% endfor %}
        );
        """
        sql_query = Template(create_table_template).render(db_name=self.dbname,
                                                        columns=column_defs,
                                                        table_name=self.new_table_name,
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
                    logger.warning("stdout:\n%s", result.stdout.strip())

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
        \\COPY {self.src_schema}.{self.new_table_name} FROM '{csv_file}' DELIMITER ',' CSV HEADER;
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
                    logger.error("stdout:\n%s", result.stdout.strip())

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
        # Use extract_columns to get structured column data
        column_data_list = self.extract_columns(dd, self.table_info['format'])

        column_definitions = []
        for variable_name, formatted_name, _, data_type, _ in column_data_list:
            sql_type = type_mapping.get(data_type, "text")
            column_definitions.append(f'"{variable_name}" {sql_type}')

        return column_definitions, self.src_data_csv


    def get_src_ddict_path(self, table_info):
        src_dd_path = self.paths['src_data_dir']
        
        if table_info.get("import_type") == 'synapse':
            ddict = table_info.get("src_file_id")

        if table_info.get("import_type") == 'pg':
            ddict = table_info.get("identifier")

        if table_info.get("import_type") == 'duckdb':
            ddict = table_info.get("identifier")

        if table_info.get("import_type") not in ['pg', 'duckdb', 'synapse']:
            logger.error(f"{table_info.get('import_type')} is not valid")
            
        return src_dd_path / Path(f"{ddict}"), ddict
    
    def get_join_conditions(self, current_table):

        join_cols = self.data_files.get(current_table, {}).get("join_cols", {})

        for join_table, left_column in join_cols.items():
            # Get the right column from the join_table's join_cols (pointing back to current_table)
            right_column = self.data_files.get(join_table, {}).get("join_cols", {}).get(current_table)

            if right_column:
                # join_table.right_column = current_table.left_column
                return f"{join_table}.{right_column} = {current_table}.{left_column}"
                

        return ''
