from dbt_pipeline_utils.scripts.helpers.data_processors import DatabaseBC
from dbt_pipeline_utils.scripts.helpers.general import *
from pathlib import Path
import subprocess
import json

class DuckDBFileProcessor(DatabaseBC):
    def __init__(self, study_config, ftd_config, table_name, table_info, paths):
        super().__init__(study_config, ftd_config, table_name, table_info, paths)

        self.profile = self.pipeline_db
        self.src_schema = 'main'
        self.src_data_csv = self.table_info['identifier'] # Identifies the csv file containing data to be imported.

    def import_data(self):
        """
        The dbt duckdb adapter has it's own functions to allow for csv import.

        This function will run using an import macro within the dbt projcet itself.
        """
        csv_path = f"{self.paths['src_data_dir']}/{self.src_data_csv}"
        tablename = Path(self.src_data_csv).stem
        fully_qualified_tablename = f"{tablename}"

        args = f'{{fq_tablename: "{fully_qualified_tablename}", csv_path: "{csv_path}"}}'

        try:
            result = subprocess.run(
                [
                    "dbt",
                    "run-operation",
                    "register_external_sources",
                    "--args",
                    args
                ],
                check=True,
            )
            if result.stderr and "ERROR" in result.stderr:
                logger.error(f"❌ DuckDB import failed with error:\n%s\nTable:{fully_qualified_tablename}", result.stderr.strip())
            else:
                logger.info(f"✅ Executed DuckDB SQL successfully. Table:{fully_qualified_tablename}\n%s")
                if result.stdout:
                    logger.warning("stdout:\n%s", result.stdout.strip())

        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Subprocess failed with return code:\n%s\n{args}", e.returncode)
            if e.stderr:
                logger.error("stderr:\n%s", e.stderr.strip())
            if e.stdout:
                logger.warning("stdout:\n%s", e.stdout.strip())

        except Exception as ex:
            logger.exception("❌ Unexpected error during Duckdb import:")

    def generate_new_table(self):
        pass

    def generate_src_sql_files(self, output_dir):
        '''
        Duckdb src table is automatically recognized by dbt.
        '''
        pass


    def generate_dbt_project_yaml(self):
        study_info = {}

        for table_id in self.data_dictionary.keys():
            stg_table_id = f"{self.study_id}_stg_{table_id}"

            study_info[table_id] = {
                stg_table_id: {
                    "+schema": self.src_schema,
                    "+materialized": "table",
                },
            }

        # Correct structure for dbt_project.yml
        dbt_config = {
            "name": self.study_id,
            "version": "1.0.0",
            "profile": self.project_id,
            "model-paths": ["models"],
            "macro-paths": ["macros"],
            "snapshot-paths": ["snapshots"],
            "clean-targets": ["target", "dbt_packages"],
            "models": {
                "+schema": self.src_schema,
                "+materialized": "table",
                **study_info,
            },
        }

        filepath = self.paths["dbtp_src_study_dir"] / "dbt_project.yml"

        write_file(filepath, dbt_config)