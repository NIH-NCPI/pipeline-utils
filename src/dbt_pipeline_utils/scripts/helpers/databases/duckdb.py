from dbt_pipeline_utils.scripts.helpers.data_processors import DatabaseBC
from dbt_pipeline_utils.scripts.helpers.general import *
from pathlib import Path
import subprocess
import json

class DuckDBFileProcessor(DatabaseBC):
    def __init__(self, study_details, file_details, paths):
        super().__init__(study_details, file_details, paths)

        self.profile = self.pipeline_db
        self.src_schema = f'{self.study_id}_src_data'
        self.src_data_csv = self.identifier # Identifies the csv file containing data to be imported.


    def import_via_macro(self):
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
                logger.error("❌ DuckDB import failed with error:\n%s", result.stderr.strip())
            else:
                logger.info("✅ Executed DuckDB SQL successfully:\n%s")
                if result.stdout:
                    logger.info("stdout:\n%s", result.stdout.strip())

        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Subprocess failed with return code:\n%s\n{args}", e.returncode)
            if e.stderr:
                logger.error("stderr:\n%s", e.stderr.strip())
            if e.stdout:
                logger.info("stdout:\n%s", e.stdout.strip())

        except Exception as ex:
            logger.exception("❌ Unexpected error during Duckdb import:")