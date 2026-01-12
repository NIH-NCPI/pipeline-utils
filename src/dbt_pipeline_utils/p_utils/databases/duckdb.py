from dbt_pipeline_utils.p_utils.databases.databases import DatabaseBC

from dataclasses import dataclass
from .databases import DatabaseBC


@DatabaseBC.register("duckdb")
class DuckdbSC(DatabaseBC):
    pass
    # , ftd_config, table_name, table_info, paths, file):
    #     super().__init__(study_config, ftd_config, table_name, table_info, paths, file)

    #     self.profile = self.pipeline_db
    #     self.src_schema = 'main'
    #     self.src_data_csv = self.file # Identifies the csv file containing data to be imported.

    # def process(self):
    #     return f"Processing using DuckDB for {self.name}"

    # def import_data(self):
    #     """
    #     The dbt duckdb adapter has it's own functions to allow for csv import.

    #     This function will run using an import macro within the dbt projcet itself.
    #     """
    #     csv_path = f"{self.paths['src_data_dir']}/{self.src_data_csv}"

    #     fully_qualified_tablename = Path(self.src_data_csv).stem

    #     args = f'{{fq_tablename: "{fully_qualified_tablename}", csv_path: "{csv_path}"}}'

    #     try:
    #         result = subprocess.run(
    #             [
    #                 "dbt",
    #                 "run-operation",
    #                 "register_external_sources",
    #                 "--args",
    #                 args
    #             ],
    #             check=True,
    #         )
    #         if result.stderr and "ERROR" in result.stderr:
    #             logger.error(f"❌ DuckDB import failed with error:\n%s\nTable:{fully_qualified_tablename}", result.stderr.strip())
    #         else:
    #             logger.info(f"✅ Executed DuckDB SQL successfully. Table:{fully_qualified_tablename}\n%s")
    #             if result.stdout:
    #                 logger.warning("stdout:\n%s", result.stdout.strip())

    #     except subprocess.CalledProcessError as e:
    #         logger.error(f"❌ Subprocess failed with return code:\n%s\n{args}", e.returncode)
    #         if e.stderr:
    #             logger.error("stderr:\n%s", e.stderr.strip())
    #         if e.stdout:
    #             logger.warning("stdout:\n%s", e.stdout.strip())

    #     except Exception as ex:
    #         logger.exception("❌ Unexpected error during Duckdb import:")

    # def generate_new_table(self):
    #     pass

    # def generate_src_sql_files(self, output_dir):
    #     '''
    #     Duckdb src table is automatically recognized by dbt.
    #     '''
    #     pass


