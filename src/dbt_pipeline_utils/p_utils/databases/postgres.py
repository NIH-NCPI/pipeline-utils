from dbt_pipeline_utils.p_utils.databases.databases import DatabaseBC

from dataclasses import dataclass
from .databases import DatabaseBC


@DatabaseBC.register("pg")
class PostgresSC(DatabaseBC):
    pass

    # , ftd_config, table_name, table_info, paths):
    #     super().__init__(study_config, ftd_config, table_name, table_info, paths)

    #     self.profile = self.pipeline_db
    #     self.src_schema = f'{self.study_id}_src_data'
    #     self.src_data_csv = self.table_info['identifier'] # Identifies the csv file containing data to be imported.

    # def generate_new_table(self):
    #     """
    #     Define the template for the CREATE TABLE statement

    #     Default postgress pipeline db
    #     """
    #     column_defs, src_table_id = self.extract_table_schema()
    #     logger.debug(f"Start pipeline db, src table creation {src_table_id}")

    #     create_table_template = """
    #     CREATE SCHEMA IF NOT EXISTS {{schema}};

    #     CREATE TABLE IF NOT EXISTS {{schema}}.{{table_name}} (
    #         {% for column in columns %}
    #         {{ column }}{% if not loop.last %},{% endif %}
    #         {% endfor %}
    #     );
    #     """
    #     sql_query = Template(create_table_template).render(
    #         db_name=self.dbname,
    #         columns=column_defs,
    #         table_name=self.new_table_name,
    #         schema=self.src_schema,
    #     )

    #     try:
    #         result = subprocess.run(
    #             [
    #                 "dbt",
    #                 "run-operation",
    #                 "run_sql",
    #                 "--profile",
    #                 f"{self.pipeline_db}",
    #                 "--args",
    #                 json.dumps({"sql": sql_query}),
    #             ],
    #             check=True,
    #         )
    #         if result.stderr and "ERROR" in result.stderr:
    #             logger.error(
    #                 "❌ PostgreSQL COPY failed with error:\n%s", result.stderr.strip()
    #             )
    #         else:
    #             logger.info("✅ Executed SQL successfully:\n%s", sql_query.strip())
    #             if result.stdout:
    #                 logger.warning("stdout:\n%s", result.stdout.strip())

    #     except subprocess.CalledProcessError as e:
    #         logger.error("❌ Subprocess failed with return code %s", e.returncode)
    #         if e.stderr:
    #             logger.error("stderr:\n%s", e.stderr.strip())
    #         if e.stdout:
    #             logger.info("stdout:\n%s", e.stdout.strip())

    #     except Exception as ex:
    #         logger.exception("❌ Unexpected error during import:")

    # def import_data(self):

    #     # Will error if not a one liner with semi colon
    #     csv_file = self.paths["src_data_dir"] / Path(f"{self.src_data_csv}")

    #     sql_query = f"""
    #     \\COPY {self.src_schema}.{self.new_table_name} FROM '{csv_file}' DELIMITER ',' CSV HEADER;
    #     """

    #     try:
    #         result = subprocess.run(
    #             ["psql", "-h", self.host, "-U", self.user, "-d", self.dbname],
    #             input=sql_query,
    #             text=True,
    #             check=True,
    #         )
    #         if result.stderr and "ERROR" in result.stderr:
    #             logger.error(
    #                 "❌ PostgreSQL COPY failed with error:\n%s", result.stderr.strip()
    #             )
    #         else:
    #             logger.info("✅ Executed SQL successfully:\n%s", sql_query.strip())
    #             if result.stdout:
    #                 logger.error("stdout:\n%s", result.stdout.strip())

    #     except subprocess.CalledProcessError as e:
    #         logger.error("❌ Subprocess failed with return code %s", e.returncode)
    #         if e.stderr:
    #             logger.error("stderr:\n%s", e.stderr.strip())
    #         if e.stdout:
    #             logger.info("stdout:\n%s", e.stdout.strip())

    #     except Exception as ex:
    #         logger.exception("❌ Unexpected error during import:")
