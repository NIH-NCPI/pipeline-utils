from dbt_pipeline_utils import logger
import subprocess


class RawDataImporter:

    def import_data(self, fq_tablename:str, args: dict):
        """
        The dbt duckdb adapter has it's own functions to allow for csv import.

        This function will run using an import macro within the dbt project itself.
        """
        # import pdb
        # pdb.set_trace()
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
                logger.error(
                    f"❌ DuckDB import failed with error:\n%s\nTable:{fq_tablename}",
                    result.stderr.strip(),
                )
            else:
                logger.info(
                    f"✅ Executed DuckDB SQL successfully. Table:{fq_tablename}\n%s"
                )
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
