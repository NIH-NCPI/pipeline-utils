from dbt_pipeline_utils.p_utils.common import type_mapping
from dbt_pipeline_utils import logger
import subprocess


def extract_table_schema(column_data_list:list):
    """Extracts column definitions from the data dictionary CSV."""
    # import pdb; pdb.set_trace()
    column_definitions = []
    for variable_name, formatted_name, _, data_type, *_ in column_data_list:
        sql_type = type_mapping.get(data_type, "text")
        column_definitions.append(f'"{variable_name}" {sql_type}')

    return column_definitions


def execute_pg_copy(dbt_table_name:str, raw_data_csv_path:str, db_vars:dict):
    '''
    The postgres adapter does not support the COPY command, so we have to execute it ourselves via subprocess. 
    '''
    # Will error if not a one liner with semi colon
    sql_query = f"""
    \\COPY {db_vars["schema"]}.{dbt_table_name} FROM '{raw_data_csv_path}' DELIMITER ',' CSV HEADER;
    """
    try:
        result = subprocess.run(
            ["psql", "-h", db_vars["host"], "-U", db_vars["user"], "-d", db_vars["dbname"], "-p", str(db_vars["port"])],
            input=sql_query,
            text=True,
            capture_output=True,  # Capture both stdout and stderr
            check=False,          # Do not raise an exception on non-zero exit code
        )

        # Log stderr if there are any errors
        if result.stderr:
            logger.error("❌ PostgreSQL COPY failed with error:\n%s", result.stderr.strip())

        # Log stdout for success messages or notices
        if result.stdout:
            logger.info("stdout:\n%s", result.stdout.strip())

        # Check return code and explicitly handle success/failure
        if result.returncode == 0:
            logger.info("✅ Executed SQL successfully:\n%s", sql_query.strip())
        else:
            logger.error("❌ PostgreSQL COPY failed with return code %s", result.returncode)

    except Exception as ex:
        logger.exception("❌ Unexpected error during import:")