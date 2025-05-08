from dbt_pipeline_utils.scripts.helpers.databases.postgres import PostgresFileProcessor
from dbt_pipeline_utils.scripts.helpers.databases.sql_server import SynapseFileProcessor
from dbt_pipeline_utils.scripts.helpers.databases.duckdb import DuckDBFileProcessor

from dbt_pipeline_utils.scripts.helpers.general import *

def file_setup(study_info, table_info, paths):
    """file_type is not necessary when processing dds"""

    processor, import_type = get_data_processor(study_info, table_info, paths)
    logger.info(f"Processed {import_type}")

    # Save local csvs for any synapse data
    if import_type == "synapse":
        processor.get_syn_file()

    # This is all duckdb needs, to import src data
    # is_dd = table_info['identifier'].endswith('_dd.csv')
    if import_type == 'duckdb':
        # duckdb dds don't need to be imported. 
        # if processor and import_script and is_dd:
            # return
        processor.import_via_macro()
        return

    return processor


def get_data_processor(study_details, file_details, paths):
    """
    Factory function to return the correct data processor class.
    
    Does not return DuckDB processors
    """

    import_type = file_details.get("import_type")

    if import_type == "synapse":
        return SynapseFileProcessor(study_details, file_details, paths), import_type
    if import_type == "pg":
        return PostgresFileProcessor(study_details, file_details, paths), import_type
    if import_type == "duckdb":
        return DuckDBFileProcessor(study_details, file_details, paths), import_type
    if import_type not in ["pg", 'synapse', 'duckdb']:
        raise ValueError(f"Unsupported file import type: {import_type}")
