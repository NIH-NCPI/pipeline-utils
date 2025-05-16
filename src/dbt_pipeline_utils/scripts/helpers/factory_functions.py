from dbt_pipeline_utils.scripts.helpers.databases.postgres import PostgresFileProcessor
from dbt_pipeline_utils.scripts.helpers.databases.sql_server import SynapseFileProcessor
from dbt_pipeline_utils.scripts.helpers.databases.duckdb import DuckDBFileProcessor

from dbt_pipeline_utils.scripts.helpers.general import *

def file_setup(study_config, ftd_config, table_name, table_info, paths):
    """file_type is not necessary when processing dds"""

    processor, import_type = get_data_processor(study_config, ftd_config, table_name, table_info, paths)
    logger.debug(f"Processed {import_type}")

    # Save local csvs for any synapse data
    if import_type == "synapse":
        processor.get_syn_file()

    return processor


def get_data_processor(study_config, ftd_config, table_name, table_info, paths):
    """
    Factory function to return the correct data processor class.
    """

    import_type = table_info.get("import_type")

    if import_type == "synapse":
        return SynapseFileProcessor(study_config, ftd_config, table_name, table_info, paths), import_type
    if import_type == "pg":
        return PostgresFileProcessor(study_config, ftd_config, table_name, table_info, paths), import_type
    if import_type == "duckdb":
        return DuckDBFileProcessor(study_config, ftd_config, table_name, table_info, paths), import_type
    if import_type not in ["pg", 'synapse', 'duckdb']:
        raise ValueError(f"Unsupported file import type: {import_type}")
