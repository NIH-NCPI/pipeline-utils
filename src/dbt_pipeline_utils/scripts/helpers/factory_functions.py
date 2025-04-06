from dbt_pipeline_utils.scripts.helpers.databases.postgres import PostgresFileProcessor
from dbt_pipeline_utils.scripts.helpers.databases.sql_server import SynapseFileProcessor
from dbt_pipeline_utils.scripts.helpers.general import *

def file_setup(study_info, table_info, paths):
    """file_type is not necessary when processing dds"""
    
    processor, source = get_data_processor(study_info, table_info, paths)
    logger.info(f"Processed {source}")

    # Save local csvs for any synapse data
    if source == "synapse":
        processor.get_syn_file()

    return processor


def get_data_processor(study_details, file_details, paths):
    """Factory function to return the correct data processor class."""

    source = file_details.get("source")

    if source == "synapse":
        return SynapseFileProcessor(study_details, file_details, paths), source
    elif source == "local_csv":
        return PostgresFileProcessor(study_details, file_details, paths), source
    else:
        raise ValueError(f"Unsupported file source: {source}")
