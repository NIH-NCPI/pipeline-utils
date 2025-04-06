import os
from dbt_pipeline_utils import logger
from pathlib import Path


def verify_files_exist(study_yml_path, info):
        identifier_path = study_yml_path / Path(f"{info['identifier']}")
        generated_path = study_yml_path / Path(f"{info['src_file_id']}")

        if not os.path.exists(identifier_path) and not os.path.exists(generated_path):
            raise ValueError(f"Error: One of the following dds are expected to exist: {identifier_path} or {generated_path}")


def validate_dfs(data_files, study_yml_path, file_ids):
    # Ensure each file_id in data_files matches one in data_dictionary
    for file_id, data_info in data_files.items():
        if file_id not in file_ids:
            raise ValueError(f"Error: data_files contains file_id '{file_id}' which is missing in data_dictionary.")

        if data_info is None:
            raise ValueError(f"Error: Missing filenames in data_files for file_id: {file_id}")

        verify_files_exist(study_yml_path, data_info)


def validate_dds(data_dictionary, study_yml_path):
    file_ids = set()

    for file_id, table_info in data_dictionary.items():
        file_ids.add(file_id)

        if table_info is None:
            raise ValueError(f"Error: Missing details for file_id: {file_id}")

        verify_files_exist(study_yml_path, table_info)

    return file_ids


def validate_study_config(study_config, study_yml_path):
    """
    Validates the study configuration (YAML) for correctness.
     - Must have a data_dictionary(DD) section
     - Must have a data_files(DF) section
     - DDs must have table details
     - DD src_table_details must have valid paths
     - DD and DF sections must contain data for the same tables
     - DFs must have filenames
     - DFs filenames must have valid paths
    """

    if "data_dictionary" not in study_config or not study_config["data_dictionary"]:
        raise ValueError("Error: The data_dictionary section must have at least one entry.")

    if "data_files" not in study_config or not study_config["data_files"]:
        raise ValueError("Error: The data_files section must have at least one entry.")

    data_dictionary = study_config["data_dictionary"]
    data_files = study_config["data_files"]

    file_ids = validate_dds(data_dictionary, study_yml_path)

    validate_dfs(data_files, study_yml_path, file_ids)

    logger.info("Validation passed: Study configuration is valid.")