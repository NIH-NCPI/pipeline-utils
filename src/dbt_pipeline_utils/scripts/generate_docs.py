import argparse
import yaml
import pandas as pd
import sys
import os
import subprocess
from jinja2 import Template
import pdb
import json

from dbt_pipeline_utils.scripts.helpers.generate_model_docs import (
    generate_model_docs,
    generate_ftd_model_docs,
)
from dbt_pipeline_utils.scripts.helpers.generate_model_tgt_docs import (
    copy_directory
)
from dbt_pipeline_utils.scripts.helpers.generate_model_run_script import (
    generate_dbt_run_script,
)
from dbt_pipeline_utils.scripts.helpers.general import *
from dbt_pipeline_utils.scripts.helpers.common import *
from dbt_pipeline_utils.scripts.helpers.validation import *
from dbt_pipeline_utils.scripts.helpers.factory_functions import *

from dbt_pipeline_utils import logger


def main(study_id, project_id, src_data_path):

    # Set paths
    paths = get_paths(study_id, project_id, src_data_path)
    # If project dirs don't exist create them. 
    # Will create the project/study models in the dir that the script is triggered to run in.
    for var, path in paths.items():
        if var.endswith("dir"):
            path.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Path {path} exists")
    validate_paths(paths)

    study_config = read_file(paths["study_yml_path"])

    # TODO Borrowed the next code blocks(2) from the import script, 
    # running this part to create any synapse dds that don't exist in the data 
    # dir. Optimize me.
    study_info = {
        "study_id": study_config["study_id"],
        "project_id": study_config["project_id"],
        "pipeline_db": study_config["pipeline_db"],
    }

    src_dd_objs = []
    for table_name, table_info in study_config["data_dictionary"].items():
        # generate hard copies of syn dd's prior to study_config validation

        study_info.update({"table_name": table_name})

        logger.debug(f"Processing data_dictionaries: {table_name}")

        processor = file_setup(study_info, table_info, paths)
        src_dd_objs.append(processor)

    print(f"Start validation of {study_id} config")
    validate_study_config(study_config, paths["src_data_dir"])
    print("End validation of study config")

    for table_id, data_info in study_config["data_files"].items():

        generate_model_docs(study_config, paths)

    generate_ftd_model_docs(study_config,
                            paths["src_data_dir"],
                            paths["ftd_study_data_dir"],
                            paths["ftd_study_yml_path"],
                            paths["dbtp_ftdc_dir"],
                            paths["dbtp_ftdc_study_docs_dir"],
                            paths["dbtp_ftdc_study_dir"],
                            paths["utils_ftd_study_data_dir"],
                            paths["utils_ftd_study_yml_path"],
                            paths["ftd_study_yml_path"],
                            paths["trans_study_data_dir"]
                            )
    # copy over the tgt model
    copy_directory(paths["utils_ftd_tgta"], paths["dbtp_tgt_a_dir"])

    ftd_config = read_file(paths["ftd_study_yml_path"])

    generate_dbt_run_script(study_config, ftd_config, scripts_dir=paths["dbtp_scripts_dir"])

    print(f"END SCRIPT")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize DBT transformation for study data.")
    
    parser.add_argument("-y", "--yaml", required=True, help="Path to the YAML study_configuration file")

    parser.add_argument("-p", "--project_id", required=True, help="The project associated with the study")

    parser.add_argument(
        "-f",
        "--filepath",
        required=False,
        help="Path to the directory containing src data files. If not set, defaults to the {dbt project}/data path",
    )

    args = parser.parse_args()

    main(study_id=args.study_id, src_data_path=args.filepath)
