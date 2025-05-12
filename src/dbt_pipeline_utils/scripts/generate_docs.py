import argparse


from dbt_pipeline_utils.scripts.helpers.pipeline_docs_generation.generation_main import (
    generate_model_docs, generate_ftd_model_docs, generate_tgt_model_docs, generate_run_script
)
from dbt_pipeline_utils.scripts.helpers.general import *
from dbt_pipeline_utils.scripts.helpers.common import *
from dbt_pipeline_utils.scripts.helpers.validate_study_config import *
from dbt_pipeline_utils.scripts.helpers.factory_functions import *
from dbt_pipeline_utils import logger


def main(study_id, project_id, tgt_id, src_data_path):

    # Set paths
    paths = get_paths(study_id, project_id, tgt_id, src_data_path)
    # If project dirs don't exist create them. 
    # Will create the project/study models in the dir that the script is triggered to run in.
    for var, path in paths.items():
        if var.endswith("dir"):
            path.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Path {path} exists")
    validate_paths(paths)

    study_config = read_file(paths["study_yml_path"])
    ftd_config = read_file(paths["ftd_study_yml_path"])


    src_dd_objs = []
    for table_name, table_info in study_config["data_dictionary"].items():

        logger.debug(f"Processing data_dictionaries: {table_name}")

        processor = file_setup(study_config, ftd_config, table_name, table_info, paths)

        if processor:
            src_dd_objs.append(processor)

    src_df_objs = []
    for table_name, table_info in study_config["data_files"].items():

        logger.debug(f"Processing data_dictionaries: {table_name}")
        processor = file_setup(study_config, ftd_config, table_name, table_info, paths)

        if processor:
            src_df_objs.append(processor)

    logger.info(f"Start validation of {study_id} config")
    validate_study_config(study_config, paths["src_data_dir"])
    logger.info("End validation of study config")

    for df_obj in src_df_objs:

        generate_model_docs(df_obj)

        generate_ftd_model_docs(df_obj)
        
        generate_basic_dbt_project_yml(df_obj.paths["dbtp_catalog_dir"], "catalog", df_obj.pipeline_db)

        generate_tgt_model_docs(df_obj)

        generate_run_script(df_obj)

    logger.info(f"END SCRIPT")
    logger.warning("REMINDER: Update the profile in the copied tgt_consensus dbt_project.yml.")
    logger.warning("REMINDER: Update the root dir packages.yml with the correct project import paths.")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize DBT transformation for study data.")
    
    parser.add_argument("-y", "--yaml", required=True, help="Path to the YAML study_configuration file")

    parser.add_argument("-p", "--project_id", required=True, help="The project associated with the study")

    parser.add_argument("-t", "--tgt_id", required=True, help="The tgt model ")
    parser.add_argument(
        "-f",
        "--filepath",
        required=False,
        help="Path to the directory containing src data files. If not set, defaults to the {dbt project}/data path",
    )

    args = parser.parse_args()

    main(study_id=args.study_id, tgt_id=args.tgt_id, src_data_path=args.filepath)
