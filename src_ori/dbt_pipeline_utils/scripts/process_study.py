import argparse
from dbt_pipeline_utils.scripts.helpers.general import *
from dbt_pipeline_utils.scripts.helpers.validate_study_config import *
from dbt_pipeline_utils.scripts.helpers.factory_functions import *
from dbt_pipeline_utils import logger


def main():
    parser = argparse.ArgumentParser(
        description="Initialize DBT transformation for study data."
    )
    parser.add_argument(
        "-s",
        "--study_id",
        required=True,
        help="The study's readable identifier. Pipeline use only.",
    )

    parser.add_argument(
        "-p",
        "--project_id",
        required=True,
        help="The project associated with the study",
    )

    parser.add_argument("-t", "--tgt_id", required=True, help="The tgt model ")
    parser.add_argument(
        "-f",
        "--filepath",
        required=False,
        help="Path to the directory containing src data files. If not set, defaults to the {dbt project}/data path",
    )

    args = parser.parse_args()
    study_id = args.study_id

    # Set paths
    paths = get_paths(study_id, args.project_id, args.tgt_id, args.filepath)

    study_config = read_file(paths["study_yml_path"])
    ftd_config = read_file(paths["ftd_study_yml_path"])

    study_info = {
        "study_id": study_config["study_id"],
        "project_id": study_config["project_id"],
        "pipeline_db": study_config["pipeline_db"],
    }

    src_dd_objs = []
    for table_name, table_info in study_config["data_dictionary"].items():
        # generate hard copies of syn dd's prior to study_config validation

        dd_study_info = study_info.copy()
        dd_study_info.update({"table_name": table_name})

        logger.debug(f"Processing data_dictionaries: {table_name}")

        processor = file_setup(study_config, ftd_config, table_name, table_info, paths)

        if processor:
            src_dd_objs.append(processor)

    src_df_objs = []
    for table_name, table_info in study_config["data_files"].items():
        # generate hard copies of syn dd's prior to study_config validation

        for file in table_info.get("identifier"):
            logger.debug(f"Processing data_files: {table_name}")
            processor = file_setup(
                study_config, ftd_config, table_name, table_info, paths, file
            )

            if processor:
                src_df_objs.append(processor)

    logger.debug(f"Start validation of {study_id} config")
    validate_study_config(study_config, paths["src_data_dir"])
    logger.debug("End validation of study config")

    for dd in src_dd_objs:

        logger.debug(f"Start pipeline db, src table creation")
        dd.generate_new_table()

    for dfile in src_df_objs:
        logger.debug(f"Importing src data into the pipeline db")
        dfile.import_data()

    logger.info(f"END SCRIPT")


if __name__ == "__main__":
    main()
