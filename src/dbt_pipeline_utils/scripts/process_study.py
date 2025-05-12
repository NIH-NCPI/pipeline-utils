import argparse

from dbt_pipeline_utils.scripts.helpers.general import *
# from dbt_pipeline_utils.scripts.helpers.common import *
from dbt_pipeline_utils.scripts.helpers.validate_study_config import *
from dbt_pipeline_utils.scripts.helpers.factory_functions import *
from dbt_pipeline_utils import logger


def main(study_id, src_data_path):

    # Set paths
    paths = get_paths(study_id, src_data_path)

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

        df_study_info = study_info.copy()
        df_study_info.update({"table_name": table_name})

        logger.debug(f"Processing data_dictionaries: {table_name}")
        processor = file_setup(study_config, ftd_config, table_name, table_info, paths)

        if processor:
            src_df_objs.append(processor)

    print(f"Start validation of {study_id} config")
    validate_study_config(study_config, paths["src_data_dir"])
    print("End validation of study config")

    for dd in src_dd_objs:

        logger.info(f"Start pipeline db, src table creation")
        dd.generate_new_table()

    for dfile in src_df_objs:

        logger.info(f"Importing src data into the pipeline db")
        dfile.import_data()

    print(f"END SCRIPT")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize DBT transformation for study data.")

    parser.add_argument(
        "-s",
        "--study_id",
        required=True,
        help="Path to the YAML study_configuration file",
    )

    parser.add_argument(
        "-f",
        "--filepath",
        required=False,
        help="Path to the directory containing src data files. If not set, defaults to the {dbt project}/data path",
    )

    args = parser.parse_args()

    main(study_id=args.study_id, src_data_path=args.filepath)
