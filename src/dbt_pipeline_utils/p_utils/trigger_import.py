import argparse
from pathlib import Path
from dbt_pipeline_utils import logger
from dbt_pipeline_utils.p_utils.general import read_file
from dbt_pipeline_utils.p_utils.factory_functions import build_pipeline_objects
from dbt_pipeline_utils.p_utils.configs import StudyConfig


def main():

    parser = argparse.ArgumentParser(
        description="Initialize DBT transformation for study data."
    )

    parser.add_argument(
        "-c",
        "--study_config_filepath",
        required=True,
        help="Path to {study}_study.yaml from root dir. Example 'data/{study_id}/{study_id}_study.yaml'",
    )
    parser.add_argument(
        "-r",
        "--path_to_root_dir",
        required=False,
        default=Path.cwd(),
        help="Path to the root directory from current dir.",
    )
    parser.add_argument(
        "-s",
        "--static_data_dir",
        required=False,
        default=Path.cwd() / 'data/static',
        help="Path to the static data directory from the repo root.",
    )

    args = parser.parse_args()

    # Set up paths and load the study config
    root_path = Path(args.path_to_root_dir).resolve()

    study_config_path = root_path / args.study_config_filepath
    raw_config = read_file(study_config_path)
    study_config = StudyConfig.from_dict(raw_config)

    static_path = Path(root_path / args.static_data_dir).resolve()
    pipeline_objects = build_pipeline_objects(static_path, study_config, study_config_path)

    for table_name, obj in pipeline_objects.items():
        logger.info(f"\n\n\n{table_name.upper()} - PROCESSING")

        obj.structure.copy_required_macros_dir()

        obj.structure.import_org_data()
