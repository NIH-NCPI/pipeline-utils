import argparse
from pathlib import Path
from dbt_pipeline_utils import logger
from dbt_pipeline_utils.p_utils.general import read_file, find_repo_root
from dbt_pipeline_utils.p_utils.factory_functions import build_pipeline_objects
from dbt_pipeline_utils.p_utils.configs import StudyConfig

def main():

    parser = argparse.ArgumentParser(
        description="Initialize DBT transformation for study data."
    )

    parser.add_argument(
        "-sc",
        "--study_config_filepath",
        required=True,
        help="Path to {study}_study.yaml from root dir. Example 'data/{study_id}/_{study_id}_study.yaml'",
    )
    parser.add_argument(
        "-d",
        "--static_data_dir",
        required=False,
        default=Path.cwd() / "data",
        help="Path to the static data dir - parent dir, from the repo root.",
    )

    args = parser.parse_args()

    # Set up paths and load the study config
    root_path = find_repo_root()

    study_config_path = root_path / args.study_config_filepath
    raw_config = read_file(study_config_path)
    study_config = StudyConfig.from_dict(raw_config)

    static_path = Path(root_path / args.static_data_dir).resolve()
    pipeline_objects = build_pipeline_objects(
        static_path, study_config, study_config_path
    )

    # TODO Only trigger generation or additions when necessary.

    any_obj = next(iter(pipeline_objects.values()))

    any_obj.structure.generate_dbt_project_yaml()

    any_obj.structure.generate_models_yml_files(  # also creates col description docs
        any_obj.int_config, any_obj.exp_config
    )

    any_obj.structure.generate_static_sql_models(config=any_obj.int_config, stage="int")

    any_obj.structure.generate_static_sql_models(config=any_obj.exp_config, stage="exp")
    any_obj.structure.copy_static_export_dir()
    any_obj.structure.copy_profiles_yml()

    for table_name, obj in pipeline_objects.items():
        logger.info(f"\n{table_name.upper()} - PROCESSING")

        obj.structure.generate_stg_dds()

        obj.structure.generate_sources_yml_files()  # also creates col description docs

        obj.structure.generate_study_sql_models()

        obj.structure.generate_run_script()


        logger.info(f"{table_name.upper()} - GENERATION COMPLETE")
