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
        "-f",
        "--study_config_filepath",
        required=True,
        help="Path to {study}_study.yaml from root dir. Example 'data/{study_id}/{study_id}_study.yaml'",
    )


    args = parser.parse_args()

    # Set up paths and load the study config
    study_config_path = Path(args.study_config_filepath)
    raw_config = read_file(study_config_path)
    study_config = StudyConfig.from_dict(raw_config)

    # Create pipeline objects
    # print(study_config)
    pipeline_objects = build_pipeline_objects(study_config, study_config_path)

    # TODO Only trigger generation or additions when necessary.

    any_obj = next(iter(pipeline_objects.values()))

    any_obj.structure.generate_dbt_project_yaml()

    any_obj.structure.generate_models_yml_files(
        any_obj.int_config,
        any_obj.exp_config
    )
    any_obj.structure.generate_int_desc_files(any_obj.int_config)
    any_obj.structure.generate_exp_desc_files(any_obj.exp_config)

    any_obj.database.generate_int_macro_model_files(any_obj.int_config)

    any_obj.database.generate_exp_macro_model_files(any_obj.exp_config)



    for table_name, obj in pipeline_objects.items():
        logger.info(f"\n\n\n{table_name.upper()}:")
        logger.info(obj)

        obj.structure.generate_stg_dds()

        obj.structure.generate_sources_yml_files()

        obj.structure.generate_study_desc_files()

        obj.database.generate_study_sql_files()

        obj.structure.generate_run_script()


    # logger.info(f"REMINDER: Update {self.tgt_id} dbt_project.yml.")
    # logger.info("REMINDER: Check the imports rootdir/packages.yml.")
    # logger.info(f"Generation complete")
