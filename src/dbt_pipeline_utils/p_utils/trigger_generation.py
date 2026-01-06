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
    # parser.add_argument(
    #     "-i",
    #     "--int_model_id",
    #     required=True,
    #     help="",
    # )

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

    any_obj.structure.generate_dbt_project_yaml(
        any_obj.int_config,
        any_obj.exp_config,
    )

    any_obj.structure.generate_models_yml_files(
        any_obj.int_config,
        any_obj.exp_config
    )
    any_obj.structure.generate_static_docs_files(any_obj.int_config, any_obj.exp_config)

    for table_name, obj in pipeline_objects.items():
        logger.info(f"\n\n\n{table_name.upper()}:")
        logger.info(obj)

        obj.structure.generate_stg_dds()

        obj.structure.generate_sources_yml_files()

        obj.structure.generate_study_docs_files()

    # df_obj.generate_model_descriptions(df_obj.paths["dbtp_src_study_model_docs_dir"])

    # # TODO we may not need this one for duckdb passing
    # df_obj.generate_src_sql_files(df_obj.paths["dbtp_src_study_model_dir"])

    # df_obj.generate_stg_sql_files(
    #     column_data,
    #     df_obj.paths["dbtp_src_study_model_dir"]
    # )

    # generate_model_docs(intermediate_obj)

    # generate_int_model_docs(df_obj)

    # generate_basic_dbt_project_yml(df_obj.paths["dbtp_catalog_dir"], "catalog", df_obj.pipeline_db)

    # generate_tgt_model_docs(df_obj)

    # generate_run_script(df_obj)

    # logger.info(f"REMINDER: Update {self.tgt_id} dbt_project.yml.")
    # logger.info("REMINDER: Check the imports rootdir/packages.yml.")
    # logger.info(f"Generation complete")
