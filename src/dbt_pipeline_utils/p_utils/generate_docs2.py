import argparse
import os
from dbt_pipeline_utils.p_utils..pipeline_generation.old.generation_main import (
    generate_model_docs#, generate_int_model_docs, generate_tgt_model_docs, generate_run_script
)
from dbt_pipeline_utils.p_utils..general import *
from dbt_pipeline_utils.p_utils..common import *
from dbt_pipeline_utils.p_utils..validation.validate_study_config import *
from dbt_pipeline_utils import logger
from dbt_pipeline_utils.p_utils..source_files import *
from dbt_pipeline_utils.p_utils..structures.project_structures import *
from dbt_pipeline_utils.p_utils..databases import *


def create_dynamic_class(study_config, file_type):
    # Determine structure class based on `project_structure`
    project_structure = study_config.get("project_structure", "single").lower()
    if project_structure == "single":
        structure_class = SingleStructureClass
    elif project_structure == "multiple":
        structure_class = MultipleStructureClass
    else:
        raise ValueError(f"Unknown project structure: {project_structure}")

    # Determine database class based on `pipeline_db`
    pipeline_db = study_config.get("pipeline_db", "duckdb").lower()
    if pipeline_db == "duckdb":
        db_class = DuckdbClass
    elif pipeline_db == "postgres":
        db_class = PostgresqlClass
    else:
        raise ValueError(f"Unknown pipeline DB: {pipeline_db}")

    # Determine structure class based on `file_type`
    if file_type == "dd":
        file_class = DataDictionary
    elif file_type == "df":
        file_class = DataFile
    else:
        raise ValueError(f"Unknown file_type: {file_type}")

    # Dynamically create a combined class
    class PipelineObject(file_class, structure_class, db_class):

        def __init__(
            self, study_id, project_id, db_profile, pipeline_db, project_structure, tgt_model, name, attribute, data_dir
        ):
            file_class.__init__(
                self,
                study_id,
                project_id,
                db_profile,
                pipeline_db,
                project_structure, 
                tgt_model,
                name,
                attribute
            )
            structure_class.__init__(self, name, data_dir)
            db_class.__init__(self, study_config)

    return PipelineObject


# def generate_int_study_yaml(paths, project_id):
#     # Directory with your data files
#     data_dir = paths['int_static_data_dir']

#     # List to store cleaned file metadata
#     int_data_dir = []

#     # Iterate through files in the directory
#     for filename in os.listdir(data_dir):
#         if not filename.endswith("template.csv"):
#             for suffix in ("-dd.csv", "_dd.csv"):
#                 if filename.endswith(suffix):
#                     clean_id = filename[:-len(suffix)]
#             table_id = clean_id.replace('-', "_").lower()
#             identifier = filename
#             pipeline_identifier = f"int_{table_id}_dd.csv"
#             int_data_dir.append((table_id, identifier, pipeline_identifier))

#     # Build the data_dictionary section
#     dds = {}
#     for table_id, identifier, pipeline_identifier in int_data_dir:
#         dds[table_id] = {
#             "identifier": identifier,
#             "pipeline_identifier": pipeline_identifier,
#         }

#     # Build final YAML dictionary
#     dbt_config = {
#         "study_id": project_id,
#         "data_dictionary": dds,
#     }

#     # Write the YAML to a file
#     write_file(paths['int_study_yml_path'], dbt_config, overwrite=True)


# Creating objects for each file listed in the config file
def create_objects_from_config(study_config, data_dir):
    study_id = study_config["study_id"]
    project_id = study_config["project_id"]
    db_profile = study_config["db_profile"]
    pipeline_db = study_config["pipeline_db"]
    project_structure = study_config["project_structure"]
    tgt_model = study_config["tgt_model"]

    # Dynamically determine the class
    DynamicClass = create_dynamic_class(study_config, file_type)

    # Create objects for data dictionaries
    dd_objs = []
    file_type = 'dd'
    for name, attributes in study_config["data_dictionary"].items():
        dd_obj = DynamicClass(study_id, project_id, db_profile, pipeline_db, project_structure, tgt_model, name, attributes, data_dir, file_type)
        dd_objs.append(dd_obj)

    # Create objects for data files
    df_objs = []
    file_type = 'df'
    for name, attributes in study_config["data_file"].items():
        dd_obj = DynamicClass(study_id, project_id, db_profile, pipeline_db, project_structure, tgt_model, name, attributes, data_dir, file_type)
        df_objs.append(dd_obj)

    return dd_objs, df_objs


def main():

    parser = argparse.ArgumentParser(description="Initialize DBT transformation for study data.")
    parser.add_argument(
            "-s",
            "--study_id",
            required=True,
            help="The study's readable identifier. Pipeline use only.",
        )

    parser.add_argument("-p", "--project_id", required=True, help="The project associated with the study")

    parser.add_argument("-t", "--tgt_model", required=True, help="The tgt/access model id.")
    parser.add_argument(
            "-d",
            "--data_dir",
            required=False,
            help="Path to the directory containing src data files. If not set, defaults to the {dbt project}/data path",
        )

    args = parser.parse_args()

    study_id=args.study_id
    project_id = args.project_id
    tgt_model=args.tgt_model
    if args.data_dir:
        data_dir = args.data_dir
    else:
        data_dir = Path.cwd() / f"data/{study_id}"

    study_config_path = Path(f"{data_dir}/{study_id}_study.yaml")

    logger.info(f'Generating the {project_id} {study_id} dbt pipeline...')
    study_config = read_file(study_config_path)

    dd_objs, df_objs = create_objects_from_config(study_config, data_dir)

    # Output the objects
    print("Data Dictionary Objects:")
    for obj in dd_objs:
        print(obj)

    print("\nData File Objects:")
    for obj in df_objs:
        print(obj)
        print()

    # generate_int_study_yaml(paths, project_id)

    # int_config = read_file(paths["int_study_yml_path"])

    logger.debug(f"Start validation of {study_id} config")
    validate_study_config(study_config, data_dir)
    logger.debug("End validation of study config")

    for df_obj in df_objs:

        generate_model_docs(df_obj)

#         generate_int_model_docs(df_obj)

#         generate_basic_dbt_project_yml(df_obj.paths["dbtp_catalog_dir"], "catalog", df_obj.pipeline_db)

#         generate_tgt_model_docs(df_obj)

#         generate_run_script(df_obj)

#     logger.info(f"REMINDER: Update {tgt_id} dbt_project.yml.")
#     logger.info("REMINDER: Check the imports rootdir/packages.yml.")
#     logger.info(f"Generation complete")

# if __name__ == "__main__":


# import pdb
# pdb.set_trace()
