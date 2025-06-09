import pandas as pd
from dbt_pipeline_utils.scripts.helpers.general import *
from dbt_pipeline_utils.scripts.helpers.common import type_mapping
from pathlib import Path
import yaml

class FTDDocGenClass():

    def generate_ftd_dbt_project_yaml(self):
        filepath = self.paths["dbtp_ftdc_dir"] / "dbt_project.yml"

        # Read existing file content if it exists
        if filepath.exists():
            with open(filepath, "r") as f:
                existing_data = yaml.safe_load(f) or {}
        else:
            existing_data = {}

        # Ensure the correct structure
        models = existing_data.get("models", {})

        # Define table-specific schema and materialization
        study_info = {}

        for table_id in self.ftd_dd.keys():
            study_info[f"{self.study_id}_ftd_{table_id}"] = {
                "+schema": self.src_schema,
                "+materialized": "table"
            }

        # Merge into the models structure
        if self.study_id not in models:
            models[self.study_id] = study_info
        else:
            models[self.study_id].update(study_info)

        dbt_config = {
            "name": "ftd",
            "version": "1.0.0",
            "profile": self.project_id,
            "model-paths": ["models"],
            "macro-paths": ["macros"],
            "clean-targets": ["target", "dbt_packages"],
            "models": models,
        }

        write_file(filepath, dbt_config, overwrite=True)


    def generate_ftd_sql_files(self, column_data):

        src_table_columns = {
            table_id: {col[1] for col in column_data.get(f"{self.study_id}_stg_{table_id}", [])}
            for table_id in self.data_dictionary.keys()
        }

        for table_id in self.ftd_dd.keys():
            new_table = f"{self.study_id}_ftd_{table_id}"
            filepath = self.paths["dbtp_ftdc_study_dir"] / f"{new_table}.sql" 

            column_definitions = []
            joins = []

            for col_name, f_col_name, _, col_data_type, _, comment, src_var_name in column_data.get(new_table, []):
                sql_type = type_mapping.get(col_data_type, "text")

                alias = 'GEN_UNKNOWN'
                for src_id, cols in src_table_columns.items():
                    if src_var_name in cols:
                        alias = src_id
                        break
                    
                src_col=f'{alias}.{src_var_name}'
                if "Foreign Key:" in comment or src_var_name == 'id':
                    src_col=f"{{{{ generate_global_id(prefix='',descriptor=[''], study_id='{self.study_id}') }}}}"


                column_definitions.append(f'{src_col}::{sql_type} as "{f_col_name}"')

            base_table = list(src_table_columns.keys())[0]
            for src_id in src_table_columns.keys():
                if src_id != base_table:
                    joins.append(f"join {{{{ ref('{self.study_id}_stg_{src_id}') }}}} as {src_id}\non {self.get_join_conditions(src_id)} ")

            
            sql_content = f"""{{{{ config(materialized='table', schema='{self.study_id}_data') }}}}

    with source as (
        select 
        {",{BACKSLASH_CHAR}n       ".join(column_definitions)}
        from {{{{ ref('{self.study_id}_stg_{base_table}') }}}} as {base_table}
        {' '.join(joins)}
    )

    select 
        * 
    from source
    """

            write_file(filepath, sql_content)

    def generate_ftd_dds(self):
        ftd_static_data_dir = self.paths["ftd_static_data_dir"]
        trans_study_data_dir = self.paths["trans_study_data_dir"]
        ftd_study_dir_path = self.paths["ftd_study_data_dir"]
        ftd_yml_path = self.paths["ftd_study_yml_path"]
        
        ftd_yml = read_file(ftd_yml_path)
        utils_ftd_dd = ftd_yml.get("data_dictionary", {})
        additions_temp_path = self.paths["static_data_dir"] / "additions_template.csv"

        if additions_temp_path.exists:
            temp = read_file(additions_temp_path)

        for table_id, table_info in utils_ftd_dd.items():
            filepath = ftd_study_dir_path / f"ftd_{table_id}_dd.csv"

            ddict = table_info.get("identifier")
            ddict_full_path = ftd_static_data_dir / ddict
            if ddict_full_path.exists:
                utils_df = read_file(ddict_full_path)
            else:
                continue

            utils_df["src_variable_name"] = utils_df["variable_name"]

            # TODO clean/map dd cols. Currently overwriting a col name inconsistancy 
            utils_df['variable_description'] = utils_df['description']

            trans_path = trans_study_data_dir / f"{table_id}_stg_additions_dd.csv"

            write_file(filepath, utils_df)
            write_file(trans_path, temp)


    def load_ftd_column_data(self):
        """Loads column names, descriptions, and data types from CSV files and stores them in a dictionary."""
        column_data = {}
        src_dd_path = self.paths["src_data_dir"]
        ftd_study_path = self.paths["ftd_study_data_dir"]

        for table_id, table_info in self.data_dictionary.items():
            stg_table_key = f"{self.study_id}_stg_{table_id}"

            ddict = table_info.get("stg_src_table_id")
            ddict_full_path = src_dd_path / ddict
            df = read_file(ddict_full_path)
            df = df.astype(str).fillna("FTD_UNKNOWN")

            column_data[stg_table_key] = self.extract_columns(df, "pipeline_format")

        for table_id, table_info in self.ftd_dd.items():
            table_key = f"{self.study_id}_ftd_{table_id}"

            ddict = table_info.get("pipeline_identifier")
            ddict_full_path = ftd_study_path / ddict
            df = read_file(ddict_full_path)
            df = df.astype(str).fillna("FTD_UNKNOWN")

            column_data[table_key] = self.extract_columns(df, "pipeline_format")

        return column_data
