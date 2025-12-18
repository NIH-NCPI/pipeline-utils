# from dbt_pipeline_utils.p_utils..common import *
from dbt_pipeline_utils.p_utils.general import *
# from dbt_pipeline_utils.p_utils..pipeline_generation.model_tests import format_tests
# import re

class DbtProperties():

    def generate_dbt_project_yml(self):
        '''
        Creates sources/{study_id}/dbt_project.yml

        Doubled
        '''

        study_info = {}

        for table_id in self.data_dictionaries.keys():
            src_table_id = self.project_structure.get_src_table_key(table_id)

            study_info[table_id] = {
                src_table_id: {
                    "+schema": self.src_schema,
                    "+materialized": "table",
                },
            }

        # Correct structure for dbt_project.yml
        dbt_config = {
            "name": self.study_id,
            "version": "1.0.0",
            "profile": self.project_id,
            "model-paths": ["models"],
            "macro-paths": ["macros"],
            "snapshot-paths": ["snapshots"],
            "clean-targets": ["target", "dbt_packages"],
            "models": {
                "+schema": self.src_schema,
                "+materialized": "table",
                **study_info,
            },
        }

        filepath = self.paths["dbtp_src_study_dir"] / "dbt_project.yml"

        write_file(filepath, dbt_config)

    def generate_dbt_models_yml(self, column_data, output_dir, int_model=None):
        """
        Generates dbt models.yml file for each table in its respective directory, including src and staging models.
        """
        if int_model:
            data_dictionaries = self.int_dd
            int_models = []
        if not int_model:
            data_dictionaries = self.data_dictionaries

        for table_id, table_info in data_dictionaries.items():
            src_models = []
            if int_model:
                table_models_dir = output_dir
                id_tables = [self.project_structure.get_int_table_key(table_id)]
            if not int_model:
                table_models_dir = output_dir / Path(f"{table_id}")
                id_tables = [self.project_structure.get_src_table_key(table_id)]

            for table_name in id_tables:

                columns_metadata = [
                    {
                        "name": col_name_code,
                        "description": f'{{{{ doc("{self.generate_doc_block_name(table_name, col_name_code)}") }}}}',
                        "data_type": col_data_type,
                        **({"tests": self.format_tests(tests, col_name_code, enums)} if tests is not None else {}),

                    }
                    for col_name, col_name_code, _, col_data_type, enums, _, _, tests in column_data.get(
                        table_name, []
                    )
                ]

                model_entry = {
                    "name": table_name,
                    "description": table_info.get("description", f"Model for {table_name}."),
                    "columns": columns_metadata
                }

                if int_model:
                    int_models.append(model_entry)
                else:
                    src_models.append(model_entry)

            if not int_model:
                models = {"version": 2, "models": src_models}
                filepath = table_models_dir / "__models.yml"
                write_file(filepath, models, overwrite=True)

        if int_model:
            # Write all int models at once
            models = {
                "version": 2,
                "models": int_models
            }
            filepath = self.paths["dbtp_ftdc_study_docs_dir"] / "__models.yml"
            write_file(filepath, models, overwrite=True)

    def generate_dbt_sources_yml(self, column_data, output_dir):
        source_tables = []

        for table_id, table_info in self.data_files.items():
            for file in table_info.get("identifier"):
                src_filename = Path(file).stem
                columns_metadata = [
                    {
                        "name": col_name,
                        "description": f'{{{{ doc("{self.generate_doc_block_name(src_filename, col_name_code)}") }}}}'
                    }

                    for col_name, col_name_code, _, _, _, _, _, _  in column_data.get(f"{src_filename}", [])
                ]

                source_tables.append({
                    "name": src_filename,
                    "description": table_info.get("description", f"Source table for {src_filename}."),
                    "columns": columns_metadata
                })

            sources_yaml = {
                "version": 2,
                "sources": [
                    {
                        "name": self.study_id,
                        "schema": self.src_schema,
                        "tables": source_tables
                    }
                ]
            }

            filepath = output_dir / "sources.yml"
            write_file(filepath, sources_yaml, overwrite=True)
