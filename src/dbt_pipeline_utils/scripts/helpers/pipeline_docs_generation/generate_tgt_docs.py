from dbt_pipeline_utils.scripts.helpers.general import *

class TgtDocGenClass():

    def copy_directory(self):
        """
        Recursively copies files and subdirectories from src_dir to dest_dir
        """
        src_dir=self.paths["tgt_static_data_dir"]
        dest_dir= self.paths["dbtp_tgt_dir"]
        for item in src_dir.rglob("*"):
            relative_path = item.relative_to(src_dir)
            target = dest_dir / relative_path

            if item.is_dir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                # Copy file contents manually
                data = read_file(item)
                write_file(target, data)

        logger.debug(f"Copied '{src_dir}' to '{dest_dir}'")

    def create_new_tgt_models(self):
        """
        Models call the tgt models that call the tgt macros.
        """

        for table_id in self.ftd_dd.keys():
            new_model = f"tgt_{table_id}"
            output_filepath = (
                self.paths["tgt_static_data_dir"] / f"models/{new_model}.sql"
            )

            sql_content = f"""{{{{ config(schema=var('target_schema')) }}}}

{{% set source_table = (var('source_table') | default(none)) %}}

{{% if source_table is not none %}}
    {{% do log("Using source_table: " ~ source_table, info=True) %}}
    {{{{ transform_{table_id}(source_table) }}}}
{{% else %}}
    {{% do log("Warning source_table: " ~ source_table, info=True) %}}
{{% endif %}}
"""

            write_file(output_filepath, sql_content)

    def create_new_tgt_macros(self, column_data):
        """
        Create tgt macros using the ftd data dictionaries.
        TODO: Enable use of other data dictionaries when necessary. There
        should be ftd dds and tgt dds eventually.
        """

        for table_id in self.ftd_dd.keys():
            new_table = f"{self.study_id}_ftd_{table_id}"
            new_macro = f"transform_{table_id}"
            output_filepath = (
                self.paths["tgt_static_data_dir"] / f"macros/{new_macro}.sql"
            )

            column_definitions = []

            for tgt_name, f_col_name, _, col_data_type, _, _, _, _ in column_data.get(
                new_table, []
            ):
                sql_type = type_mapping.get(col_data_type, "text")

                column_definitions.append(f'  {f_col_name}::{sql_type} as "{tgt_name}"')

            sql_content = f"""{{{{% macro transform_participant(source_table) %}}}}

select 
{",\n".join(column_definitions)}
from {{{{ ref(source_table) }}}}
{{%- endmacro -%}}
"""

            write_file(output_filepath, sql_content)

    def generate_tgt_dbt_project_yaml(self):
        filepath = self.paths["tgt_static_data_dir"] / "dbt_project.yml"

        models = {}
        study_info = {}

        for table_id in self.ftd_dd.keys():
            study_info[f"tgt_{table_id}"] = {"+materialized": "table"}

        models["tgt"] = study_info

        dbt_config = {
            "name": "tgt",
            "version": "1.0.0",
            "profile": self.project_id,
            "model-paths": ["models"],
            "macro-paths": ["macros"],
            "clean-targets": ["target", "dbt_packages"],
            "models": models,
        }

        write_file(filepath, dbt_config, overwrite=True)
