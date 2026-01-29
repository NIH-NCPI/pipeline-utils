from jinja2 import Template
from typing import Type, Dict, ClassVar, Any, List
from pathlib import Path
from dbt_pipeline_utils import logger
from dataclasses import dataclass, field
from dbt_pipeline_utils.p_utils.common import type_mapping
from dbt_pipeline_utils.p_utils.general import normalize_name, write_file


@dataclass(slots=True)
class SqlModelGenerator:
    study_id: str
    project_id: str

    def generate_study_sql(
        self,
        dd_filepath
        ):
        """Generates staging SQL files dynamically for each table based on the data dictionary."""

        src_table_key = normalize_name(
            dd_filepath, trailing=False, extension="drop"
        )
        column_data = self.load_column_data(dd_filepath)

        column_definitions = []
        id_list = []
        for col_name, column_name_code, _, col_data_type, *_ in column_data.get(
            src_table_key, []
        ):
            if column_name_code.endswith("_id"):
                id_list.append(column_name_code)
            sql_type = type_mapping.get(col_data_type, "text")
            column_definitions.append(
                f'"{col_name}"::{sql_type} as "{column_name_code}"'
            )

        sql_content = f"""
select 
    ROW_NUMBER() OVER () AS "{self.project_id}_index",
{",\n       ".join(column_definitions)}
"""
        return sql_content.strip()

    def generate_cdm_sql(
        self,
        *,
        column_data: dict,
        dd_key: str,
        stage: str,
    ) -> str:

        column_definitions = []

        for (
            col_name,
            f_col_name,
            _,
            col_data_type,
            _,
            comment,
            src_var_name,
            _,
        ) in column_data.get(dd_key, []):

            sql_type = type_mapping.get(col_data_type, "text")
            src_col = src_var_name

            if stage == "stb":
                if (comment and "Foreign Key:" in comment) or src_var_name == "id":
                    src_col = (
                        f"{{{{ generate_global_id(prefix='', descriptor=[''], study_id='{self.study_id}') }}}}"
                    )
                column_definitions.append(f'null::{sql_type} as "{f_col_name}"')


            elif stage == "int":
                column_definitions.append(f'{src_col}::{sql_type} as "{f_col_name}"')

            elif stage == "exp":
                column_definitions.append(f'{src_col}::{sql_type} as "{col_name}"')

            else:
                raise ValueError(f"Unsupported stage: {stage}")

        return "select\n" "    " + ",\n    ".join(column_definitions)

    def generate_int_macro_model(self, macro_name):

        content = f"""

{{{{ config(schema=var('target_schema')) }}}}

{{-% set source_table = (var('source_table') | default(none)) -%}}

{{-% if source_table is not none -%}}
    {{-% do log("Using source_table: " ~ source_table, info=True) -%}}
    {{{{ {macro_name}(source_table) }}}}
{{% else %}}
    {{-% do log("Warning source_table: " ~ source_table, info=True) -%}}
{{-% endif -%}}
        """

        return content.strip()

    def convert_to_model(self, sql_content, source_tablename):
        src_table = normalize_name(source_tablename, trailing=False, extension="drop")

        return f"""{{{{ config(materialized='table') }}}}

    {sql_content}
    from {{{{ source('{self.study_id}', '{src_table}') }}}}
    """

    def convert_to_ref_model(self, sql_content, source_tablename):
        src_table = normalize_name(source_tablename, trailing=False, extension="drop")

        return f"""{{{{ config(materialized='table') }}}}

    {sql_content}
    from {{{{ ref('{src_table}') }}}}
    """

    def convert_to_macro(self, filename, sql_content, params):
        return f"""{{% macro {filename}({params}) %}}
    {sql_content}
    from {{{{ ref(source_table) }}}}
{{%- endmacro %}}
    """

    def convert_to_attr_model(self, sql_content):
        return f"""{{% set source_table = var('source_table', none ) %}}
{{% if source_table %}}
    {sql_content}
    from {{{{ ref(source_table) }}}}


{{% else %}}
    select 1 as compile_placeholder where false
{{% endif %}}
    """

    def study_select_sql(
        self,
        *,
        column_data: dict,
        dd_key: str,
    ) -> str:
        column_defs = []

        for col_name, col_code, _, col_type, *_ in column_data.get(dd_key, []):
            sql_type = type_mapping.get(col_type, "text")
            column_defs.append(f'"{col_name}"::{sql_type} as "{col_code}"')

        return (
            "select\n"
            f'    ROW_NUMBER() OVER () AS "{self.project_id}_index",\n'
            "    " + ",\n    ".join(column_defs)
        )

    # def generate_macro_model_files(
    #     self,
    #     config,
    #     *,
    #     stage: str,
    #     metadata_dir_key: str,
    #     model_dir_key: str,
    #     macro_dir_key: str | None = None,
    #     table_prefix: str | None = None,
    #     model_type: str = "model",
    #     macro_params: str | None = None,
    #     write_mode: str = "overwrite",
    # ):
    #     """
    #     Generate dbt model and/or macro files for a given stage (int or exp).

    #     """

    #     for tablename, info in config.data_dictionary.items():

    #         dd_filepath = self.paths[metadata_dir_key] / info.identifier

    #         sql_content = sqlgen.generate_cdm_sql(
    #             column_data=column_data,
    #             dd_key=dd_key,
    #             stage=stage,
    #         )

    #         # Normalize table / model name
    #         model_name_parts = [table_prefix, tablename] if table_prefix else [tablename]
    #         model_name = normalize_name(
    #             model_name_parts,
    #             trailing=False,
    #             extension="drop",
    #         )

    #         if model_type == "model_macro":
    #             if not macro_dir_key:
    #                 raise ValueError("macro_dir_key is required for model_macro")

    #             macro_content = self.convert_to_macro(
    #                 model_name,
    #                 sql_content,
    #                 params=macro_params,
    #             )

    #             macro_filepath = self.paths[macro_dir_key] / f"{model_name}.sql"
    #             write_file(macro_filepath, macro_content, mode=write_mode)

    #             # Generate model that triggers the macro
    #             model_content = self.generate_int_macro_model(model_name)

    #         elif model_type == "model":
    #             model_content = self.convert_to_model(
    #                 sql_content,
    #                 model_name,
    #             )

    #         elif model_type == "attr_model":
    #             model_content = self.convert_to_attr_model(sql_content)

    #         else:
    #             raise ValueError(f"Unrecognized model_type: {model_type}")

    #         model_filepath = self.paths[model_dir_key] / f"{model_name}.sql"
    #         write_file(model_filepath, model_content, mode=write_mode)
