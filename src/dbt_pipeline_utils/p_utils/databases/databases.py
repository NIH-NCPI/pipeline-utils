from jinja2 import Template
from typing import Type, Dict, ClassVar, Any, List
from pathlib import Path
from dbt_pipeline_utils import logger
from dbt_pipeline_utils.p_utils.databases.database_context import DatabaseContext
from dataclasses import dataclass, field
from dbt_pipeline_utils.p_utils.common import type_mapping
from dbt_pipeline_utils.p_utils.general import normalize_name, write_file


@dataclass
class DatabaseBC:
    """
    Base class for database-related operations.
    """

    context: DatabaseContext

    # Pre-initialized attributes for convenience
    paths: Dict[str, Path] = field(init=False)
    src_table_prefix: str = field(init=False)
    int_table_prefix: str = field(init=False)
    exp_table_prefix: str = field(init=False)
    df_identifiers: List[str] = field(init=False)
    dd_identifier: str = field(init=False)
    study_id: str = field(init=False)
    project_id: str = field(init=False)
    table_name: str = field(init=False)

    # Registry for subclasses
    _REGISTRY: ClassVar[Dict[str, Type["DatabaseBC"]]] = {}

    def __post_init__(self):
        """
        Pre-initialize frequently accessed attributes from the context.
        """
        self.paths = self.context.paths
        self.src_table_prefix = self.context.src_table_prefix
        self.int_table_prefix = self.context.int_table_prefix
        self.exp_table_prefix = self.context.exp_table_prefix
        self.df_identifiers = self.context.df_identifiers
        self.dd_identifier = self.context.dd_identifier
        self.study_id = self.context.study_id
        self.project_id = self.context.project_id
        self.table_name = self.context.table_name

    @classmethod
    def register(cls, key: str):
        def decorator(subclass: Type["DatabaseBC"]):
            cls._REGISTRY[key] = subclass
            return subclass

        return decorator

    @classmethod
    def define_db(cls, database_key: str, *, context: DatabaseContext) -> "DatabaseBC":
        if database_key not in cls._REGISTRY:
            raise KeyError(f"No structure registered for key '{database_key}'")
        return cls._REGISTRY[database_key](context=context)

    def load_column_data(self, dd_filepath: Path) -> Dict[str, Any]:
        """
        Delegate load_column_data to StructureBC.
        """
        return self.context.structure.load_column_data(dd_filepath)

    # def get_db_vars(self):
    #     """Loads specific key-value pairs from a YAML file based on the profile type."""
    #     profile_keys = ["host", "user", "dbname", "schema"] # update if not, pipeline_db: postgres

    #     config = read_file(self.profiles_path)
    #     env_section = config.get(self.pipeline_db, {}).get("outputs", {}).get("dev", {})

    #     env_vars = {key: env_section.get(key) for key in profile_keys}

    #     return env_vars

    def generate_study_sql(
        self,
        dd_filepath,
        source_tablename
        ):
        """Generates staging SQL files dynamically for each table based on the data dictionary."""

        src_table = normalize_name(source_tablename, trailing=False, extension="drop")

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
with source as (
select 
{",\n       ".join(column_definitions)}
from {{{{ source({self.study_id}, {src_table}) }}}}
)

select 
ROW_NUMBER() OVER () AS {self.project_id}_index,
source.*
from source
"""
        return sql_content.strip()

    def generate_cdm_sql(self, dd_filepath, stage):

        src_table_key = normalize_name(dd_filepath, trailing=False, extension="drop")
        column_data = self.load_column_data(dd_filepath)

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
        ) in column_data.get(src_table_key, []):
            sql_type = type_mapping.get(col_data_type, "text")

            src_col = src_var_name
            if stage == 'int':
                if (comment and "Foreign Key:" in comment) or src_var_name == "id":
                    src_col = f"  {{{{ generate_global_id(prefix='',descriptor=[''], study_id='{self.study_id}') }}}}"
                column_definitions.append(f'{src_col}::{sql_type} as "{f_col_name}"')
            elif stage == "exp":
                column_definitions.append(f'{src_col}::{sql_type} as "{col_name}"')

        sql_content = f"""select 
    {",\n    ".join(column_definitions)}
    from {{{{ ref(source_table) }}}}
"""

        return sql_content.strip()

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

    def convert_to_model(self, sql_content):
        return f"""{{{{ config(materialized='table') }}}}

    {sql_content}
    """

    def convert_to_macro(self, filename, sql_content, params):
        return f"""{{% macro {filename}({params}) %}}
    {sql_content}
{{%- endmacro %}}
    """

    def generate_study_sql_files(self):

        for src_file in self.df_identifiers:

            # generate sql
            sql_content = self.generate_study_sql(
                dd_filepath=self.paths["study_data_dir"] / self.dd_identifier,
                source_tablename=src_file,
            )

            model_content = self.convert_to_model(sql_content)

            # generate the output file. SQL for study models.
            new_model_name = normalize_name(
            [self.src_table_prefix, src_file], trailing=False, extension="drop"
        )

            gen_model_filepath = (
                self.paths["pl_proj_study_dir"]
                / self.table_name
                / f"{new_model_name}.sql"
            )

            # Will not overwrite an existing file.
            write_file(gen_model_filepath, model_content, mode="create")

    def generate_int_macro_model_files(self, int_config):

        for tablename, info in list(int_config.data_dictionary.items()):

            dd_filepath = self.paths["static_int_metadata_dir"] / info.identifier

            model_content = self.generate_cdm_sql(dd_filepath=dd_filepath, stage="int")

            # Generate the model files to trigger the macros
            model_filename = normalize_name(
                [self.int_table_prefix, tablename],
                trailing=False,
                extension="drop",
            )
            gen_model_filepath = self.paths["pl_sp_int_dir"] / f"{model_filename}.sql"

            write_file(gen_model_filepath, model_content, mode="create")

    def generate_exp_macro_model_files(self, exp_config):

        for tablename, info in list(exp_config.data_dictionary.items()):

            dd_filepath = self.paths["static_exp_metadata_dir"] / info.identifier

            sql_content = self.generate_cdm_sql(dd_filepath=dd_filepath, stage="exp")

            new_table = normalize_name(
                ["transform", tablename],
                trailing=False,
                extension="drop",
            )
            macro_content = self.convert_to_macro(
                new_table, sql_content, params="source_table"
            )

            gen_macro_filepath = (
                self.paths["static_exp_model_dir"] / f"macros/{new_table}.sql"
            )

            # Will not overwrite an existing file.
            write_file(gen_macro_filepath, macro_content, mode="create")

            # Generate the model files to trigger the macros
            model_content = self.generate_int_macro_model(new_table)
            model_filename = normalize_name(
                [self.exp_table_prefix, tablename],
                trailing=False,
                extension="drop",
            )
            gen_model_filepath = (
                self.paths["static_exp_model_dir"] / f"models/{model_filename}.sql"
            )

            write_file(gen_model_filepath, model_content, mode="create")
