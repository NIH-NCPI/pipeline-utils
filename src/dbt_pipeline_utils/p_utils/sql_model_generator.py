from dataclasses import dataclass
from dbt_pipeline_utils.p_utils.common import type_mapping
from dbt_pipeline_utils.p_utils.files import normalize_name


@dataclass(slots=True)
class SqlModelGenerator:
    study_id: str
    project_id: str


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

    def duckdb_src_query(self, column_data: dict, table_path: str, dd_key: str = None) -> str: # type: ignore
        """
        Generate DuckDB SQL query using column_data directly.
        
        Args:
            column_data: Dictionary with table key containing list of column tuples
            table_path: Path to the CSV file
            dd_key: Key to access column_data. If None, uses first available key.
        
        Returns:
            SQL query string
        """
        # Get the key if not provided
        if dd_key is None:
            dd_key = next(iter(column_data.keys())) if column_data else None
        
        if not dd_key or dd_key not in column_data:
            raise ValueError(f"Invalid dd_key '{dd_key}'. Available keys: {list(column_data.keys())}")
        
        # Extract column definitions from column_data
        columns = []
        csv_columns = {}
        
        for col_original, col_formatted, *_ in column_data[dd_key]:
            columns.append(f'\n    "{col_original}"::TEXT AS "{col_formatted}"')
            csv_columns[col_original] = 'VARCHAR'
        
        # Build column definitions string for CSV read
        csv_col_defs = ",\n        ".join(
            f"'{col}': 'VARCHAR'" for col in csv_columns.keys()
        )
        
        query = f"""{{{{ config(materialized='table') }}}}
SELECT
{",".join(columns)}
FROM read_csv('{table_path}', AUTO_DETECT=FALSE, HEADER=TRUE, columns={{
        {csv_col_defs}
    }})"""
        
        return query
