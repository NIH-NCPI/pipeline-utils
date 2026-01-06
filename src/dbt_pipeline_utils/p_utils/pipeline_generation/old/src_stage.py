from dbt_pipeline_utils.p_utils..common import *
from dbt_pipeline_utils.p_utils..general import *
# from dbt_pipeline_utils.p_utils..pipeline_generation.model_tests import format_tests
# import re

class SourceStage:
    """Base class for defining pipeline stages."""
    def __init__(self):
        pass

        def generate_src_sql_files(self, output_dir):
            """Generates SQL files dynamically for each table in its respective directory."""

            if self.table_info['import_type'] != 'duckdb':

                for table_id in self.data_dictionary.keys():
                    src_table_id = self.get_src_table_key(table_id)
                    sql_content = f"""{{{{ config(materialized='table') }}}}

    select * from {self.src_schema}.{table_id}
    """
                    filepath = output_dir / Path(table_id) / f"{src_table_id}.sql"

                    write_file(filepath, sql_content, overwrite=True)

        def generate_stg_sql_files(self, column_data, output_dir):
            """Generates staging SQL files dynamically for each table based on the data dictionary."""

            for table_id, table_info in self.data_dictionary.items():
                src_table = self.get_src_table_key(table_id)
                new_table = f"{self.study_id}_stg_{table_id}"
                filepath = output_dir / Path(table_id) / f"{new_table}.sql"

                column_definitions = []
                id_list = []
                for col_name, column_name_code, _, col_data_type, _, _, _, _  in column_data.get(src_table, []):
                    if column_name_code.endswith("_id"):
                        id_list.append(column_name_code)
                    sql_type = type_mapping.get(col_data_type, "text")
                    column_definitions.append(f'"{col_name}"::{sql_type} as "{column_name_code}"')

                sql_content = f"""{{{{ config(materialized='table') }}}}

    with source as (
        select
          {",\n       ".join(column_definitions)}
        from {{{{ source('{self.study_id}','{src_table}') }}}}
    )

    select
      ROW_NUMBER() OVER () AS ftd_index,
      source.*
    from source
    """

                # Write SQL file to the correct directory
                write_file(filepath, sql_content)

    def generate_stg_dds(self):
        """Generates staging SQL files dynamically for each table based on the data dictionary.
        open the src dd and apply minimal transformations"""

        for table_id, table_info in self.data_dictionaries.items():
            filepath = self.paths[
                "study_data_dir"
            ] / self.project_structure.get_src_dd_key(table_id)

            logger.debug(f"Processing table: {table_id}")
            ddict_full_path = self.paths['src_data_dir'] / table_info['identifier']

            stg_df = read_file(ddict_full_path)

            column_data = self.load_src_column_data(src_only=True)

            column_mapping = {
                col_name: column_name_code
                for col_name, column_name_code, _, _, _, _, _, _ in column_data.get(self.project_structure.get_raw_table_key(), [])
            }

            format_type = table_info.get("format")

            # Map from the original format to pipeline_format
            original_format_map = DD_FORMATS.get(format_type, {})
            pipeline_format_map = DD_FORMATS["pipeline_format"]

            # Convert src_variable_name based on the original format
            stg_df[pipeline_format_map["src_variable_name"]] = stg_df.get(original_format_map["variable_name"], "")

            # Convert variable_name field to match pipeline_format
            variable_name_key = original_format_map.get("variable_name", "variable_name")
            if variable_name_key in stg_df.columns:
                stg_df[pipeline_format_map["variable_name"]] = (
                    stg_df[variable_name_key].map(column_mapping).fillna(stg_df[variable_name_key])
                )

            # Rename all columns according to pipeline_format
            rename_map = {original_format_map[key]: pipeline_format_map[key] for key in pipeline_format_map if key in original_format_map}
            stg_df.rename(columns=rename_map, inplace=True)

            t_path = self.paths["src_data_dir"] / Path(
                f"ftd_transformations/{table_id}_stg_additions_dd.csv"
            )
            if t_path.exists():  
                transformations = read_file(t_path)
                stg_df = pd.concat([stg_df, transformations])
            else:
                pass

            write_file(filepath, stg_df)
