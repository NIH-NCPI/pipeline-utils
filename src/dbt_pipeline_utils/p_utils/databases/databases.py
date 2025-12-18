from jinja2 import Template
from typing import Type, Dict, ClassVar
from dataclasses import dataclass


@dataclass
class DatabaseBC:
    pipeline_db: str

    _REGISTRY: ClassVar[Dict[str, Type["DatabaseBC"]]] = {}

    @classmethod
    def register(cls, key: str):
        def decorator(subclass: Type["DatabaseBC"]):
            cls._REGISTRY[key] = subclass
            return subclass

        return decorator

    @classmethod
    def define_db(cls, pipeline_db: str) -> "DatabaseBC":
        try:
            return cls._REGISTRY[pipeline_db](pipeline_db)
        except KeyError:
            raise ValueError(f"Unsupported pipeline_db: {pipeline_db}")

    #              , ftd_config, table_name, table_info, paths, file):
    #     self.study_config = study_config
    #     self.ftd_config = ftd_config
    #     self.table_name = table_name
    #     self.table_info = table_info
    #     self.paths = paths
    #     self.profiles_path = paths.get("profiles_path_home")
    #     self.file = file
    #     self.profile = ""
    #     self.src_schema = ""
    #     self.src_data_csv =  ""

    #     study_details = {
    #         "study_id": self.study_config["study_id"],
    #         "project_id": self.study_config["project_id"],
    #         "pipeline_db": self.study_config["pipeline_db"],
    #         "data_dictionary" : self.study_config.get("data_dictionary", {}),
    #         "data_files" : self.study_config.get("data_files", {}),
    #         "ftd_dd": self.ftd_config.get("data_dictionary", {})
    #     }

    #     for key, value in study_details.items():
    #         setattr(self, key, value)

    #     self.new_table_name = Path(self.get_src_table_key(self.table_name)).stem

    #     # Make the profile_keys into attributes
    #     pipeline_db_vars = self.get_db_vars()
    #     for key, value in pipeline_db_vars.items():
    #         setattr(self, key, value)

    # def get_db_vars(self):
    #     """Loads specific key-value pairs from a YAML file based on the profile type."""
    #     profile_keys = ["host", "user", "dbname", "schema"] # update if not, pipeline_db: postgres

    #     config = read_file(self.profiles_path)
    #     env_section = config.get(self.pipeline_db, {}).get("outputs", {}).get("dev", {})

    #     env_vars = {key: env_section.get(key) for key in profile_keys}

    #     return env_vars

    # def extract_table_schema(self):
    #     """Extracts column definitions from the data dictionary CSV."""

    #     full_file_path = self.paths['src_data_dir']  / Path(f'{self.src_data_csv}')
    #     dd = read_file(full_file_path)
    #     # Use extract_columns to get structured column data
    #     column_data_list = self.extract_columns(dd, self.table_info['format'])

    #     column_definitions = []
    #     for variable_name, formatted_name, _, data_type, _ in column_data_list:
    #         sql_type = type_mapping.get(data_type, "text")
    #         column_definitions.append(f'"{variable_name}" {sql_type}')

    #     return column_definitions, self.src_data_csv

    # def get_src_ddict_path(self, table_info):
    #     src_dd_path = self.paths['src_data_dir']

    #     if table_info.get("import_type") == 'synapse':
    #         ddict = table_info.get("src_file_id")

    #     if table_info.get("import_type") == 'pg':
    #         ddict = table_info.get("identifier")

    #     if table_info.get("import_type") == 'duckdb':
    #         ddict = table_info.get("identifier")

    #     if table_info.get("import_type") not in ['pg', 'duckdb', 'synapse']:
    #         logger.error(f"{table_info.get('import_type')} is not valid")

    #     return src_dd_path / Path(f"{ddict}"), ddict

    # def get_join_conditions(self, current_table):

    #     join_cols = self.data_files.get(current_table, {}).get("join_cols", {})

    #     for join_table, left_column in join_cols.items():
    #         # Get the right column from the join_table's join_cols (pointing back to current_table)
    #         right_column = self.data_files.get(join_table, {}).get("join_cols", {}).get(current_table)

    #         if right_column:
    #             # join_table.right_column = current_table.left_column
    #             return f"{join_table}.{right_column} = {current_table}.{left_column}"

    #     return ''
