# from dbt_pipeline_utils.p_utils..common import *
# from dbt_pipeline_utils.p_utils..general import *

# class Helpers():

#     def extract_columns(self, df, dd_format):
#         """
#         Extracts relevant column information based on the dictionary format.
#         """

#         column_map = DD_FORMATS[dd_format]  # Define dd column expectations
#         column_data_list = []

#         for idx, row in df.iterrows():
#             try:
#                 # variable_name (required — if NaN, fallback to 'unknown')
#                 variable_name = row.get(column_map["variable_name"]) or None
#                 formatted_variable_name = normalize_str(variable_name) or None

#                 description = row.get(column_map["description"]) or None

#                 data_type = row.get(column_map["data_type"])
#                 if pd.isna(data_type):
#                     data_type = "string"

#                 enumerations = row.get(column_map["enumerations"]) or None
#                 comment = row.get(column_map["comment"]) or None
#                 src_variable_name = row.get(column_map["src_variable_name"]) or None
#                 tests = row.get(column_map["tests"]) or None

#                 column_data_list.append((
#                     variable_name,
#                     formatted_variable_name,
#                     description,
#                     data_type,
#                     enumerations,
#                     comment,
#                     src_variable_name,
#                     tests
#                 ))

#             except Exception as e:
#                 print(f"Error at row {idx}: {e}")
#                 print(f"Row content: {row}")
#                 raise

#         return column_data_list

#     def get_column_dict(self, src_only=None):# load_src_column_data
#         """Loads column names, descriptions, and data types from CSV files and stores them in a dictionary."""
#         column_data = {}
#         src_dd_path = self.paths["src_data_dir"]

#         for table_id, table_info in self.data_dictionaries.items():

#             ddict_full_path = self.paths["src_data_dir"] / table_info['identifier']

#             dd_format = table_info.get("format")
#             src_df = read_file(ddict_full_path)

#             column_data[self.project_structure.get_raw_table_key()] = self.extract_columns(src_df, dd_format)

#             if not src_only:

#                 stg_ddict_full_path = src_dd_path / self.project_structure.get_src_dd_key(table_id)
#                 stg_df = read_file(stg_ddict_full_path)

#                 dd_format = table_info.get("format")

#                 src_table_key = self.project_structure.get_src_table_key(table_id)
#                 column_data[src_table_key] = self.extract_columns(stg_df, "pipeline_format")

#         return column_data
