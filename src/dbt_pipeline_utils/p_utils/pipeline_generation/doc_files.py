# from dbt_pipeline_utils.p_utils..common import *
from dbt_pipeline_utils.p_utils.general import *
# from dbt_pipeline_utils.p_utils..pipeline_generation.model_tests import format_tests
import re

class DocFiles():
    def __init__(self):
        pass
    
    def generate_doc_block_name(self, table_name, column_name):
        """
        Ensures dbt doc block names consist of only letters, numbers and underscores, as dbt expects.
        """
        name = f"{table_name}_{column_name}"
        return re.sub(r"[^a-zA-Z0-9_]", "_", name)


    # def generate_column_descriptions(self, column_data, output_dir, ftd_model=None):
    #     """Generates a separate column_descriptions.md for each table in its respective docs directory."""
    #     filepath = output_dir / "column_descriptions.md"
    #     if ftd_model:
    #         data_dictionary = self.ftd_dd
    #     if not ftd_model:
    #         data_dictionary = self.data_dictionary

    #     # Read existing file content if it exists
    #     if not filepath.exists():
    #         filepath.touch()
    #     existing_data = filepath.read_text().rstrip()

    #     # Use regex to extract existing col_doc_ids
    #     existing_col_doc_ids = set(re.findall(r"\{%\s*docs\s+([\w\d_]+)\s*%\}", existing_data))

    #     new_descriptions = []

    #     for table_id, table_info in data_dictionary.items():

    #         if ftd_model:
    #             # Only process the FTD table key
    #             table_keys = [f"{self.study_id}_ftd_{table_id}"]
    #         else:
    #             # Process both src and stg table keys in a single loop
    #             src_table_key = self.get_src_table_key(table_id)
    #             table_keys = [f"{src_table_key}", f"{self.study_id}_stg_{table_id}"]

    #         for table_key in table_keys:

    #             table_description = table_info.get("description", f"Model for {table_key}.")
    #             table_desc_id = f"{table_key}_description"
    #             table_desc_block = f"{{% docs {table_desc_id} %}}\n{table_description}\n{{% enddocs %}}\n"

    #             # Add table description if not already present
    #             if table_desc_id not in existing_col_doc_ids:
    #                 new_descriptions.append(table_desc_block)
    #                 existing_col_doc_ids.add(table_desc_id)

    #             for col_name, col_name_code, col_description, _, _, _, _, _  in column_data.get(table_key, []):
    #                 col_doc_id = generate_doc_block_name(table_key, col_name_code)
    #                 col_desc_block = f"{{% docs {col_doc_id} %}}\n{col_description}\n{{% enddocs %}}\n"

    #                 if col_doc_id not in existing_col_doc_ids:
    #                     new_descriptions.append(col_desc_block)
    #                     existing_col_doc_ids.add(col_doc_id)  # Ensure newly added col_doc_id is tracked

    #     new_data = "\n\n".join(new_descriptions).strip()

    #     if new_data:
    #         data = existing_data + "\n\n" + new_data if existing_data else new_data
    #         write_file(filepath, data, overwrite=True) # Needs to add any new data to an existing file.
    #     else:
    #         logger.debug(f"No updates needed: {filepath}")

#     def generate_model_descriptions(self, output_dir):
#         """Generates model_descriptions.md using the specified format."""
#         model_descriptions = []

#         # Group tables by prefix (e.g., "moo_src_", "moo_stg_")
#         grouped_tables = {}
#         for table_id, table_info in self.data_dictionary.items():
#             prefix = table_id.split("_")[0]  # Assumes prefix is the first part of table_id
#             grouped_tables.setdefault(prefix, []).append((table_id, table_info))

#         for prefix, tables in grouped_tables.items():
#             model_descriptions.append(f"### {prefix.capitalize()} Models\n")

#             for table_id, table_info in tables:
#                 src_table_id = self.get_src_table_key(table_id)
#                 src_description = table_info.get(
#                     "description", f"Model for {src_table_id}."
#                 )
#                 model_descriptions.append(
#                     f"{{% docs {src_table_id} %}}\n{src_description}\n{{% enddocs %}}\n"
#                 )

#                 stg_table_id = f"{self.study_id}_stg_{table_id}"
#                 stg_description = table_info.get("description", f"Model for {stg_table_id}.")
#                 model_descriptions.append(f"{{% docs {stg_table_id} %}}\n{stg_description}\n{{% enddocs %}}\n")

#                 data = "\n".join(model_descriptions)

#                 filepath = output_dir / "model_descriptions.md"

#                 write_file(filepath, data)
