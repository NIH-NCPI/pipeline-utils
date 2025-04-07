from dbt_pipeline_utils.scripts.helpers.common import *
from dbt_pipeline_utils.scripts.helpers.general import *

class DocGeneration():
    """Base class for defining pipeline stages."""
    
    def extract_columns(df, dd_format):
        """
        Extracts relevant column information based on the dictionary format.
        """

        column_map = DD_FORMATS[dd_format]  # Define dd column expectations
        
        column_data_list = [
            (
                row[column_map["variable_name"]],
                row[column_map["formatted_variable_name"]].lower().replace(" ", "_"),
                row.get(column_map["description"]),
                row.get(column_map["data_type"]),
                row.get(column_map["src_variable_name"]),
            )
            for _, row in df.iterrows()
        ]

        return column_data_list
    
    def load_src_column_data(data_dictionary, study_path, study_id, src_only=None):
        """
        Loads column names, descriptions, and data types from CSV files and stores them in a dictionary.
        """
        column_data = {}

        for table_id, table_info in data_dictionary.items():
            ddict = table_info.get("src_table_details")
            dd_format = table_info.get("src_table_format")
            ddict_full_path = study_path / ddict
            src_df = read_file(ddict_full_path)

            src_table_key = f"{study_id}_src_{table_id}"
            column_data[src_table_key] = extract_columns(src_df, dd_format)

            if not src_only:
                # Process staging data if needed
                stg_ddict = table_info.get("stg_src_table_id")
                stg_ddict_full_path = study_path / stg_ddict
                stg_df = read_file(stg_ddict_full_path)

                stg_table_key = f"{study_id}_stg_{table_id}"
                column_data[stg_table_key] = extract_columns(stg_df, "pipeline_format")

        return column_data


    def load_ftd_column_data(data_dictionary, study_path, ftd_dd, ftd_study_path, study_id):
        """Loads column names, descriptions, and data types from the FTD stage files."""
        column_data = {}

        # Process staging data
        for table_id, table_info in data_dictionary.items():
            stg_table_key = f"{study_id}_stg_{table_id}"
            ddict = table_info.get("stg_src_table_id")
            ddict_full_path = study_path / ddict
            df = read_file(ddict_full_path)
            df = df.astype(str).fillna("FTD_UNKNOWN")

            column_data[stg_table_key] = extract_columns(df, "pipeline_format")

        # Process FTD data if available
        if ftd_dd:
            for table_id, table_info in ftd_dd.items():
                table_key = f"{study_id}_ftd_{table_id}"
                ddict = table_info.get("src_table_details")
                dd_format = table_info.get("src_table_format")
                ddict_full_path = ftd_study_path / ddict
                df = read_file(ddict_full_path)
                df = df.astype(str).fillna("FTD_UNKNOWN")

                column_data[table_key] = extract_columns(df, dd_format)

        return column_data


