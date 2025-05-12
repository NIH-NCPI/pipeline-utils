from dbt_pipeline_utils.scripts.helpers.data_processors import DatabaseBC
from pathlib import Path

class PostgresFileProcessor(DatabaseBC):
    def __init__(self, study_config, ftd_config, table_name, table_info, paths):
        super().__init__(study_config, ftd_config, table_name, table_info, paths)

        self.profile = self.pipeline_db
        self.src_schema = f'{self.study_id}_src_data'
        self.src_data_csv = self.identifier # Identifies the csv file containing data to be imported.        self.src_dd_file_name = table_info.get("src_file_id")
