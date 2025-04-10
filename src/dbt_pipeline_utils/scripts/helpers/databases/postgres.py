from dbt_pipeline_utils.scripts.helpers.data_processors import DatabaseBC
from pathlib import Path

class PostgresFileProcessor(DatabaseBC):
    def __init__(self, study_details, file_details, paths):
        super().__init__(study_details, file_details, paths)

        self.profile = self.pipeline_db
        self.src_schema = f'{self.study_id}_src_data'
        self.src_data_csv = self.identifier # Identifies the csv file containing data to be imported.