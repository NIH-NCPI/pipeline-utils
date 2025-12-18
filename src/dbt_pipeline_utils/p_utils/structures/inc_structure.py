from pathlib import Path


import dbt_pipeline_utils
from dbt_pipeline_utils import logger
from dbt_pipeline_utils.p_utils.structures.project_structure import (
    StructureBC,
)
@StructureBC.register("inc")
class IncStructureSC(StructureBC):

    def __init__(self, structure_type, study_id, project_id, tgt_model, data_dir):
        super().__init__(structure_type, study_id, project_id, tgt_model, data_dir)
        self.paths = self.get_paths(
            self.study_id, self.project_id, self.tgt_model, self.data_dir
        )
        # self.stg_table_id= f"{self.study_id}_stg_{table_id}"

    def __repr__(self):
        return f"SingleProjectClass(paths={self.paths is not None})"

    def get_paths(self):
        pass
