from dataclasses import dataclass

@dataclass
class PipelineObject():
    file: str
    def __init__(
        self,
        study_config,
        int_config,
        file: str,
        file_type,
        table_id,
        attributes,
        data_dir,
        tgt_model,
    ):
        self.study_id = study_config["study_id"]
        self.project_id = study_config["project_id"]
        self.db_profile = study_config["db_profile"]
        self.pipeline_db = study_config["pipeline_db"]
        self.structure_type = study_config["project_structure"]
        self.data_dictionaries = study_config["data_dictionary"]
        self.data_files = study_config["data_file"]
        self.int_config = int_config
        self.tgt_model = tgt_model
        self.file = file
        self.file_type = file_type
        self.table_id = table_id
        self.attributes = attributes

        # Set the Pipeline Structure
        if self.structure_type == "single":
            self.project_structure = SingleStructureClass(
                self.structure_type,
                self.study_id,
                self.project_id,
                self.tgt_model,
                data_dir,
                file,
            )
        elif self.structure_type == "multiple":
            self.project_structure = MultipleStructureClass(
                self.structure_type,
                self.study_id,
                self.project_id,
                self.tgt_model,
                data_dir,
                self.table_id,
                file,
            )
        else:
            raise ValueError(f"Unknown structure type: {self.structure_type}")

        # Set the Database. Handles sql creation and importing data.
        # if self.pipeline_db == "duckdb":
        #     self.db = DuckdbClass(self.pipeline_db)
        # elif self.pipeline_db == "pg":
        #     self.db = PostgresqlClass(self.pipeline_db)
        # else:
        #     raise ValueError(f"Unknown structure type: {self.structure_type}")

        self.db = DatabaseBC.connect_db(pipeline_db)

    def process(self):
        return {
            "study_id": self.study_id,
            "project_id": self.project_id,
            "file_type": self.file_type,
            "structure_description": self.project_structure.describe_structure(),
            "paths_exist": self.project_structure.paths,
        }


def create_pipeline_objects(study_config, int_config, data_dir, tgt_model):
    '''
    For each study_config identifier(dd or df), Create a PipelineObject that 
    will dynamically set attributes and methods required by a specific pipeline's
    needs(defined in the study_config). 

    '''

    pipeline_objects = []

    for file_type, files in [
        ("dd", study_config["data_dictionary"]),
        ("df", study_config["data_file"]),
    ]:
        for table_id, attributes in files.items():
            for file in attributes["identifier"]:
                pipeline_obj = PipelineObject(
                    study_config,
                    int_config,
                    file,
                    file_type,
                    table_id,
                    attributes,
                    data_dir,
                    tgt_model,
                )
                pipeline_objects.append(pipeline_obj)

    return pipeline_objects
