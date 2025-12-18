from dataclasses import dataclass, field
from typing import List, Dict, Optional
from pathlib import Path
from dbt_pipeline_utils.p_utils.general import read_file
from dbt_pipeline_utils.p_utils.configs import StudyConfig, InternalConfig
from dbt_pipeline_utils.p_utils.databases.databases import DatabaseBC
from dbt_pipeline_utils.p_utils.structures.project_structure import StructureBC


@dataclass
class PipelineObject:
    name: str

    # data files
    df_identifiers: List[str]
    df_import_type: str
    join_cols: Dict[str, str]

    # data dictionary
    dd_identifier: str
    dd_import_type: str
    dd_format: str

    # pipeline metadata
    study_id: str
    project_id: str
    db_profile: str
    project_structure: str
    tgt_model_id: str
    data_dir: Path
    pipeline_db: str
    int_model_id: str
    study_tables: list[str]

    # dbt_project defaults
    src_dbtp_def: dict[str, str]
    src_dbtp_all: dict[str, str]
    int_dbtp_def: dict[str, str]
    int_dbtp_all: dict[str, str]

    # derived runtime objects
    db: DatabaseBC = field(init=False)
    structure: StructureBC = field(init=False)
    paths: Dict[str, Path] = field(init=False)
    int_config: Optional["InternalConfig"] = field(
        init=False, default=None
    )

    def __post_init__(self) -> None:
        self.db = DatabaseBC.define_db(self.pipeline_db)
        self.structure = StructureBC.define_structure(
            self.project_structure,
            study_id=self.study_id,
            project_id=self.project_id,
            int_model_id=self.int_model_id,
            tgt_model_id=self.tgt_model_id,
            db_profile=self.db_profile,
            study_config_dir=self.data_dir,
            src_dbtp_def=self.src_dbtp_def,
            src_dbtp_all=self.src_dbtp_all,
            int_dbtp_def=self.int_dbtp_def,
            int_dbtp_all=self.int_dbtp_all,
            study_tables=self.study_tables,
        )
        self.paths = self.structure.get_paths()

    def load_internal_config(self) -> None:
        """
        Load a secondary YAML config once paths are initialized.
        """
        config_path = self.paths["static_int_dir"] / Path(f"{self.int_model_id}_study.yaml")
        if not config_path.exists():
            raise FileNotFoundError(f"Config not found: {config_path}")

        raw_config = read_file(config_path)
        self.int_config = InternalConfig.from_dict(raw_config, self.structure.int_table_prefix)


def build_pipeline_objects(
    study_config: StudyConfig,
) -> dict[str, PipelineObject]:

    pipeline_objects: dict[str, PipelineObject] = {}

    for table_name, df_cfg in study_config.data_file.items():
        dd_cfg = study_config.data_dictionary[table_name]

        po = PipelineObject(
            name=table_name,
            df_identifiers=df_cfg.identifiers,
            df_import_type=df_cfg.import_type,
            join_cols=df_cfg.join_cols,
            dd_identifier=dd_cfg.identifier,
            dd_import_type=dd_cfg.import_type,
            dd_format=dd_cfg.format,
            study_id=study_config.study_id,
            project_id=study_config.project_id,
            db_profile=study_config.db_profile,
            project_structure=study_config.project_structure,
            tgt_model_id=study_config.tgt_model_id,
            data_dir=study_config.data_dir,
            pipeline_db=study_config.pipeline_db,
            int_model_id=study_config.int_model_id,
            study_tables=study_config.study_tables,
            src_dbtp_def=study_config.dbt_proj_config["src"].dbt_dict(),
            src_dbtp_all=study_config.dbt_proj_config["src_all"].dbt_dict(),
            int_dbtp_def=study_config.dbt_proj_config["int"].dbt_dict(),
            int_dbtp_all=study_config.dbt_proj_config["int_all"].dbt_dict(),
        )

        pipeline_objects[table_name] = po

        any_po = next(iter(pipeline_objects.values()))
        any_po.load_internal_config()

        for po in pipeline_objects.values():
            po.int_config = any_po.int_config

    return pipeline_objects
