from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from pathlib import Path
from dbt_pipeline_utils.p_utils.general import read_file
from dbt_pipeline_utils.p_utils.configs import StudyConfig, InternalConfig, ExportConfig
from dbt_pipeline_utils.p_utils.databases.databases import DatabaseBC
from dbt_pipeline_utils.p_utils.structures.project_structure import StructureBC


@dataclass
class PipelineObject:
    study_config_path: Path
    name: str

    # pipeline metadata
    structure_key: str
    exp_model_name: str
    pipeline_data_dir: Path
    pipeline_db: str
    int_model_name: str
    study_tables: list[str]

    # study metadata
    study_id: str
    project_id: str
    db_profile: str
    study_data_dir: Path

    # dbt_project defaults
    src_dbtp_def: dict[str, Any]
    int_dbtp_def: dict[str, Any]
    exp_dbtp_def: dict[str, Any]


    # data files
    df_identifiers: List[str]
    df_import_type: str
    join_cols: Dict[str, str]

    # data dictionary
    dd_identifier: str
    dd_import_type: str
    dd_format: str

    # late-bound
    db: DatabaseBC = field(init=False)
    structure: StructureBC = field(init=False)
    paths: Dict[str, Path] = field(init=False)

    int_config: Optional[InternalConfig] = field(init=False, default=None)
    exp_config: Optional[ExportConfig] = field(init=False, default=None)

    def __post_init__(self) -> None:
        self.db = DatabaseBC.define_db(self.pipeline_db)

    def finalize(self) -> None:
        if self.int_config is None or self.exp_config is None:
            raise RuntimeError("Configs must be loaded before finalizing PipelineObject")

        self.structure = StructureBC.define_structure(
            self.structure_key,
            study_id=self.study_id,
            project_id=self.project_id,
            db_profile=self.db_profile,
            study_config_path=self.study_config_path,
            src_dbtp_def=self.src_dbtp_def,
            int_dbtp_def=self.int_dbtp_def,
            exp_dbtp_def=self.exp_dbtp_def,
            study_tables=self.study_tables,
            int_model_prefix=self.int_config.model_prefix,
            int_model_name=self.int_config.model_name,
            exp_model_prefix=self.exp_config.model_prefix,
            exp_model_name=self.exp_config.model_name,
        )

        self.paths = self.structure.get_paths()

    def load_internal_config(self) -> None:
        """
        Load a secondary YAML config once paths are initialized.
        """
        config_path = Path.cwd() / f"data/static/common_data_models/internal/metadata/{self.int_model_name}/{self.int_model_name}_study.yaml"
        if not config_path.exists():
            raise FileNotFoundError(f"Config not found: {config_path}")

        raw_config = read_file(config_path)
        self.int_config = InternalConfig.from_dict(raw_config)

    def load_export_config(self) -> None:
        """
        Load a secondary YAML config once paths are initialized.
        """
        config_path = (
            Path.cwd()
            / f"data/static/common_data_models/export/metadata/{self.exp_model_name}/{self.exp_model_name}_study.yaml"
        )

        if not config_path.exists():
            raise FileNotFoundError(f"Config not found: {config_path}")

        raw_config = read_file(config_path)
        self.exp_config = ExportConfig.from_dict(
            raw_config
        )


def build_pipeline_objects(
    study_config: StudyConfig, study_config_path: Path
) -> dict[str, PipelineObject]:

    pipeline_objects: dict[str, PipelineObject] = {}

    # Phase 1: build objects
    for table_name, df_cfg in study_config.data_file.items():
        dd_cfg = study_config.data_dictionary[table_name]

        pipeline_objects[table_name] = PipelineObject(
            study_config_path=study_config_path,
            name=table_name,
            df_identifiers=df_cfg.identifiers,
            df_import_type=df_cfg.import_type,
            join_cols=df_cfg.join_cols,
            dd_identifier=dd_cfg.identifier,
            dd_import_type=dd_cfg.import_type,
            dd_format=dd_cfg.format,
            study_id=study_config.study.study_id,
            project_id=study_config.study.project_id,
            db_profile=study_config.study.db_profile,
            structure_key=study_config.pipeline.structure,
            exp_model_name=study_config.pipeline.exp_model_name,
            pipeline_data_dir=Path(study_config.pipeline.pipeline_data_dir),
            pipeline_db=study_config.pipeline.db,
            study_data_dir=Path(study_config.study.study_data_dir),
            int_model_name=study_config.pipeline.int_model_name,
            study_tables=study_config.study_tables,
            src_dbtp_def=study_config.dbt_proj_config["src"].dbt_dict(),
            int_dbtp_def=study_config.dbt_proj_config["int"].dbt_dict(),
            exp_dbtp_def=study_config.dbt_proj_config["exp"].dbt_dict(),
        )

    # Phase 2: load shared configs ONCE
    any_po = next(iter(pipeline_objects.values()))

    any_po.load_internal_config()
    any_po.load_export_config()

    # Phase 3: inject + finalize
    for po in pipeline_objects.values():
        po.int_config = any_po.int_config
        po.exp_config = any_po.exp_config
        po.finalize()

    return pipeline_objects
