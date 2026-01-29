from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from pathlib import Path
from dbt_pipeline_utils.p_utils.general import read_file
from dbt_pipeline_utils.p_utils.configs import StudyConfig, InternalConfig, ExportConfig

from dbt_pipeline_utils.p_utils.structures.project_structure import StructureBC
from dbt_pipeline_utils.p_utils.structures.structure_context import StructureContext

@dataclass
class PipelineObject:
    study_config_path: Path
    table_name: str

    # pipeline metadata
    structure_key: str
    database_key: str
    exp_model_name: str
    pipeline_data_dir: Path
    pipeline_db: str
    int_model_name: str
    src_model_type: str
    stb_model_type: str
    int_model_type: str
    exp_model_type: str
    study_tables: list[str]
    src_tables: list[str]

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
    dataset_id: str

    # data dictionary
    dd_identifier: str
    dd_import_type: str
    dd_format: str

    # late-bound
    structure: StructureBC = field(init=False)
    paths: Dict[str, Path] = field(init=False)

    int_config: Optional[InternalConfig] = field(init=False, default=None)
    exp_config: Optional[ExportConfig] = field(init=False, default=None)

    # def __post_init__(self) -> None:
    #     self.db = DatabaseBC.define_db(self.pipeline_db)

    def finalize(self) -> None:
        """
        Finalize the PipelineObject by building the StructureContext,
        defining the structure, and computing paths.
        """
        if not self.int_config or not self.exp_config:
            raise RuntimeError(
                "Internal and Export configs must be loaded before finalize()"
            )

        s_context = StructureContext(
            table_name=self.table_name,
            df_identifiers=self.df_identifiers,
            dd_identifier=self.dd_identifier,
            dd_format=self.dd_format,
            study_id=self.study_id,
            project_id=self.project_id,
            db_profile=self.db_profile,
            dataset_id=self.dataset_id,
            int_model_name=self.int_config.model_name,
            int_model_prefix=self.int_config.model_prefix,
            int_tables=self.int_config.int_tables,
            int_format=self.int_config.int_format,
            exp_tables=self.exp_config.exp_tables,
            exp_model_name=self.exp_config.model_name,
            exp_model_prefix=self.exp_config.model_prefix,
            exp_format=self.exp_config.exp_format,
            study_tables=self.study_tables,
            src_model_type=self.src_model_type,
            stb_model_type=self.stb_model_type,
            int_model_type=self.int_model_type,
            exp_model_type=self.exp_model_type,
            src_tables=self.src_tables,
            study_config_path=self.study_config_path,
            study_data_dir=self.study_data_dir,
            pipeline_data_dir=self.pipeline_data_dir,
            src_dbtp_def=self.src_dbtp_def,
            int_dbtp_def=self.int_dbtp_def,
            exp_dbtp_def=self.exp_dbtp_def,
        )

        self.structure = StructureBC.define_structure(
            self.structure_key,
            context=s_context,
        )

        self.paths = self.structure.get_paths()

    def load_internal_config(self, static_dir: Path) -> None:
        """
        Load a secondary YAML config once paths are initialized.
        """
        config_path = (
            static_dir
            / f"static/common_data_models/internal/metadata/{self.int_model_name}/_{self.int_model_name}_study.yaml"
        )
        if not config_path.exists():
            raise FileNotFoundError(f"Config not found: {config_path}")

        raw_config = read_file(config_path)
        self.int_config = InternalConfig.from_dict(raw_config)

    def load_export_config(self, static_dir: Path) -> None:
        """
        Load a secondary YAML config once paths are initialized.
        """
        config_path = (
            static_dir
            / f"static/common_data_models/export/metadata/{self.exp_model_name}/_{self.exp_model_name}_study.yaml"
        )

        if not config_path.exists():
            raise FileNotFoundError(f"Config not found: {config_path}")

        raw_config = read_file(config_path)
        self.exp_config = ExportConfig.from_dict(raw_config)


def build_pipeline_objects( static_dir: Path,
    study_config: StudyConfig, study_config_path: Path
) -> dict[str, PipelineObject]:

    pipeline_objects: dict[str, PipelineObject] = {}

    for t_name, df_cfg in study_config.data_file.items():
        dd_cfg = study_config.data_dictionary[t_name]

        pipeline_objects[t_name] = PipelineObject(
            study_config_path=study_config_path,
            table_name=t_name,
            df_identifiers=df_cfg.identifiers,
            df_import_type=df_cfg.import_type,
            dataset_id=df_cfg.dataset_id,
            dd_identifier=dd_cfg.identifier,
            dd_import_type=dd_cfg.import_type,
            dd_format=dd_cfg.format,
            study_id=study_config.study.study_id,
            project_id=study_config.study.project_id,
            db_profile=study_config.study.db_profile,
            structure_key=study_config.pipeline.structure,
            database_key=study_config.pipeline.db,
            exp_model_name=study_config.pipeline.exp_model_name,
            pipeline_data_dir=Path(study_config.pipeline.pipeline_data_dir),
            pipeline_db=study_config.pipeline.db,
            study_data_dir=Path(study_config.study.study_data_dir),
            int_model_name=study_config.pipeline.int_model_name,
            src_model_type=study_config.pipeline.src_model_type,
            stb_model_type=study_config.pipeline.stb_model_type,
            int_model_type=study_config.pipeline.int_model_type,
            exp_model_type=study_config.pipeline.exp_model_type,
            study_tables=study_config.study_tables,
            src_tables=study_config.src_tables,
            src_dbtp_def=study_config.dbt_proj_config["src"].dbt_dict(),
            int_dbtp_def=study_config.dbt_proj_config["int"].dbt_dict(),
            exp_dbtp_def=study_config.dbt_proj_config["exp"].dbt_dict(),
        )

    any_po = next(iter(pipeline_objects.values()))

    any_po.load_internal_config(static_dir)
    any_po.load_export_config(static_dir)

    for po in pipeline_objects.values():
        po.int_config = any_po.int_config
        po.exp_config = any_po.exp_config
        po.finalize()

    return pipeline_objects
