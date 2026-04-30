from dataclasses import dataclass, field
from typing import List, Dict, Optional
from pathlib import Path
from dbt_pipeline_utils.p_utils.files import read_file
from dbt_pipeline_utils.p_utils.configs import StudyConfig, InternalConfig, ExportConfig

from dbt_pipeline_utils.p_utils.structures.project_structure import StructureBC
from dbt_pipeline_utils.p_utils.structures.structure_context import StructureContext

@dataclass
class PipelineObject:
    study_config_path: Path
    table_name: str

    # pipeline metadata
    project_config: str
    database_key: str
    exp_model_name: str
    pipeline_data_dir: Path
    pipeline_db: str
    pipeline_structure: str
    int_model_name: str
    combined_model_prefix: str
    src_model_type: str
    stb_model_type: str
    int_model_type: str
    exp_model_type: str
    study_tables: list[str]
    src_tables: list[str]

    # study metadata
    study_id: str
    project_id: str
    project_name: str
    db_profile: str
    study_data_dir: Path
    dag_id: Optional[str]

    # data files
    df_identifiers: List[str]
    dataset_id: str

    # data dictionary
    dd_identifier: str
    dd_format: str

    # late-bound
    structure: StructureBC = field(init=False)
    paths: Dict[str, Path] = field(init=False)

    int_config: Optional[InternalConfig] = field(init=False, default=None)
    exp_config: Optional[ExportConfig] = field(init=False, default=None)

    def _resolve_secondary_config_path(
        self,
        base_dir: Path,
        stage: str,
        model_name: str,
    ) -> Path:
        """Resolve metadata config path across known static-data layouts."""
        relative = Path(
            f"common_data_models/{stage}/metadata/{model_name}/_{model_name}_study.yaml"
        )

        candidates: list[Path] = [
            base_dir / "static" / relative,
            base_dir / relative,
            self.study_data_dir / "static" / relative,
            self.study_data_dir / "temp" / "static" / relative,
            self.study_data_dir.parent / "temp" / "static" / relative,
        ]

        checked: list[Path] = []
        for candidate in candidates:
            resolved = candidate.resolve()
            if resolved in checked:
                continue
            checked.append(resolved)
            if resolved.exists():
                return resolved

        checked_paths = "\n".join(f" - {path}" for path in checked)
        raise FileNotFoundError("Config not found. Checked paths:\n" f"{checked_paths}")

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
            project_name=self.project_name,
            pipeline_structure=self.pipeline_structure,
            db_profile=self.db_profile,
            dataset_id=self.dataset_id,
            int_model_name=self.int_config.model_name,
            int_model_prefix=self.int_config.model_prefix,
            combined_model_prefix=self.combined_model_prefix,
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
            dag_id=self.dag_id,
            pipeline_data_dir=self.pipeline_data_dir,
        )

        self.structure = StructureBC(
            context=s_context,
            proj_config_path=self.project_config,
            study_config_path=self.study_config_path,
        )

        self.paths = self.structure.get_paths()

        # Ensure DAG stage always has a resolved output directory path.
        if "dag_dir" not in self.paths:
            self.paths["dag_dir"] = Path(
                self.paths.get("pl_dag_dir", Path(self.paths["pl_dir"]) / "dags")
            )
        self.paths["dag_dir"].mkdir(parents=True, exist_ok=True)

    def load_internal_config(self, static_dir: Path) -> None:
        """
        Load a secondary YAML config once paths are initialized.
        """
        config_path = self._resolve_secondary_config_path(
            base_dir=static_dir,
            stage="internal",
            model_name=self.int_model_name,
        )

        raw_config = read_file(config_path)
        self.int_config = InternalConfig.from_dict(raw_config)

    def load_export_config(self, static_dir: Path) -> None:
        """
        Load a secondary YAML config once paths are initialized.
        """
        config_path = self._resolve_secondary_config_path(
            base_dir=static_dir,
            stage="export",
            model_name=self.exp_model_name,
        )

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
            dataset_id=df_cfg.dataset_id,
            dd_identifier=dd_cfg.identifier,
            dd_format=dd_cfg.format,
            study_id=study_config.study.study_id,
            project_id=study_config.study.project_id,
            project_name=study_config.study.project_name,
            db_profile=study_config.study.db_profile,
            project_config=study_config.study.project_config,
            database_key=study_config.study.db,
            exp_model_name=study_config.study.exp_model_name,
            pipeline_data_dir=Path(study_config.study.pipeline_data_dir),
            pipeline_db=study_config.study.db,
            pipeline_structure=study_config.study.structure,
            study_data_dir=Path(study_config.study.study_data_dir),
            int_model_name=study_config.study.int_model_name,
            combined_model_prefix=study_config.study.combined_model_prefix,
            src_model_type=study_config.study.src_model_type,
            stb_model_type=study_config.study.stb_model_type,
            int_model_type=study_config.study.int_model_type,
            exp_model_type=study_config.study.exp_model_type,
            study_tables=study_config.study_tables,
            src_tables=study_config.src_tables,
            dag_id=study_config.study.dag_id,
        )

    any_po = next(iter(pipeline_objects.values()))

    try:

        any_po.load_internal_config(static_dir) 
    except:
        import pdb
        pdb.set_trace()
    any_po.load_export_config(static_dir)

    for po in pipeline_objects.values():
        po.int_config = any_po.int_config
        po.exp_config = any_po.exp_config
        po.finalize()

    return pipeline_objects
