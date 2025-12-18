from dataclasses import dataclass, field
from typing import Type, Dict, ClassVar
from pathlib import Path
from dbt_pipeline_utils import logger
from dbt_pipeline_utils.p_utils.general import write_file

from typing import Dict, Type, ClassVar

@dataclass
class StructureBC:
    study_id: str
    project_id: str
    int_model_id: str
    tgt_model_id: str
    db_profile: str
    study_tables: list[str]

    # derived at runtime
    int_table_prefix: str = field(init=False)

    # optional paths
    study_config_dir: Path | None = None
    study_data_dir: Path | None = None

    # dbt defaults
    src_dbtp_def: Dict = field(default_factory=dict)
    src_dbtp_all: Dict = field(default_factory=dict)
    int_dbtp_def: Dict = field(default_factory=dict)
    int_dbtp_all: Dict = field(default_factory=dict)

    def __post_init__(self):
        if self.study_config_dir is not None:
            self.study_config_dir = Path(self.study_config_dir)

        if self.study_data_dir is not None:
            self.study_data_dir = Path(self.study_data_dir)

        self.int_table_prefix = f"{self.study_id}_{self.int_model_id}"

    # Registry for subclasses
    _REGISTRY: ClassVar[Dict[str, Type["StructureBC"]]] = {}

    @classmethod
    def register(cls, key: str):
        def decorator(subclass: Type["StructureBC"]):
            cls._REGISTRY[key] = subclass
            return subclass

        return decorator

    @classmethod
    def define_structure(
        cls,
        key: str,
        *,
        study_id: str,
        project_id: str,
        int_model_id: str,
        tgt_model_id: str,
        db_profile: str,
        study_config_dir=None,
        src_dbtp_def: dict[str,str] = None,
        src_dbtp_all: dict[str,str] = None,
        int_dbtp_def: dict[str,str] = None,
        int_dbtp_all: dict[str,str] = None,
        study_tables: list[str],
    ) -> "StructureBC":

        try:
            return cls._REGISTRY[key](
                study_id=study_id,
                project_id=project_id,
                int_model_id=int_model_id,
                tgt_model_id=tgt_model_id,
                db_profile=db_profile,
                study_config_dir=study_config_dir,
                src_dbtp_def = src_dbtp_def or {},
                src_dbtp_all = src_dbtp_all or {},
                int_dbtp_def = int_dbtp_def or {},
                int_dbtp_all = int_dbtp_all or {},
                study_tables=study_tables,
            )
        except KeyError:
            raise ValueError(f"Unsupported project_structure: {key}")

    def validate_paths(self, paths_dict):
        logger.debug("Starting dir path validation")
        for key, path in paths_dict.items():
            if not path.exists():
                logger.warning(f"Warning: {key} does not exist - {path}")
                if key == "int_study_yml_path":
                    logger.info(
                        f"Ignore warning for int_study_yml_path on the first generation run."
                    )
        logger.debug("SUCCESS: End dir path validation")

    def generate_base_dbt_project_yml(self, filepath, name, default_profile, mode):
        if not isinstance(filepath, Path):
            filepath = Path(filepath)
        # Correct structure for dbt_project.yml
        data = {
            "name": name,
            "version": "1.0.0",
            "profile": default_profile,
            "model-paths": ["models"],
            "macro-paths": ["macros"],
            "seed-paths": ["seeds"],
            "snapshot-paths": ["snapshots"],
            "clean-targets": ["target", "dbt_packages"],
        }

        filepath = filepath / "dbt_project.yml"

        write_file(filepath, data, mode=mode)
