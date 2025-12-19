from dataclasses import dataclass, field
from typing import Type, Dict, ClassVar
from pathlib import Path
from dbt_pipeline_utils import logger
from dbt_pipeline_utils.p_utils.general import write_file, get_existing_yaml

from typing import Dict, Type, ClassVar

@dataclass
class StructureBC:
    study_id: str
    project_id: str
    db_profile: str
    study_tables: list[str]
    int_model_prefix: str
    int_model_name: str
    exp_model_prefix: str
    exp_model_name: str

    # derived at runtime
    int_table_prefix: str = field(init=False)

    # optional paths
    study_config_path: Path | None = None
    study_data_dir: Path | None = None

    # dbt defaults
    src_dbtp_def: Dict = field(default_factory=dict)
    src_dbtp_all: Dict = field(default_factory=dict)
    int_dbtp_def: Dict = field(default_factory=dict)
    int_dbtp_all: Dict = field(default_factory=dict)
    exp_dbtp_def: Dict = field(default_factory=dict)
    exp_dbtp_all: Dict = field(default_factory=dict)

    def __post_init__(self):
        if self.study_config_path is not None:
            self.study_config_dir = Path(self.study_config_path).parent

        if self.study_data_dir is not None:
            self.study_data_dir = Path(self.study_data_dir)

        self.int_table_prefix = f"{self.study_id}_{self.int_model_prefix}"

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
        db_profile: str,
        study_config_path=None,
        src_dbtp_def: dict[str, str] = None,
        src_dbtp_all: dict[str, str] = None,
        int_dbtp_def: dict[str, str] = None,
        int_dbtp_all: dict[str, str] = None,
        exp_dbtp_def: dict[str, str] = None,
        exp_dbtp_all: dict[str, str] = None,
        study_tables: list[str],
        int_model_prefix: str,
        int_model_name: str,
        exp_model_prefix: str,
        exp_model_name: str,
    ) -> "StructureBC":

        try:
            return cls._REGISTRY[key](
                study_id=study_id,
                project_id=project_id,
                db_profile=db_profile,
                study_config_path=study_config_path,
                src_dbtp_def=src_dbtp_def or {},
                src_dbtp_all=src_dbtp_all or {},
                int_dbtp_def=int_dbtp_def or {},
                int_dbtp_all=int_dbtp_all or {},
                exp_dbtp_def=exp_dbtp_def or {},
                exp_dbtp_all=exp_dbtp_all or {},
                study_tables=study_tables,
                int_model_prefix=int_model_prefix,
                int_model_name=int_model_name,
                exp_model_prefix=exp_model_prefix,
                exp_model_name=exp_model_name,
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

    def dbt_project_add_models(
        self,
        filepath: Path,
        table_names: list,
        dbtp_def_each: dict,
        dbtp_def_all: dict,
    ) -> None:

        # Load existing YAML (empty dict if missing)
        filepath = filepath / "dbt_project.yml"
        existing = get_existing_yaml(filepath)

        models = existing.setdefault("models", {})
        study_models = models.setdefault(self.study_id, {})

        # Add top-level defaults only if dbtp_def_all has any non-None values
        top_defaults = {k: v for k, v in dbtp_def_all.items() if v is not None}
        if top_defaults:
            # Merge top-level defaults without overwriting existing tables
            for k, v in top_defaults.items():
                if k not in study_models:
                    study_models[k] = v

        # Add individual tables
        for table_id in table_names:
            # Skip if already exists
            if table_id not in study_models:
                study_models[table_id] = {}

            table_defaults = {k: v for k, v in dbtp_def_each.items() if v is not None}
            study_models[table_id].update(table_defaults)

    def dbt_project_add_vars(self):
        pass
