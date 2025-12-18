from dataclasses import dataclass
from pathlib import Path
import dbt_pipeline_utils
from dbt_pipeline_utils import logger
from dbt_pipeline_utils.p_utils.general import write_file, get_existing_yaml
from .project_structure import StructureBC
from typing import Dict


@StructureBC.register("anvil")
@dataclass
class AnvilStructureSC(StructureBC):
    pass
    # study_id: str
    # project_id: str
    # int_model_id: str
    # tgt_model_id: str
    # db_profile: str
    # int_table_prefix: str
    # study_config_dir: Path | None = None
    # study_data_dir: Path | None = None
    # src_dbtp_def= Dict
    # int_dbtp_def= Dict

    # def __post_init__(self) -> None:
    #     if self.study_config_dir is not None:
    #         self.study_config_dir = Path(self.study_config_dir)

    #     if self.study_data_dir is not None:
    #         self.study_data_dir = Path(self.study_data_dir)

    #     self.int_table_prefix = f"{self.study_id}_{self.int_model_id}"

    def get_paths(self) -> dict[str, Path]:
        home_profiles = Path.home() / ".dbt/profiles.yml"

        utils_root_dir = Path(dbt_pipeline_utils.__file__).resolve().parent

        pl_root_dir = Path.cwd()
        pl_profiles = pl_root_dir / "profiles.yml"
        pl_project_dir = pl_root_dir / self.project_id
        pl_catalog_dir = pl_project_dir / "catalog"
        pl_scripts_dir = pl_project_dir / "scripts"

        pl_sources_dir = pl_catalog_dir / "sources"
        pl_src_study_dir = pl_sources_dir / self.study_id
        pl_src_study_model_dir = pl_src_study_dir / "models"
        pl_src_study_model_docs_dir = pl_src_study_model_dir / "docs"

        pl_data_dir = (
            self.study_config_dir
            if self.study_config_dir is not None
            else pl_root_dir / f"data/{self.study_id}"
        )

        study_data_dir = (
            self.study_data_dir
            if self.study_data_dir is not None
            else pl_data_dir
        )

        pl_int_dir = pl_catalog_dir / self.int_model_id
        pl_int_models_dir = pl_int_dir / Path("models")
        pl_int_study_dir = pl_int_models_dir / self.study_id
        pl_int_study_docs_dir = pl_int_study_dir / Path("docs")

        pl_tgt_dir = pl_catalog_dir / self.tgt_model_id
        pl_tgt_docs_dir = pl_tgt_dir / Path("docs")
        pl_tgt_models_dir = pl_tgt_dir / Path("models")
        pl_tgt_models_docs_dir = pl_tgt_models_dir / Path("docs")

        static_data_dir = pl_data_dir / Path("static")
        cdm_dir = static_data_dir / Path("common_data_models")
        static_internal_dir = cdm_dir / Path("internal")
        static_int_dir = static_internal_dir / Path(self.int_model_id)

        static_export_dir = cdm_dir / Path("export")
        static_tgt_dir = static_export_dir / Path(self.tgt_model_id)

        paths = {
            "pl_profiles": pl_profiles,
            "home_profiles": home_profiles,
            "utils_root_dir": utils_root_dir,
            "pl_root_dir": pl_root_dir,
            "pl_project_dir": pl_project_dir,
            "pl_catalog_dir": pl_catalog_dir,
            "pl_scripts_dir": pl_scripts_dir,
            "pl_sources_dir": pl_sources_dir,
            "pl_src_study_dir": pl_src_study_dir,
            "pl_src_study_model_dir": pl_src_study_model_dir,
            "pl_src_study_model_docs_dir": pl_src_study_model_docs_dir,
            "pl_data_dir": pl_data_dir,
            "src_data_dir": study_data_dir,
            "static_data_dir": static_data_dir,
            "pl_int_dir": pl_int_dir,
            "pl_int_models_dir": pl_int_models_dir,
            "pl_int_study_dir": pl_int_study_dir,
            "pl_int_study_docs_dir": pl_int_study_docs_dir,
            "pl_tgt_dir": pl_tgt_dir,
            "pl_tgt_docs_dir": pl_tgt_docs_dir,
            "pl_tgt_models_dir": pl_tgt_models_dir,
            "pl_tgt_models_docs_dir": pl_tgt_models_docs_dir,
            "static_int_dir": static_int_dir,
            "static_tgt_dir": static_tgt_dir,
        }

        for name, path in paths.items():
            if name.endswith("dir"):
                path.mkdir(parents=True, exist_ok=True)

        return paths

    def dbt_project_additions(
        self,
        filepath: Path,
        table_names: list,
        dbtp_def_each: dict,
        dbtp_def_all: dict,
    ) -> None:

        # Load existing YAML (empty dict if missing)
        existing = get_existing_yaml(filepath)

        models = existing.setdefault("models", {})
        study_models = models.setdefault(self.study_id, {})

        # Add top-level defaults only if dbtp_def_all has any non-None values
        top_defaults = {
            k: v for k, v in dbtp_def_all.items() if v is not None
        }
        if top_defaults:
            # Merge top-level defaults without overwriting existing tables
            for k, v in top_defaults.items():
                if k not in study_models:
                    study_models[k] = v

        # Add individual tables
        for table_id in table_names:
            # Skip if already exists
            if table_id in study_models:
                continue

            # Include dbtp_def_each only if values are not None
            table_defaults = {k: v for k, v in dbtp_def_each.items() if v is not None}
            study_models[table_id] = table_defaults

        # Write back
        write_file(filepath, {"models": {self.study_id: study_models}}, mode="merge")

    def generate_dbt_project_yaml(self, paths, int_config):
        # base yaml
        self.generate_base_dbt_project_yml(
            paths["pl_root_dir"], 'base', self.db_profile, 'create'
        )
        # catalog yaml
        self.generate_base_dbt_project_yml(
            paths["pl_catalog_dir"], "catalog", self.db_profile, "create"
        )
        # catalog/sources/{study} yaml
        self.generate_base_dbt_project_yml(
            paths["pl_src_study_dir"], self.study_id, self.db_profile, "create"
        )
        # ftd yaml
        self.generate_base_dbt_project_yml(
            paths["pl_int_dir"], self.int_model_id, self.db_profile, "create"
        )
        # tgt yaml
        self.generate_base_dbt_project_yml(
            paths["pl_tgt_dir"], self.tgt_model_id, self.db_profile, "create"
        )

        filepath = paths["pl_int_dir"] / "dbt_project.yml"
        self.dbt_project_additions(filepath, int_config.int_tables, self.int_dbtp_def, self.int_dbtp_all)

        filepath = paths["pl_src_study_dir"] / "dbt_project.yml"
        self.dbt_project_additions(
            filepath, self.study_tables, self.src_dbtp_def, self.src_dbtp_all
        )

    # def get_paths(
    #     self,
    #     study_id,
    #     project_id,
    #     # int_model_id,
    #     # tgt_model_id,
    #     study_config_dir=None,
    #     study_data_dir=None,
    # ):
    #     """
    #     For automatic validation of dir path creation, end the dir variables with "dir"
    #     """
    #     # dbt profiles path
    #     # Requirement - profiles.yml path
    #     home_profiles = Path.home() / ".dbt/profiles.yml"

    #     # dbt_pipeline_utils paths
    #     utils_root_dir = Path(dbt_pipeline_utils.__file__).resolve().parent

    #     # dbt project paths
    #     pl_root_dir = Path.cwd()
    #     pl_profiles = pl_root_dir / "profiles.yml"
    #     pl_project_dir = pl_root_dir / project_id
    #     pl_catalog_dir = pl_project_dir / Path("catalog")
    #     pl_scripts_dir = pl_project_dir / Path("scripts")

    #     pl_sources_dir = pl_catalog_dir / Path("sources")
    #     pl_src_study_dir = pl_sources_dir / study_id
    #     pl_src_study_model_dir = pl_src_study_dir / Path("models")
    #     pl_src_study_model_docs_dir = pl_src_study_model_dir / Path("docs")

    #     # pl_int_dir = pl_catalog_dir / Path(int_model_id)
    #     # pl_int_models_dir = pl_int_dir / Path("models")
    #     # pl_int_study_dir = pl_int_models_dir / study_id
    #     # pl_int_study_docs_dir = pl_int_study_dir / Path("docs")

    #     # pl_tgt_dir = pl_catalog_dir / Path(tgt_model_id)
    #     # tgt_docs_dir = pl_tgt_dir / Path("docs")
    #     # tgt_models_dir = pl_tgt_dir / Path("models")
    #     # tgt_models_docs_dir = tgt_models_dir / Path("docs")

    #     # src data filepaths.
    #     pl_data_dir = pl_root_dir / Path(f"data/{study_id}")
    #     study_data_dir = pl_root_dir / Path(f"data/{study_id}")
    #     if study_config_dir is not None:
    #         pl_data_dir = Path(f"{study_config_dir}")
    #     if study_data_dir is not None:
    #         study_data_dir = Path(f"{study_data_dir}")

    #     # int_study_data_dir = src_data_dir / Path("int_data_dictionaries")
    #     # trans_study_data_dir = src_data_dir / Path("int_transformations")

    #     # static data paths
    #     static_data_dir = pl_data_dir / Path("static")
    #     # int_static_data_dir = static_data_dir / Path("int_data_dictionaries")
    #     # tgt_static_data_dir = static_data_dir / Path(f"{tgt_model_id}")

    #     # int_study_yml_path = src_data_dir / "int_study.yaml"

    #     paths = {
    #         "pl_profiles": pl_profiles,
    #         "home_profiles": home_profiles,
    #         "utils_root_dir": utils_root_dir,
    #         "pl_root_dir": pl_root_dir,
    #         "pl_project_dir": pl_project_dir,
    #         "pl_catalog_dir": pl_catalog_dir,
    #         "pl_scripts_dir": pl_scripts_dir,
    #         "pl_sources_dir": pl_sources_dir,
    #         "pl_src_study_dir": pl_src_study_dir,
    #         "pl_src_study_model_dir": pl_src_study_model_dir,
    #         "pl_src_study_model_docs_dir": pl_src_study_model_docs_dir,
    #         # "pl_int_dir": pl_int_dir,
    #         # "pl_int_models_dir": pl_int_models_dir,
    #         # "pl_int_study_dir": pl_int_study_dir,
    #         # "pl_int_study_docs_dir": pl_int_study_docs_dir,
    #         # "pl_tgt_dir": pl_tgt_dir,
    #         # "tgt_docs_dir": tgt_docs_dir,
    #         # "tgt_models_dir": tgt_models_dir,
    #         # "tgt_models_docs_dir": tgt_models_docs_dir,
    #         "pl_data_dir": pl_data_dir,
    #         "src_data_dir": study_data_dir,
    #         # "int_study_data_dir": int_study_data_dir,
    #         # "trans_study_data_dir": trans_study_data_dir,
    #         "static_data_dir": static_data_dir,
    #         # "int_static_data_dir": int_static_data_dir,
    #         # "tgt_static_data_dir": tgt_static_data_dir,
    #         # "int_study_yml_path": int_study_yml_path,
    #     }

    #     for var, path in paths.items():
    #         if var.endswith("dir"):
    #             path.mkdir(parents=True, exist_ok=True)
    #             logger.debug(f"Path {path} exists")

    #         self.validate_paths(paths)

    #     return paths
