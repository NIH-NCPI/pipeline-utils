from dataclasses import dataclass
from pathlib import Path
import dbt_pipeline_utils
from dbt_pipeline_utils import logger
from dbt_pipeline_utils.p_utils.general import write_file, get_existing_yaml
from .project_structure import StructureBC
from typing import Dict


@StructureBC.register("anvil")
class AnvilStructureSC(StructureBC):
    pass

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
            self.pipeline_data_dir
            if self.pipeline_data_dir is not None
            else pl_root_dir / f"data/{self.study_id}"
        )

        study_data_dir = (
            self.study_data_dir
            if self.study_data_dir is not None
            else pl_root_dir / f"data/{self.study_id}"
        )

        pl_int_dir = pl_catalog_dir / self.int_model_name
        pl_int_models_dir = pl_int_dir / Path("models")
        pl_int_study_dir = pl_int_models_dir / self.study_id
        pl_int_study_docs_dir = pl_int_study_dir / Path("docs")

        pl_exp_dir = pl_catalog_dir / self.exp_model_name
        pl_exp_docs_dir = pl_exp_dir / Path("docs")
        pl_exp_models_dir = pl_exp_dir / Path("models")
        pl_exp_models_docs_dir = pl_exp_models_dir / Path("docs")

        static_data_dir = pl_root_dir / Path("data/static")
        cdm_dir = static_data_dir / Path("common_data_models")
        static_internal_dir = cdm_dir / Path("internal")
        static_int_metadata_dir = static_internal_dir / Path(f"metadata/{self.int_model_name}")
        static_int_additions_dir = static_int_metadata_dir / Path("additions")

        static_export_dir = cdm_dir / Path("export")
        static_exp_metadata_dir = static_export_dir / Path(
            f"metadata/{self.exp_model_name}"
        )
        static_exp_model_dir = static_export_dir / self.exp_model_name

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
            "study_data_dir": study_data_dir,
            "static_data_dir": static_data_dir,
            "pl_int_dir": pl_int_dir,
            "pl_int_models_dir": pl_int_models_dir,
            "pl_int_study_dir": pl_int_study_dir,
            "pl_int_study_docs_dir": pl_int_study_docs_dir,
            "pl_exp_dir": pl_exp_dir,
            "pl_exp_docs_dir": pl_exp_docs_dir,
            "pl_exp_models_dir": pl_exp_models_dir,
            "pl_exp_models_docs_dir": pl_exp_models_docs_dir,
            "static_int_metadata_dir": static_int_metadata_dir,
            "static_int_additions_dir": static_int_additions_dir,
            "static_exp_metadata_dir": static_exp_metadata_dir,
            "static_exp_model_dir": static_exp_model_dir,
        }

        for name, path in paths.items():
            if name.endswith("dir"):
                path.mkdir(parents=True, exist_ok=True)

        return paths

    def generate_dbt_project_yaml(self, int_config, exp_config):

        if int_config is None or exp_config is None:
            raise RuntimeError(
                "Internal and export configs must be present to generate dbt_project files"
            )

        src_dir = self.paths["pl_src_study_dir"]
        int_dir = self.paths["pl_int_dir"]
        exp_dir = self.paths["static_exp_model_dir"]

        # generate all of the dbt_project files if they don't exist.
        self.generate_base_dbt_project_yml(
            self.paths["pl_root_dir"], 'base', self.db_profile, 'create'
        )
        self.generate_base_dbt_project_yml(
            self.paths["pl_catalog_dir"], "catalog", self.db_profile, "create"
        )
        self.generate_base_dbt_project_yml(
           src_dir , self.study_id, self.db_profile, "create"
        )
        self.generate_base_dbt_project_yml(
           int_dir , self.int_model_name, self.db_profile, "create"
        )
        self.generate_base_dbt_project_yml(
           exp_dir , self.exp_model_name, self.db_profile, "create"
        )

        # Add new vars if they don't already exist.
        self.dbt_project_add_vars(
            src_dir, self.src_dbtp_def
        )
        self.dbt_project_add_vars(
            int_dir, self.int_dbtp_def
        )
        self.dbt_project_add_vars(
            exp_dir, self.exp_dbtp_def
        )

        # Add new models if they don't already exist
        self.dbt_project_add_models(
            src_dir, self.study_tables, self.src_dbtp_def
        )
        self.dbt_project_add_models(
            int_dir, int_config.int_tables, self.int_dbtp_def
            )
        self.dbt_project_add_models(
            exp_dir, exp_config.exp_tables, self.exp_dbtp_def
        )

    def generate_stg_dds(self):
        input_dd_path = self.paths["study_data_dir"] / self.dd_identifier

        output_path = (
            self.paths["study_data_dir"] / self.study_id / Path(self.int_gen_dd_name)
        )
        input_dd_format = self.dd_format
        additions_filepath = (
            input_dd_path
            / self.paths["static_int_additions_dir"]
            / self.int_stg_additions_name
        )

        self.generate_dds(input_dd_path, output_path, input_dd_format, additions_filepath)

    def generate_models_yml_files(self, int_config, exp_config):

        self.generate_models_yml(
            config=int_config,
            table_prefix = self.int_table_prefix,
            input_dd_dir=self.paths["static_int_metadata_dir"],
            output_dir=self.paths["pl_int_study_docs_dir"],
        )

        self.generate_models_yml(
            config=exp_config,
            table_prefix=self.exp_table_prefix,
            input_dd_dir=self.paths["static_exp_metadata_dir"],
            output_dir=self.paths["static_exp_model_dir"],
        )

    def generate_sources_yml_files(self):

        self.generate_sources_yml(
            input_dd_dir=self.paths["study_data_dir"],
            output_dir=self.paths["pl_src_study_model_dir"],
        )

    def generate_study_docs_files(self):

        # src tables - used in sources.yml
        self.generate_study_column_descriptions(
            table_prefix=f"{self.src_table_prefix}",
            input_dd_dir=self.paths["study_data_dir"],
            output_dir=self.paths["pl_src_study_model_docs_dir"],
        )

    def generate_static_docs_files(self, int_config, exp_config):
        '''
        For each dd specified in a config file, create the dbt 'docs' associated
        with the models.yml files.

        '''
    
        # int tables - used in models.yml


        self.generate_static_column_descriptions(
            table_prefix=f"{self.int_table_prefix}",
            config=int_config,
            input_dd_dir=self.paths["static_int_metadata_dir"],
            output_dir=self.paths["pl_int_study_docs_dir"],
        )
