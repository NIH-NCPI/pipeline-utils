from pathlib import Path
import subprocess

import dbt_pipeline_utils
from dbt_pipeline_utils import logger
from dbt_pipeline_utils.p_utils.structures.project_structure import (
    StructureBC,
)
from dbt_pipeline_utils.p_utils.sql_model_generator import SqlModelGenerator
from dbt_pipeline_utils.p_utils.raw_data_importer import RawDataImporter

from dbt_pipeline_utils.p_utils.general import (
    read_file,
    write_file,
    get_existing_yaml,
    normalize_name,
    shorten_identifier,
    copy_directory,
    copy_file
)

@StructureBC.register("inc")
class IncStructureSC(StructureBC):
    pass

    def get_paths(self) -> dict[str, Path]:
        home_profiles = Path.home() / ".dbt/profiles.yml"

        utils_root_dir = Path(dbt_pipeline_utils.__file__).resolve().parent
        p_utils_dir = utils_root_dir / "p_utils"

        utils_macros_dir = p_utils_dir / 'macros'
        utils_data_dir = utils_root_dir / '../../data'
        utils_files_dir = utils_data_dir / 'static/files'

        pl_project_dir = Path.cwd()
        pl_root_dir = pl_project_dir.parent

        pl_profiles = pl_project_dir / "profiles.yml"

        pl_commands_dir = pl_project_dir / "run_commands" / self.project_id / self.study_id

        pl_macros_dir = pl_project_dir / "macros"
        pl_models_dir = pl_project_dir / "models"
        pl_models_proj_dir = pl_models_dir / self.project_id
        pl_proj_study_dir = pl_models_proj_dir / self.study_id
        pl_src_study_model_docs_dir = pl_proj_study_dir / "docs"

        pl_data_dir = (
            self.pipeline_data_dir
            if self.pipeline_data_dir is not None
            else pl_project_dir / f"../data/{self.study_id}"
        )

        study_data_dir = (
            self.study_data_dir
            if self.study_data_dir is not None
            else pl_project_dir / f"../data/{self.study_id}"
        )

        pl_int_dir = pl_models_dir / "access"
        pl_sp_int_dir = pl_int_dir / self.int_model_name
        pl_int_models_dir = pl_sp_int_dir / "models"
        pl_int_docs_dir = pl_int_dir / "docs"

        pl_exp_dir = pl_models_dir / "export"
        pl_sp_exp_dir = pl_exp_dir / self.exp_model_name

        static_data_dir = pl_data_dir / "static"
        cdm_dir = static_data_dir / "common_data_models"

        static_internal_dir = cdm_dir / "internal"
        static_int_metadata_dir = static_internal_dir / f"metadata/{self.int_model_name}"
        static_int_additions_dir = static_int_metadata_dir / "additions"

        static_export_dir = cdm_dir / "export"
        static_exp_metadata_dir = static_export_dir / f"metadata/{self.exp_model_name}"
        static_sp_exp_dir = static_export_dir / self.exp_model_name
        static_exp_model_dir = static_sp_exp_dir / "models"

        paths = {
            "pl_profiles": pl_profiles,
            "home_profiles": home_profiles,
            "utils_macros_dir": utils_macros_dir,
            "utils_files_dir": utils_files_dir,
            "pl_root_dir": pl_root_dir,
            "pl_project_dir": pl_project_dir,
            "pl_commands_dir": pl_commands_dir,
            "pl_macros_dir": pl_macros_dir,
            "pl_models_dir": pl_models_dir,
            "pl_models_proj_dir": pl_models_proj_dir,
            "pl_proj_study_dir": pl_proj_study_dir,
            "pl_src_study_model_docs_dir": pl_src_study_model_docs_dir,
            "pl_data_dir": pl_data_dir,
            "study_data_dir": study_data_dir,
            "pl_int_dir": pl_int_dir,
            "pl_sp_int_dir": pl_sp_int_dir,
            "pl_int_models_dir": pl_int_models_dir,
            "pl_int_docs_dir": pl_int_docs_dir,
            "pl_exp_dir": pl_exp_dir,
            "static_data_dir": static_data_dir,
            "static_int_metadata_dir": static_int_metadata_dir,
            "static_int_additions_dir": static_int_additions_dir,
            "static_sp_exp_dir": static_sp_exp_dir,
            "static_exp_metadata_dir": static_exp_metadata_dir,
            "static_exp_model_dir": static_exp_model_dir,
        }

        dirs_to_create: list[Path] = [
            pl_commands_dir,
            pl_models_dir,
            pl_models_proj_dir,
            pl_proj_study_dir,
            pl_src_study_model_docs_dir,
            pl_data_dir,
            study_data_dir,
            pl_int_dir,
            pl_sp_int_dir,
            pl_int_models_dir,
            pl_int_docs_dir,
            pl_exp_dir,
            # pl_sp_exp_dir, # Excluded, because this dir will be created
            static_data_dir,
            static_int_metadata_dir,
            static_int_additions_dir,
            static_sp_exp_dir,
            static_exp_metadata_dir,
            static_exp_model_dir,
        ]

        for directory in dirs_to_create:
            if not directory.exists():
                directory.mkdir(parents=True)

        return paths

    def generate_dbt_project_yaml(self):

        root_dir = self.paths["pl_project_dir"]
        # generate all of the dbt_project files if they don't exist.
        self.generate_base_dbt_project_yml(root_dir, "base", self.db_profile, "create")

        # Add new vars if they don't already exist.
        self.dbt_project_add_vars(
            root_dir, self.src_dbtp_def
        )  # TODO - Vars for each stage here
        # import pdb
        # pdb.set_trace()
        # Add new models if they don't already exist

        src_model_tables = []
        for src_file in self.df_identifiers:
            src_model_tables.append(
                normalize_name(
                    [self.src_table_prefix, src_file],
                    trailing=False,
                    extension="drop",
                )
            )
        self.dbt_project_add_models(root_dir, src_model_tables, self.src_dbtp_def)
        self.dbt_project_add_models(
            root_dir, self.int_prefixed_tables, self.int_dbtp_def
        )
        self.dbt_project_add_models(
            root_dir, self.exp_prefixed_tables, self.exp_dbtp_def
        )

    def generate_stg_dds(self):
        input_dd_path = self.paths["study_data_dir"] / self.dd_identifier

        output_path = self.paths["study_data_dir"] / Path(self.int_gen_dd_name)
        input_dd_format = self.dd_format
        additions_filepath = (
            input_dd_path
            / self.paths["static_int_additions_dir"]
            / self.int_stg_additions_name
        )

        self.generate_dds(
            input_dd_path, output_path, input_dd_format, additions_filepath
        )

    def generate_models_yml_files(self, int_config, exp_config):

        self.generate_models_yml(
            config=int_config,
            table_prefix=self.int_table_prefix,
            input_dd_dir=self.paths["static_int_metadata_dir"],
            output_dir=self.paths["pl_int_docs_dir"],
        )

        self.generate_models_yml(
            config=exp_config,
            table_prefix=self.exp_table_prefix,
            input_dd_dir=self.paths["static_exp_metadata_dir"],
            output_dir=self.paths["static_sp_exp_dir"],
        )

    def generate_sources_yml_files(self):

        self.generate_sources_yml(
            input_dd_dir=self.paths["study_data_dir"],
            output_dir=self.paths["pl_src_study_model_docs_dir"],
        )

    # def generate_study_desc_files(self):

    #     self.generate_column_descriptions(
    #         table_prefix=self.src_table_prefix,
    #         input_dd_dir=self.paths["study_data_dir"],
    #         output_dir=self.paths["pl_src_study_model_docs_dir"],
    #         mode="study",
    #     )

    # def generate_int_desc_files(self, int_config):

    # self.generate_column_descriptions(
    #     table_prefix=self.int_table_prefix,
    #     input_dd_dir=self.paths["static_int_metadata_dir"],
    #     output_dir=self.paths["pl_int_docs_dir"],
    #     mode="cdm",
    #     config=int_config,
    # )

    # def generate_exp_desc_files(self, exp_config):

    #     self.generate_column_descriptions(
    #         table_prefix=self.exp_table_prefix,
    #         input_dd_dir=self.paths["static_exp_metadata_dir"],
    #         output_dir=self.paths["static_sp_exp_dir"],
    #         mode="cdm",
    #         config=exp_config,
    #     )

    def generate_run_script(self):
        self.generate_dbt_run_script(self.paths["pl_commands_dir"])

    def generate_static_sql_models(self, *, config, stage:str):
        """
        Generate static dbt models based on a config object.
        Stage, directories, and table prefixes are inferred from the StructureContext.
        """
        sqlgen = SqlModelGenerator(
            study_id=self.study_id,
            project_id=self.project_id,
        )

        # Infer stage and directories
        if stage == 'int':
            metadata_dir = self.paths["static_int_metadata_dir"]
            model_dir = self.paths["pl_int_models_dir"]
            macro_dir = None
            table_prefix = self.int_model_prefix
            src_table_prefix = self.int_model_prefix 
            model_type = self.int_model_type
            if self.int_model_type == 'model_macro':
                macro_dir = self.paths['pl_sp_int_dir'] / "macros"

        elif stage == 'exp':
            metadata_dir = self.paths["static_exp_metadata_dir"]
            model_dir = self.paths["static_exp_model_dir"]
            macro_dir = None
            table_prefix = self.exp_model_prefix
            src_table_prefix = self.int_model_prefix
            model_type = self.exp_model_type

        else:
            raise ValueError("Unrecognized config type")

        for tablename, info in config.data_dictionary.items():

            dd_path = metadata_dir / info.identifier
            dd_key = normalize_name(dd_path, trailing=False, extension="drop")
            column_data = self.load_column_data(dd_path)

            sql = sqlgen.generate_cdm_sql(
                column_data=column_data,
                dd_key=dd_key,
                stage=stage,
            )

            model_name = normalize_name(
                [table_prefix, tablename], trailing=False, extension="drop"
            )
            src_ref = normalize_name(
                [src_table_prefix, tablename], trailing=False, extension="drop"
            )

            if  model_type == "model_macro":
                if macro_dir is None:
                    raise ValueError("Macro directory required for model_macro")
                macro_sql = sqlgen.convert_to_macro(
                    model_name, sql, params="source_table"
                )
                write_file(macro_dir / f"{model_name}.sql", macro_sql)
                model_sql = self.generate_int_macro_model(model_name)

            elif model_type == "model":
                model_sql = sqlgen.convert_to_ref_model(sql, src_ref)

            elif model_type == "attr_model":
                model_sql = sqlgen.convert_to_attr_model(sql)

            else:

                raise ValueError(f"Unrecognized model_type: {model_type}")

            write_file(model_dir / f"{model_name}.sql", model_sql)

    def generate_study_sql_models(self):
        """
        Generate study-level dbt models using the SQL generator.
        """

        sqlgen = SqlModelGenerator(
            study_id=self.study_id,
            project_id=self.project_id,
        )

        dd_path = self.paths["study_data_dir"] / self.dd_identifier
        dd_key = normalize_name(dd_path, trailing=False, extension="drop")
        column_data = self.load_column_data(dd_path)

        for src_file in self.df_identifiers:

            sql = sqlgen.study_select_sql(
                column_data=column_data,
                dd_key=dd_key,
            )

            model_name = normalize_name(
                [self.src_table_prefix, src_file],
                trailing=False,
                extension="drop",
            )

            if self.src_model_type == "model":
                content = sqlgen.convert_to_model(sql, src_file)
                out_path = (
                    self.paths["pl_proj_study_dir"]
                    / self.table_name
                    / f"{model_name}.sql"
                )

            elif self.src_model_type == "attr_model":
                content = sqlgen.convert_to_attr_model(sql)
                out_path = (
                    self.paths["pl_proj_study_dir"]
                    / self.table_name
                    / f"{model_name}.sql"
                )

            elif self.src_model_type == "model_macro":
                content = sqlgen.convert_to_macro(
                    name=model_name,
                    sql_body=sql,
                    params="source_table",
                )
                out_path = (
                    self.paths["pl_proj_study_dir"] / "macros" / f"{model_name}.sql"
                )

            else:
                raise ValueError(f"Unrecognized src_model_type: {self.src_model_type}")

            write_file(out_path, content, mode="create")

    def import_org_data(self):
        importer = RawDataImporter()

        for df in self.df_identifiers:

            raw_data_csv_path = (self.paths["study_data_dir"] / df).resolve()

            dbt_tablename = normalize_name(df, trailing=False, extension="drop")

            args = f'{{fq_tablename: "{dbt_tablename}", csv_path: "{raw_data_csv_path}"}}'

            importer.import_data(dbt_tablename, args)

    def copy_static_export_dir(self):

        src_dir = Path(self.paths["static_sp_exp_dir"])
        dest_root = Path(self.paths["pl_exp_dir"]) 

        target_dir = dest_root / src_dir.name

        if target_dir.exists():
            logger.info(f"Destination already contains directory: {target_dir}")

        if not target_dir.exists():
            copy_directory(src_dir, target_dir)

    def generate_dbt_run_script(self, run_script_dir):
        """Generates a dbt run Bash script dynamically based on a YAML configuration."""

        commands_list = [
            "#!/bin/bash",
            "dbt clean",
            'dbt deps || { echo "Error: dbt deps failed. Exiting..."; exit 1; }',
            "dbt seed #--full-refresh",
        ]

        int_vars = {}
        tgt_vars = {}
        first_src_table = normalize_name(
            self.src_prefixed_tables[0], trailing=False, extension="drop"
        )
        commands_list.append("# Source tables")
        for table in self.src_prefixed_tables:
            tablename = normalize_name(table, trailing=False, extension="drop")
            commands_list.append(self.generate_run_command("model", tablename))

        for table_id in self.int_prefixed_tables:
            int_vars[table_id] = {
                "source_table": first_src_table,
                "target_schema": self.int_dbtp_def["+schema"],
            }

        commands_list.append("# Internal tables run commands")
        for table, args in int_vars.items():
            commands_list.append(self.generate_run_command("model", table, args))

        commands_list.append("# Internal table tests")
        for table, args in int_vars.items():
            commands_list.append(self.generate_run_command("test", table, args))

        commands_list.append("# Export model run commands")
        for table_id in self.exp_prefixed_tables:
            tablename = normalize_name(table_id, trailing=False, extension="drop")
            commands_list.append(self.generate_run_command("model", tablename))

        # Final script content
        data = "\n".join(commands_list) + "\n"
        filepath = run_script_dir / f"run_{self.dataset_id}.sh"

        # Write the script to a file
        write_file(filepath, data, mode="create")

        # Edit script permissions
        subprocess.run(["chmod", "+x", filepath], check=True)

    def copy_required_macros_dir(self):

        src_dir = self.paths["utils_macros_dir"] / 'import_required'
        dest_root = self.paths["pl_macros_dir"]

        if dest_root.exists():
            logger.info(f"Destination already contains directory: {dest_root}")

        if not dest_root.exists():
            copy_directory(src_dir, dest_root)

    def copy_profiles_yml(self):

        src_filepath = (self.paths["utils_files_dir"] / "profiles.yml").resolve()
        dest_filepath = (self.paths["pl_profiles"]).resolve()

        if dest_filepath.exists():
            logger.info(f"File exists - Not copying: {dest_filepath}")

        if not dest_filepath.exists():
            copy_file(src_filepath, dest_filepath)
