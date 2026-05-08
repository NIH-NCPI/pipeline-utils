"""
Contains the base class for project structures.

To run integration tests with 'doctests':
python -m dbt_pipeline_utils.p_utils.structures.project_structure
"""

from dataclasses import dataclass, field
from typing import Dict, Any
from pathlib import Path
import re
import json
import yaml
from importlib.resources import files as importlib_files
import pandas as pd
import dbt_pipeline_utils
from dbt_pipeline_utils import logger, find_repo_root
from dbt_pipeline_utils.p_utils.structures.structure_context import StructureContext
from dbt_pipeline_utils.p_utils.files import (
    read_file,
    write_file,
    get_existing_yaml,
    normalize_name,
    shorten_identifier,
    copy_directory,
    copy_file,
)
from dbt_pipeline_utils.p_utils.sql_model_generator import SqlModelGenerator
from dbt_pipeline_utils.p_utils.dags_generator import DagGenerator
from dbt_pipeline_utils.p_utils.common import DD_FORMATS
from dbt_pipeline_utils.p_utils.dbt_testing import DbtTesting


@dataclass
class StructureBC:
    context: StructureContext
    proj_config_path: str
    study_config_path: Path
    paths: Dict[str, Path] = field(init=False)

    def __post_init__(self) -> None:
        self.paths = self.get_paths()

    # Optional delegation to context
    def __getattr__(self, name):
        if hasattr(self.context, name):
            return getattr(self.context, name)
        raise AttributeError(name)

    def _load_path_config(self) -> dict:
        """Load and cache the structure's path config YAML, resolving like other project paths using _resolve_root."""
        if not hasattr(self, "_path_config_cache"):
            config_path = Path(self._resolve_root("repo_root") / self.proj_config_path)
            with config_path.open() as fh:
                self._path_config_cache = yaml.safe_load(fh)
        return self._path_config_cache

    def _resolve_root(self, root_name: str) -> Path:
        """Map a root-type name to a concrete Path."""
        root_map = {
            "cwd": Path.cwd(),
            "repo_root": find_repo_root(),
            "home_dbt": Path.home() / ".dbt",
            "package": Path(dbt_pipeline_utils.__file__).resolve().parent,
        }
        if root_name not in root_map:
            raise KeyError(
                f"Unknown root type '{root_name}'. " f"Available: {list(root_map)}"
            )
        return root_map[root_name]

    def get_paths(self) -> Dict[str, Path]:
        """Resolve all paths from the structure's YAML config."""
        config = self._load_path_config()

        template_vars = {
            "project_id": self.project_id,
            "study_id": self.study_id,
            "int_model_name": self.int_model_name,
            "exp_model_name": self.exp_model_name,
            "study_data_dir": str(self.study_data_dir),
            "pipeline_data_dir": str(self.pipeline_data_dir),
        }

        def render_template(template: str, key_name: str) -> str:
            try:
                return template.format(**template_vars)
            except KeyError as exc:
                missing_var = exc.args[0]
                raise KeyError(
                    f"Missing template variable '{missing_var}' while resolving "
                    f"path key '{key_name}' from template '{template}'. "
                    f"Available variables: {sorted(template_vars.keys())}"
                ) from exc

        paths: Dict[str, Path] = {}

        def resolve_entry_base(entry: dict, key_name: str) -> Path:
            root_name = entry.get("root")
            if root_name:
                return self._resolve_root(root_name)

            # Backward-compatible: allow "base" in paths entries.
            base_name = entry.get("base")
            if base_name:
                if base_name in paths:
                    return paths[base_name]
                return self._resolve_root(base_name)

            raise KeyError(f"Missing 'root' or 'base' for path key '{key_name}'.")

        # --- regular paths ---
        for key, entry in (config.get("paths") or {}).items():
            if entry is None:
                continue
            root = resolve_entry_base(entry, key)
            tmpl = entry.get("template") or ""
            paths[key] = root / render_template(tmpl, key) if tmpl else root

        # --- overridable paths (fall back to template when context attr is None) ---
        for key, entry in (config.get("overridable_paths") or {}).items():
            if entry is None:
                continue
            ctx_attr = entry.get("context_attr", key)
            override_val = getattr(self, ctx_attr, None)
            if override_val is not None:
                override_root_name = entry.get("override_root")
                if override_root_name:
                    paths[key] = self._resolve_root(override_root_name) / str(
                        override_val
                    )
                else:
                    paths[key] = Path(override_val)
            else:
                root = resolve_entry_base(entry, key)
                tmpl = entry.get("template") or ""
                paths[key] = root / render_template(tmpl, key) if tmpl else root

        # --- derived paths (relative to another already-resolved path) ---
        for key, entry in (config.get("derived_paths") or {}).items():
            if entry is None:
                continue
            base_path = paths[entry["base"]]
            tmpl = entry.get("template") or ""
            paths[key] = base_path / render_template(tmpl, key) if tmpl else base_path

        # --- importlib.resources paths (Traversable, never mkdir'd) ---
        resource_keys: set = set()
        for key, tmpl in (config.get("resource_paths") or {}).items():
            if tmpl is None:
                continue
            base = importlib_files("dbt_pipeline_utils")
            resource_path = base / tmpl if tmpl else base
            paths[key] = Path(str(resource_path))
            resource_keys.add(key)

        # Create all *_dir paths (skip read-only resource paths)
        for name, path in paths.items():
            if name.endswith("dir") and name not in resource_keys:
                Path(path).mkdir(parents=True, exist_ok=True)

        return paths

    def get_stage_path(self, stage: str, slot: str = "models_dir") -> Path:
        """Return the resolved path for a data stage and slot.

        Parameters
        ----------
        stage : str
            Logical stage name as defined in the structure's YAML config
            (e.g. ``"sources"``, ``"intermediate"``, ``"export"``).
        slot : str
            Path slot within that stage (e.g. ``"models_dir"``, ``"docs_dir"``,
            ``"macros_dir"``).
        """
        config = self._load_path_config()
        stages = config.get("stages", {})
        if stage not in stages:
            raise KeyError(f"Unknown stage '{stage}'. Available: {list(stages)}")
        stage_cfg = stages[stage]
        if slot not in stage_cfg:
            raise KeyError(
                f"Unknown slot '{slot}' for stage '{stage}'. "
                f"Available: {list(stage_cfg)}"
            )
        return self.paths[stage_cfg[slot]]

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

    def dbt_project_add_tags(
        self,
        filepath: Path,
        study_id: str,
    ) -> None:
        """
        Add +tags under the correct study group(s) in dbt_project.yml.
        """
        filepath = filepath / "dbt_project.yml"
        existing = get_existing_yaml(filepath)

        all_models = existing.setdefault("models", {})

        pkg_name = existing.get("name")
        if pkg_name and pkg_name in all_models:
            pkg_models = all_models[pkg_name]
        else:
            pkg_models = all_models
        config_key = pkg_models.get(self.project_name, {})
        study_group = config_key.setdefault(study_id, {})
        # Set +tags directly under the study_id group
        study_group["+tags"] = [study_id]

        write_file(filepath, existing, mode="overwrite")

    def _schema_overrides(self) -> dict[str, str]:
        return {
            "src": self.src_schema,
            "int": self.int_schema,
            "stb": self.stb_schema,
            "access": self.access_schema,
            "combined": self.combined_schema,
            "exp": self.export_schema,
        }

    def _ensure_group(self, parent: dict, keys: list[str]) -> dict:
        node = parent
        for key in keys:
            node = node.setdefault(key, {})
        return node

    def dbt_project_add_schema_groups(self, filepath: Path) -> None:
        """Add group-level schema config blocks in dbt_project.yml."""
        yaml_path = filepath / "dbt_project.yml"
        existing = get_existing_yaml(yaml_path)
        all_models = existing.setdefault("models", {})
        schema_overrides = self._schema_overrides()

        # Remove legacy global schema to avoid overriding group-level schemas.
        all_models.pop("+schema", None)

        pkg_name = existing.get("name")
        if pkg_name:
            pkg_models = all_models.setdefault(pkg_name, {})
        else:
            pkg_models = all_models

        src_group = self._ensure_group(pkg_models, ["include", self.study_id, "src"])
        src_group["+schema"] = schema_overrides["src"]

        int_group = self._ensure_group(pkg_models, ["include", self.study_id, "int"])
        int_group["+schema"] = schema_overrides["int"]

        access_group = self._ensure_group(pkg_models, ["access"])
        access_group["+schema"] = schema_overrides["access"]

        combined_group = self._ensure_group(pkg_models, ["combined"])
        combined_group["+schema"] = schema_overrides["combined"]

        export_group = self._ensure_group(pkg_models, ["export"])
        export_group.pop("+schema", None)
        export_model_group = self._ensure_group(export_group, [self.exp_model_name])
        export_model_group["+schema"] = schema_overrides["exp"]

        write_file(yaml_path, existing, mode="overwrite")

    def dbt_project_add_vars(self, filepath: Path, dbtp_defs: dict) -> None:
        """
        Add or update project-level vars in dbt_project.yml.
        """
        filepath = filepath / "dbt_project.yml"
        existing = get_existing_yaml(filepath)

        vars_section = existing.setdefault("vars", {})
        if vars_section != {}:
            logger.debug(f"No action performed. File '{filepath}' already has defined 'vars'. ")
            pass

        incoming_vars = dbtp_defs.get("vars")
        if incoming_vars:
            for k, v in incoming_vars.items():
                vars_section[k] = v

        write_file(filepath, existing, mode="overwrite")

    def as_str_or_none(self, v):
        return None if v is None or pd.isna(v) else str(v)

    def extract_columns(self, df, dd_format):
        """
        Extracts relevant column information based on the dictionary format.
        """

        column_map = DD_FORMATS[dd_format]  # Define dd column expectations
        column_data_list = []

        if df is not None:
            df = df.astype("string").where(pd.notna(df), None)
        else:
            logger.error('Fail test')
            import pdb
            pdb.set_trace()

        for idx, row in df.iterrows():
            try:
                variable_name = self.as_str_or_none(
                    row.get(column_map["variable_name"])
                )
                formatted_variable_name = (
                    normalize_name(variable_name, extension="drop")
                    if variable_name
                    else ""
                )

                description = self.as_str_or_none(row.get(column_map["description"]))
                data_type = self.as_str_or_none(row.get(column_map["data_type"]))
                enumerations = self.as_str_or_none(row.get(column_map["enumerations"]))
                comment = self.as_str_or_none(row.get(column_map["comment"]))
                src_variable_name = self.as_str_or_none(
                    row.get(column_map["src_variable_name"], formatted_variable_name)
                )
                tests = self.as_str_or_none(row.get(column_map["tests"]))

                column_data_list.append(
                    (
                        variable_name,
                        formatted_variable_name,
                        description,
                        data_type,
                        enumerations,
                        comment,
                        src_variable_name,
                        tests,
                    )
                )

            except Exception as e:
                print(f"Error at row {idx}: {e}")
                print(f"Row content: {row}")
                raise

        return column_data_list

    def load_column_data(self, src_dd_path):
        """Loads column names, descriptions, and data types from CSV files and stores them in a dictionary."""
        column_data = {}

        src_df = read_file(src_dd_path)
        key = normalize_name(src_dd_path, trailing=False, extension='drop')
        column_data[key] = self.extract_columns(src_df, self.dd_format)

        return column_data

    def build_column_metadata(
        self,
        *,
        table_name: str,
        table_prefix: str,
        dd_filepath: Path,
        include_tests: bool = True,
    ):
        """
        Returns:
        - column metadata for yaml
        - column doc identifiers (doc_id, description)
        """

        src_table_key = normalize_name(dd_filepath, trailing=False, extension="drop")
        column_data = self.load_column_data(dd_filepath)

        columns = []
        doc_blocks = []

        for (
            col_name,
            col_name_code,
            col_description,
            col_data_type,
            enums,
            _,
            _,
            tests,
        ) in column_data.get(src_table_key, []):

            doc_id = self.generate_doc_block_name(table_name, col_name_code, table_prefix)

            column_entry = {
                "name": col_name,
                "description": f'{{{{ doc("{doc_id}") }}}}',
            }
            tester = DbtTesting
            if include_tests and tests:

                column_entry["tests"] = tester.format_tests(tests, col_name_code, enums)

            columns.append(column_entry)

            doc_blocks.append(
                (
                    doc_id,
                    col_description.strip()
                    if isinstance(col_description, str) and col_description.strip()
                    else col_name,
                )
            )

        return columns, doc_blocks

    def generate_dds(
        self, input_dd_path, output_path, input_dd_format, additions_filepath=None
    ):
        """Generates dd files dynamically for each table based on the data dictionary.
        open the src dd and apply minimal transformations

        Output dd will be in DD_FORMATS['pipeline_format']
        Input dd format must be mapped in DD_FORMATS

        src_ = original dd column"""

        src_table_key = input_dd_path.stem
        logger.debug(f"Attempting to generate dd: '{output_path}'")

        input_dd = read_file(input_dd_path)

        column_data = self.load_column_data(input_dd_path)
        column_mapping = {
            col_name: column_name_code
            for col_name, column_name_code, _, _, _, _, _, _ in column_data.get(
                src_table_key, []
            )
        }

        # Map from the original format to pipeline_format
        original_format_map = DD_FORMATS.get(input_dd_format)
        if original_format_map is None:
            raise ValueError(f"Unsupported DD format: {input_dd_format}")
        pipeline_format_map = DD_FORMATS["pipeline_format"]

        # Convert src_variable_name based on the original format
        input_dd[pipeline_format_map["src_variable_name"]] = input_dd.get(
            original_format_map["variable_name"], ""
        )

        # Convert variable_name field to match pipeline_format
        variable_name_key = original_format_map.get("variable_name", "variable_name")
        if variable_name_key in input_dd.columns:
            input_dd[pipeline_format_map["variable_name"]] = (
                input_dd[variable_name_key]
                .map(column_mapping)
                .fillna(input_dd[variable_name_key])
            )

        # Rename all columns according to pipeline_format
        rename_map = {
            original_format_map[key]: pipeline_format_map[key]
            for key in pipeline_format_map
            if key in original_format_map
        }
        input_dd.rename(columns=rename_map, inplace=True)

        if additions_filepath and additions_filepath.exists():
            transformations = read_file(additions_filepath)
            input_dd = pd.concat([input_dd, transformations])
        else:
            logger.debug(f"There are no additions at path: {additions_filepath} ")

        write_file(output_path, input_dd)

    def generate_doc_block_name(self, table_name, column_name, table_prefix=""):
        """
        Ensures dbt doc block names consist of only letters, numbers and underscores, as dbt expects.
        """
        name = normalize_name([table_name, column_name], trailing=False, extension='drop')
        result = shorten_identifier(name)
        return result

    def generate_column_descriptions(
        self,
        *,
        output_dir: Path,
        doc_blocks: list[tuple[str, str]],
    ):
        """
        Writes column_descriptions.md using precomputed identifiers.
        """

        output_filepath = output_dir / "_column_descriptions.md"

        if not output_filepath.exists():
            output_filepath.touch()

        existing = output_filepath.read_text().rstrip()

        existing_ids = set(re.findall(r"\{%\s*docs\s+([\w\d_]+)\s*%\}", existing))

        new_blocks = []

        for doc_id, description in doc_blocks:
            if doc_id in existing_ids:
                continue

            new_blocks.append(f"{{% docs {doc_id} %}}\n{description}\n{{% enddocs %}}")
            existing_ids.add(doc_id)

        if new_blocks:
            content = (
                existing + "\n\n" + "\n\n".join(new_blocks)
                if existing
                else "\n\n".join(new_blocks)
            )
            write_file(output_filepath, content, mode="overwrite")

    def generate_models_yml(
        self,
        *,
        config,
        table_prefix: str,
        input_dd_dir: Path,
        output_dir: Path,
    ):
        """
        Generates __models.yml and column_descriptions.md together.
        """

        models = []
        all_doc_blocks: list[tuple[str, str]] = []

        for tablename, dd_info in config.data_dictionary.items():

            dd_filepath = input_dd_dir / dd_info.identifier
            table_name = normalize_name(
                [table_prefix, tablename], trailing=False, extension="drop"
            )
            try: 
                columns, doc_blocks = self.build_column_metadata(
                    table_name=table_name,
                    table_prefix=table_prefix,
                    dd_filepath=dd_filepath,
                )
            except:
                import pdb; pdb.set_trace()

            models.append(
                {
                    "name": table_name,
                    "description": f"Model for {table_name}.",
                    "columns": columns,
                }
            )

            all_doc_blocks.extend(doc_blocks)

        write_file(
            output_dir / "__models.yml",
            {"models": models},
            mode="merge",
        )

        self.generate_column_descriptions(
            output_dir=output_dir,
            doc_blocks=all_doc_blocks,
        )

    def generate_sources_yml(
        self,
        *,
        input_dd_dir: Path,
        output_dir: Path,
    ):
        """
        Generate / update sources.yml and column docs consistently.
        """

        filepath = output_dir / "sources.yml"
        existing = get_existing_yaml(filepath)

        sources = existing.setdefault("sources", [])

        study_block = next(
            (s for s in sources if s.get("name") == self.study_id),
            None,
        )

        if study_block is None:
            study_block = {
                "name": self.study_id,
                "schema": self.src_schema,
                "tables": [],
            }
            sources.append(study_block)

        tables = study_block.setdefault("tables", [])
        existing_table_names = {t["name"] for t in tables}

        dd_filepath = input_dd_dir / self.dd_identifier

        all_doc_blocks: list[tuple[str, str]] = []

        for filename in self.df_identifiers:

            table_name = normalize_name(filename, trailing=False, extension="drop")

            if table_name in existing_table_names:
                continue

            normalized_table = normalize_name(
                filename, trailing=True, extension="drop"
            )

            columns, doc_blocks = self.build_column_metadata(
                table_name=normalized_table,
                table_prefix=self.src_table_prefix,
                dd_filepath=dd_filepath,
                include_tests=False,
            )

            tables.append(
                {
                    "name": table_name,
                    "description": f"Source table for {table_name}.",
                    "columns": columns,
                }
            )

            all_doc_blocks.extend(doc_blocks)

        write_file(filepath, existing, mode="merge")

        # Emit column docs using the SAME identifiers
        self.generate_column_descriptions(
            output_dir=output_dir,
            doc_blocks=all_doc_blocks,
        )

    def generate_run_command(self, operation, model, args=None):
        """Generates a dbt run command for models or macros with optional arguments."""

        if operation == "macro":
            all_args = (
                f"--args '{' '.join(f'\"{k}\": \"{v}\"' for k, v in args.items())}'"
                if args
                else ""
            )
            op = f"dbt run-operation {model} {all_args}".strip()

        if operation == "model":
            all_args = f"--vars '{json.dumps(args)}'" if args else ""
            op = f"dbt run --select {model} {all_args}".strip()

        if operation == "test":
            all_args = f"--vars '{json.dumps(args)}'" if args else ""
            op = f"dbt test --select {model} {all_args}".strip()

        return op

    def generate_dbt_run_script(self, run_script_dir):
        """Generates a dbt run Bash script for src, stb, combined, and export stages using regular dbt run commands."""
        commands_list = [
            "#!/bin/bash",
            "set -e",
            "dbt clean",
            'dbt deps || { echo "Error: dbt deps failed. Exiting..."; exit 1; }',
            "dbt seed #--full-refresh",
        ]

        # Source stage
        commands_list.append("# Source tables")
        for table in self.src_prefixed_tables:
            tablename = normalize_name(table, trailing=False, extension='drop')
            commands_list.append(self.generate_run_command("model", tablename))

        # stb stage
        commands_list.append("# Stable tables")
        for table in getattr(self, "stb_prefixed_tables", []):
            tablename = normalize_name(table, trailing=False, extension="drop")
            commands_list.append(self.generate_run_command("model", tablename))

        # combined stage
        commands_list.append("# Combined tables")
        for table in getattr(self, "combined_prefixed_tables", []):
            tablename = normalize_name(table, trailing=False, extension="drop")
            commands_list.append(self.generate_run_command("model", tablename))

        # export stage
        commands_list.append("# Export tables")
        for table in getattr(self, "exp_prefixed_tables", []):
            tablename = normalize_name(table, trailing=False, extension="drop")
            commands_list.append(self.generate_run_command("model", tablename))

        # Final script content
        data = "\n".join(commands_list) + "\n"
        filepath = run_script_dir / f"run_{self.dataset_id}.sh"

        # Write the script to a file
        write_file(filepath, data, mode='create')

        # Edit script permissions
        filepath.chmod(0o755)

    # ------------------------------------------------------------------
    # Config-driven generation methods (shared by all structures)
    # ------------------------------------------------------------------

    def generate_dbt_project_yaml(self):
        """Create dbt_project.yml files as defined in the structure config."""
        config = self._load_path_config()
        project_defs = config.get("dbt_projects") or config.get("dbt_project") or []
        template_vars = {
            "project_id": self.project_id,
            "study_id": self.study_id,
            "int_model_name": self.int_model_name,
            "exp_model_name": self.exp_model_name,
        }

        for proj in project_defs:
            dir_path = self.paths[proj["dir_key"]]
            name = proj["name"].format(**template_vars)
            self.generate_base_dbt_project_yml(dir_path, name, self.db_profile, "merge")

        for _stage, stage_cfg in config.get("dbt_project_stages", {}).items():
            dir_path = self.paths[stage_cfg["dir_key"]]
            self.dbt_project_add_vars(dir_path, {"vars": {}})

            # Ensure stable models are also schema-pinned.
            if stage_cfg["dbtp_def"] == "src":
                self.dbt_project_add_tags(dir_path, self.study_id)

        # Apply schema at model-group level instead of per-model.
        for proj in project_defs:
            self.dbt_project_add_schema_groups(self.paths[proj["dir_key"]])

    def generate_stg_dds(self):
        config = self._load_path_config()
        format_attr = config.get("stg_dd_format_attr", "dd_format")
        input_dd_path = self.paths["study_data_dir"] / self.dd_identifier
        output_path = self.paths["study_data_dir"] / Path(self.int_gen_dd_name)
        input_dd_format = getattr(self, format_attr)
        additions_filepath = (
            input_dd_path
            / self.paths["static_int_additions_dir"]
            / self.int_stg_additions_name
        )
        self.generate_dds(
            input_dd_path, output_path, input_dd_format, additions_filepath
        )

    def generate_models_yml_files(self, int_config, exp_config):
        config = self._load_path_config()
        int_yml_dir = self.paths[config["stages"]["access"]["yml_dir"]]
        exp_yml_dir = self.paths[config["stages"]["export"]["yml_dir"]]

        self.generate_models_yml(
            config=int_config,
            table_prefix=self.int_table_prefix,
            input_dd_dir=self.paths["static_int_metadata_dir"],
            output_dir=int_yml_dir,
        )

        self.generate_models_yml(
            config=exp_config,
            table_prefix=self.exp_table_prefix,
            input_dd_dir=self.paths["static_exp_metadata_dir"],
            output_dir=exp_yml_dir,
        )

    def generate_sources_yml_files(self):
        config = self._load_path_config()
        sources_yml_dir = self.paths[config["stages"]["sources"]["yml_dir"]]
        self.generate_sources_yml(
            input_dd_dir=self.paths["study_data_dir"],
            output_dir=sources_yml_dir,
        )

    def generate_run_script(self):
        config = self._load_path_config()
        run_script_dir_key = config.get("run_script_dir", "pl_scripts_dir")
        self.generate_dbt_run_script(self.paths[run_script_dir_key])

    def generate_study_sql_models(self):
        """Generate study-level dbt models using the SQL generator."""
        config = self._load_path_config()
        study_sql_method = config.get("study_sql_method", "study_select_sql")

        sqlgen = SqlModelGenerator(study_id=self.study_id, project_id=self.project_id)
        dd_path = self.paths["study_data_dir"] / self.dd_identifier
        dd_key = normalize_name(dd_path, trailing=False, extension="drop")
        column_data = self.load_column_data(dd_path)

        src_models_dir = self.get_stage_path("sources", "models_dir")

        for src_file in self.df_identifiers:
            if study_sql_method == "duckdb_src_query":
                sql = sqlgen.duckdb_src_query(
                    column_data=column_data,
                    table_path=self.paths["study_data_dir"] / src_file,
                )
            else:
                sql = sqlgen.study_select_sql(column_data=column_data, dd_key=dd_key)

            model_name = normalize_name(
                [self.src_table_prefix, src_file], trailing=False, extension="drop"
            )

            if self.src_model_type == "model":
                if study_sql_method == "duckdb_src_query":
                    content = sql
                else:
                    content = sqlgen.convert_to_model(sql, src_file)
                out_path = src_models_dir / self.table_name / f"{model_name}.sql"

            elif self.src_model_type == "attr_model":
                content = sqlgen.convert_to_attr_model(sql)
                out_path = src_models_dir / self.table_name / f"{model_name}.sql"

            elif self.src_model_type == "model_macro":
                content = sqlgen.convert_to_macro(
                    filename=model_name, sql_content=sql, params="source_table"
                )
                out_path = src_models_dir / "macros" / f"{model_name}.sql"

            else:
                raise ValueError(f"Unrecognized src_model_type: {self.src_model_type}")

            write_file(out_path, content, mode="create")

    def generate_static_sql_models(self, *, config, stage: str):
        """Generate static dbt models; stage directories come from the YAML config."""
        sqlgen = SqlModelGenerator(study_id=self.study_id, project_id=self.project_id)

        if stage == "stb":
            model_dir = self.get_stage_path("stable", "models_dir")
            macro_dir = None
            table_prefix = self.stb_table_prefix
            src_table_prefix = self.src_table_prefix
            model_type = self.stb_model_type
            if model_type == "model_macro":
                proj_dir = self.paths.get("pl_models_proj_dir")
                macro_dir = Path(proj_dir) / "macros" if proj_dir else None

        elif stage in ("access", "int"):
            model_dir = self.get_stage_path("access", "models_dir")
            macro_dir = None
            table_prefix = self.int_model_prefix
            src_table_prefix = self.combined_model_prefix
            model_type = self.int_model_type
            if model_type == "model_macro":
                base = self.get_stage_path("access", "macros_dir")
                macro_dir = Path(base) / "macros"

        elif stage == "exp":
            model_dir = self.get_stage_path("export", "models_dir")
            macro_dir = None
            table_prefix = self.exp_model_prefix
            src_table_prefix = self.int_model_prefix
            model_type = self.exp_model_type

        elif stage == "combined":
            model_dir = self.get_stage_path("combined", "models_dir")

            include_int_dir = self.paths["pl_dir"] / "models" / "include"
            for target_table in config.data_dictionary.keys():
                combined_model = normalize_name(
                    [self.combined_model_prefix, target_table],
                    trailing=False,
                    extension="drop",
                )

                # Union only stb models for the same logical table across studies,
                # e.g. *_stb_accesspolicy for combined_accesspolicy.
                pattern = f"*/int/*_stb_{target_table}.sql"
                discovered = sorted(p.stem for p in include_int_dir.glob(pattern))

                if not discovered:
                    discovered = [
                        normalize_name(
                            [self.stb_table_prefix, target_table],
                            trailing=False,
                            extension="drop",
                        )
                    ]

                union_blocks = [
                    f"select *\nfrom {{{{ ref('{model_name}') }}}}"
                    for model_name in discovered
                ]

                if union_blocks:
                    body_sql = "\nunion all\n".join(union_blocks) + "\n"
                else:
                    body_sql = "select 1 as _placeholder where 1 = 0\n"

                model_sql = "{{ config(materialized='table') }}\n\n" + body_sql
                write_file(model_dir / f"{combined_model}.sql", model_sql)
            return

        else:
            raise ValueError(f"Unrecognized stage: {stage!r}")

        metadata_dir = (
            self.paths["static_exp_metadata_dir"]
            if stage == "exp"
            else self.paths["static_int_metadata_dir"]
        )

        for tablename, info in config.data_dictionary.items():
            dd_path = metadata_dir / info.identifier
            dd_key = normalize_name(dd_path, trailing=False, extension="drop")

            column_data = self.load_column_data(dd_path)

            sql = sqlgen.generate_cdm_sql(
                column_data=column_data, dd_key=dd_key, stage=stage
            )

            model_name = normalize_name(
                [table_prefix, tablename], trailing=False, extension="drop"
            )

            if stage == "stb":
                tablename = self.df_identifiers[0]

            src_ref = normalize_name(
                [src_table_prefix, tablename], trailing=False, extension="drop"
            )

            if model_type == "model_macro":
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

    def generate_study_desc_files(self):
        dd_filepath = self.paths["study_data_dir"] / self.dd_identifier
        all_doc_blocks: list[tuple[str, str]] = []

        for filename in self.df_identifiers:
            normalized_table = normalize_name(filename, trailing=True, extension="drop")
            _, doc_blocks = self.build_column_metadata(
                table_name=normalized_table,
                table_prefix=self.src_table_prefix,
                dd_filepath=dd_filepath,
                include_tests=False,
            )
            all_doc_blocks.extend(doc_blocks)

        self.generate_column_descriptions(
            output_dir=self.get_stage_path("sources", "docs_dir"),
            doc_blocks=all_doc_blocks,
        )

    def generate_int_desc_files(self, int_config):
        all_doc_blocks: list[tuple[str, str]] = []
        for tablename, dd_info in int_config.data_dictionary.items():
            dd_filepath = self.paths["static_int_metadata_dir"] / dd_info.identifier
            table_name = normalize_name(
                [self.int_table_prefix, tablename], trailing=False, extension="drop"
            )
            _, doc_blocks = self.build_column_metadata(
                table_name=table_name,
                table_prefix=self.int_table_prefix,
                dd_filepath=dd_filepath,
            )
            all_doc_blocks.extend(doc_blocks)

        self.generate_column_descriptions(
            output_dir=self.get_stage_path("stable", "docs_dir"),
            doc_blocks=all_doc_blocks,
        )

    def generate_exp_desc_files(self, exp_config):
        all_doc_blocks: list[tuple[str, str]] = []
        for tablename, dd_info in exp_config.data_dictionary.items():
            dd_filepath = self.paths["static_exp_metadata_dir"] / dd_info.identifier
            table_name = normalize_name(
                [self.exp_table_prefix, tablename], trailing=False, extension="drop"
            )
            _, doc_blocks = self.build_column_metadata(
                table_name=table_name,
                table_prefix=self.exp_table_prefix,
                dd_filepath=dd_filepath,
            )
            all_doc_blocks.extend(doc_blocks)

        self.generate_column_descriptions(
            output_dir=self.get_stage_path("export", "docs_dir"),
            doc_blocks=all_doc_blocks,
        )

    def generate_dag(self):
        daggen = DagGenerator(study_id=self.study_id, project_name=self.project_name, dag_id=self.dag_id)

        daggen.generate_dag_from_study_config(self.paths["pl_dag_dir"])

    # ------------------------------------------------------------------
    # Copy methods (enabled/disabled per structure via features config)
    # ------------------------------------------------------------------

    def _feature(self, name: str) -> bool:
        return self._load_path_config().get("features", {}).get(name, False)

    def copy_static_export_dir(self):
        if not self._feature("copy_export_dir"):
            return
        src_dir = Path(self.paths["static_sp_exp_dir"])
        target_dir = Path(self.paths["pl_exp_dir"]) / src_dir.name
        if not target_dir.exists():
            copy_directory(src_dir, target_dir)
        else:
            logger.debug(
                f"Nothing to do - destination already contains directory: {target_dir}"
            )

    def copy_project_macros_dir(self):
        if not self._feature("copy_macros_dir"):
            return
        src_dir = (
            Path(self.paths["utils_macros_dir"]) / self.project_id / "harmonization"
        )
        target_dir = Path(self.paths["pl_macros_dir"]) / src_dir.name
        if not target_dir.exists():
            copy_directory(src_dir, target_dir)
        else:
            logger.debug(
                f"Nothing to do - destination contains directory: {target_dir}"
            )

    def copy_profiles_yml(self):
        if not self._feature("copy_profiles"):
            return
        src_filepath = (Path(self.paths["utils_files_dir"]) / "profiles.yml").resolve()
        dest_filepath = Path(self.paths["pl_profiles"]).resolve()
        if not dest_filepath.exists():
            copy_file(src_filepath, dest_filepath)
        else:
            logger.debug(f"File exists - Not copying: {dest_filepath}")

    def copy_import_macros(self):
        if not self._feature("copy_import_macros"):
            return
        src_filepath = (
            Path(self.paths["utils_macros_dir"])
            / "import_required/register_external_sources.sql"
        )
        dest_filepath = (
            Path(self.paths["pl_macros_dir"]) / "register_external_sources.sql"
        )
        if not dest_filepath.exists():
            copy_file(src_filepath, dest_filepath)
        else:
            logger.debug(f"File exists - Not copying: {dest_filepath}")


if __name__ == "__main__":
    import doctest

    doctest.testmod(verbose=True)
