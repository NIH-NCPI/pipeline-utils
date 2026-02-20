"""
Contains the base class for project structures.

To run integration tests with 'doctests':
python -m dbt_pipeline_utils.p_utils.structures.project_structure
"""

from dataclasses import dataclass, field
from typing import Type, Dict, ClassVar
from pathlib import Path
import re
import json
import subprocess
import pandas as pd
from dbt_pipeline_utils import logger
from dbt_pipeline_utils.p_utils.structures.structure_context import StructureContext
from dbt_pipeline_utils.p_utils.general import (
    read_file,
    write_file,
    get_existing_yaml,
    normalize_name,
    shorten_identifier
)
from dbt_pipeline_utils.p_utils.common import DD_FORMATS
from dbt_pipeline_utils.p_utils.dbt_testing import DbtTesting


@dataclass
class StructureBC:
    context: StructureContext
    paths: Dict[str, Path] = field(init=False)

    # Registry for subclasses
    _REGISTRY: ClassVar[Dict[str, Type["StructureBC"]]] = {}

    def __post_init__(self) -> None:
        self.paths = self.get_paths()

    # Optional delegation to context
    def __getattr__(self, name):
        if hasattr(self.context, name):
            return getattr(self.context, name)
        raise AttributeError(name)

    @classmethod
    def register(cls, key: str):
        def decorator(subclass: Type["StructureBC"]):
            cls._REGISTRY[key] = subclass
            return subclass

        return decorator

    @classmethod
    def define_structure(
        cls, structure_key: str, *, context: StructureContext
    ) -> "StructureBC":
        if structure_key not in cls._REGISTRY:
            raise KeyError(f"No structure registered for key '{structure_key}'")
        return cls._REGISTRY[structure_key](context=context)

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

    def dbt_project_add_models(self, filepath: Path, table_names: list[str], dbtp_defs: dict) -> None:
        """
        Add top-level model defaults (+schema, +materialized)
        """
        filepath = filepath / "dbt_project.yml"
        existing = get_existing_yaml(filepath)

        all_models = existing.setdefault("models", {})

        for key in ("+schema", "+materialized"):
            val = dbtp_defs.get(key)
            if val is not None and key not in all_models:
                all_models[key] = val

        # Add empty tables
        for table_id in table_names:
            table_id = normalize_name(table_id, trailing=False, extension='drop')
            all_models.setdefault(table_id, {})

        write_file(filepath, existing, mode="overwrite")

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
        def as_str_or_none(v):
            return None if v is None or pd.isna(v) else str(v)

        for idx, row in df.iterrows():
            try:
                variable_name = as_str_or_none(row.get(column_map["variable_name"]))
                formatted_variable_name = normalize_name(variable_name, extension=None)

                description = as_str_or_none(row.get(column_map["description"]))
                data_type = as_str_or_none(row.get(column_map["data_type"]))
                enumerations = as_str_or_none(row.get(column_map["enumerations"]))
                comment = as_str_or_none(row.get(column_map["comment"]))
                src_variable_name = as_str_or_none(
                    row.get(column_map["src_variable_name"], formatted_variable_name)
                )
                tests = as_str_or_none(row.get(column_map["tests"]))

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

        if additions_filepath.exists():
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

        output_filepath = output_dir / "column_descriptions.md"

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

            columns, doc_blocks = self.build_column_metadata(
                table_name=table_name,
                table_prefix=table_prefix,
                dd_filepath=dd_filepath,
            )

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
                "schema": self.src_dbtp_def["+schema"],
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
        """Generates a dbt run Bash script dynamically based on a YAML configuration."""

        commands_list = [
            "#!/bin/bash",
            "set -e",
            "dbt clean",
            'dbt deps || { echo "Error: dbt deps failed. Exiting..."; exit 1; }',
            "dbt seed #--full-refresh",
        ]

        int_vars = {}
        tgt_vars = {}
        first_src_table = normalize_name(
            self.src_prefixed_tables[0],
            trailing=False,
            extension="drop"
        )
        commands_list.append("# Source tables")
        for table in self.src_prefixed_tables:
            tablename = normalize_name(table, trailing=False, extension='drop')
            commands_list.append(self.generate_run_command("model", tablename))

        for table_id in self.int_prefixed_tables:
            int_vars[table_id] = {
                "source_table": first_src_table,
                "target_schema": self.int_dbtp_def["+schema"],
            }

        for table_id in self.exp_prefixed_tables:
            ref_table = table_id.replace(self.exp_table_prefix,self.int_table_prefix)
            tgt_vars[table_id] = {
                "source_table": ref_table,
                "target_schema": self.exp_dbtp_def['+schema'],
            }

        commands_list.append("# Internal tables run commands")
        for table, args in int_vars.items():
            commands_list.append(self.generate_run_command("model", table, args))

        commands_list.append("# Internal table tests")
        for table, args in int_vars.items():
            commands_list.append(self.generate_run_command("test", table, args))

        commands_list.append("# Export model run commands")
        for table, args in tgt_vars.items():
            commands_list.append(self.generate_run_command("model", table, args))

        # Final script content
        data = "\n".join(commands_list) + "\n"
        filepath = run_script_dir / f"run_{self.dataset_id}.sh"

        # Write the script to a file
        write_file(filepath, data, mode='create')

        # Edit script permissions
        filepath.chmod(0o755)


def get_model_names(self, prefix, table_list):
    model_list = []
    for file in table_list:

        model_list.append( normalize_name(
            [prefix, file],
            trailing=False,
            extension="drop",
        ))
    return model_list

def import_org_data(self):

    for df in self.df_identifiers:

        raw_data_csv_path = (self.paths["study_data_dir"] / df).resolve()

        dbt_tablename = normalize_name(df, trailing=False, extension="drop")

        macro_args = f"{{fq_tablename: '{dbt_tablename}', csv_path: '{raw_data_csv_path}'}}"
        run_dbt_macro(macro_args, "register_external_sources")

if __name__ == "__main__":
    import doctest

    doctest.testmod(verbose=True)
