"""
Contains the base class for project structures.

To run integration tests with 'doctests':
python -m dbt_pipeline_utils.p_utils.structures.project_structure
"""

from dataclasses import dataclass, field
from typing import Type, Dict, ClassVar, Any
from pathlib import Path
import re
import pandas as pd
from dbt_pipeline_utils import logger
from dbt_pipeline_utils.p_utils.structures.structure_context import StructureContext
from dbt_pipeline_utils.p_utils.general import (
    read_file,
    write_file,
    get_existing_yaml,
    normalize_name,
)
from dbt_pipeline_utils.p_utils.common import DD_FORMATS

from typing import Dict, Type, ClassVar


from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict
from .structure_context import StructureContext


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

    # def __post_init__(self):
    #     if self.study_config_path is not None:
    #         self.study_config_dir = Path(self.study_config_path).parent

    #     if self.study_data_dir is not None:
    #         self.study_data_dir = Path(self.study_data_dir)

    #     self.int_table_prefix = f"{self.study_id}_{self.int_model_prefix}"
    #     self.int_gen_dd_name = f"{self.table_name}_{self.int_model_prefix}_dd.csv"
    # self.paths = self.get_paths()

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

        models = existing.setdefault("models", {})
        study_models = models.setdefault(self.study_id, {})

        for key in ("+schema", "+materialized"):
            val = dbtp_defs.get(key)
            if val is not None and key not in study_models:
                study_models[key] = val

        # Add empty tables
        for table_id in table_names:
            study_models.setdefault(table_id, {})

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

        df = df.astype("string").where(pd.notna(df), None)

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
                    row.get(column_map["src_variable_name"])
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
        # import pdb
        # pdb.set_trace()
        src_df = read_file(src_dd_path)
        key = normalize_name(src_dd_path, trailing=False, extension='drop')
        column_data[key] = self.extract_columns(src_df, self.dd_format)

        return column_data

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
        name = normalize_name([table_prefix, table_name, column_name], trailing=False, extension='drop')
        return name

    def format_tests(self, tests, col, enums=None):
        """
        Formats the tests string into dbt-compatible test definitions.

        Args:
            tests: A pipe-delimited string of tests, e.g., "not_null|accepted_values".
            enums: enumeration values, e.g., 'HTP', 'Other'.
                                    Used when "accepted_values" is one of the tests.

        Returns:
            list: A list of dictionaries representing the formatted dbt tests.
        """
        test_list = tests.split("|")

        if enums is not None:
            enums = [enum.strip() for enum in enums.split(";")]

        formatted_tests = []
        is_required = "not_null" in test_list

        for test in test_list:
            test = test.strip()

            if test == "accepted_values" and enums and not is_required:
                formatted_tests.append(
                    {
                        "accepted_values": {
                            "values": enums,
                            "config": {"where": f"{col} is not null"},
                        }
                    }
                )
            if test == "accepted_values" and enums and is_required:
                formatted_tests.append({"accepted_values": {"values": enums}})

            if test == "not_null":
                formatted_tests.append(test)

        return formatted_tests

    def generate_models_yml(self, config, table_prefix, input_dd_dir, output_dir):
        """
        Generates dbt models.yml file for each table in its respective directory, including src and staging models.

        Doctest
        -------
        >>> from pathlib import Path
        >>> from collections import namedtuple
        >>> InternalDataDictionary = namedtuple("InternalDataDictionary", ["identifier", "pipeline_identifier", "table_id"])
        >>> int_config = type("C", (), {})()
        >>> int_config.data_dictionary = {
        ...     "AccessPolicy_data_access_type": InternalDataDictionary(
        ...         identifier=Path("AccessPolicy_data_access_type-dd.csv"),
        ...         pipeline_identifier=Path("ftd_accesspolicy_data_access_type_dd.csv"),
        ...         table_id="ftd_accesspolicy_data_access_type",
        ...     )
        ... }
        >>> for dd_name, details in int_config.data_dictionary.items():
        ...     print(dd_name, details.table_id)
        AccessPolicy_data_access_type ftd_accesspolicy_data_access_type
        """
        models = []

        for dd_name, details in config.data_dictionary.items():

            input_dd_path = input_dd_dir / details.identifier
            src_table_key = normalize_name(input_dd_path, trailing=False, extension='drop')

            column_data = self.load_column_data(input_dd_path)

            columns_metadata = [
                {
                    "name": col_name_code,
                    "description": f'{{{{ doc("{self.generate_doc_block_name(src_table_key, col_name_code, table_prefix)}") }}}}',
                    "data_type": col_data_type,
                    **(
                        {"tests": self.format_tests(tests, col_name_code, enums)}
                        if tests is not None
                        else {}
                    ),
                }
                for _, col_name_code, _, col_data_type, enums, _, _, tests in column_data.get(
                    src_table_key, []
                )
            ]

            t_name = normalize_name([table_prefix, input_dd_path], trailing=False, extension='drop')
            model_entry = {
                "name": t_name,
                "description": f"Model for {t_name}.",
                "columns": columns_metadata,
            }

            models.append(model_entry)

        models = {"models": models}
        filepath = output_dir / "__models.yml"
        write_file(filepath, models, mode="merge")

    # def dbt_project_add_models(
    #     self, filepath: Path, table_names: list[str], dbtp_defs: dict
    # ) -> None:
    #     """
    #     Add top-level model defaults (+schema, +materialized)
    #     """
    #     filepath = filepath / "dbt_project.yml"
    #     existing = get_existing_yaml(filepath)

    #     models = existing.setdefault("models", {})
    #     study_models = models.setdefault(self.study_id, {})

    #     for key in ("+schema", "+materialized"):
    #         val = dbtp_defs.get(key)
    #         if val is not None and key not in study_models:
    #             study_models[key] = val

    #     # Add empty tables
    #     for table_id in table_names:
    #         study_models.setdefault(table_id, {})

    #     write_file(filepath, existing, mode="overwrite")

    def generate_sources_yml(self, input_dd_dir: Path, output_dir: Path):
        """
        Append source-table entries for this PipelineObject.
        Do not duplicate existing tables.
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

        dd_path = input_dd_dir / self.dd_identifier
        src_table_key = dd_path.stem
        column_data = self.load_column_data(dd_path)
        for filename in self.df_identifiers:
            table_name = normalize_name(filename, trailing=True, extension="drop")

            columns_metadata = [
                {
                    "name": col_name,
                    "description": (
                        f'{{{{ doc("{self.generate_doc_block_name(table_name, col_name_code, self.src_table_prefix)}") }}}}'
                    ),
                }
                for col_name, col_name_code, _, _, _, _, _, _ in column_data.get(
                    src_table_key, []
                )
            ]

            existing_table_names = {t["name"] for t in tables}

            # for df_identifier in self.df_identifiers:
            table_name = Path(filename).stem

            if table_name in existing_table_names:
                logger.debug(f"No updates needed: {filepath}")
                continue

            tables.append(
                {
                    "name": table_name,
                    "description": f"Source table for {table_name}.",
                    "columns": columns_metadata,
                }
            )

        write_file(filepath, existing, mode="overwrite")

    def generate_study_column_descriptions(
        self, table_prefix, input_dd_dir, output_dir
    ):
        """Generates a separate column_descriptions.md for each DF table in its docs directory."""

        output_filepath = output_dir / "column_descriptions.md"

        if not output_filepath.exists():
            output_filepath.touch()

        existing_data = output_filepath.read_text().rstrip()

        existing_col_doc_ids = set(
            re.findall(r"\{%\s*docs\s+([\w\d_]+)\s*%\}", existing_data)
        )

        new_descriptions = []

        for data_file_name in self.df_identifiers:

            dd_filepath = input_dd_dir / self.dd_identifier
            src_table_key = normalize_name(
                dd_filepath, trailing=False, extension="drop"
            )
            column_data = self.load_column_data(dd_filepath)
            table_name = normalize_name(data_file_name, trailing=True, extension="drop")

            for _, col_name_code, col_description, *_ in column_data.get(src_table_key, []):
                col_doc_id = self.generate_doc_block_name(
                    table_name, col_name_code, table_prefix
                )

                # skip if this column doc already exists
                if col_doc_id in existing_col_doc_ids:
                    continue

                col_desc_block = (
                    f"{{% docs {col_doc_id} %}}\n{col_description}\n{{% enddocs %}}"
                )

                new_descriptions.append(col_desc_block)
                existing_col_doc_ids.add(col_doc_id)

        new_data = "\n\n".join(new_descriptions).strip()

        if new_data:
            data = existing_data + "\n" + new_data if existing_data else new_data
            write_file(output_filepath, data, mode="overwrite")
        else:
            logger.debug(f"No updates needed: {output_filepath}")

    def generate_static_column_descriptions(
        self, table_prefix, config, input_dd_dir, output_dir
    ):
        """Generates a separate column_descriptions.md for each DF table in its docs directory."""

        output_filepath = output_dir / "column_descriptions.md"

        if not output_filepath.exists():
            output_filepath.touch()

        existing_data = output_filepath.read_text().rstrip()

        existing_col_doc_ids = set(
            re.findall(r"\{%\s*docs\s+([\w\d_]+)\s*%\}", existing_data)
        )

        new_descriptions = []
        for key, value in config.data_dictionary.items():
            dd_filename = value.identifier

            dd_filepath = input_dd_dir / dd_filename
            src_table_key = normalize_name(
                dd_filepath, trailing=False, extension="drop"
            )
            column_data = self.load_column_data(dd_filepath)

            for _, col_name_code, col_description, *_ in column_data.get(
                src_table_key, []
            ):
                col_doc_id = self.generate_doc_block_name(
                    src_table_key, col_name_code, table_prefix
                )

                # skip if this column doc already exists
                if col_doc_id in existing_col_doc_ids:
                    continue

                col_desc_block = (
                    f"{{% docs {col_doc_id} %}}\n{col_description}\n{{% enddocs %}}"
                )

                new_descriptions.append(col_desc_block)
                existing_col_doc_ids.add(col_doc_id)

        new_data = "\n\n".join(new_descriptions).strip()

        if new_data:
            data = existing_data + "\n" + new_data if existing_data else new_data
            write_file(output_filepath, data, mode="overwrite")
        else:
            logger.debug(f"No updates needed: {output_filepath}")

    # def generate_model_descriptions(self, output_dir):
    #     """Generates model_descriptions.md using the specified format."""
    #     model_descriptions = []

    #     # Group tables by prefix (e.g., "moo_src_", "moo_stg_")
    #     grouped_tables = {}
    #     for table_id, table_info in self.data_dictionary.items():
    #         prefix = table_id.split("_")[0]  # Assumes prefix is the first part of table_id
    #         grouped_tables.setdefault(prefix, []).append((table_id, table_info))

    #     for prefix, tables in grouped_tables.items():
    #         model_descriptions.append(f"### {prefix.capitalize()} Models\n")

    #         for table_id, table_info in tables:
    #             src_table_id = self.get_src_table_key(table_id)
    #             src_description = table_info.get(
    #                 "description", f"Model for {src_table_id}."
    #             )
    #             model_descriptions.append(
    #                 f"{{% docs {src_table_id} %}}\n{src_description}\n{{% enddocs %}}\n"
    #             )

    #             stg_table_id = f"{self.study_id}_stg_{table_id}"
    #             stg_description = table_info.get("description", f"Model for {stg_table_id}.")
    #             model_descriptions.append(f"{{% docs {stg_table_id} %}}\n{stg_description}\n{{% enddocs %}}\n")

    #             data = "\n".join(model_descriptions)

    #             filepath = output_dir / "model_descriptions.md"

    #             write_file(filepath, data)


if __name__ == "__main__":
    import doctest

    doctest.testmod(verbose=True)
