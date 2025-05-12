from dbt_pipeline_utils.scripts.helpers.common import *
from dbt_pipeline_utils.scripts.helpers.general import *
import re

class DocGeneration():
    """Base class for defining pipeline stages."""
    
    def extract_columns(self, df, dd_format):
        """
        Extracts relevant column information based on the dictionary format.
        """

        column_map = DD_FORMATS[dd_format]  # Define dd column expectations
        
        column_data_list = [
            (
                row[column_map["variable_name"]],
                row[column_map["formatted_variable_name"]].lower().replace(" ", "_"),
                row.get(column_map["description"]),
                row.get(column_map["data_type"]),
                row.get(column_map["src_variable_name"]),
            )
            for _, row in df.iterrows()
        ]

        return column_data_list
    
    def load_src_column_data(self, src_only=None):
        """Loads column names, descriptions, and data types from CSV files and stores them in a dictionary."""
        column_data = {}
        src_dd_path = self.paths["src_data_dir"]

        for table_id, table_info in self.data_dictionary.items():
            src_table_key = self.get_src_table_key(table_id)

            ddict_full_path, ddict = self.get_src_ddict_path(table_info)

            dd_format = table_info.get("format")
            src_df = read_file(ddict_full_path)

            column_data[src_table_key] = self.extract_columns(src_df, dd_format)

            if not src_only:

                stg_ddict = table_info.get("stg_src_table_id")
                stg_ddict_full_path = src_dd_path / stg_ddict
                stg_df = read_file(stg_ddict_full_path)

                dd_format = table_info.get("format")

                stg_table_key = f"{self.study_id}_stg_{table_id}"
                column_data[stg_table_key] = self.extract_columns(stg_df, "pipeline_format")

        return column_data


    def generate_dbt_project_yaml(self):
        study_info = {}

        for table_id in self.data_dictionary.keys():
            stg_table_id = f"{self.study_id}_stg_{table_id}"

            study_info[table_id] = {
                stg_table_id: {
                    "+schema": f"{self.study_id}_data",
                    "+materialized": "table",
                },
            }

        # Correct structure for dbt_project.yml
        dbt_config = {
            "name": self.study_id,
            "version": "1.0.0",
            "profile": self.project_id,
            "model-paths": ["models"],
            "macro-paths": ["macros"],
            "snapshot-paths": ["snapshots"],
            "clean-targets": ["target", "dbt_packages"],
            "models": {
                "+schema": f"{self.study_id}_data",
                "+materialized": "table",
                **study_info,
            },
        }

        filepath = self.paths["dbtp_src_study_dir"] / "dbt_project.yml"

        write_file(filepath, dbt_config)


    def generate_dbt_models_yml(self, column_data, output_dir, ftd_model=None):
        """
        Generates dbt models.yml file for each table in its respective directory, including src and staging models.
        """
        if ftd_model:
            data_dictionary = self.ftd_dd
            ftd_models = []
        if not ftd_model:
            data_dictionary = self.data_dictionary

        for table_id, table_info in data_dictionary.items():
            src_models = []
            if ftd_model:
                table_models_dir = output_dir 
                id_tables = [f"{self.study_id}_ftd_{table_id}"]
            if not ftd_model:
                table_models_dir = output_dir / Path(f"{table_id}")
                # id_tables = [f"{self.study_id}_src_{table_id}", f"{self.study_id}_stg_{table_id}"]
                # if self.table_info['import_type'] == 'duckdb':
                id_tables = [f"{self.study_id}_stg_{table_id}"]

            for table_name in id_tables:

                columns_metadata = [
                    {
                        "name": col_name,
                        "description": f'{{{{ doc("{table_name}_{col_name_code}") }}}}',
                        "data_type": col_data_type,
                    }
                    for col_name, col_name_code, _, col_data_type, _ in column_data.get(
                        table_name, []
                    )
                ]

                model_entry = {
                    "name": table_name,
                    "description": table_info.get("description", f"Model for {table_name}."),
                    "columns": columns_metadata
                }

                if ftd_model:
                    ftd_models.append(model_entry)
                else:
                    src_models.append(model_entry)

            if not ftd_model:
                models = {"version": 2, "models": src_models}
                filepath = table_models_dir / "__models.yml"
                write_file(filepath, models, overwrite=True)

        if ftd_model:
            # Write all ftd models at once
            models = {
                "version": 2,
                "models": ftd_models
            }
            filepath = self.paths["dbtp_ftdc_study_docs_dir"] / "__models.yml"
            write_file(filepath, models, overwrite=True)

    def generate_dbt_sources_yml(self, column_data, output_dir):
        source_tables = []

        for table_id, table_info in self.data_files.items():
            src_filename = Path(table_info['identifier']).stem
            columns_metadata = [
                {
                    "name": col_name,
                    "description": f'{{{{ doc("{src_filename}_{col_name_code}") }}}}'
                }

                for col_name, col_name_code, _, _, _ in column_data.get(f"{src_filename}", [])
            ]


            source_tables.append({
                "name": src_filename,
                "description": table_info.get("description", f"Source table for {src_filename}."),
                "columns": columns_metadata
            })

        sources_yaml = {
            "version": 2,
            "sources": [
                {
                    "name": self.study_id,
                    "schema": self.src_schema,
                    "tables": source_tables
                }
            ]
        }

        filepath = output_dir / "sources.yml"
        write_file(filepath, sources_yaml, overwrite=True)


    def generate_column_descriptions(self, column_data, output_dir, ftd_model=None):
        """Generates a separate column_descriptions.md for each table in its respective docs directory."""
        filepath = output_dir / "column_descriptions.md"
        if ftd_model:
            data_dictionary = self.ftd_dd
        if not ftd_model:
            data_dictionary = self.data_dictionary

        # Read existing file content if it exists
        if not filepath.exists():
            filepath.touch()
        existing_data = filepath.read_text().rstrip()

        # Use regex to extract existing col_doc_ids
        existing_col_doc_ids = set(re.findall(r"\{%\s*docs\s+([\w\d_]+)\s*%\}", existing_data))

        new_descriptions = []

        for table_id, table_info in data_dictionary.items():

            if ftd_model:
                # Only process the FTD table key
                table_keys = [f"{self.study_id}_ftd_{table_id}"]
            else:
                # Process both src and stg table keys in a single loop
                src_table_key = self.get_src_table_key(table_id)
                table_keys = [f"{src_table_key}", f"{self.study_id}_stg_{table_id}"]

            for table_key in table_keys:

                table_description = table_info.get("description", f"Model for {table_key}.")
                table_desc_id = f"{table_key}_description"
                table_desc_block = f"{{% docs {table_desc_id} %}}\n{table_description}\n{{% enddocs %}}\n"

                # Add table description if not already present
                if table_desc_id not in existing_col_doc_ids:
                    new_descriptions.append(table_desc_block)
                    existing_col_doc_ids.add(table_desc_id)

                for col_name, col_name_code, col_description, _, _ in column_data.get(table_key, []):
                    col_doc_id = f"{table_key}_{col_name_code}"
                    col_desc_block = f"{{% docs {col_doc_id} %}}\n{col_description}\n{{% enddocs %}}\n"

                    if col_doc_id not in existing_col_doc_ids:
                        new_descriptions.append(col_desc_block)
                        existing_col_doc_ids.add(col_doc_id)  # Ensure newly added col_doc_id is tracked

        new_data = "\n\n".join(new_descriptions).strip()

        if new_data:
            data = existing_data + "\n\n" + new_data if existing_data else new_data
            write_file(filepath, data, overwrite=True) # Needs to add any new data to an existing file.
        else:
            logger.info(f"No updates needed: {filepath}")


    def generate_model_descriptions(self, output_dir):
        """Generates model_descriptions.md using the specified format."""
        model_descriptions = []

        # Group tables by prefix (e.g., "moo_src_", "moo_stg_")
        grouped_tables = {}
        for table_id, table_info in self.data_dictionary.items():
            prefix = table_id.split("_")[0]  # Assumes prefix is the first part of table_id
            grouped_tables.setdefault(prefix, []).append((table_id, table_info))

        for prefix, tables in grouped_tables.items():
            model_descriptions.append(f"### {prefix.capitalize()} Models\n")

            for table_id, table_info in tables:
                src_table_id = self.get_src_table_key(table_id)
                src_description = table_info.get(
                    "description", f"Model for {src_table_id}."
                )
                model_descriptions.append(
                    f"{{% docs {src_table_id} %}}\n{src_description}\n{{% enddocs %}}\n"
                )

                stg_table_id = f"{self.study_id}_stg_{table_id}"
                stg_description = table_info.get("description", f"Model for {stg_table_id}.")
                model_descriptions.append(f"{{% docs {stg_table_id} %}}\n{stg_description}\n{{% enddocs %}}\n")

                data = "\n".join(model_descriptions)

                filepath = output_dir / "model_descriptions.md"

                write_file(filepath, data)


    def generate_src_sql_files(self, output_dir):
        """Generates SQL files dynamically for each table in its respective directory."""

        for table_id in self.data_dictionary.keys():
            src_table_id = self.get_src_table_key(table_id)
            sql_content = f"""{{{{ config(materialized='table') }}}}

    select * from {self.src_schema}.{table_id}
    """
            filepath = output_dir / Path(table_id) / f"{src_table_id}.sql"

            write_file(filepath, sql_content, overwrite=True)


    def generate_stg_sql_files(self, column_data, output_dir):
        """Generates staging SQL files dynamically for each table based on the data dictionary."""

        for table_id, table_info in self.data_dictionary.items():
            src_table = self.get_src_table_key(table_id)
            new_table = f"{self.study_id}_stg_{table_id}"
            filepath = output_dir / Path(table_id) / f"{new_table}.sql"

            column_definitions = []
            for col_name, column_name_code, _, col_data_type, _ in column_data.get(src_table, []):
                sql_type = type_mapping.get(col_data_type, "text")
                column_definitions.append(f'"{col_name}"::{sql_type} AS "{column_name_code}"')

            sql_content = f"""{{{{ config(materialized='table') }}}}

    with source as (
        select 
        {",\n       ".join(column_definitions)}
        from {{{{ source('{self.study_id}','{src_table}') }}}}
    )

    select 
        *,
        concat(study_code, '-', participant_global_id) as ftd_key
    from source
    """

            # Write SQL file to the correct directory
            write_file(filepath, sql_content)


    def generate_stg_dds(self):
        """Generates staging SQL files dynamically for each table based on the data dictionary.
        open the src dd and apply minimal transformations"""

        for table_id, table_info in self.data_dictionary.items():
            src_table_key = f"{self.study_id}_src_{table_id}"
            src_dd_path = self.paths["src_data_dir"]
            filepath = src_dd_path / f"{table_id}_stg_dd.csv"

            logger.info(f"Processing table: {table_id}")

            ddict_full_path, ddict = self.get_src_ddict_path(table_info)

            stg_df = read_file(ddict_full_path)

            column_data = self.load_src_column_data(src_only=True)

            column_mapping = {
                col_name: column_name_code
                for col_name, column_name_code, _, _, _ in column_data.get(src_table_key, [])
            }

            format_type = table_info.get("format")

            # Map from the original format to pipeline_format
            original_format_map = DD_FORMATS.get(format_type, {})
            pipeline_format_map = DD_FORMATS["pipeline_format"]

            # Convert src_variable_name based on the original format
            stg_df[pipeline_format_map["src_variable_name"]] = stg_df.get(original_format_map["variable_name"], "")

            # Convert variable_name field to match pipeline_format
            variable_name_key = original_format_map.get("variable_name", "variable_name")
            if variable_name_key in stg_df.columns:
                stg_df[pipeline_format_map["variable_name"]] = (
                    stg_df[variable_name_key].map(column_mapping).fillna(stg_df[variable_name_key])
                )

            # Rename all columns according to pipeline_format
            rename_map = {original_format_map[key]: pipeline_format_map[key] for key in pipeline_format_map if key in original_format_map}
            stg_df.rename(columns=rename_map, inplace=True)

            t_path = src_dd_path / Path(f"ftd_transformations/{table_id}_stg_additions_dd.csv")
            if t_path.exists():  
                transformations = read_file(t_path)
                stg_df = pd.concat([stg_df, transformations], ignore_index=True)
            else:
                pass

            write_file(filepath, stg_df)
            


