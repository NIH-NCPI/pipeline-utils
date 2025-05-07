import os
import yaml
import pandas as pd
import dbt_pipeline_utils
from pathlib import Path
from dbt_pipeline_utils.scripts.helpers.common import *

from dbt_pipeline_utils import logger


def read_file(filepath):
    if not os.path.exists(filepath):
        logger.warning(f"File does not exist: {filepath}")
        return
    
    file_handlers = {
        ".yaml": lambda: yaml.safe_load(open(filepath, "r")),
        ".yml": lambda: yaml.safe_load(open(filepath, "r")),
        ".csv": lambda: pd.read_csv(filepath, header=0),
        ".xlsx": lambda: pd.read_excel(filepath, header=0),
        ".sql": Path(filepath).read_text
    }

    file_ext = os.path.splitext(filepath)[-1].lower()

    if file_ext not in file_handlers:
        raise ValueError(f"Unsupported file type: {file_ext}")

    logger.debug(f"Reading {file_ext} from file: {filepath}")
    data = file_handlers[file_ext]()

    logger.debug(f"Read {filepath} successful")
    return data


def write_file(filepath, data):
    """Creates a directory for the table and writes a YAML, SQL, BASH, or Markdown file based on the extension."""

    # Ensure the directory exists
    if not filepath.parent.exists():
        filepath.parent.mkdir(parents=True, exist_ok=True)

    file_extension = filepath.suffix

    # Stops the overwrite of existing sql files
    if file_extension == ".sql":
        if filepath.is_file():
            logger.info(f"File: {filepath.name} exists. Delete the existing file, before generating a new one.")
            return

    file_handlers = {
        ".yaml": lambda: yaml.dump(data, open(filepath, "w", encoding="utf-8"), default_flow_style=False, sort_keys=False, indent=2),
        ".yml": lambda: yaml.dump(data, open(filepath, "w", encoding="utf-8"), default_flow_style=False, sort_keys=False, indent=2),
        ".csv": lambda: data.to_csv(filepath, index=False),
        ".sql": lambda: open(filepath, "w", encoding="utf-8").write(data),
        ".md": lambda: open(filepath, "w", encoding="utf-8").write(data),
        ".sh": lambda: open(filepath, "w", encoding="utf-8").write(data),
    }

    if file_extension not in file_handlers:
        raise ValueError(f"Unsupported file type: {file_extension}")
    
    logger.debug(f"Writing {file_extension} to file: {filepath}")
    file_handlers[file_extension]()


    logger.info(f"Generated: {Path(filepath).name}")

def extract_columns(dd, format):
    """
    Returns None where data is not available

    """
    column_map = DD_FORMATS[format]  # Define dd column expectations

    column_data_list = [
        (
            row[column_map["variable_name"]],
            row[column_map["formatted_variable_name"]].lower().replace(" ", "_"),
            row.get(column_map["description"]),
            row.get(column_map["data_type"]),
            row.get(column_map["src_variable_name"]),
        )
        for _, row in dd.iterrows()
    ]

    return column_data_list

def get_paths(study_id, project_id, src_data_path=None):
    """
    For automatic validation of dir path creation, end the dir variables with "dir"
    """
    # dbt profiles path

    profiles_path = Path.home() / ".dbt/profiles.yml"

    # dbt_pipeline_utils paths
    utils_root_dir = Path(dbt_pipeline_utils.__file__).resolve().parent
    utils_ftd_study_data_dir = utils_root_dir / Path("data/ftd_data_dictionaries")
    utils_ftd_tgta = utils_root_dir / Path("data/tgt_consensus_a")
    utils_ftd_study_yml_path =  utils_ftd_study_data_dir / 'ftd_study.yaml'

    # dbt project paths
    dbtp_root_dir = Path.cwd()
    dbtp_p_dir = dbtp_root_dir / Path(f"{project_id}")
    dbtp_scripts_dir = dbtp_p_dir / Path("scripts")
    dbtp_sources_dir = dbtp_p_dir / Path("catalog/sources")
    dbtp_src_study_dir = dbtp_sources_dir / study_id
    dbtp_src_study_model_dir = dbtp_src_study_dir / Path("models")
    dbtp_src_study_model_docs_dir = dbtp_src_study_model_dir / Path("docs")

    dbtp_ftdc_dir = dbtp_p_dir / Path("catalog/ftd_consensus")
    dbtp_ftdc_models_dir = dbtp_p_dir / dbtp_ftdc_dir / Path("models")
    dbtp_ftdc_study_dir =  dbtp_ftdc_models_dir / study_id
    dbtp_ftdc_study_docs_dir = dbtp_ftdc_study_dir / Path("docs")

    dbtp_tgt_a_dir = dbtp_p_dir / Path("catalog/tgt_consensus_a")
    tgt_docs_dir = dbtp_tgt_a_dir / Path("docs")
    tgt_models_dir = dbtp_tgt_a_dir / Path("models")
    tgt_models_docs_dir = tgt_models_dir / Path("docs")

    # src data filepaths
    # overwrite src_data_dir if a different dir is specified in the args
    src_data_dir = dbtp_root_dir / Path(f"data/{study_id}")
    if src_data_path is not None:
        src_data_dir = Path(f"{src_data_path}")

    study_yml_path = src_data_dir / f"{study_id}_study.yaml"
    ftd_study_data_dir = src_data_dir / Path("ftd_data_dictionaries")
    trans_study_data_dir = src_data_dir / Path("ftd_transformations")

    ftd_study_yml_path =  src_data_dir / 'ftd_study.yaml'

    return {
        "profiles_path": profiles_path,
        "utils_root_dir": utils_root_dir,
        "utils_ftd_study_data_dir": utils_ftd_study_data_dir,
        "utils_ftd_study_yml_path": utils_ftd_study_yml_path,
        "utils_ftd_tgta": utils_ftd_tgta,
        "dbtp_root_dir": dbtp_root_dir,
        "dbtp_p_dir": dbtp_p_dir,
        "dbtp_scripts_dir": dbtp_scripts_dir,
        "dbtp_sources_dir": dbtp_sources_dir,
        "dbtp_src_study_dir": dbtp_src_study_dir,
        "dbtp_src_study_model_dir": dbtp_src_study_model_dir,
        "dbtp_src_study_model_docs_dir": dbtp_src_study_model_docs_dir,
        "dbtp_ftdc_dir": dbtp_ftdc_dir,
        "dbtp_ftdc_models_dir": dbtp_ftdc_models_dir,
        "dbtp_ftdc_study_dir": dbtp_ftdc_study_dir,
        "dbtp_ftdc_study_docs_dir":dbtp_ftdc_study_docs_dir,
        "dbtp_tgt_a_dir": dbtp_tgt_a_dir,
        "tgt_docs_dir": tgt_docs_dir,
        "tgt_models_dir": tgt_models_dir,
        "tgt_models_docs_dir": tgt_models_docs_dir,
        "src_data_dir": src_data_dir,
        "study_yml_path": study_yml_path,
        "ftd_study_data_dir": ftd_study_data_dir,
        "trans_study_data_dir":trans_study_data_dir,
        "ftd_study_yml_path": ftd_study_yml_path
    }


def validate_paths(paths_dict):
    print('Starting dir path validation')
    for key, path in paths_dict.items():
        if not path.exists():
            print(f"Warning: {key} does not exist - {path}")
    print('SUCCESS: End dir path validation')


def create_model_table_abs_path(study_id, base_dir, table):
    paths = get_paths(study_id)
    t = Path(table)
    if base_dir == 'src':
        table_path = paths["dbtp_src_study_model_dir"] / t
    elif base_dir == 'ftd':
        table_path = paths["dbtp_ftdc_study_dir"] / t
    else:
        print(f"create_model_table_path does not recognize {base_dir}. Choices ['src','ftd']")

    abs_table_path = table_path.resolve()
    return abs_table_path

def load_src_column_data(data_dictionary, src_dd_path, study_id, src_only=None):
    """Loads column names, descriptions, and data types from CSV files and stores them in a dictionary."""
    column_data = {}

    for table_id, table_info in data_dictionary.items():

        ddict_full_path, ddict = get_src_ddict_path(src_dd_path, table_info)

        dd_format = table_info.get("format")
        src_df = read_file(ddict_full_path)

        src_table_key = f"{study_id}_src_{table_id}"
        column_data[src_table_key] = extract_columns(src_df, dd_format)

        if not src_only:

            stg_ddict = table_info.get("stg_src_table_id")
            stg_ddict_full_path = src_dd_path / stg_ddict
            stg_df = read_file(stg_ddict_full_path)

            dd_format = table_info.get("format")

            stg_table_key = f"{study_id}_stg_{table_id}"
            column_data[stg_table_key] = extract_columns(stg_df, "pipeline_format")

    return column_data


def load_ftd_column_data(data_dictionary, src_dd_path, ftd_dd, ftd_study_path, study_id):
    """Loads column names, descriptions, and data types from CSV files and stores them in a dictionary."""
    column_data = {}

    for table_id, table_info in data_dictionary.items():
        stg_table_key = f"{study_id}_stg_{table_id}"

        ddict = table_info.get("stg_src_table_id")
        ddict_full_path = src_dd_path / ddict
        df = read_file(ddict_full_path)
        df = df.astype(str).fillna("FTD_UNKNOWN")

        column_data[stg_table_key] = extract_columns(df, "pipeline_format")

    for table_id, table_info in ftd_dd.items():
        table_key = f"{study_id}_ftd_{table_id}"

        ddict = table_info.get("identifier")
        ddict_full_path = ftd_study_path / ddict
        df = read_file(ddict_full_path)
        df = df.astype(str).fillna("FTD_UNKNOWN")

        column_data[table_key] = extract_columns(df, "pipeline_format")

    return column_data

def generate_basic_dbt_project_yml(filepath, name, default_profile):

    # Correct structure for dbt_project.yml
    dbt_config = {
        "name": name,
        "version": "1.0.0",
        "profile": default_profile,
        "model-paths": ["models"],
        "macro-paths": ["macros"],
        "snapshot-paths": ["snapshots"],
        "clean-targets": ["target", "dbt_packages"],
    }

    filepath = filepath / "dbt_project.yml"

    write_file(filepath, dbt_config)



def get_src_ddict_path(src_dd_path, table_info):
        if table_info.get("import_type") == 'synapse':
            ddict = table_info.get("src_file_id")

        if table_info.get("import_type") == 'pg':
            ddict = table_info.get("identifier")

        if table_info.get("import_type") == 'duckdb':
            ddict = table_info.get("identifier")

        if table_info.get("import_type") not in ['pg', 'duckdb', 'synapse']:
            logger.error(f"{table_info.get("import_type")} is not valid")
            
        return src_dd_path / Path(f"{ddict}"), ddict