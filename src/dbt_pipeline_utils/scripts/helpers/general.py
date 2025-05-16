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


def write_file(filename, data, overwrite=False):
    """Creates a directory for the table and writes a YAML, SQL, BASH, or Markdown file based on the extension."""

    # Ensure the directory exists
    if not filename.parent.exists():
        filename.parent.mkdir(parents=True, exist_ok=True)

    file_extension = filename.suffix

    # Stops the overwrite of existing sql files
    if not overwrite:
        if filename.is_file():
            parent_dirs = filename.parent.parent.parent.parent
            logger.debug(f"File: {filename.relative_to(parent_dirs)} exists. Delete the existing file, before generating a new one.")
            return

    file_handlers = {
        ".yaml": lambda: yaml.dump(data, open(filename, "w", encoding="utf-8"), default_flow_style=False, sort_keys=False, indent=2),
        ".yml": lambda: yaml.dump(data, open(filename, "w", encoding="utf-8"), default_flow_style=False, sort_keys=False, indent=2),
        ".csv": lambda: data.to_csv(filename, index=False),
        ".sql": lambda: open(filename, "w", encoding="utf-8").write(data),
        ".md": lambda: open(filename, "w", encoding="utf-8").write(data),
        ".sh": lambda: open(filename, "w", encoding="utf-8").write(data),
    }

    if file_extension not in file_handlers:
        raise ValueError(f"Unsupported file type: {file_extension}")
    
    logger.debug(f"Writing {file_extension} to file: {filename}")
    file_handlers[file_extension]()

    logger.debug(f"Generated: {Path(filename).name}")


def get_paths(study_id, project_id, tgt_model_id=None, src_data_path=None):
    """
    For automatic validation of dir path creation, end the dir variables with "dir"
    """
    # dbt profiles path

    profiles_path = Path.home() / ".dbt/profiles.yml"

    # dbt_pipeline_utils paths
    utils_root_dir = Path(dbt_pipeline_utils.__file__).resolve().parent

    # dbt project paths
    dbtp_root_dir = Path.cwd()
    dbtp_p_dir = dbtp_root_dir / Path(f"{project_id}")
    dbtp_catalog_dir = dbtp_p_dir / Path("catalog")
    dbtp_scripts_dir = dbtp_p_dir / Path("scripts")
    dbtp_sources_dir = dbtp_p_dir / Path("catalog/sources")
    dbtp_src_study_dir = dbtp_sources_dir / study_id
    dbtp_src_study_model_dir = dbtp_src_study_dir / Path("models")
    dbtp_src_study_model_docs_dir = dbtp_src_study_model_dir / Path("docs")

    dbtp_ftdc_dir = dbtp_p_dir / Path("catalog/ftd_consensus")
    dbtp_ftdc_models_dir = dbtp_p_dir / dbtp_ftdc_dir / Path("models")
    dbtp_ftdc_study_dir =  dbtp_ftdc_models_dir / study_id
    dbtp_ftdc_study_docs_dir = dbtp_ftdc_study_dir / Path("docs")

    dbtp_tgt_dir = dbtp_p_dir / Path(f"catalog/{tgt_model_id}")
    tgt_docs_dir = dbtp_tgt_dir / Path("docs")
    tgt_models_dir = dbtp_tgt_dir / Path("models")
    tgt_models_docs_dir = tgt_models_dir / Path("docs")

    # src data filepaths
    # overwrite src_data_dir if a different dir is specified in the args
    src_data_dir = dbtp_root_dir / Path(f"data/{study_id}")
    if src_data_path is not None:
        src_data_dir = Path(f"{src_data_path}")

    study_yml_path = src_data_dir / f"{study_id}_study.yaml"
    ftd_study_data_dir = src_data_dir / Path("ftd_data_dictionaries")
    trans_study_data_dir = src_data_dir / Path("ftd_transformations")

    # static data paths
    static_data_dir = dbtp_root_dir / Path(f"data/static")
    ftd_static_data_dir = static_data_dir / Path("ftd_data_dictionaries")
    tgt_static_data_dir = static_data_dir / Path(f"{tgt_model_id}")

    ftd_study_yml_path =  src_data_dir / 'ftd_study.yaml'

    return {
        "profiles_path": profiles_path,
        "utils_root_dir": utils_root_dir,
        "dbtp_root_dir": dbtp_root_dir,
        "dbtp_p_dir": dbtp_p_dir,
        "dbtp_catalog_dir": dbtp_catalog_dir,
        "dbtp_scripts_dir": dbtp_scripts_dir,
        "dbtp_sources_dir": dbtp_sources_dir,
        "dbtp_src_study_dir": dbtp_src_study_dir,
        "dbtp_src_study_model_dir": dbtp_src_study_model_dir,
        "dbtp_src_study_model_docs_dir": dbtp_src_study_model_docs_dir,
        "dbtp_ftdc_dir": dbtp_ftdc_dir,
        "dbtp_ftdc_models_dir": dbtp_ftdc_models_dir,
        "dbtp_ftdc_study_dir": dbtp_ftdc_study_dir,
        "dbtp_ftdc_study_docs_dir":dbtp_ftdc_study_docs_dir,
        "dbtp_tgt_dir": dbtp_tgt_dir,
        "tgt_docs_dir": tgt_docs_dir,
        "tgt_models_dir": tgt_models_dir,
        "tgt_models_docs_dir": tgt_models_docs_dir,
        "src_data_dir": src_data_dir,
        "study_yml_path": study_yml_path,
        "ftd_study_data_dir": ftd_study_data_dir,
        "trans_study_data_dir":trans_study_data_dir,
        "ftd_static_data_dir":ftd_static_data_dir,
        "tgt_static_data_dir":tgt_static_data_dir,
        "ftd_study_yml_path": ftd_study_yml_path
    }


def validate_paths(paths_dict):
    logger.debug('Starting dir path validation')
    for key, path in paths_dict.items():
        if not path.exists():
            logger.warning(f"Warning: {key} does not exist - {path}")
    logger.debug('SUCCESS: End dir path validation')


def create_model_table_abs_path(study_id, base_dir, table):
    paths = get_paths(study_id)
    t = Path(table)
    if base_dir == 'src':
        table_path = paths["dbtp_src_study_model_dir"] / t
    elif base_dir == 'ftd':
        table_path = paths["dbtp_ftdc_study_dir"] / t
    else:
        logger.error(f"create_model_table_path does not recognize {base_dir}. Choices ['src','ftd']")

    abs_table_path = table_path.resolve()
    return abs_table_path


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


def copy_directory(src_dir, dest_dir):
    """
    Recursively copies files and subdirectories from src_dir to dest_dir
    """

    for item in src_dir.rglob("*"):
        relative_path = item.relative_to(src_dir)
        target = dest_dir / relative_path

        if item.is_dir():
            target.mkdir(parents=True, exist_ok=True)
        else:
            # Copy file contents manually
            data = read_file(item)
            write_file(target, data)        
            
            logger.debug(f"Copied '{src_dir}' to '{dest_dir}'")