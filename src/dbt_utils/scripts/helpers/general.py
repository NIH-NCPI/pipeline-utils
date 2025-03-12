import os
import yaml
import pandas as pd
import dbt_utils
from dbt_utils.scripts.helpers import common
from pathlib import Path


def read_file(filepath):

    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File does not exist: {filepath}")

    file_ext = os.path.splitext(filepath)[-1].lower()

    if file_ext in [".yaml", ".yml"]:
        print(f"Reading YAML from file: {filepath}")
        with open(filepath, "r") as file:
            data = yaml.safe_load(file)
        print(f"YAML data {filepath} loaded.")
        return data

    elif file_ext == ".csv":
        print(f"Reading CSV from file: {filepath}")
        data = pd.read_csv(filepath, header=0)
        print(f"CSV data {filepath} loaded.")
        return data

    else:
        raise ValueError(f"Unsupported file type: {file_ext}")

    
def write_file(filepath, data):
    """Creates a directory for the table and writes a YAML, SQL, BASH, or Markdown file based on the extension."""
    
    # Ensure the directory exists
    if not filepath.parent.exists():
        filepath.parent.mkdir(parents=True, exist_ok=True)

    # Determine the file type and write accordingly
    file_extension = filepath.suffix

    with open(filepath, "w", encoding="utf-8") as file:
        if file_extension in [".yml",".yaml"]:
            yaml.dump(data, file, default_flow_style=False, sort_keys=False, indent=2)
        elif file_extension in [".sql", ".md", ".sh"]:
            file.write(data)
        elif file_extension == ".csv":
            data.to_csv(filepath, index=False)
        else:
            raise ValueError(f"Unsupported file type: {file_extension}")


    print(f"Generated: {filepath}")

def get_paths(study_id):

    # dbt_utils paths
    utils_root_dir = Path(dbt_utils.__file__).resolve().parent
    utils_ftd_study_data_dir = utils_root_dir / Path("data/ftd_data_dictionaries")
    utils_ftd_study_yml_path =  utils_ftd_study_data_dir / 'ftd_study.yaml'

    # dbt project paths
    dbtp_root_dir = Path.cwd()
    dbtp_scripts_dir = dbtp_root_dir / Path("scripts")
    dbtp_sources_dir = dbtp_root_dir / Path("catalog/sources")
    dbtp_src_study_dir = dbtp_sources_dir / study_id
    dbtp_src_study_model_dir = dbtp_src_study_dir / Path("models")
    dbtp_src_study_model_docs_dir = dbtp_src_study_model_dir / Path("docs")

    dbtp_ftdc_dir = dbtp_root_dir / Path("catalog/ftd_consensus")
    dbtp_ftdc_models_dir = dbtp_root_dir / dbtp_ftdc_dir / Path("models")
    # dbtp_ftdc_model_docs_dir = dbtp_ftdc_models_dir / Path("docs")
    dbtp_ftdc_study_dir =  dbtp_ftdc_models_dir / study_id
    dbtp_ftdc_study_docs_dir = dbtp_ftdc_study_dir / Path("docs")


    dbtp_tgt_a_dir = dbtp_root_dir / Path("catalog/tgt_consensus_a")
    tgt_docs_dir = dbtp_tgt_a_dir / Path("docs")
    tgt_models_dir = dbtp_tgt_a_dir / Path("models")
    tgt_models_docs_dir = tgt_models_dir / Path("docs")

    # src data filepaths
    dbtp_study_data_dir = dbtp_root_dir / Path(f"data/{study_id}")
    study_yml_path = dbtp_study_data_dir / f"{study_id}_study.yaml"

    ftd_study_data_dir = dbtp_study_data_dir / Path("ftd_data_dictionaries")
    trans_study_data_dir = dbtp_study_data_dir / Path("ftd_transformations")

    ftd_study_yml_path =  ftd_study_data_dir / 'ftd_study.yaml'

    return {
        "utils_root_dir": utils_root_dir,
        "utils_ftd_study_data_dir": utils_ftd_study_data_dir,
        "utils_ftd_study_yml_path": utils_ftd_study_yml_path,
        "dbtp_root_dir": dbtp_root_dir,
        "dbtp_scripts_dir": dbtp_scripts_dir,
        "dbtp_sources_dir": dbtp_sources_dir,
        "dbtp_src_study_dir": dbtp_src_study_dir,
        "dbtp_src_study_model_dir": dbtp_src_study_model_dir,
        "dbtp_src_study_model_docs_dir": dbtp_src_study_model_docs_dir,
        "dbtp_ftdc_dir": dbtp_ftdc_dir,
        "dbtp_ftdc_models_dir": dbtp_ftdc_models_dir,
        "dbtp_ftdc_study_dir": dbtp_ftdc_study_dir,
        # "dbtp_ftdc_model_docs_dir": dbtp_ftdc_model_docs_dir,
        "dbtp_ftdc_study_docs_dir":dbtp_ftdc_study_docs_dir,
        "dbtp_tgt_a_dir": dbtp_tgt_a_dir,
        "tgt_docs_dir": tgt_docs_dir,
        "tgt_models_dir": tgt_models_dir,
        "tgt_models_docs_dir": tgt_models_docs_dir,
        "dbtp_study_data_dir": dbtp_study_data_dir,
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

def extract_columns(df):
    return [
        (
            row["variable_name"],
            row["variable_name"].lower().replace(" ", "_"),
            row.get("variable_description"),
            row.get("data_type").lower(),
            row.get("src_variable_name")
        )
        for _, row in df.iterrows()
    ]

def load_src_column_data(data_dictionary, src_study_path, study_id, raw_only=None):
    """Loads column names, descriptions, and data types from CSV files and stores them in a dictionary."""
    column_data = {}

    for table_id, table_info in data_dictionary.items():

        ddict = table_info.get("table_details")
        ddict_full_path = src_study_path / ddict
        raw_df = read_file(ddict_full_path)

        raw_table_key = f"{study_id}_raw_{table_id}"
        column_data[raw_table_key] = extract_columns(raw_df)

        if not raw_only:

            stg_ddict = table_info.get("stg_table_details")
            stg_ddict_full_path = src_study_path / stg_ddict
            stg_df = read_file(stg_ddict_full_path)

            stg_table_key = f"{study_id}_stg_{table_id}"
            column_data[stg_table_key] = extract_columns(stg_df)

    return column_data

def load_ftd_column_data(data_dictionary, src_study_path, ftd_dd, ftd_study_path, study_id):
    """Loads column names, descriptions, and data types from CSV files and stores them in a dictionary."""
    column_data = {}

    for table_id, table_info in data_dictionary.items():
        stg_table_key = f"{study_id}_stg_{table_id}"

        ddict = table_info.get("stg_table_details")
        ddict_full_path = src_study_path / ddict
        df = read_file(ddict_full_path)
        df = df.astype(str).fillna("FTD_UNKNOWN")

        column_data[stg_table_key] = extract_columns(df)

    for table_id, table_info in ftd_dd.items():
        table_key = f"{study_id}_ftd_{table_id}"

        ddict = table_info.get("table_details")
        ddict_full_path = ftd_study_path / ddict
        df = read_file(ddict_full_path)
        df = df.astype(str).fillna("FTD_UNKNOWN")


        column_data[table_key] = extract_columns(df)

    return column_data