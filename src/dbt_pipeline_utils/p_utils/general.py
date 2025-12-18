import os
import yaml
import re
import pandas as pd
import dbt_pipeline_utils
from pathlib import Path
# from dbt_pipeline_utils.p_utils..common import *

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


def write_file(
    filepath: Path,
    data,
    *,
    mode: str = "overwrite",
) -> None:
    filepath = Path(filepath)

    logger.debug(f"write_file '{mode}' mode. file: '{tail_path(filepath,depth=3)}'")

    try:
        filepath.parent.mkdir(parents=True, exist_ok=True)

        suffix = filepath.suffix.lower()

        if mode not in {"create", "overwrite", "merge"}:
            raise ValueError(f"Invalid mode: {mode}")

        # CREATE
        if mode == "create" and filepath.exists():
            logger.debug(f"File exists, skipping create: {filepath}")
            return

        # YAML FILES
        if suffix in {".yml", ".yaml"}:
            if mode == "merge" and filepath.exists():
                with filepath.open("r", encoding="utf-8") as f:
                    existing = yaml.safe_load(f) or {}

                if not isinstance(existing, dict):
                    raise ValueError(f"Cannot merge into non-dict YAML: {filepath}")

                data = deep_merge(existing, data)

            with filepath.open("w", encoding="utf-8") as f:
                yaml.safe_dump(
                    data,
                    f,
                    sort_keys=False,
                    default_flow_style=False,
                    indent=2,
                )
            return

        # TEXT FILES
        if suffix in {".md", ".sh"}:
            file_mode = "a" if mode == "merge" else "w"
            with filepath.open(file_mode, encoding="utf-8") as f:
                f.write(data)
            return

        # SQL FILES - Don't overwrite.
        if suffix in {".sql"} and filepath.exists():
            return
        elif suffix in {".sql"} and not filepath.exists():
            file_mode = "w"
            with filepath.open(file_mode, encoding="utf-8") as f:
                f.write(data)
            return

        # CSV
        if suffix == ".csv":
            data.to_csv(filepath, index=False)
            return

        raise ValueError(f"Unsupported file type: {suffix}")

    except Exception as e:
        logger.exception(
            "Unexpected error in write_file",
            extra={
                "path": str(filepath),
                "mode": mode,
            },
        )
        raise

def deep_merge(existing: dict, incoming: dict) -> dict:
    for key, value in incoming.items():
        if (
            key in existing
            and isinstance(existing[key], dict)
            and isinstance(value, dict)
        ):
            deep_merge(existing[key], value)
        else:
            existing[key] = value
    return existing


def tail_path(path: Path, depth: int = 2) -> str:
    parts = path.parts
    return Path(*parts[-(depth + 1) :]).as_posix()

def create_model_table_abs_path(study_id, base_dir, table):
    paths = get_paths(study_id)
    t = Path(table)
    if base_dir == 'src':
        table_path = paths["dbtp_src_study_model_dir"] / t
    elif base_dir == 'int':
        table_path = paths["dbtp_intc_study_dir"] / t
    else:
        logger.error(f"create_model_table_path does not recognize {base_dir}. Choices ['src','int']")

    abs_table_path = table_path.resolve()
    return abs_table_path

def get_existing_yaml(filepath):

    if filepath.exists():
        with open(filepath, "r") as f:
            existing_data = yaml.safe_load(f) or {}
    else:
        existing_data = {}

    return existing_data


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
