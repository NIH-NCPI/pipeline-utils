from dbt_pipeline_utils.scripts.helpers.general import *
from pathlib import Path

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

    print(f"Copied '{src_dir}' to '{dest_dir}'")
