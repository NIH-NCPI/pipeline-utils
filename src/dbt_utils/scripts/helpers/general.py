import os
import yaml
import pandas as pd
import importlib.resources as pkg_resources

def read_file(filepath, pkg_file=False):

    if pkg_file:
        package_root = pkg_resources.files("dbt_utils")
        if not isinstance(filepath, str):  
            raise TypeError(f"Expected a string for filepath, but got {type(filepath)}")  
        filepath = package_root / filepath

    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File does not exist: {filepath}")

    file_ext = os.path.splitext(filepath)[-1].lower()

    if file_ext in [".yaml", ".yml"]:
        print(f"Reading YAML from file: {filepath}")
        with open(filepath, "r") as file:
            data = yaml.safe_load(file)
        print("YAML data loaded.")
        return data

    elif file_ext == ".csv":
        print(f"Reading CSV from file: {filepath}")
        data = pd.read_csv(filepath)
        print("CSV data loaded.")
        return data

    else:
        raise ValueError(f"Unsupported file type: {file_ext}")

    
def write_file(target_dir, filename, data):
    """Creates a directory for the table and writes a YAML, SQL, BASH, or Markdown file based on the extension."""
    
    # Ensure the directory exists
    os.makedirs(target_dir, exist_ok=True)

    # Define the output file path
    output_file = os.path.join(target_dir, filename)

    # Determine the file type and write accordingly
    file_extension = os.path.splitext(filename)[-1].lower()

    with open(output_file, "w", encoding="utf-8") as file:
        if file_extension == ".yml":
            yaml.dump(data, file, default_flow_style=False, sort_keys=False, indent=4)
        elif file_extension in [".sql", ".md", ".sh"]:
            file.write(data)
        else:
            raise ValueError(f"Unsupported file type: {file_extension}")

    print(f"Generated: {output_file}")