import json
from dbt_pipeline_utils.scripts.helpers.general import *
import subprocess

def generate_run_command(operation, model, args=None):
    """Generates a dbt run command for models or macros with optional arguments."""
    
    if operation == 'macro':
        if args:
            args_parts = ["--args '{"] + [f'"{k}": "{v}"' for k, v in args.items()] + ["}'"]
            all_args = " ".join(args_parts)
        else:
            all_args = ""
        
        op = f'dbt run-operation {model} {all_args}'.strip()
    
    elif operation == 'model':
        if args:
            args_parts = ['--vars "{'] + [f'{k}: {v}' for k, v in args.items()] + ['}"']
            all_args = " ".join(args_parts)
        else:
            all_args = ""
        
        op = f'dbt run --select {model} {all_args}'.strip()

    else:
        raise ValueError("Invalid operation type. Use 'macro' or 'model'.")
    
    return op


def generate_run_command(operation, model, args=None):
    """Generates a dbt run command for models or macros with optional arguments."""
    
    if operation == 'macro':
        all_args = f"--args '{' '.join(f'\"{k}\": \"{v}\"' for k, v in args.items())}'" if args else ""
        op = f'dbt run-operation {model} {all_args}'.strip()
    
    elif operation == 'model':
        all_args = f"--vars '{json.dumps(args)}'" if args else ""
        op = f'dbt run --select +{model} {all_args}'.strip()

    else:
        raise ValueError("Invalid operation type. Use 'macro' or 'model'.")
    
    return op

def generate_dbt_run_script(study_config, ftd_config, scripts_dir):
    """Generates a dbt run Bash script dynamically based on a YAML configuration."""
    study_id = study_config.get("study_id", "study")

    commands_list = [
    "#!/bin/bash",
    "set -e  # Exit on error",
    "dbt clean",
    'dbt deps || { echo "Error: dbt deps failed. Exiting..."; exit 1; }',
]

    # for table_id, table_info in study_config["data_dictionary"].items():
    #     src_table_id = f"{study_id}_src_{table_id}"
    #     stg_table_id = f"{study_id}_stg_{table_id}"

    #     commands_list.append(generate_run_command("model", src_table_id))
    #     commands_list.append(generate_run_command("model", stg_table_id))

    # commands_list.append("# Run FTD tables")

    # for table_id, table_info in ftd_config["data_dictionary"].items():
    #     commands_list.append(generate_run_command("model", f"{study_id}_ftd_{table_id}"))

    commands_list.append("# Run Target tables") 

    tgt_tables = {
        "participant": {"source_table": f"{study_id}_ftd_participant", "target_schema": f"{study_id}_tgt_data"},
        "condition": {"source_table": f"{study_id}_ftd_condition", "target_schema": f"{study_id}_tgt_data"},
        "event": {"source_table": f"{study_id}_ftd_event", "target_schema": f"{study_id}_tgt_data"},
        "measurement": {"source_table": f"{study_id}_ftd_measurement", "target_schema": f"{study_id}_tgt_data"}
    }

    for table, args in tgt_tables.items():
        commands_list.append(generate_run_command("model", f"tgt_{table}", args))

    commands_list.append("# Run sanity checks") 

    schemas = [
        {"schema_name": "dbo_src_data"},
        {"schema_name": f"dbo_{study_id}_data"},
        {"schema_name": f"dbo_{study_id}_ftd_data"},
        {"schema_name": f"dbo_{study_id}_tgt_data"},
    ]

    for schema in schemas:  
        commands_list.append(generate_run_command("macro", "print_database_stats", schema))

    # Final script content
    data = "\n".join(commands_list) + "\n"
    filepath = scripts_dir / f"run_{study_id}.sh"

    # Write the script to a file
    write_file(filepath, data)

    # Edit script permissions
    subprocess.run(["chmod", "+x", filepath], check=True)