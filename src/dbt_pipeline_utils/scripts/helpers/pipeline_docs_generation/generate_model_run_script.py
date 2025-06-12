import json
from dbt_pipeline_utils.scripts.helpers.general import *
import subprocess

class RunScriptClass():

    def generate_run_command(self, operation, model, args=None):
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


    def generate_run_command(self, operation, model, args=None):
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

    def generate_dbt_run_script(self):
        """Generates a dbt run Bash script dynamically based on a YAML configuration."""
        study_id = self.study_id
        scripts_dir = self.paths["dbtp_scripts_dir"]

        commands_list = [
        "#!/bin/bash",
        "set -e  # Exit on error",
        "dbt clean",
        'dbt deps || { echo "Error: dbt deps failed. Exiting..."; exit 1; }',
    ]

        commands_list.append("# Run Target tables") 



        tgt_tables = {}
        for table_id, table_info in self.ftd_dd.items():
           tgt_tables[f"{table_id}"]= {"source_table": f"{self.study_id}_ftd_{table_id}", "target_schema": f"{self.study_id}_tgt_data"}


        for table, args in tgt_tables.items():
            commands_list.append(self.generate_run_command("model", f"tgt_{table}", args))

        # Final script content
        data = "\n".join(commands_list) + "\n"
        filepath = scripts_dir / f"run_{study_id}.sh"

        # Write the script to a file
        write_file(filepath, data, overwrite=False)

        # Edit script permissions
        subprocess.run(["chmod", "+x", filepath], check=True)