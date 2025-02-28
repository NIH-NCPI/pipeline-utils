import subprocess

def run_etl_script(script_path):
    """Runs an ETL script located at script_path."""
    try:
        result = subprocess.run(["bash", script_path], capture_output=True, text=True, check=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        return f"Error running script: {e.stderr}"
