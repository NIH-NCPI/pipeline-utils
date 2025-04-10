# dbt-utils
{description}

Running the script locally or working on a branch?
Create and activate a virtual environment (recommended):
[Click here] for more on virtual environments.

** NOTE: SOME projects recommend pyenv. 
TODO: Give more info about pyenv

```
# Step 1: cd into the directory to store the venv

# Step 2: run this code. It will create the virtual env named utils_venv in the current directory.
python3 -m venv utils_venv

# Step 3: run this code. It will activate the utils_venv environment
source utils_venv/bin/activate # On Windows: venv\Scripts\activate

# You are ready for installations! 
# If you want to deactivate the venv run:
deactivate
```
Install the package
If working on a new feature it is possible to install a package version within the remote or local branch NOTE If testing changes to dbt_pipeline_utils in the dbt project don't forget to deploy a dbt project branch with the correct dbt_pipeline_utils version in the requirements.txt file! NOTE Any new env variables created, e.g. api keys, will need to be added to the dbt project deployment files.
# remote
pip install git+https://github.com/NIH-NCPI/pipeline_utils.git@{branch_name}

# local
pip install -e .

# Locutus should install using the following command.
pip install git+https://github.com/NIH-NCPI/pipeline_utils.git

# A re-install might be required while testing any changes to this repo, use this command to force the reinstall and ensure the latest version.
pip install --force-reinstall --no-cache-dir git+https://github.com/NIH-NCPI/pipeline_utils.git


# Developers
To add a new src_dd_format --> scripts/helpers/common.py
 - always update the readme options
Large datafiles
 - data can also be retrieved via the Synapse API. Many possibilities to improve on synapse data handling. Forcing it through the csv route for now.

Pipeline db is assumed -hardcoded in some areas- to be postgres.
 Data from synapse might not refresh/download if it sees that the data file already exists. If this is a problem check the synapse docs.