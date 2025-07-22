import pandas as pd
from pathlib import Path

from scripts.cleaning_helpers import *
from dbt_pipeline_utils import logger

files = ['Discovery mappings.csv',
         'Family mappings.csv',
         'Sample mappings.csv',
        #  'Sequencing mappings.csv', # No mappings
         'Subject mappings.csv',
         'CMG Broad (Addendum) mappings.csv',
         'Yale CMG (Addendum) mappings.csv']
study = 'cmg'
id = 'bh'
output_fp = f'data/output/{study}/{id}/{study}_{id}_annotations.csv'


# Create filepaths for each table to read in
filepaths = []
for file in files:
    fp = f'data/input/{study}/{id}/{file}'
    filepaths.append(fp)

# import the ori data
cols=['local code',
      'text',
      'table_name',
      'parent_varname',
      'local code system',
      'mapping relationship',
      'code','display',
      'code system',
      'comment']


# Read in and format the src files
reformatted_dfs = []
for path in filepaths:
    # import the ori data
    logger.info(f"Reading in {path}")
    ori_data = pd.read_csv(path, usecols=cols)

    # Clean col names
    df=ori_data.copy()
    formatted_names = []
    for col in cols:
        formatted_name = normalize_varnames(col)
        df[formatted_name] = df[col]
        df['table'] = Path(path).stem \
            .lower() \
            .replace(' mappings','') \
            .replace(' ','_') \
            .replace('(','') \
            .replace(')','')
        formatted_names.append(formatted_name)

    reformatted_dfs.append(df)

# merge annotations into one file
merged_df = pd.concat(reformatted_dfs, ignore_index=True)

reorg= merged_df[formatted_names + ['table']]

# Print cleaned file to output dir
reorg.to_csv(output_fp, index=None)