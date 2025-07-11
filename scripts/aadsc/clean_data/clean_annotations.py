import pandas as pd

from dbt_pipeline_utils.scripts.helpers.common import *
from dbt_pipeline_utils import logger


ori_data_path = "data/input/annotations.csv"
new_file_path = "data/output/annotations2.csv"


def normalize_varnames(varname):
    name = str(varname).lower().replace(" ", "_").replace(",", "_").replace("-", "_")
    return name

def clean_and_split(row, prefix):
    row = str(row)
    row = row.replace(';', ',').replace(' ','')
    row = row.replace(prefix, '')
    return row.split(',')

def add_prefix_to_list(codes, prefix):
    return [f"{prefix}{code}" for code in codes if code]



# import the ori data
cols=['Source Column [PL]','ICD-9 Codes','ICD-10 Codes','ICD-O Codes','MONDO Label','MONDO Code','HPO Label','HPO Code']
ori_data = pd.read_csv(ori_data_path, usecols=cols)

# Clean names
df=ori_data.copy()
df['condition_name'] = df['Source Column [PL]'].apply(normalize_varnames)

# Split and clean the code columns
df.loc[df['ICD-9 Codes'].notnull(), 'icd9_codes'] = df.loc[df['ICD-9 Codes'].notnull(), 'ICD-9 Codes'].apply(lambda x: clean_and_split(x, 'ICD9:'))
df.loc[df['icd9_codes'].notnull(), 'icd9_codes_with_prefix'] = df.loc[df['icd9_codes'].notnull(), 'icd9_codes'].apply(lambda x: add_prefix_to_list(x, 'ICD9:'))

df.loc[df['ICD-10 Codes'].notnull(), 'icd10_codes'] = df.loc[df['ICD-10 Codes'].notnull(), 'ICD-10 Codes'].apply(lambda x: clean_and_split(x, 'ICD10:'))
df.loc[df['icd10_codes'].notnull(), 'icd10_codes_with_prefix'] = df.loc[df['icd10_codes'].notnull(), 'icd10_codes'].apply(lambda x: add_prefix_to_list(x, 'ICD10CM:'))

df.loc[df['ICD-O Codes'].notnull(), 'icdO_codes'] = df.loc[df['ICD-O Codes'].notnull(), 'ICD-O Codes'].apply(lambda x: clean_and_split(x, 'ICDO:'))
df.loc[df['icdO_codes'].notnull(), 'icdO_codes_with_prefix'] = df.loc[df['icdO_codes'].notnull(), 'icdO_codes'].apply(lambda x: add_prefix_to_list(x, 'ICDO:'))

df.loc[df['MONDO Code'].notnull(), 'mondo_codes'] = df.loc[df['MONDO Code'].notnull(), 'MONDO Code'].apply(lambda x: clean_and_split(x, 'MONDO:'))
df.loc[df['mondo_codes'].notnull(), 'mondo_codes_with_prefix'] = df.loc[df['mondo_codes'].notnull(), 'mondo_codes'].apply(lambda x: add_prefix_to_list(x, 'MONDO:'))

df.loc[df['HPO Code'].notnull(), 'hpo_codes'] = df.loc[df['HPO Code'].notnull(), 'HPO Code'].apply(lambda x: clean_and_split(x, 'HP:'))
df.loc[df['hpo_codes'].notnull(), 'hpo_codes_with_prefix'] = df.loc[df['hpo_codes'].notnull(), 'hpo_codes'].apply(lambda x: add_prefix_to_list(x, 'HP:'))

# Expand, for one code per row
df_f = df[['condition_name', 'icd9_codes_with_prefix', 'icd10_codes_with_prefix', 'icdO_codes_with_prefix',
       'MONDO Label', 'mondo_codes_with_prefix', 'HPO Label', 'hpo_codes_with_prefix']]

df_explode = []

# Iterate through each row in the DataFrame
for _, row in df.iterrows():
    
    # Expand mondo_codes_with_prefix
    if isinstance(row['mondo_codes_with_prefix'], list):  # Only process if it's a list
        for i in row['mondo_codes_with_prefix']:
            df_explode.append({
                'condition_name': row['condition_name'],
                'icd9_codes_with_prefix': None,
                'icd10_codes_with_prefix': None,
                'icdO_codes_with_prefix': None,
                'mondo_label': row['MONDO Label'],
                'mondo_codes_with_prefix': i,
                'hpo_label': None,
                'hpo_codes_with_prefix': None
            })
    
    # Expand icd9_codes_with_prefix
    if isinstance(row['icd9_codes_with_prefix'], list):  # Only process if it's a list
        for i in row['icd9_codes_with_prefix']:
            df_explode.append({
                'condition_name': row['condition_name'],
                'icd9_codes_with_prefix': i,
                'icd10_codes_with_prefix': None,
                'icdO_codes_with_prefix': None,
                'mondo_label': None,
                'mondo_codes_with_prefix': None,
                'hpo_label': None,
                'hpo_codes_with_prefix': None
            })
    
    # Expand icd10_codes_with_prefix
    if isinstance(row['icd10_codes_with_prefix'], list):  # Only process if it's a list
        for i in row['icd10_codes_with_prefix']:
            df_explode.append({
                'condition_name': row['condition_name'],
                'icd9_codes_with_prefix': None,
                'icd10_codes_with_prefix': i,
                'icdO_codes_with_prefix': None,
                'mondo_label': None,
                'mondo_codes_with_prefix': None,
                'hpo_label': None,
                'hpo_codes_with_prefix': None
            })
    
    # Expand icdO_codes_with_prefix
    if isinstance(row['icdO_codes_with_prefix'], list):  # Only process if it's a list
        for i in row['icdO_codes_with_prefix']:
            df_explode.append({
                'condition_name': row['condition_name'],
                'icd9_codes_with_prefix': None,
                'icd10_codes_with_prefix': None,
                'icdO_codes_with_prefix': i,
                'mondo_label': None,
                'mondo_codes_with_prefix': None,
                'hpo_label': None,
                'hpo_codes_with_prefix': None
            })
    
    # Expand hpo_codes_with_prefix
    if isinstance(row['hpo_codes_with_prefix'], list):  # Only process if it's a list
        for i in row['hpo_codes_with_prefix']:
            df_explode.append({
                'condition_name': row['condition_name'],
                'icd9_codes_with_prefix': None,
                'icd10_codes_with_prefix': None,
                'icdO_codes_with_prefix': None,
                'mondo_label': None,
                'mondo_codes_with_prefix': None,
                'hpo_label': row['HPO Label'],
                'hpo_codes_with_prefix': i
            })

df_explode = pd.DataFrame(df_explode)

icd_map = {
    'change_in_skin_texture': 'Changes in skin texture',
    'do_not_resuscitate_status': 'Do not resuscitate'
}

for key, value in icd_map.items():
    df_explode.loc[df_explode['condition_name'] == key, 'icd10cm_label'] = value

additions = pd.DataFrame([
    {'condition_name': 'bmi', 'icd9_codes_with_prefix': None, 'icd10_codes_with_prefix': None, 'icd10cm_label': None,
    'icdO_codes_with_prefix': None, 'mondo_label': None, 'mondo_codes_with_prefix': None, 'hpo_label': None,
    'hpo_codes_with_prefix': None, 'loinc_code': 'LOINC:39156-5', 'loinc_label': 'Body mass index (BMI)'},
    {'condition_name': 'height', 'icd9_codes_with_prefix': None, 'icd10_codes_with_prefix': None, 'icd10cm_label': None,
    'icdO_codes_with_prefix': None, 'mondo_label': None, 'mondo_codes_with_prefix': None, 'hpo_label': None,
    'hpo_codes_with_prefix': None, 'loinc_code': 'LOINC:8302-2', 'loinc_label': 'Body height'},
    {'condition_name': 'weight', 'icd9_codes_with_prefix': None, 'icd10_codes_with_prefix': None, 'icd10cm_label': None,
    'icdO_codes_with_prefix': None, 'mondo_label': None, 'mondo_codes_with_prefix': None, 'hpo_label': None,
    'hpo_codes_with_prefix': None, 'loinc_code': 'LOINC:29463-7', 'loinc_label': 'Body weight'}
])

all_an = pd.concat([df_explode, additions], ignore_index=True)

all_an.to_csv(new_file_path, index=False)

logger.info(df_explode)