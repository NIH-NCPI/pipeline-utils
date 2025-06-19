import pandas as pd

from dbt_pipeline_utils.scripts.helpers.common import *
from dbt_pipeline_utils import logger


input_dd_path = "data/input/chicoine_down_syndrome_extract_dd.csv"
df_path = "data/input/chicoine_down_syndrome_extract.csv"
column_map = DD_FORMATS['pipeline_format'] 

def normalize_varnames(varnames):
    return varnames.lower().replace(" ", "_").replace(",", "_").replace("-", "_")

# import the dd
ori_dd = pd.read_csv(input_dd_path)

# Get the descriptions from the original file, and var names as a join col.
ori_desc = ori_dd.iloc[:, :2]
ori_desc['merge_col'] = ori_desc['variable_name'].apply(normalize_varnames)
logger.info(ori_desc.head())

# MANUAL CLEAN
mapping = {
    'age_at_last_encounter': 'age',
    'change_in_skin_texture_': 'change_in_skin_texture',
    'disorder_ofadrenalgland,_unspecified': 'disorder_of_adrenal_gland-unspecified',
    'date_of_extraction': 'extraction_date',
    'height_at_last_encounter': 'height',
    'nonalcoholicsteatohepatitis': 'nonalcoholic_steatohepatitis',
    'unspecified_blepharitis_unspecified_eye,_unspecified_eyelid': 'unspecified_blepharitis_unspecified_eye-_unspecified_eyelid',
    'vitaminb12deficiency': 'vitamin_b12_deficiency',
    'xerosis_cutis_': 'xerosis_cutis',
    'weight_at_last_encounter': 'weight'
}
for key, value in mapping.items():
    ori_desc.loc[ori_desc['merge_col'] == key, 'merge_col'] = value


# import the data file column names
df = pd.read_csv(df_path)
cols_df = pd.DataFrame({'variable_name': df.columns})
original_order = df.columns.tolist()

# make a join column for the df columns
cols_df['merge_col'] = cols_df['variable_name'].apply(normalize_varnames)

logger.info(original_order)

mapping = {
    'auto_other_specify': 'EQ_DD_COL_UNKNOWN',
    'auto_narcolepsy_status': 'EQ_DD_COL_UNKNOWN',
    'endo_vitamind_status': 'EQ_DD_COL_UNKNOWN',
    'masked_id': 'EQ_DD_COL_UNKNOWN',
    'neuro_dementia_status': 'EQ_DD_COL_UNKNOWN',
    'psych_odd_status': 'EQ_DD_COL_UNKNOWN',
    'psych_panic_status': 'EQ_DD_COL_UNKNOWN',
    'psych_schizophrenia_status': 'EQ_DD_COL_UNKNOWN',
}
for key, value in mapping.items():
    cols_df.loc[cols_df['merge_col'] == key, 'variable_description'] = value


# datafile column names left join column descriptions
merged_dd = cols_df.merge(ori_desc,
                          how='outer',
                          on=['merge_col'],
                          indicator=True)
# logger.info(merged_dd.head())
# logger.info(column_map.keys())


left = merged_dd[merged_dd['_merge']=='left_only']
both = merged_dd[merged_dd['_merge']=='both']
right = merged_dd[merged_dd['_merge']=='right_only']

# to csv for sanity check
left.to_csv("data/output/left.csv")
both.to_csv("data/output/both.csv")
right.to_csv("data/output/right.csv")


# After cleaning. Get the joining data and the 'not joining' cols from the dfiles
clean_merged_df = merged_dd[merged_dd['_merge'].isin(['left_only','both'])]

# make dd into pipeline format
new_dd = pd.DataFrame(columns = column_map.keys())
logger.info(new_dd)

clean_merged_df = clean_merged_df.rename(columns={'variable_name_x': 'variable_name',
                               'variable_description_y': 'description'
                               })	

clean_df =clean_merged_df[['variable_name','description']]
clean_df = clean_df.assign(data_type='string', min=None, max=None, units=None, enumerations=None, comment=None)


# Reorder columns to match the original order
clean_df = clean_df.set_index('variable_name') 
clean_df = clean_df.loc[original_order]      

clean_df.to_csv("data/output/new_dd.csv")


print(f'sanity check - counts original_df_cols:{len(cols_df)} new_df_cols:{len(clean_df)}')