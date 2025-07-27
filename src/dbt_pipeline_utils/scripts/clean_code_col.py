'''
Cleans a datafile column that contains codes. 
Expects a column with data similar to: '|HP:0004323|HP:0000234|''|HP:0004323||HP:0000234|'
'''

import argparse
import re
from dbt_pipeline_utils.scripts.helpers.general import *
from dbt_pipeline_utils.scripts.helpers.common import *
from dbt_pipeline_utils import logger

def clean_codes(codes, curies):
    
    for c in curies:
        codes = codes.replace(c,f'|{c}')
        codes = codes.replace(c,f'{c}:')
        codes = codes.replace(f'{c}::',f'{c}:')
    codes = codes.replace(' ', '')  
    codes = codes.replace("''", '')  
    codes = codes.replace('"', '') 
    codes = codes.replace('|||', '|') 
    codes = codes.replace('||', '|')  
    codes = codes.replace("|'","'").replace("'|","'")  
    codes = codes.replace("| ","|").replace(" |","|").replace(" '","'").replace("' ","'") 
    codes = codes.replace("'","").replace('"',"")
    return codes

def is_valid_format(code):
    # Check format STRING:12345 (uppercase string, colon, string)
    return bool(re.fullmatch(r'[A-Z]+:.*?', code))

def create_flag_column(codes):
    # Check if each code matches the valid format
    return [is_valid_format(code) for code in codes.split('|') if code.strip()]
    
def main(df,column,curies):
    df = read_file(df)

    df['cleaned_col'] = df[column].apply(lambda x: clean_codes(x, curies))
    df['correct_format'] = df['cleaned_col'].apply(create_flag_column)

    t=df[['cleaned_col','correct_format']]
    t.to_csv('col_clean.csv', index=False)

    # Catch all codes that might not be cleaned properly
    questionable = df[df['correct_format'].apply(lambda x: not all(x))]
    if len(questionable)>=1:
        logger.warning(f': {questionable[['cleaned_col','correct_format']]}')

    logger.info('Clean codes are returned')
    return df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get metadata for a code using the available locutus OntologyAPI connection.")
    
    parser.add_argument("-df", "--data_file", required=True, help="File containing the codes requiring metadata. Format: 'path/to/datafile.csv'")
    parser.add_argument("-c", "--column", required=True, help="Column name containing the codes requiring metadata. The utils can do some amount of cleaning. # Format: 'ExactFieldName'")
    parser.add_argument("-o", "--ontologies", required=True, help="List of ontology prefixes")

    args = parser.parse_args()

    curies=args.ontologies.split(',')

    main(df=args.data_file,
         column=args.column,
         curies=curies
         )