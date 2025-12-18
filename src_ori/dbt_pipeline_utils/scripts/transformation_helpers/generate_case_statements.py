import csv
from dbt_pipeline_utils import logger


def main(study_id):
    # Initialize a dictionary to store case statements grouped by target field
    case_statements = {}
    csv_file_path = f'data/static/enumerations/{study_id}_enums.csv' 

    # Open and read the CSV file
    with open(csv_file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        
        for row in reader:
            # Skip rows not in use
            if row['in_use'] != 'T':
                continue
            
            # Extract relevant data from the row
            src_table = row['src_table']
            src_field = row['src_field']
            expected_src_value = row['expected_src_value']
            equivalent_model_value = row['equivalent_model_value']
            tgt_field = row['tgt_field']
            
            # Initialize the case statement for a target field if not already done
            if tgt_field not in case_statements:
                case_statements[tgt_field] = []
            
            # Add `WHEN` clauses to the case statement
            if expected_src_value and expected_src_value != 'else':
                case_statements[tgt_field].append(
                    f"    when  {src_table}.{src_field} = {expected_src_value}")
                case_statements[tgt_field].append(
                        f"      then {equivalent_model_value}")
            elif expected_src_value == 'else':
                # Handle the ELSE clause for `else`
                case_statements[tgt_field].append(
                    f"    else {equivalent_model_value}"
                )
    
    # Generate the final SQL case statements
    sql_statements = []
    for tgt_field, conditions in case_statements.items():
        # Combine all `WHEN` clauses and the `ELSE` clause into a single case statement
        case_statement = f"    case\n" + "\n".join(conditions) + f"\nend as \"{tgt_field}\","
        sql_statements.append(case_statement)
    
    r =  "\n\n".join(sql_statements)

    logger.info(f"Generated using the csv located at {csv_file_path}\n \n {r}")

