
def normalize_varnames(varnames):
    return varnames.lower().replace(" ", "_").replace(",", "_").replace("-", "_")

def explode_rows_on_col_value(df,col,delimiter):
    '''
    Creates a new row for every item in a cell, split by the delimeter.

    Example params (df, 'Tables', ', ')
    input data:
    Tables, other_data
    "Subject, Sample", other_data

    output data:
    Tables, other_data
    Subject, other_data
    Sample, other_data

    '''
    
    df[col] = df[col].str.split(delimiter)

    df = df.explode(col).reset_index(drop=True)

    return df

