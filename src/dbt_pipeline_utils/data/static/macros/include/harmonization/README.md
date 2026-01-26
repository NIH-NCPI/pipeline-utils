# macros

## generate_global_id

    This macro generates the sql required to create a primary or foreign key
    by the md5 on a composite string method.

    Example usage - lacking double brackets:
        generate_md5_composite_key("p", ['id','dob'])
    generates the following sql
        'p' || '_' || md5(studyid || CAST(coalesce(id, '') AS TEXT) || '|' || CAST(coalesce(dob, '') AS TEXT))
    results in an id with the following format
        p_a32b29ffb1a85590c4a6d4cbeec18636


## get_columns

    This macro generates a list of column names from the table(reference) specified
    in the argument `from`. The list will exclude columns listed in the argument `except`.

    Example usage, ignore the variable names, this is more specific:
    {% set relation = ref('{study}_stg_{table}') %}
    {% set constant_columns = ['subject_id'] %}
    {% set condition_columns = get_columns(relation=relation, exclude=constant_columns) %}

    The macro generates the following list
        ['subject_id','other_col_1', 'other_col_2',...]

## register_external_sources.sql
    Will create a database table, then import the data from a csv into the table.
    Use this when not importing directly into the database via dbt models.

## run_sql
    Required by pipeline_utils import script. That script can be used, but for the 
    most part it isn't used anymore. Currently importing csv data into duckdb via the
    first model run. 
    

