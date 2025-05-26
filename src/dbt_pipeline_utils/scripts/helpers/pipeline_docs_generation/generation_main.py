import pandas as pd

def generate_model_docs(df_obj):
    """Main function to generate dbt model files, loading column data once."""
    
    df_obj.generate_dbt_project_yaml()

    df_obj.generate_stg_dds()

    column_data = df_obj.load_src_column_data()

    df_obj.generate_dbt_models_yml(column_data, df_obj.paths["dbtp_src_study_model_dir"])
    
    df_obj.generate_dbt_sources_yml(column_data, df_obj.paths["dbtp_src_study_model_dir"])

    # column_description files build don't 'overwrite', they will update. Delete the file to refresh.
    df_obj.generate_column_descriptions(column_data, df_obj.paths["dbtp_src_study_model_docs_dir"])

    df_obj.generate_model_descriptions(df_obj.paths["dbtp_src_study_model_docs_dir"])
    
    # TODO we may not need this one for duckdb passing
    df_obj.generate_src_sql_files(df_obj.paths["dbtp_src_study_model_dir"])

    df_obj.generate_stg_sql_files(
        column_data,
        df_obj.paths["dbtp_src_study_model_dir"]
    )

def generate_ftd_model_docs(df_obj):
    """Main function to generate dbt model files, loading column data once."""

    # NOTE: The data/project_id/ftd_study.yaml is also generated. See generate_docs.generate_ftd_study_yaml.

    df_obj.generate_ftd_dds()

    column_data = df_obj.load_ftd_column_data()

    df_obj.generate_dbt_models_yml(column_data, df_obj.paths["dbtp_ftdc_study_docs_dir"], ftd_model=True)
    
    df_obj.generate_ftd_sql_files(column_data)

    df_obj.generate_ftd_dbt_project_yaml()
    
    df_obj.generate_column_descriptions(column_data,df_obj.paths["dbtp_ftdc_study_docs_dir"],ftd_model=True,)

def generate_tgt_model_docs(df_obj):
    """The tgt model should only need to be generated once, and then small tweaks made."""

    # copy over the tgt model
    df_obj.copy_directory()

def generate_run_script(df_obj):
    df_obj.generate_dbt_run_script()