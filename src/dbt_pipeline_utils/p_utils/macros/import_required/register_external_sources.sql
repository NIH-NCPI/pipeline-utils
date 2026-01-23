{% macro register_external_sources(fq_tablename, csv_path) %}
    {% do adapter.execute("CREATE SCHEMA IF NOT EXISTS " ~ fq_tablename ~ ";") %}
    {% do adapter.execute("CREATE OR REPLACE VIEW " ~ fq_tablename ~ " AS SELECT * FROM read_csv_auto('" ~ csv_path ~ "');") %}
{% endmacro %}