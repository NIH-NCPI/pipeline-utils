{% macro run_sql(sql) %}
    {% do run_query(sql) %}
{% endmacro %}
