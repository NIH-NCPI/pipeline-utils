{% macro transform_condition(source_table) %}
WITH src_condition AS (
    SELECT
        *
    FROM {{ ref(source_table) }}
)


SELECT
    *
FROM src_condition c
{% endmacro %}
