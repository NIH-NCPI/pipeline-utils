{% macro transform_study(source_table) %}
WITH src_study AS (
    SELECT
        *
    FROM {{ ref(source_table) }}
)
SELECT * FROM src_study
{% endmacro %}
