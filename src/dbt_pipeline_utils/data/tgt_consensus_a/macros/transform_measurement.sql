{% macro transform_measurement(source_table) %}
WITH src_condition AS (
    SELECT
        *
    FROM {{ ref(source_table) }}
)
SELECT * FROM src_condition
WHERE measure_value IS NOT NULL -- place logic here that includes measurements
{% endmacro %}
