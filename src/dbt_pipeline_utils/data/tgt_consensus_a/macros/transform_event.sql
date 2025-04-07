{% macro transform_event(source_table) %}
    {% if not source_table %}
        {{ exceptions.raise_compiler_error("source_table variable is not set. Please pass it using --vars.") }}
    {% endif %}

    WITH src_condition AS (
        SELECT *
        FROM {{ ref(source_table) }}
    )

    SELECT * FROM src_condition
{% endmacro %}
