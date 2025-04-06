-- Specifies the schema dynamically instead of the usual location in dbt_profile.yaml
{{ config(
    schema=var('target_schema')
) }}

{% set source_table = (var('source_table') | default(none)) %}

{% if source_table is not none %}
  {% do log("Using source_table: " ~ source_table, info=True) %}
  {{ transform_participant(source_table) }}
{% else %}
  {% do log("Skipping transform_event because source_table is not set.", info=True) %}
{% endif %}

