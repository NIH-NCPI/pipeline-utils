{% macro get_columns(relation, exclude=[]) %}
  {{ return(dbt_utils.get_filtered_columns_in_relation(
    from=relation,
    except=exclude
  )) }}
{% endmacro %}
