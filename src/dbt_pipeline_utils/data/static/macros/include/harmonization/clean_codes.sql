{%- macro clean_codes(column_name, expected_prefixes, sp_chars) -%}
  {%- set ns = namespace(expr=column_name) -%}
  {%- for prefix in expected_prefixes -%}
    {%- set ns.expr = "replace(" ~ ns.expr ~ ", '" ~ prefix ~ "', '|" ~ prefix ~ "')" -%}
  {%- endfor -%}
  {%- set chars_to_remove = sp_chars | join('') -%}
(
    select string_agg(distinct trim(code), '|' order by trim(code))
    from (
        select unnest(string_to_array(trim(both '|' 
        from translate({{ ns.expr }}, '{{ chars_to_remove }}', '')),'|')) as code
        ) as split_codes
    where code is not null and length(trim(code)) > 0
)
{%- endmacro -%}
