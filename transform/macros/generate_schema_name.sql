{#
    dbt's default behaviour concatenates the target schema with the custom
    schema, so `+schema: silver` against target schema `main` produces
    `main_silver`. This override uses the custom schema verbatim, giving the
    clean `bronze` / `silver` / `gold` layering the medallion design assumes.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
