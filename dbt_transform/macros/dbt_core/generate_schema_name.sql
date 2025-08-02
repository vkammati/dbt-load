{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {%- if var("prefix_schema") -%}{{ var("prefix") }}{%- endif -%}{{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
