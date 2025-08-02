{% macro default__create_schema(relation) -%}
  {%- call statement('create_schema') -%}

    {% set schema_name = relation.without_identifier()|string %}
    {% set schema_loc = var('schema_location') %}

    {%- if schema_loc == 'n/a' -%}
        create schema if not exists {{ schema_name }}
    {%- else -%}
        create schema if not exists {{ schema_name }} MANAGED LOCATION '{{ schema_loc }}'
    {%- endif -%}

  {% endcall %}
{% endmacro %}
