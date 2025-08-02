{% macro model_table_match() %}

    {# >> Get all models and seeds of your project #}
    {% set all_schemas = [] %}
    {% set all_models = [] %}

    {% if execute %}
      {% for node in graph.nodes.values()
         | selectattr("resource_type", "in", ["model", "seed", "snapshot"])  %}

        {% set materialized = node.config.materialized
            |replace('incremental', 'table')
            |replace('snapshot', 'table')
            |replace('seed', 'table') %}
        {% do all_models.append([node.schema, node.alias, materialized]) %}

        {% if node.schema not in all_schemas %}
            {% do all_schemas.append(node.schema) %}
        {% endif %}
      {% endfor %}
    {% endif %}

    {% do log(">> The schemas present in this project are " ~ all_schemas, info=true) %}

    {# >> Get all tables and view of your Unity Catalog #}
    {% set query %}
        select
            table_schema,
            table_name,
            case
                when table_type in ('EXTERNAL', 'MANAGED') then 'table'
                else lower(table_type)
            end as table_type
        from system.information_schema.tables
        where table_catalog = '{{ env_var("DBX_UNITY_CATALOG") }}'
        and table_schema in ( {{ "'" + "', '".join(all_schemas) + "'" }} )
        and table_schema != '{{ env_var("DBX_ELEMENTARY_SCHEMA",default="") }}'
        and table_name != 'alembic_version'
        {%- if var("prefix_table") -%} and table_name like '{{ var("prefix") }}%'{%- endif -%}
    {% endset %}

    {% set results = run_query(query) %}

    {# >> Compare if table/view is missing a related model #}
    {% for row in results %}
        {% set my_table = [row[0],row[1],row[2]] %}

        {% if my_table not in all_models %}
            {% do log("Missing model or materialization type doesn't match: " ~ my_table, info=true) %}
        {% endif %}
    {% endfor %}

{% endmacro %}
