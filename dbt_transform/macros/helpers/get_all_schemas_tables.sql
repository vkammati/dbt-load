{% macro get_all_schemas_tables(all_schemas,unity_catalog=none) %}

    {% if unity_catalog is none %}
        {% set unity_catalog = env_var("DBX_UNITY_CATALOG", default="") -%}
    {% endif %}

    -- Get all tables and view of your schemas
    {% set query %}
        select table_catalog, table_schema, table_name
        from system.information_schema.tables
        where table_catalog = '{{ unity_catalog }}'
        and table_schema in ( {{ "'" + "', '".join(all_schemas) + "'"}} )
        order by 1,2,3
    {% endset %}

    {% set results = run_query(query) %}

    {{ return(results) }}

{% endmacro %}
