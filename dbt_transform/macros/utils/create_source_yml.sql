{% macro create_source_yml(all_schemas,unity_catalog=none) %}

    {% if unity_catalog is none %}
        {% set unity_catalog = env_var("DBX_UNITY_CATALOG", default="") -%}
    {% endif %}

    {% set source_list=get_all_schemas_tables(all_schemas,unity_catalog) %}

    {% set sources_yaml=[''] %}
    {% do sources_yaml.append('version: 2') %}
    {% do sources_yaml.append('') %}
    {% do sources_yaml.append('sources:') %}

    {% for schm in all_schemas %}
        {% set table_list=get_all_schemas_tables([schm],unity_catalog) %}

        {% if table_list|length > 0 %}
            {% do sources_yaml.append('  - name: ' ~ schm ) %}
            {% do sources_yaml.append('    schema: ' ~ schm ) %}
            {% do sources_yaml.append('    catalog: ' ~ unity_catalog ) %}
            {% do sources_yaml.append('    tables: ' ) %}


            {% for table_name in table_list %}
                {% do sources_yaml.append('      - name: ' ~  table_name[2]) %}
            {% endfor %}
        {% endif %}
    {% endfor %}

    {% if execute %}
        {% set joined = sources_yaml | join ('\n') %}
        {{ log(joined, info=True) }}
        {% do return(joined) %}
    {% endif %}

{% endmacro %}
