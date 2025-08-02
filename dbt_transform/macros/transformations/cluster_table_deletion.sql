{% macro cluster_table_deletion(table_name,key_list) %}

{%- set load_type = var("load_type") -%}
{%- if load_type == 'delta' -%}
    {% set sql -%}
        merge into {{ this }} t1
        using (
            select distinct
                {% for keys in key_list %}
                    {{ keys }}
                    {% if not loop.last %} , {% endif %}
                {% endfor %}
            from {{ source('raw-ds-dbt',table_name)}}
        ) t2
        on
        {% for keys in key_list %}
            t1.{{ keys }} = t2.{{ keys }}
            {% if not loop.last %} AND {% endif %}
        {% endfor %}
        when matched then delete
    {%- endset %}
    {% do run_query(sql) %}
{%- endif -%}
{% endmacro %}
