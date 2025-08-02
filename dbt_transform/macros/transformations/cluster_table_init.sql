{% macro cluster_table_init(table_name,header_key_list,key_list,order_list) %}

{%- set load_type = var("load_type") -%}
{%- if load_type == 'init' -%}
    {% set sql -%}
        merge into {{ this }} t1
        using (
            select {% for keys in key_list %}
                    {{ keys }}
                    {% if not loop.last %} , {% endif %}
                {% endfor %}
            from
                (select distinct
                    {% for keys in key_list %}
                        {{ source('raw-ds-dbt',table_name) }}.{{ keys }}
                        {% if not loop.last %} , {% endif %}
                    {% endfor %} ,

                    row_number() over (partition by
                        {% for keys in header_key_list %}
                            {{ source('raw-ds-dbt',table_name) }}.{{ keys }}
                            {% if not loop.last %} , {% endif %}
                        {% endfor %}

                        order by
                            {% for keys in order_list %}
                            {{ keys }} desc
                            {% if not loop.last %} , {% endif %}
                            {% endfor %}
                        ) as row_num
                from {{ source('raw-ds-dbt',table_name)}})
            where row_num <> 1

            union

            select {% for keys in key_list %}
                    {{ source('raw-ds-dbt',table_name) }}.{{ keys }}
                    {% if not loop.last %} , {% endif %}
                {% endfor %}
            from {{ source('raw-ds-dbt',table_name)}}
            where OPFLAG='D'

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
