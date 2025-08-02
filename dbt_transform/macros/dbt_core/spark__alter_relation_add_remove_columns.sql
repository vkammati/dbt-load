{% macro spark__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}

  {% if remove_columns %}
    {{ log("Executing custom 'spark__alter_relation_add_remove_columns' to drop column(s): " ~ remove_columns, info=true) }}

    {% set sql -%}
      alter {{ relation.type }} {{ relation }} drop columns (
              {%- for column in remove_columns -%}
                {{ column.name }} {{ ',' if not loop.last }}
              {%- endfor -%}
      )
    {%- endset -%}

    {% do run_query(sql) %}
  {% endif %}

  {% if add_columns%}
    {{ return(dbt.spark__alter_relation_add_remove_columns(relation, add_columns, none)) }}
  {% endif %}
{% endmacro %}
