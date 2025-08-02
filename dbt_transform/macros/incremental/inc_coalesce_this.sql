{% macro inc_coalesce_this(column_source,column_target='') %}

    {%- set col = column_source.split('.') -%}

    {%- if column_target == '' -%}
        {%- set column_target = col[1] -%}
    {%- endif -%}

    {%- if is_incremental() -%}
        coalesce({{ column_source }} ,t.{{ column_target }}) as {{ column_target }}
    {%- else -%}
        {{ column_source }} as {{ column_target }}
    {%- endif -%}

{% endmacro %}
