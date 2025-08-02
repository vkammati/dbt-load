{%- macro add_hash_key(column_list) -%}

    {%- set model_name = model.name -%}
    {%- if model_name[0:5] in ['euh__','cur__', 'dim__','fct__'] -%}
        {%- set model_name = model_name[5:] -%}
    {%- elif model_name[0:4] in ['eh__'] -%}
        {%- set model_name = model_name[4:] -%}
    {%- endif -%}

    {{ dbt_utils.generate_surrogate_key(column_list) }} as {{ model_name }}_sk

{%- endmacro -%}
