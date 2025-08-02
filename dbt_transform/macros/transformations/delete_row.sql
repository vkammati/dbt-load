{%- macro delete_row(delete_flag) -%}

{%- set all_columns = adapter.get_columns_in_relation(this) -%}

{%- for col in all_columns if delete_flag == col.name %}
    delete from {{ this }}
    where {{ delete_flag }} is not null
{%- endfor %}

{%- endmacro -%}
