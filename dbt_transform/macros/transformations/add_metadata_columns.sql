{%- macro add_metadata_columns() -%}
    {{ dbt.current_timestamp() }} as insert_date,
    cast('{{var("job_id")}}' as string) as job_id,
    cast('{{var("run_id")}}' as string) as run_id
{%- endmacro -%}
