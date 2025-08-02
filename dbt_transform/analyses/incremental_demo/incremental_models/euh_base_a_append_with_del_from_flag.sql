{{
  config(
    materialized = 'incremental',
    on_schema_change='sync_all_columns',
    incremental_strategy= "append",
    tags = "inc_demo",
    )
}}

with cte_raw_source_base as (
    select
        id,
        name,
        email,
        null as operation,
        raw_insert_date
    from {{ source('example_incremental_demo','raw_source_base_a') }}
    union all
    select
        id,
        null,
        null,
        operation,
        raw_insert_date
    from {{ source('example_incremental_demo','raw_source_base_a_del') }}
    where operation = 'D'
)
select
    *,
    {{ add_metadata_columns() }}
from cte_raw_source_base
