{{
  config(
    materialized = 'incremental',
    on_schema_change='sync_all_columns',
    unique_key=['id'],
    tags = "inc_demo",
    post_hook = '{{ delete_row("operation") }}',
    )
}}

with cte_raw_source_base as (
    select
        id,
        country,
        null as operation,
        raw_insert_date
    from {{ source('example_incremental_demo','raw_source_child_b') }}
    union all
    select
        id,
        null,
        operation,
        raw_insert_date
    from {{ source('example_incremental_demo','raw_source_child_b_del') }}
    where operation = 'D'
),
cte_deduplicate as (
    select
        id,
        country,
        operation,
        raw_insert_date,
        row_number() over (partition by id order by raw_insert_date desc) as rn
    from cte_raw_source_base
)

select
    id,
    country,
    operation,
    raw_insert_date,
    {{ add_metadata_columns() }}
from cte_deduplicate
where rn = 1
