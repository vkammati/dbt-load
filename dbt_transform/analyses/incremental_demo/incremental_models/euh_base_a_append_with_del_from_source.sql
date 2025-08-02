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
    *
from {{ source('example_incremental_demo','raw_source_base_a', append_delete = true) }}
)

select
    *,
    {{ add_metadata_columns() }}
from cte_raw_source_base
