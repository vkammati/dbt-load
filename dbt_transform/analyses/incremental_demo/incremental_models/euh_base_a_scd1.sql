{{
  config(
    materialized = 'incremental',
    on_schema_change='sync_all_columns',
    unique_key=['id'],
    tags = "inc_demo",
    )
}}

select
    *,
    {{ add_metadata_columns() }}
from {{ source('example_incremental_demo','raw_source_base_a') }}
