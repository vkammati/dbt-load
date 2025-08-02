{{
  config(
    materialized = 'incremental',
    on_schema_change='sync_all_columns',
    unique_key=['id'],
    tags = "inc_demo",

    )
}}

select
    a.id,
    a.name,
    a.email,
    b.country,
    {{ add_metadata_columns() }}
from {{ ref('euh_base_a_scd1') }} as a
left join {{ ref('euh_child_b_scd1') }} as b
       on a.id = b.id
