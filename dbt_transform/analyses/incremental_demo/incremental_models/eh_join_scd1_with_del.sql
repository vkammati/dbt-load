{{
  config(
    materialized = 'incremental',
    on_schema_change='sync_all_columns',
    unique_key=['id'],
    tags = "inc_demo",
    post_hook = '{{ delete_row("deleted_date") }}',
    )
}}

with cte_all_changes as (
    select id from {{ ref('eh_base_a_scd2_with_del') }}
    union
    select id from {{ ref('euh_child_b_scd1_with_del_from_flag') }}
)
, cte_inc_source_a as (
    select
        *
    from {{ ref('eh_base_a_scd2_with_del', cdf_incremental=false) }}
    where is_current = true
    and id in (
        select id from cte_all_changes
    )
)
, cte_inc_source_b as (
    select
        *
    from {{ ref('euh_child_b_scd1_with_del_from_flag', cdf_incremental=false) }}
    where id in (
        select id from cte_all_changes
    )
)

select
    a.id,
    a.name,
    a.email,
    b.country,
    a.operation,
    {{ add_metadata_columns() }}
from cte_inc_source_a as a
left join cte_inc_source_b as b
    on a.id = b.id
