{{
  config(
    materialized = 'view',
    tags = "inc_demo",
    )
}}

with cte_euh_base_a_append as (
select
    id,
    name,
    email,
    raw_insert_date
from {{ ref('euh_base_a_append') }}
),
{{ apply_scd_type_2(
    input_cte_name='cte_euh_base_a_append',
    unique_key_columns=['id'],
    valid_from_column='raw_insert_date',
) }}

select
    *,
    {{ add_metadata_columns() }}
from cte_euh_base_a_append_scd
