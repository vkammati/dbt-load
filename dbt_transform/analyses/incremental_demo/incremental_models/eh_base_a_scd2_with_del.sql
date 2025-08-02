{{
  config(
    materialized = 'view',
    tags = "inc_demo",
    )
}}

with cte_euh_base_a_append_with_del_from_flag as (
select *
from {{ ref('euh_base_a_append_with_del_from_flag') }}
),
{{ apply_scd_type_2(
    input_cte_name='cte_euh_base_a_append_with_del_from_flag',
    unique_key_columns=['id'],
    valid_from_column='raw_insert_date',
) }}

select
    *
from cte_euh_base_a_append_with_del_from_flag_scd
