{{
  config(
    materialized = 'view',
    tags = "inc_demo",
    )
}}

select
    a.id,
    a.name,
    a.email,
    b.country,
    a.operation,
    {{ add_metadata_columns() }}
from {{ ref('euh_base_a_scd1_with_del_from_flag') }} as a
left join {{ ref('euh_child_b_scd1_with_del_from_flag') }} as b
       on a.id = b.id
