{{
  config(
    materialized = 'incremental',
    incremental_strategy= "append",
    on_schema_change='sync_all_columns',
    )
}}

with
cte_source_order as (
    select
        order_nr,
        ordered_at::date,
        customer_number,
        payment_method,
        OrderDistinctProductCount,
        OrderTotalAmount,
        OrderTotalPayableAmount,
        OrderTotalPayableAmountWithoutVAT,
        order_details,
        hash_key
    from {{source("example_big_order_table","big_order_table")}}
),
cte_update as (
    select
        order_nr,
        ordered_at::date,
        customer_number||substr(rand(),2,4) as customer_number,
        payment_method,
        OrderDistinctProductCount,
        OrderTotalAmount,
        OrderTotalPayableAmount,
        OrderTotalPayableAmountWithoutVAT,
        order_details,
        hash_key
    from cte_source_order
    where year(ordered_at) = 2022 and month(ordered_at) = substr(rand(),3,1)::int+1
),
cte_insert as (
    select
        order_nr||substr(rand(),2,4) as order_nr,
        date_add(ordered_at,365)::date as ordered_at,
        customer_number||substr(rand(),2,4) as customer_number,
        payment_method,
        OrderDistinctProductCount,
        OrderTotalAmount,
        OrderTotalPayableAmount,
        OrderTotalPayableAmountWithoutVAT,
        order_details,
        md5(hash_key||order_nr||substr(rand(),2,4)) as hash_key
    from cte_source_order
    where year(ordered_at) = 2022 and month(ordered_at) = substr(rand(),3,1)::int+1
)

{% if var("run_id") == "first" %}
    select * from cte_source_order
{% elif var("run_id") == "update" %}
    select * from cte_update
{% elif var("run_id") == "insert" %}
    select * from cte_insert
{% elif var("run_id") == "update_insert" %}
    select * from cte_update
    union all
    select * from cte_insert
{% else %}
    select * from cte_source_order where 1=2
{% endif %}
