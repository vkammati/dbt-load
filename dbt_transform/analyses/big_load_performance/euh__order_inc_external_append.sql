{{
  config(
    materialized = 'incremental',
    on_schema_change='sync_all_columns',
    partition_by=['year','month','day'],
    incremental_strategy= "append",
    location_root=var('schema_location'),
    )
}}


with
order as (
    select *
    from {{ ref('raw__big_order_table') }}
),

final as (
    select
        ord.order_nr, ord.hash_key,
        r.col.order_detail_nr,
        ord.ordered_at,
        ord.customer_number as customer_nr,
        ord.payment_method,
        ord.`OrderDistinctProductCount` as order_distinct_product_count,
        ord.`OrderTotalAmount` as order_total_amount,
        ord.`OrderTotalPayableAmount` as order_total_payable_amount,
        ord.`OrderTotalPayableAmountWithoutVAT` as order_total_payable_amount_without_vat,
        r.col.product_nr,
        r.col.amount,
        r.col.price,
        r.col.`priceWithoutVAT` as price_without_vat,
        year(ord.ordered_at) as year,
        month(ord.ordered_at) as month,
        day(ord.ordered_at) as day,
        {{ add_metadata_columns() }}
    from order as ord
        lateral view EXPLODE(order_details) r
)

select *
from final
