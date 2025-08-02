{{
  config(
    materialized = 'incremental',
    unique_key=['MANDT', 'DTLNUM','SPBUP'],
    on_schema_change = 'ignore',
    tags=["FI-EUH-D-HANA"]
    )
}}
with seq_cte as(

  select * from {{source('raw-ds-dbt','ZSTVA_PE_MC_DTL_STN')}}
),
dedup_cte1 as(
  select * , row_number() over(partition by MANDT,DTLNUM,SPBUP order by LAST_DTM desc ) as row_count
  from seq_cte
),
dedup_cte2 as(
  select * from dedup_cte1 where row_count = 1
)

select * except(row_count) from dedup_cte2
{% if is_incremental() %}
where
 ingested_at > (select max(ingested_at) from {{this}})
{% endif %}
