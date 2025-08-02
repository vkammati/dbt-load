{{
  config(
    materialized = 'incremental',
    unique_key=['MANDT', 'LIFNR','BUKRS'],
    on_schema_change = 'ignore',
    tags=["MD-ATTR-EUH-SD-D-HANA"]
    )
}}
with source as(

  select * from {{source('raw-ds-dbt','LFB1_STN')}}
)
select *  from source
