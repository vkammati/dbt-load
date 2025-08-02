{{
  config(
    materialized = 'incremental',
    unique_key=['MANDT', 'KUNNR','BUKRS'],
    on_schema_change = 'ignore',
    tags=["MD-ATTR-EUH-SD-D-HANA"]
    )
}}

with source as(

  select * from {{source('raw-ds-dbt','KNB1_TSAP')}}
)
select *  from source
