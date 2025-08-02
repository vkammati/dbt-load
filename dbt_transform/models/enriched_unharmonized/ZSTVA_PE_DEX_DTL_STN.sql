{{
  config(
    materialized = 'incremental',
    unique_key=['MANDT', 'DTLNUM'],
    on_schema_change = 'ignore',
    tags=["FI-EUH-D-HANA"]
    )
}}
with source as(

  select * from {{source('raw-ds-dbt','ZSTVA_PE_DEX_DTL_STN')}}
)
select *  from source
