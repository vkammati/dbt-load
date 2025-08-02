{{
  config(
    materialized = 'incremental',
    unique_key=['MANDT', 'BUKRS','BELNR','GJAHR','BUZEI'],
    on_schema_change = 'ignore',
    tags=["FI-EUH-D-HANA"]
    )
}}

with source as(

  select * from {{source('raw-ds-dbt','BSEG_TSAP')}}
)

select * from source
