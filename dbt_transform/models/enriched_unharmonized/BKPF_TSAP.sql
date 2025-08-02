-- CICD test1
{{
  config(
    materialized = 'incremental',
    unique_key=['MANDT', 'BUKRS','BELNR','GJAHR'],
    on_schema_change = 'ignore',
    tags=["FI-EUH-D-HANA"]
    )
}}
with source as(

  select * from {{source('raw-ds-dbt','BKPF_TSAP')}}
)
select *  from source
