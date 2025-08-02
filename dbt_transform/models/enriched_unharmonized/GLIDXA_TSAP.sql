{{
  config(
    materialized = 'incremental',
    unique_key=['RCLNT', 'AWREF','AWTYP','AWORG','RLDNR','DOCCT','RYEAR','DOCNR'],
    on_schema_change = 'ignore',
    tags=["FI-EUH-D-HANA"]
    )
}}
with source as(

  select * from {{source('raw-ds-dbt','GLIDXA_TSAP')}}
)
select * from source
