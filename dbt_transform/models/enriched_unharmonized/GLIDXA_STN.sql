{{
  config(
    materialized = 'incremental',
    on_schema_change = 'ignore',
    tags=["FI-EUH-D-HANA"]
    )
}}

select * from {{source('raw-ds-dbt','GLIDXA_STN')}}
