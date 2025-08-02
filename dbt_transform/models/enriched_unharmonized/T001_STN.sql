{{
  config(
    materialized = 'table',
    on_schema_change = 'ignore',
    tags=["MD-ATTR-EUH-FI-D-HANA"]
    )
}}


select * from {{source('raw-ds-dbt','T001_STN')}}
