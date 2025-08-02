{{
  config(
    materialized = 'table',
    on_schema_change = 'ignore',
    tags=["MD-ATTR-EUH-COPA-D-BW"]
    )
}}

with
zoh_agrdp as(
    select *
    from {{source('raw-ds-dbt','ZOH_CMRDP')}}
)


select * from zoh_agrdp
