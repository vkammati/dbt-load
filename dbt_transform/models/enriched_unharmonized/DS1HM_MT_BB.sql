{{
  config(
    materialized = 'incremental',
    unique_key=['MANDT','RYEAR','RBUKRS','RLDNR','DOCCT','DOCNR','DOCLN'],
    on_schema_change = 'ignore',
    post_hook="DELETE FROM {{ this }} WHERE OPFLAG = 'D'",
    tags=["FI-EUH"]
    )
}}

with
source as (
  select *, "HANA" as _dbt_source_relation, '' as AESID, '' as AERUNID, '' as AEDATTM, '' as OPFLAG, '' as AERECNO
  from {{ source('raw-ds-dbt','DS1HM_MT_BB') }}
  union
  select *, "AECORSOFT" AS _dbt_source_relation, '' as LAST_ACTION_CD, '' as LAST_DTM, '' as SYSTEM_ID
  from {{ source('raw-ds-dbt','AECORSOFT_DS1HM_MT_BB')}}
)

select *
from source
