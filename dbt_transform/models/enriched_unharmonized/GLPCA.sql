{{
  config(
    materialized = 'incremental',
    unique_key=['RCLNT', 'GL_SIRID'],
    on_schema_change = 'ignore',
    post_hook="DELETE FROM {{ this }} WHERE OPFLAG = 'D'",
    tags=["FI-EUH"]
    )
}}

with
source as (
  select *, "HANA" as _dbt_source_relation, '' as AESID, '' as AERUNID, '' as AEDATTM, '' as OPFLAG, '' as AERECNO
  from {{ source('raw-ds-dbt','GLPCA') }}
  union
  select *, "AECORSOFT" AS _dbt_source_relation, '' as LAST_ACTION_CD, '' as LAST_DTM, '' as SYSTEM_ID
  from {{ source('raw-ds-dbt','AECORSOFT_GLPCA')}}
)

select *
from source
