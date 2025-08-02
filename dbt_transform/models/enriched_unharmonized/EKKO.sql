{{
  config(
    materialized = 'incremental',
    unique_key=['MANDT', 'EBELN'],
    on_schema_change = 'ignore',
    post_hook="DELETE FROM {{ this }} WHERE OPFLAG = 'D'",
    tags=["HM-EUH-D"]
    )
}}

with
source as (
    select *, "HANA" as _dbt_source_relation, '' as AESID, '' as AERUNID, '' as AEDATTM, '' as OPFLAG, '' as AERECNO
    from {{ source('raw-ds-dbt','EKKO') }}
    union
    select *, "AECORSOFT" AS _dbt_source_relation, '' as LAST_ACTION_CD, '' as LAST_DTM, '' as SYSTEM_ID, '' as ZPUR_ORD_SAP_ID, '' as ZCUR_ID, '' as ZSH_COMP_ID, '' as ZLIFNR_ID, '' as ZCONT_TYP_ID, '' as ZPUR_ORD_NO, '' as ZPUR_ORD_CAT_ID, '' as ZPUR_DOC_TYP_ID, '' as ZINCO_TERM_TYPE_ID
    from {{ source('raw-ds-dbt','AECORSOFT_EKKO')}}
    where AEDATTM is not null
)

select *
from source
