{{
  config(
    materialized = 'incremental',
    unique_key=['MANDT','GJAHR','BUKRS','BELNR','BUZEI'],
    on_schema_change = 'ignore',
    pre_hook="{{ cluster_table_deletion('AECORSOFT_BSEG',['MANDT','GJAHR','BUKRS','BELNR']) }}",
    post_hook="{{ cluster_table_init('AECORSOFT_BSEG',['MANDT','GJAHR','BUKRS','BELNR'],['MANDT','GJAHR','BUKRS','BELNR','BUZEI'],['AEDATTM']) }}",
    tags=["CLUSTER-EUH"]
    )
}}

with
source as (
  select *, "HANA" as _dbt_source_relation, '' as AESID, '' as AERUNID, '' as AEDATTM, '' as OPFLAG, '' as AERECNO, '' as ACROBJTYPE, '' as ACROBJ_ID, '' as ACRSOBJ_ID, '' as ACRITMTYPE, '' as VALOBJTYPE, '' as VALOBJ_ID, '' as VALSOBJ_ID, '' as IRN
  from {{ source('raw-ds-dbt','BSEG') }}
  union
  select *, "AECORSOFT" AS _dbt_source_relation, '' as LAST_ACTION_CD, '' as LAST_DTM, '' as SYSTEM_ID,'' as ZFINT_LINE_ID,'' as ZFINT_HEADER_ID, '' as ZGL_ACC_ID, '' as ZCOST_CTR_ID, '' as ZCTRL_AREA_ID, '' as ZMEINS_ID, '' as ZVBUND_ID, '' as ZPLANT_ID, '' as ZKTOSL_ID, '' as ZGST_PART_ID, '' as ZTEST_BSEG_CLUSTER, '' as ZTEST
  from {{ source('raw-ds-dbt','AECORSOFT_BSEG')}}
)

select *
from source
