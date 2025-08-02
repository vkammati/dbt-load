{{
  config(
    materialized = 'incremental',
    unique_key=['COMP_CODE','FISCPER','`/BIC/ZCOPA_VV`','PROFIT_CTR','REF_DOC_NR','`/BIC/ZCO_DOCNO`','CURRENCY','CURTYPE','KNCOUNTER','KNART','AGREEMENT','COSTELMNT','REFER_ITM','`/BIC/ZCO_ITMNO`','`/BIC/ZCON_UNIT`','`/BIC/ZBUSTRANS`','`/BIC/ZMRN_T`','`/BIC/ZSALPRDCN`','`/BIC/ZPRICTYP`','`/BIC/ZBUSAST`','`/BIC/ZBOLDT`'],
    on_schema_change = 'ignore',
    tags=["COPA-EUH-D-BW"]
    )
}}

with
source as (
select *
from {{source('raw-ds-dbt','ZOH_COGDP')}}
)
select * from source
