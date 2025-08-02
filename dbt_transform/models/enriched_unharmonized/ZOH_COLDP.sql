{{
  config(
    materialized = 'incremental',
    unique_key=['COMP_CODE','FISCPER','`/BIC/ZCOPA_VV`','PROFIT_CTR','REF_DOC_NR','`/BIC/ZCO_DOCNO`','CURRENCY','CURTYPE','KNCOUNTER','KNART','AGREEMENT','COSTELMNT','REFER_ITM','`/BIC/ZCO_ITMNO`','`/BIC/ZCON_UNIT`','`/BIC/ZBUSTRANS`','`/BIC/ZMRN_T`','`/BIC/ZSALPRDCN`','`/BIC/ZPRICTYP`','`/BIC/ZBUSAST`','`/BIC/ZBOLDT`'],
    on_schema_change = 'ignore',
    tags=["COPA-EUH-D-BW"]
    )
}}

with
zoh_cogdp as (
select *
from {{source('raw-ds-dbt','ZOH_COLDP')}}
),

zoh_coldp_deduplicated1 as (
  select *, row_number() over (partition by COMP_CODE,FISCPER,`/BIC/ZCOPA_VV`,PROFIT_CTR,REF_DOC_NR,`/BIC/ZCO_DOCNO`,CURRENCY,CURTYPE,KNCOUNTER,KNART,AGREEMENT,COSTELMNT,REFER_ITM,`/BIC/ZCO_ITMNO`,`/BIC/ZCON_UNIT`,`/BIC/ZBUSTRANS`,`/BIC/ZMRN_T`,`/BIC/ZSALPRDCN`,`/BIC/ZPRICTYP`,`/BIC/ZBUSAST`,`/BIC/ZBOLDT` order by `TIMESTAMP` desc) as row_number
  from zoh_cogdp
),

zoh_coldp_deduplicated2 as (
  select * except (row_number)
  from zoh_coldp_deduplicated1
  where row_number = 1
)

select * from zoh_coldp_deduplicated2
{% if is_incremental() %}
where
 ingested_at > (select max(ingested_at) from {{this}})
{% endif %}
