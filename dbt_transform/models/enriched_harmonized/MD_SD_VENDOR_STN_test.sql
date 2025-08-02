{{
  config(
    materialized = 'incremental',
    unique_key=['Account_Number_Of_Supplier'],
    on_schema_change = 'ignore',
    tags=["MD-EH-SD-D"]
    )
}}

with
lfa1_stn_cte as (
    select
    MANDT as Client,
    LIFNR as Account_Number_Of_Supplier,
    ZLAND1_ID as Country_Id,
    LAND1 as Country_Key,
    NAME1 as Name_1,
    ORT01 as City,
    TELBX as Telebox_Number,
    LAST_DTM as LAST_DTM,
    ingested_at as ingested_at
    from {{ ref('LFA1_STN')}}
    {% if is_incremental() %}
     where
     ingested_at > (select max(ingested_at) from {{this}})
    {% endif %}
),
lfb1_stn_cte as (
    select
    MANDT AS Client,
    LIFNR AS LFB1_Account_Number_Of_Supplier,
    BUKRS AS Company_Code,
    ingested_at AS ingested_at
    from {{ ref('LFB1_STN')}}
    {% if is_incremental() %}
     where
     ingested_at > (select max(Ingested_at) from {{this}})
    {% endif %}
)

select Account_Number_Of_Supplier,Country_Id,Country_Key,Name_1,City,Telebox_Number,LAST_DTM,Company_Code, current_timestamp() as Ingested_at
from lfa1_stn_cte left join lfb1_stn_cte
on lfa1_stn_cte.Account_Number_Of_Supplier = lfb1_stn_cte.LFB1_Account_Number_Of_Supplier
