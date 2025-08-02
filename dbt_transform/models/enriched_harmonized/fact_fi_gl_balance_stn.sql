{{
  config(
    materialized = 'incremental',
    unique_key=['Source_ID','Reference_Document_Number','Reference_Procedure','Reference_Organisational_Units','Ledger','Document_Type','Fiscal_Year','Doc_Number'],
    on_schema_change = 'ignore',
    tags=["FI-EH-D"]
    )
}}

with
rename_filter_glidxa_stn as (
    select
    RCLNT AS Client,
AWREF AS Reference_Document_Number,
AWTYP AS Reference_Procedure,
AWORG AS Reference_Organisational_Units,
RLDNR AS Ledger,
DOCCT AS Document_Type,
RYEAR AS Fiscal_Year,
DOCNR AS Doc_Number,
BUDAT AS Posting_Date_In_The_Document,
RRCTY AS Record_Type,
RVERS AS Version,
RCOMP AS Company,
BUKRS AS Company_Code,
DOCTY AS FI_SL_Document_Type,
BELNR AS Accounting_Document_Number,
LAST_DTM AS LAST_DTM,
 ingested_at AS ingested_at
    from {{ref('GLIDXA_STN')}}
    where RCLNT = '280'
)


select rename_filter_glidxa_stn.* except (ingested_at, Client),
    current_timestamp() as Ingested_At,
    ('{{set_system_id()}} ' || "_" || rename_filter_glidxa_stn.Client) as Source_ID
    from rename_filter_glidxa_stn
    {% if is_incremental() %}
     where ingested_at > (select max(Ingested_at) from {{this}})
    {% endif %}
