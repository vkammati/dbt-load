{{
  config(
    materialized = 'incremental',
    unique_key=['Source_ID','Chart_Of_Accounts','GL_Account_Number','Company_Code','Chart_Of_Accounts_SKAT'],
    on_schema_change = 'ignore',
    tags=["MD-EH-FI-D"]
    )
}}

with
rename_filter_ska1_stn as (
select
    MANDT AS  Client,
    KTOPL AS  Chart_Of_Accounts,
    SAKNR AS  GL_Account_Number,
    XBILK AS  Indicator_Account_Is_A_Balance_Sheet_Account,
    SAKAN AS  GL_Account_Number_Significant_Length,
    BILKT AS  Group_Account_Number,
    ERDAT AS  Date_On_Which_The_Record_Was_Created,
    ERNAM AS  Name_Of_Person_Who_Created_The_Object,
    GVTYP AS  PL_Statement_Account_Type,
    KTOKS AS  GL_Account_Group,
    MUSTR AS  Number_Of_The_Sample_Account,
    VBUND AS  Company_Id_Of_Trading_Partner,
    XLOEV AS  Indicator_Account_Marked_For_Deletion,
    XSPEA AS  Indicator_Account_Is_Blocked_For_Creation,
    XSPEB AS  Indicator_Is_Account_Blocked_For_Posting,
    XSPEP AS  Indicator_Account_Blocked_For_Planning,
    MCOD1 AS  Search_Term_For_Using_Matchcode,
    FUNC_AREA AS  Functional_Area,
    ingested_at AS  Ingested_at,
    LAST_DTM AS LAST_DTM
  from {{ref('SKA1_STN')}}
  where MANDT = '280'
  ),
rename_filter_skb1_stn as (
select
  MANDT AS  Client,
    SAKNR AS  GL_Account_Number_SKB1,
    BUKRS AS  Company_Code,
    BEGRU AS  Authorization_Group,
    BUSAB AS  Accounting_Clerk_Abbreviation,
    DATLZ AS  Date_Of_The_Last_Interest_Calculation_Run,
    ERDAT AS  Date_On_Which_The_Record_Was_Created_SKB1,
    ERNAM AS  Name_Of_Person_Who_Created_The_Object_SKB1,
    FDGRV AS  Planning_Group,
    FDLEV AS  Planning_Level,
    FIPLS AS  Financial_Budget_Item,
    FSTAG AS  Field_Status_Group,
    HBKID AS  Short_Key_For_A_House_Bank,
    HKTID AS  Id_For_Account_Details,
    KDFSL AS  Key_For_Exchange_Rate_Differences_In_Foreign_Currency_Accts,
    MITKZ AS  Account_Is_Reconciliation_Account,
    MWSKZ AS  Tax_Category_In_Account_Master_Record,
    STEXT AS  GL_Account_Additional_Text,
    VZSKZ AS  Interest_Calculation_Indicator,
    WAERS AS  Account_Currency,
    WMETH AS  Indicator_Account_Managed_In_External_System,
    XGKON AS  Cash_Receipt_Account_Cash_Disbursement_Account,
    XINTB AS  Indicator_Is_Account_Only_Posted_To_Automatically,
    XKRES AS  Indicator_Can_Line_Items_Be_Displayed_By_Account,
    XLOEB AS  Indicator_Account_Marked_For_Deletion_SKB1,
    XNKON AS  Indicator_Supplement_For_Automatic_Postings,
    XOPVW AS  Indicator_Open_Item_Management,
    XSPEB AS  Indicator_Is_Account_Blocked_For_Posting_SKB1,
    ZINDT AS  Key_Date_Of_Last_Interest_Calculation,
    ZINRT AS  Interest_Calculation_Frequency_In_Months,
    ZUAWA AS  Key_For_Sorting_According_To_Assignment_Numbers,
    ALTKT AS  Alternative_Account_Number_In_Company_Code,
    XMITK AS  Indicator_ReconcilAcct_Ready_For_Input_At_Time_Of_Posting,
    RECID AS  Recovery_Indicator,
    FIPOS AS  Commitment_Item,
    XMWNO AS  Indicator_Tax_Code_Is_Not_A_Required_Field,
    XSALH AS  Indicator_Manage_Balances_In_Local_Currency_Only,
    BEWGP AS  Valuation_Group,
    INFKY AS  Inflation_Key,
    TOGRU AS  Tolerance_Group_For_GL_Accounts,
    XLGCLR AS  Clearing_Specific_To_Ledger_Groups,
    MCAKEY AS  Mca_Key,
    ingested_at AS  Ingested_at
from {{ref('SKB1_STN')}}
where MANDT = '280'
),

rename_filter_skat_stn as (
select
MANDT AS  Client,
    SAKNR AS  GL_Account_Number_SKAT,
    SPRAS AS  Language_Key,
    KTOPL AS  Chart_Of_Accounts_SKAT,
    TXT20 AS  GL_Account_Short_Text,
    TXT50 AS  GL_Account_Long_Text,
    MCOD1 AS  Search_Term_For_Matchcode_Search,
    ingested_at AS  Ingested_at
from {{ref('SKAT_STN')}}
where MANDT = '280'
),

ska1_stn_delta as (
    select GL_Account_Number as GL_Account_Number_delta
    from rename_filter_ska1_stn
    {% if is_incremental() %}
     where ingested_at > (select max(Ingested_at) from {{this}})
    {% endif %}
),
skb1_stn_delta as(
    select GL_Account_Number_SKB1
    from rename_filter_skb1_stn
    {% if is_incremental() %}
     where ingested_at > (select max(Ingested_at) from {{this}})
    {% endif %}
),

skat_stn_delta as(
    select GL_Account_Number_SKAT
    from rename_filter_skat_stn
    {% if is_incremental() %}
     where ingested_at > (select max(Ingested_at) from {{this}})
    {% endif %}
),



ska1_skb1_skat_stn_pk as(
    select GL_Account_Number_delta from ska1_stn_delta
    union
    select GL_Account_Number_SKB1 from skb1_stn_delta
    union
    select GL_Account_Number_SKAT from skat_stn_delta
),

ska1_stn_full(
    select rename_filter_ska1_stn.* except (ingested_at) from rename_filter_ska1_stn inner join ska1_skb1_skat_stn_pk on
    rename_filter_ska1_stn.GL_Account_Number = ska1_skb1_skat_stn_pk.GL_Account_Number_delta
),


ska1_skb1_skat_stn_final(
  select ska1_stn_full.*,
  rename_filter_skb1_stn.* except (Client,GL_Account_Number_SKB1,ingested_at),
  rename_filter_skat_stn.* except (Client,GL_Account_Number_SKAT,ingested_at)
  from ska1_stn_full
  left join rename_filter_skb1_stn
  on ska1_stn_full.GL_Account_Number = rename_filter_skb1_stn.GL_Account_Number_SKB1
  left join rename_filter_skat_stn
  on (ska1_stn_full.GL_Account_Number = rename_filter_skat_stn.GL_Account_Number_SKAT and  ska1_stn_full.Chart_Of_Accounts = rename_filter_skat_stn.Chart_Of_Accounts_SKAT)
)

select ska1_skb1_skat_stn_final.* except (Client),
current_timestamp() as Ingested_At,
('{{ set_system_id() }}' || "_" || ska1_skb1_skat_stn_final.Client) as Source_ID
from ska1_skb1_skat_stn_final

-- select * from ska1_stn_delta
