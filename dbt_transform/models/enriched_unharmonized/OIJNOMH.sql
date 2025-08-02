{{
  config(
    materialized = 'incremental',
    unique_key=['MANDT', 'NOMTK'],
    on_schema_change = 'ignore',
    post_hook="DELETE FROM {{ this }} WHERE OPFLAG = 'D'",
    tags=["HM-EUH-D"]
    )
}}

with
source as (
    select *, "HANA" as _dbt_source_relation, '' as aesid, '' as aerunid, '' as aedattm, '' as opflag, '' as aerecno
    from {{ source('raw-ds-dbt','OIJNOMH') }}
    union
    select *, "AECORSOFT" AS _dbt_source_relation, '' as last_action_cd, '' as last_dtm, '' as system_id, '' as zmd_tr_id, '' as znom_id, '' as zveh_vess_id
    from {{ source('raw-ds-dbt','AECORSOFT_OIJNOMH') }}
    where AEDATTM is not null
)

select *
from source
