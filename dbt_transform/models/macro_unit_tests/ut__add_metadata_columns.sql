{{ config(
    materialized="ephemeral",
) }}

-- the input for 'this' will be set in the unit test.
select
    id,
    {{ add_metadata_columns() }}
from {{ this }}
