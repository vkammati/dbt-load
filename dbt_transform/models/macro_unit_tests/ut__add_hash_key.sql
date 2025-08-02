{{ config(
    materialized="ephemeral",
) }}

-- the input for 'this' will be set in the unit test.
select
    {{ add_hash_key(['id', 'name', 'date', 'country']) }},
    id,
    name,
    date,
    country
from {{ this }}
