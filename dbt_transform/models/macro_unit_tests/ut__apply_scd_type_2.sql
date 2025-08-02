{{ config(
    materialized="ephemeral",
) }}


with
-- the input for 'this' will be set in the unit test.
fixture as (
    select *
    from {{ this }}
),

-- Apply the scd 2 macro. This will generate multiple cte's, which will deduplicate the
-- data and determine valid_from, valid_to and is_current fields.
{{ apply_scd_type_2(
    input_cte_name='fixture',
    unique_key_columns=['id'],
    valid_from_column='update_date',
) }}


select *
from fixture_scd
