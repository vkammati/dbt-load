{{ config(
    materialized="ephemeral",
) }}



-- This macro generates a date dimension table based on the specified start and end years
{{ generate_date_dimension(
    start_year=var("start_year", 2022),
    end_year=var("end_year", None)) }}

-- This model is only meant to be used in unit test. By getting the final select
-- statement from a 'var', this model can be used in multiple unit tests, all setting
-- the 'select_statment' var do a different value.
{{ var("select_statement", "select 1 as dummy") }}
