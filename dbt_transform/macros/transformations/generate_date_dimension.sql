{%- macro generate_date_dimension(start_year, end_year=None) -%}
    {%- set start_year_int = start_year | int -%}
    {%- set current_year = modules.datetime.datetime.now().year -%}
    {%- set end_year_int = (end_year if end_year is not none else current_year  + 1) | int -%}

with date_range as (
    select explode(
        sequence(
            to_date('{{ start_year_int }}-01-01'),
            to_date('{{ end_year_int }}-12-31'),
            interval 1 day
        )
    ) as date
),

final as (
    select
        date,
        date_add(date, 1) as next_day_date,
        year(date) as year,
        (year(date) ||  quarter(date)) as year_quarter,
        (year(date) ||  month(date)) as year_month,
        (year(date) ||  dayofyear(date)) as year_day_of_year,
        quarter(date) as quarter,
        concat('Q', quarter(date)) as quarter_name,
        month(date) as month,
        date_format(date, 'MMMM') as month_name,
        date_format(date, 'MMM') as month_abbr,
        day(date) as day,
        date_format(date, 'EEEE') as day_name,
        (dayofweek(date) + 5) % 7 + 1 as day_of_week, -- Monday=1, Sunday=7
        weekofyear(date) as week_of_year,
        date_format(date, 'yyyy-MMMM') as year_month_name_long,
        case when dayofweek(date) in (1, 7) then true else false end as is_weekend,
        last_day(date) as last_day_of_month,
        case when day(date) = 1 then true else false end as is_quarter_start,
        case when day(date) = day(last_day(date)) then true else false end as is_quarter_end
    from date_range
)

{%- endmacro -%}
