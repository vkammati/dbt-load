{% macro apply_scd_type_2(input_cte_name, unique_key_columns, valid_from_column, exclude_from_check_columns = [], first_valid_from_date = none) -%}

{% set last_valid_to_date = '9999-12-31' %}

{{input_cte_name}}_with_checksum as (
    select
        *,
        md5(array_join(
            array({{input_cte_name}}.* except ({{valid_from_column}}
            {%- for col in exclude_from_check_columns -%}
                , {{col}}
            {%- endfor %}))
          , '|', '')) as row_checksum
    from {{input_cte_name}}
),

{{input_cte_name}}_with_previous_checksum as (
    select
        *,
        lag(row_checksum, 1) over (partition by
            {%- for col in unique_key_columns -%}
                {%- if not loop.first %},{% endif %} {{col}}
            {%- endfor %} order by {{valid_from_column}}) as previous_checksum
    from {{input_cte_name}}_with_checksum
),

{{input_cte_name}}_with_next_change_and_remove_unchanged as (
    select
        *,
        lead({{valid_from_column}}, 1) over (partition by
            {%- for col in unique_key_columns -%}
                {%- if not loop.first %},{% endif %} {{col}}
            {%- endfor %} order by {{valid_from_column}}) as next_change_at
    from {{input_cte_name}}_with_previous_checksum
    where row_checksum != coalesce(previous_checksum, '')
),

{{input_cte_name}}_scd as (
  select
      * except (row_checksum, previous_checksum, next_change_at),
      to_timestamp(
        {%- if first_valid_from_date is none -%}
        {{valid_from_column}}
        {%- else -%}
        case when previous_checksum is null then cast('{{first_valid_from_date}}' as date) else {{valid_from_column}} end
        {%- endif -%}
        ) as valid_from,
      to_timestamp(coalesce(try_subtract(next_change_at, interval '1' millisecond), '9999-12-31')) as valid_to,
      case when next_change_at is null then true else false end as is_current
  from {{input_cte_name}}_with_next_change_and_remove_unchanged
)

{%- endmacro %}
