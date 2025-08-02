{%- macro source(source_name, table_name) -%}
    -- The 'source' macro is only augmented from the core functionality to add out-of-the-box incremental filtering when
    -- used in incremental models.
    -- for more information how to use 'builtins', checkout this page: https://docs.getdbt.com/reference/dbt-jinja-functions/builtins.

    -- Call builtins.source based on provided positional arguments
    {% set rel = None %}
    {% set rel = builtins.source(source_name, table_name) %}

    -- Enable incremental logic for this relation if needed
    {% do return(get_incremental_relation(source_type="source", source_identifier=[source_name, table_name], source_relation=rel, cdf_incremental=kwargs.get('cdf_incremental'), append_delete=kwargs.get('append_delete'))) %}

{%- endmacro -%}
