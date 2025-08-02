{%- macro ref() -%}
    -- The bulk of this macro comes from this documentation: https://docs.getdbt.com/reference/dbt-jinja-functions/builtins.
    -- It is only augmented from the core functionality to add out-of-the-box incremental filtering when used in incremental
    -- models

    -- Extract user-provided positional and keyword arguments
    {% set version = kwargs.get('version') %}
    {% set packagename = none %}
    {%- if (varargs | length) == 1 -%}
        {% set modelname = varargs[0] %}
    {%- else -%}
        {% set packagename = varargs[0] %}
        {% set modelname = varargs[1] %}
    {% endif %}

    -- Call builtins.ref based on provided positional arguments
    {% set rel = None %}
    {% set identifier = None %}
    {% if packagename is not none %}
        {% set rel = builtins.ref(packagename, modelname, version=version) %}
        {% set identifier = [packagename, modelname] %}
    {% else %}
        {% set rel = builtins.ref(modelname, version=version) %}
        {% set identifier = [project_name, modelname] %}
    {% endif %}

    -- Enable incremental logic for this relation if needed
    {% do return(get_incremental_relation(source_type="ref", source_identifier=identifier, source_relation=rel, cdf_incremental=kwargs.get('cdf_incremental'), append_delete=kwargs.get('append_delete'))) %}

{%- endmacro -%}
