{% macro get_local_user_name() %}

    {% set var_user_name = env_var("USERNAME", default=env_var("USER", default="")) -%}
    {% set user_name = var_user_name | replace(".","_") | lower -%}

    {{ return(user_name) }}

{% endmacro %}
