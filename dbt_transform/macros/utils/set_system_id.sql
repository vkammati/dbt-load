
{% macro set_system_id() %}
    {% set env_name = env_var('DBX_UNITY_CATALOG', 'default_value') %}

    {% set system_id = 'Unknown' %}
    {% if env_name == 'cross_ds-unitycatalog-dev' %}
        {% set system_id = 'D80' %}
    {% elif env_name == 'cross_ds-unitycatalog-tst' %}
        {% set system_id = 'A80' %}
    {% elif env_name == 'cross_ds-unitycatalog-pre' %}
        {% set system_id = 'B80' %}
    {% elif env_name == 'cross_ds-unitycatalog-prd' %}
        {% set system_id = 'P80' %}
    {% endif %}

    {{ return(system_id) }}
{% endmacro %}
