-- Sometimes, a property can be returned as a string or a list depending on how it is
-- configured. That can make it hard to work with. This helper function takes a string
-- or list as argument and will always return it as a list.
--  - If a list is supplied, it is simly returned "as is"
--  - if a string is supplied, it will be returned as a list with the string as its only
--    element
--  - if an empty string or 'None' is supplied, an empty list is returned,

{% macro get_list(string_or_list) %}

    {% if string_or_list is none or string_or_list == ""  %}
        {% set list = [] %}
    {% elif string_or_list is string %}
        {% set list = [string_or_list] %}
    {% else %}
        {% set list = string_or_list %}
    {% endif %}

    {{ return(list) }}

{% endmacro %}
