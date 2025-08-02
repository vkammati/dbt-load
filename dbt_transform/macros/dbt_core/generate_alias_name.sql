{% macro generate_alias_name(custom_alias_name=none, node=none) -%}

    {%- if custom_alias_name -%}

        {%- if var("prefix_table") -%}{{ var("prefix") }}{%- endif -%}{{ custom_alias_name | trim }}

    {%- elif node.version -%}

        {%- if var("prefix_table") -%}{{ var("prefix") }}{%- endif -%}{{ return(node.name ~ "_v" ~ (node.version | replace(".", "_"))) }}

    {%- else -%}

        {%- if var("prefix_table") -%}{{ var("prefix") }}{%- endif -%}{{ node.name }}

    {%- endif -%}

{%- endmacro %}
