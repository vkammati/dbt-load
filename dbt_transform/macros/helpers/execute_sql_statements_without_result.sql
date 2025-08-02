{%- macro execute_sql_statements_without_result(sql_statements = [], log_statement = true) -%}

    {%- for sql_statement in sql_statements -%}
        {%- if sql_statement != '' and sql_statement is not none -%}
            {{- log("Executing 'execute_statements_without_result' with statement: " ~ sql_statement, info=log_statement) -}}

            {%- call statement(None, fetch_result=False, auto_begin=False) -%}
            {{sql_statement}}
            {%- endcall -%}
        {%- endif -%}
    {%- endfor -%}

{%- endmacro -%}
