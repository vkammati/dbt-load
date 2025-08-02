
-- This macro is meant apply all table and column-level tags to the the UC table/view. Because
-- tags are actually meant for dbt orchestration, not all tags are per defintion meant for UC.
-- these tags can be provided as an ignore list.
{% macro commit_model_tags_to_uc(tags_to_ignore = []) %}
    {% if execute and model.config.materialization not in ['ephemeral'] %}
        -- All alter statements will be added to an array and then sequentially executed
        {%- set alter_statements = [] -%}

        -- First table-level tags, if there are any
        {% if model.config.tags %}
            -- UC supports tags with and without values and one can set multiple tags in 1 sql statement.
            -- However, it is not allowed to mix tags with and without values in one statement. Therefor
            -- the parse_uc_tags will split them into two groups and also filter out any tags that must
            -- be ignored.
            {%- set all_tags = parse_uc_tags(model.config.tags, tags_to_ignore) -%}

            --Loop over the returned dictionary and apply the tags (if any) for both keys
            {%- for key, value in all_tags|items -%}
                {% if value|length > 0 %}
                    --Note: alter 'table' is also allowed for views in this case so need to check materialization
                    {% do alter_statements.append("alter table "~this~" set tags("~value|join(',')~")") %}
                {% endif %}
            {%- endfor %}
        {% endif %}

        -- Second, loop over all columns and check if there are any tags set
        {%- for col in model.columns -%}
            {% if model.columns[col].tags|length > 0 %}
                -- Just as with the table-level tags, the column-level tags need to be split and filtered.
                {%- set all_tags = parse_uc_tags(model.columns[col].tags, tags_to_ignore) -%}

                {%- for key, value in all_tags|items -%}
                    {% if value|length > 0 %}
                        {% do alter_statements.append("alter table "~this~" alter column "~col~" set tags("~value|join(',')~")") %}
                    {% endif %}
                {%- endfor %}
            {% endif %}
        {%- endfor %}

        -- Now that all required tags have been gathered, simply execute them all. This is needed because
        -- it is not possible to return more than one sql statement from a post-hook using a macro.
        {% do execute_sql_statements_without_result(alter_statements, False) %}

        -- The macro itself doesn't need to return anything. This is to make sure that any code printed in
        -- here (like comments) are not accidentailly picked up as sql that needs to be executed.
        {{ return('') }}
    {% endif %}
{% endmacro %}


-- This macro will parse all source nodes and sync all table and column level tags
-- with UC
{% macro commit_all_source_tags_to_uc(tags_to_ignore = []) %}
    {% if execute %}
        -- First, we query the information_schema to find out which table and column level tags are currently set
        -- Only the tables the service principal used has access to will be retruned so no need to filter here.
        -- in this macro, only sources that are in this project will be synced, nothing else. All other information
        -- coming in through this query will be ignored.
        -- The query will aggregate all data to table level tothat it matches the graph structure which will make
        -- the flow easier to and faster.
        {% set query %}
            select  concat(catalog_name, '.', schema_name, '.', table_name) as full_name, to_json((named_struct('tags', array_agg(table_tags)[0], 'columns',  array_agg(column_tags)))) config
            from (
                select
                    catalog_name, schema_name, table_name, named_struct("column_name", column_name, "tags", array_agg(case when coalesce(tag_value,'') = '' then tag_name else concat(tag_name,'=',tag_value) end)) as column_tags, null as table_tags
                from system.information_schema.column_tags
                group by catalog_name, schema_name, table_name, column_name

                union

                select
                    catalog_name, schema_name, table_name, null, array_agg(case when coalesce(tag_value,'') = '' then tag_name else concat(tag_name,'=',tag_value) end) as table__tags
                from system.information_schema.table_tags
                group by catalog_name, schema_name, table_name
            ) column_tags
            group by catalog_name, schema_name, table_name;
        {% endset %}

        -- Fetch result
        {% set results = run_query(query) %}

        {% do log("Starting 'commit_all_source_tags_to_uc' " ~source_key, info=true) %}
        -- Now, loop over all source nodes (this is the "should be"-state) and compare to the query result (the "current"-state)
        {% for node in graph.sources.values() -%}
            {% set source_key = node.database~"."~node.schema~"."~node.identifier %}

            {% do log("  Processing: " ~source_key, info=true) %}

            {% set result_row = results|selectattr("full_name", "equalto", source_key)|list %}
            {% if result_row %}
                -- If the node is also in the query result, this means there is already a table/column level tag set
                {% set result_config = fromjson(result_row|map(attribute="config")|first) %}

                -- First compare table level properies and sync if needed.
                {% if node.tags|sort != result_config.tags|sort %}
                    {% do log("    table tags are not equal. Sync table level tags.", info=true) %}
                    {% do commit_source_tags_to_uc_sync(table_name=node.relation_name, column_name=None, current_tags=result_config.tags, should_be_tags=node.tags, tags_to_ignore=tags_to_ignore ) %}
                {% else %}
                    {% do log("    table tags are equal.") %}
                {% endif %}

                -- Now, compare and possibly syn all columns
                {% for col in node.columns %}
                    {% do log("    Column: " ~col, info=true) %}

                    {% set result_column = result_config.columns|selectattr("column_name", "equalto", col)|list %}
                    {% if result_column %}
                        -- If the column is also in the query result, this means there is already a column level tag set
                        {% if node.columns[col].tags|sort != result_column[0].tags|sort %}
                            {% do log("      column tags are not equal. Sync column level tags.", info=true) %}
                            {% do commit_source_tags_to_uc_sync(table_name=node.relation_name, column_name=col, current_tags=result_column[0].tags, should_be_tags=node.columns[col].tags, tags_to_ignore=tags_to_ignore ) %}
                        {% else %}
                            {% do log("      column tags are equal.") %}
                        {% endif %}
                    {% else %}
                        -- No column level tags yet for this column, so add them
                        {% do log("      column tags need to be added.", info=true) %}
                        {% do commit_source_tags_to_uc_sync(table_name=node.relation_name, column_name=col, current_tags=[], should_be_tags=node.columns[col].tags, tags_to_ignore=tags_to_ignore ) %}
                    {% endif %}
                {%- endfor %}
                --Check for columns in query result that are not set in source (anymore)
                {% for col in result_config.columns|rejectattr("column_name", "in", node.columns|list)|list %}
                    {% do log("    this column has tags that need to be removed: " ~ col.column_name, info=true) %}
                    {% do commit_source_tags_to_uc_sync(table_name=node.relation_name, column_name=col.column_name, current_tags=col.tags, should_be_tags=[], tags_to_ignore=tags_to_ignore ) %}
                {%- endfor %}

            {% elif not result_row and (node.tags or node.columns|list)  %}
                --Tags set for source but not in result query. Need to be added. First table level (if any)
                {% if node.tags %}
                    {% do log("    table tags need to be added.", info=true) %}
                    {% do commit_source_tags_to_uc_sync(table_name=node.relation_name, column_name=None, current_tags=[], should_be_tags=node.tags, tags_to_ignore=tags_to_ignore ) %}
                {%- endif %}
                --And then for each column
                {% for col in node.columns %}
                    {% do log("    Column: " ~col, info=true) %}
                    {% if node.columns[col].tags %}
                        {% do log("      column tags need to be added.", info=true) %}
                        {% do commit_source_tags_to_uc_sync(table_name=node.relation_name, column_name=col, current_tags=[], should_be_tags=node.columns[col].tags, tags_to_ignore=tags_to_ignore ) %}
                    {%- endif %}
                {%- endfor %}

            {% endif %}
        {%- endfor %}
    {% endif %}
{% endmacro %}


-- This macro is meant to work in conjunction with 'commit_model_tags_to_uc' and 'commit_all_source_tags_to_uc'.
-- It is used to parse a sequence of strings named 'tags' into a dictionary with two keys: tags_with_value
-- and tags_without_value. Both will be a list of strings, properly quoted and key/values split for use in
-- an alter statement. Any tags passed in via de 'tags_to_ignore' list will be ignored.
{% macro parse_uc_tags(tags, tags_to_ignore = []) %}
    -- In jinja, variables set or changed within a loop are reset after the loop. To overcome this, the
    -- variable must be set inside a namespace which is created before the loop. For more information,
    -- see: https://jinja.palletsprojects.com/en/2.11.x/templates/#assignments
    {% set ns = namespace(tags_with_value=[], tags_without_value=[]) %}
    {% for tag in tags|reject("in", tags_to_ignore)|list %}
        -- If the tag contains an '='-sign, this means this will be deployed as a tag with a value.
        -- Otherwise, it will be a simple tag without a value.
        {% if '=' in tag %}
            {% set split_tag = tag.split("=") %}
            {% do ns.tags_with_value.append("'"~split_tag[0]~"' = '"~split_tag[1]~"'") %}
        {% else %}
            {% do ns.tags_without_value.append("'"~tag~"'") %}
        {% endif %}
    {% endfor %}

    -- Return both lists in one dictionary
    {{ return({"tags_with_value": ns.tags_with_value, "tags_without_value": ns.tags_without_value}) }}
{% endmacro %}


-- This macro is meant to work in conjunction with 'commit_all_source_tags_to_uc'. It receives a list of
-- currently applied tags (current_tags), a list of all the tags that should be there (should_be_tags)
-- and a list of tags to ignore (tags_to_ignore) and caclulate all sql statements that need to be executed to
-- get from the 'current tags' to the 'should be tags' without any (optional) 'tags to ignore'. It will also
-- execute those sql statements.
-- This macro can be use on both table (set column_name to None) and column level.
{% macro commit_source_tags_to_uc_sync(table_name, column_name, current_tags, should_be_tags, tags_to_ignore ) %}
    {% do log("Syncing table "~table_name~", column: "~column_name~", current_tags: "~current_tags~", should_be_tags: "~should_be_tags) %}

    --First remove all tags that should not be there anymore. For simplicity reasons, tags that change value
    --are first removed here and then added with the new value later on.
    {% set alter_statements = [] %}

    {% for tag_to_remove in current_tags|reject("in", should_be_tags)|list  %}
        {% set tag = tag_to_remove if '=' not in tag_to_remove else tag_to_remove.split("=")[0]  %}
        {% if column_name %}
            {%- set alter_column = " alter column "~ column_name -%}
        {% endif %}
        {% do alter_statements.append("alter table "~table_name~alter_column~" unset tags('"~tag~"')") %}
    {% endfor %}

    -- Now simply execute the sql statement(s). Do the removal first to prevent accidentally removed chnaged tags.
    {% do execute_sql_statements_without_result(alter_statements, True) %}


    -- Reset array
    {% set alter_statements = [] %}

    -- Now parse  the tags in to tags with and without value while igonoring the ignore list and calculate sql
    -- statemets to set them all.
    {%- set all_tags = parse_uc_tags(should_be_tags, tags_to_ignore) -%}
    {%- for key, value in all_tags|items -%}
        {% if value|length > 0 %}
            {%- set alter_column = "" -%}
            {% if column_name %}
                {%- set alter_column = " alter column "~ column_name -%}
            {% endif %}

            {% do alter_statements.append("alter table "~table_name~alter_column~" set tags("~value|join(',')~")") %}
        {% endif %}
    {%- endfor %}

    -- Finally, simply execute the rest of the sql statement(s).
    {% do execute_sql_statements_without_result(alter_statements, True) %}

{% endmacro %}
