-- DBT comes with incremental support via an 'incremental' materialization. However, this only effect whether the model is loaded via
-- a 'merge' or a 'create or replace' statement. It is up to the user to filter the input to a model (using 'ref or 'source') to only
-- yield the rows that are inserted or updated since the last run. This macro add out-of-the-box support to filter the input from a
-- 'ref' or 'source' using Change Delta Feed (CDF).
{%- macro get_incremental_relation(source_type, source_identifier, source_relation) -%}
    -- The incremental loading logic is used by default and can be disabled by supplying the named argument 'cdf_incremental=False'
    -- as a parameter to the ref/source macro. Alternatively, it can also be disabled by setting the config property 'cdf_incremental'
    -- to False for one or multiple models. If so, no incremental logic will be applied and the default behaviour of source/ref is used.
    {% set cdf_incremental_param = kwargs.get('cdf_incremental') | as_bool %}
    {% set cdf_incremental_config = model.config.get('cdf_incremental') | as_bool %}
    {% set cdf_incremental = False if cdf_incremental_param == False else cdf_incremental_config != False %}

    --This incremental logic is also only meant for incremental models within the project (not 3rd party packages).
    --Var "project_name" is a built-in global variable that contains the name of the main project.
    {% if cdf_incremental == True and model.fqn[0] == project_name %}
        -- Add a post-hook to set the latest version on the target table. Unfortunately, post_hook must be
        -- set before the 'execute' phase and therefore cannot be triggered on incremental models only.
        -- The macro called from the post-hook will check if anything needs to be executed.
        {{
            config(
                post_hook=["{{ get_incremental_relation_post_hook('"~source_type~"', "~source_identifier~") }}"]
            )
        }}

        {% if execute %}
            --This incremental logic is only meant for incremental models within the project (not 3rd party packages).
            --Var "project_name" is a built-in global variable that contains the name of the main project.
            {% if model.config.materialized in ['table', 'incremental'] and model.fqn[0] == project_name %}

                -- In case the relation is a 'source' instead of a 'ref', assume it is a table (not a view) or 'cdf_incremental'
                -- is set to false. However, if the source is a 'ref' to another model an additional check is needed because
                -- views or ephemeral model cannot have CDF enabled and can therefore not be used to get incremental data from.
                -- Tables will always be 'created or replaced' so, unless the model itself is filtered, will always return
                -- all rows. However for these the version is still fetched and stored because that makes it possible to change
                -- their materialization into 'incremental' without the need to doing a full refresh. This does come with a
                -- small overhead which can be removed by only executing the logic for incremental sources.

                -- Here we fetch the model (source or ref) which is used in various places in this macro.
                {% set source_model = get_incremental_relation_source_model(source_type, source_identifier) %}

                --Incremental logic applies to both 'source' and 'ref' and to 'models' materialized as 'incremental' or 'table'
                --or 'snapshots'. 'Seeds' are not supported as these are always fully loaded.
                {% if
                    (source_type == "source" and source_model.config.cdf_incremental != False)
                    or source_model.config.materialized in ['table', 'incremental', 'snapshot']
                %}
                    -- Reload the source_relation to check two things: the relation exists and
                    -- the relation is not a view. If it does not (yet) exist or it is a view,
                    -- the 'describe history' query will fail during compilation.
                    {% set reloaded_source_relation = load_relation(source_relation) %}
                    {% if reloaded_source_relation is none or reloaded_source_relation.is_view %}
                        {% set results = [] %}
                    {% else %}
                        -- Get the delta.LastUpdateVersion of the source. This can only be fetched using 'describe'
                        -- on the source table and get the latest value. Also checking for possible destructive operations
                        -- like 'create or replace' or enabling CDF. CDF does not work across those operations.
                        {% set source_tblproperties_query %}
                        with hist as (
                            describe history {{source_relation}}
                        ),
                        hist_source_last_update_version as (
                            select
                                max(version) as source_last_update_version
                            from hist
                        ),
                        hist_source_first_valid_update_version as (
                            select
                                max(version) as source_first_valid_update_version
                            from hist
                            where
                                operation like '%CREATE%TABLE%'
                            or operation IN (
                                'REPLACE TABLE AS SELECT',
                                'DROP COLUMNS',
                                'RENAME COLUMN'
                                )
                            or operationParameters::string like '%delta.enableChangeDataFeed\":\"true%'
                        )
                        select *
                        from hist_source_last_update_version
                        inner join hist_source_first_valid_update_version
                        {% endset %}
                        -- Executing 'describe history' should always lead to a result.
                        {% set results = run_query(source_tblproperties_query) %}

                        --We also check if CDF is enabled at the moment
                        {% set source_cdf_enabled_query %}
                        show tblproperties {{source_relation}} (delta.enableChangeDataFeed)
                        {% endset %}
                        -- This query will always yield a 'value'. If CDF is not enabled, a warning will be shown.
                        {% set cdf_enabled_results = run_query(source_cdf_enabled_query) %}
                        {% set cdf_enabled = cdf_enabled_results.columns['value'][0]|lower == "true"  %}
                    {% endif %}
                    {% if results|length > 0 %}
                        -- Get the values from the result set. A NULL value is translated into a -1 to prevent
                        -- confusion with 0 which is a valid version in CDF.
                        {% set source_last_update_version = results.columns['source_last_update_version'][0]|int(-1)  %}
                        {% set source_first_valid_update_version = results.columns['source_first_valid_update_version'][0]|int(-1)  %}
                        {% if source_last_update_version >= 0 %}
                            -- In some cases a destructive operation is done on a table (for instance a 'create or replace'). In
                            -- those case only changes after that operation should be fetched
                            {% set source_last_update_version = source_first_valid_update_version if source_first_valid_update_version > source_last_update_version else source_last_update_version %}

                            -- Add the current source version to the table properties which will automatically be added upon table creation
                            -- For incremental runs a post-hook is neeeded to update the table properties.
                            -- First determine the name of the property
                            {% set table_property_name =  get_incremental_relation_table_property(source_type, source_identifier) %}
                            {% do model.config["tblproperties"].update({table_property_name: source_last_update_version}) %}

                            -- Check the last version of this source that has been loaded in this target model. Only needed when
                            -- is_incremental is true. For full-refresh runs or the first run (target table not yet created) the
                            -- default behaviour of 'source/ref' should apply with addition of table property above for future runs.
                            {% if is_incremental() %}
                                {% set target_last_update_version = -1 %}
                                {% set target_tblproperties_query %}
                                    show tblproperties {{this}} (`{{table_property_name}}`)
                                {% endset %}

                                -- This should always yield a value, unless the model was created manually or before this
                                -- incremental logic was part of the project. Those situations are self cleaning but might
                                -- lead to duplicate rows in its first run.
                                {% set results = run_query(target_tblproperties_query) %}
                                {% if results|length > 0 %}
                                    {% set target_last_update_version = results.columns['value'][0]|int(-1)  %}
                                {% endif %}

                                -- Soft deletion. Append data which was deleted with flag "deleted_date" timestamp.
                                {% set request_append_delete = kwargs.get('append_delete')|as_bool|default(false, true) %}

                                {{ log(source_type~": target_last_update_version: '"~target_last_update_version~"', source_last_update_version: '"~source_last_update_version~"', table_name "~source_relation.identifier~", inside model " ~ model.name) }}
                                -- Asserted status at this point:
                                --   - this ref/source is used in a model with 'incremental' materialization
                                --   - that model is within the 'main' dbt project; not a 3rd party package
                                --   - this is an incremental run (no full-refresh or initial run)
                                --   - no explicit exlusion using 'cdf_incremental=False' has been provided
                                --   - the source is either a 'source' or a model with 'table' or 'incremental' materializaton
                                --   - the source is a delta table with some data written to it.
                                -- Possible situations at this point:
                                --   1. the source object does not have CDF enabled
                                --        --> generate warning; return default result of source/ref
                                --   2. the target table exists but has an unknown source version
                                --        --> generate warning; return default result of source/ref. This should resolve itself after 1 run.
                                --   3. the target is already up-to-date (source version = target version)
                                --        --> generate select statement to return no rows
                                --   4. the source has an update that has not been loaded into the target yet (source version > target version)
                                --        --> generate select statement using CDF to return only changed rows
                                --   5. the source is behind the target version (source version < target version). This can happen if the source table is manually deleted.
                                --        --> generate error; a full refresh should be run on the target table (or it should be dropped as well).

                                {% if cdf_enabled == False %}
                                    -- Situation 1.
                                    {% do exceptions.warn("Incremental model '"~model.name~"' uses the '"~source_type~"' macro to refer to '"~source_identifier~"' but Change Data Feed is not enabled on this table. Please enable CDF on '"~source_identifier~"'.") %}
                                {% elif target_last_update_version == -1 %}
                                    -- Situation 2.
                                    {% do exceptions.warn("Incremental model '"~model.name~"' uses the '"~source_type~"' macro to refer to '"~source_identifier~"' but the last loaded version of the data cannot be determine. This issue should correct itself but please make sure no duplicate rows were loaded.") %}
                                {% elif source_last_update_version == target_last_update_version %}
                                    -- Situation 3. Source version is equal to target version, nothing needs to be loaded and the return value is altered
                                    -- so that it returns no rows and speed up performance. The table property is also popped because no post-hook
                                    -- is needed in this case.
                                    {% do model.config["tblproperties"].pop(table_property_name) %}
                                    {% set sql_select_statement_false %}
                                        (
                                            select *
                                            {{-",null::timestamp as deleted_date" if request_append_delete }}
                                            from {{source_relation}}
                                            where False
                                        )
                                    {% endset %}
                                    {% do return(sql_select_statement_false) %}
                                {% elif source_last_update_version > target_last_update_version %}
                                    -- Situation 4. Source contains updates that need to be loaded into the target. A select using the
                                    -- table-valued 'table_changes' function is returned to fetch only the inserts and updates since the
                                    -- last run. In other words, all changes from target_last_update_version + 1 and higher.
                                    {% set target_last_update_version = target_last_update_version + 1 %}
                                    -- In some cases however, a destructive operation (a 'create or replace' or a 'dropped column') has
                                    -- occured on the source after the last load. It is currently not possible to get CDF results accross
                                    -- destructive operations. If the 'first valid version' is higher than the last version loaded into
                                    -- the target, we take this last valid version as the starting point of this run.
                                    {% set target_last_update_version = source_first_valid_update_version if source_first_valid_update_version > target_last_update_version else target_last_update_version %}

                                    -- All that is left, is to build the sql statement that will only return the changes using the
                                    -- 'table_changes' function. Notice that:
                                    --     - It is filtered to only return inserted and updated rows, no deleted rows.
                                    --     - The metadata columns it adds, are removed from the result to make sure the schema of the source
                                    --       is not any different from a non-incremental run.
                                    --     - In case the source model (either a 'source' or a 'model') has a unique_key defined, the query
                                    --       should only return 1 row per unique key and that row should be the last/current update. By default
                                    --       this will not be the case if a row was updated multiple time since the last run. To fix this, the
                                    --       query is wrapped inside another query that does the deduplication.

                                    -- First determine if the source model (either a 'source' or a 'model') has a unique_key defined
                                    {% set only_row_last_commit = source_model.config.unique_key is not none and source_model.config.unique_key|length > 0 %}
                                    -- This update checks whether the order_by column has been specified by the user. If it has been specified,
                                    -- the sorting is done based on both the order_by and _commit_version columns.
                                    -- If it has not been specified, the sorting is done based only on the _commit_version column.
                                    {% set order_by = source_model.config.order_by if source_model.config.order_by is not none else '_commit_version' %}
                                    -- Build the sql statement
                                    {% set sql_select_statement %}
                                        (
                                            {{ "select * except (edp_incremental_row_last_commit_version) from (" if only_row_last_commit }}
                                                select * except(_change_type, _commit_version, _commit_timestamp)
                                            {{ ", row_number() over (partition by "~get_list(source_model.config.unique_key)|join(", ") ~" order by "~get_list(source_model.config.order_by)|join(", ") ~" desc) as edp_incremental_row_last_commit_version " if only_row_last_commit }}
                                            {{ ", case when _change_type = 'delete' then _commit_timestamp end as deleted_date" if request_append_delete}}
                                                from table_changes('{{source_relation}}', {{target_last_update_version}})
                                                where _change_type in ('insert', 'update_postimage'{{-",'delete'" if request_append_delete }})
                                            {{") where edp_incremental_row_last_commit_version = 1" if only_row_last_commit}}
                                        )
                                    {% endset %}

                                    -- Return the resulting sql statement
                                    {% do return(sql_select_statement) %}

                                {% elif source_last_update_version < target_last_update_version %}
                                    -- Unexpected situation generate error
                                    {{ exceptions.raise_compiler_error("Last update on source '"~source_identifier~"' ("~source_last_update_version~") is lower than version stored on target '"~model.name~"' ("~target_last_update_version~"). Please update target model with --full-refresh flag to sync versions and prevent duplicate rows.") }}
                                {% else %}
                                    -- Unexpected situation generate error
                                    {{ exceptions.raise_compiler_error("Unexpected situation in incremental model '"~model.name~"' when using the '"~source_type~"' macro to refer to '"~source_identifier~"'. Details: target_last_update_version='"~target_last_update_version~"', source_last_update_version='"~source_last_update_version~"'") }}
                                {% endif %}
                            {% endif %}
                        {% endif %}
                    {% endif %}
                {% endif %}
            {% endif %}
        {% endif %}
    {% endif %}

    -- If the incremental logic cannot or should not be implemented, the relation is returned
    -- unchanged resulting in the default behaviour of the source/ref macros.
    {% do return(source_relation) %}
{%- endmacro -%}



-- This macro is a "helper" macro to 'get_incremental_relation'. It is called as a post-hook on an incremental model to update the
-- last version loaded from a source/ref when needed.
{%- macro get_incremental_relation_post_hook(source_type, source_identifier) -%}
    {%- if execute -%}
        {%- set table_property_name = get_incremental_relation_table_property(source_type, source_identifier) -%}

        {#- post-hook is always added for each source but only needs to do something for incremental runs. -#}
        {%- if is_incremental() %}
            {# if the post-hook needs to be executes, this table property is set during the run by the main macro. -#}
            {% if table_property_name in model.config["tblproperties"] -%}
            {{ log("post_hook 'get_incremental_relation_post_hook' for model '" ~ this ~"'. Setting table property '"~ table_property_name ~"' to '"~model.config["tblproperties"][table_property_name]~"'") }}

            ALTER TABLE {{this}} SET TBLPROPERTIES(`{{table_property_name}}` = {{model.config["tblproperties"][table_property_name]}});

            {%- endif -%}
        {%- endif -%}
    {%- endif -%}
{%- endmacro -%}




-- This macro is a "helper" macro to 'get_incremental_relation'. Its sole purpose is to
-- translate a unique identification of a source or model, that is the same before _and_
-- during the execute phase (!) into the name of the table property that holds the latest
-- CDF commit loaded from this table. This property contains fully qualified, three part
-- name of the table in UC.
{%- macro get_incremental_relation_table_property(source_type, source_identifier) -%}
    {% if execute %}
        -- Get the model or source that is used in the source/ref macro
        {% set model = get_incremental_relation_source_model(source_type, source_identifier) %}

        -- If the model/source was uniquely identified, use it to return the table property name
        {% if model is not none  %}
            {% do return("edp.incremental."~model.database~"."~model.schema~"."~model.name~".lastUpdateVersion") %}
        {% endif %}
    {% endif %}
{%- endmacro -%}


-- This macro is a "helper" macro to 'get_incremental_relation'. It is called to find the
-- model or source that is used in the source/ref macro using the graph nodes and return
-- it. If it cannot find the node or it finds more than one, and error will be raised.
{%- macro get_incremental_relation_source_model(source_type, source_identifier) -%}
    {% if execute %}
        {% set models = None %}

        -- In case of a 'ref', find the model
        {% if source_type == "ref" %}
            {% set models = graph.nodes.values()
                | selectattr("package_name", "equalto", source_identifier[0])
                | selectattr("name", "equalto", source_identifier[1])
                | list %}
        -- In case of a 'source', find the source
        {% elif source_type == "source" %}
            {% set models = graph.sources.values()
                | selectattr("source_name", "equalto", source_identifier[0])
                | selectattr("name", "equalto", source_identifier[1])
                | list %}
        {% else  %}
            -- Unexpected situation generate error
            {{ exceptions.raise_compiler_error("Unexpected source_type '"~source_type~"'.") }}
        {% endif %}

        -- If the model/source was uniquely identified, use it to return the table property name
        {% if models|length == 1 %}
            {% do return(models|first) %}
        {% else  %}
            -- No or multiple models/sources found. Generate error.
            {{ exceptions.raise_compiler_error("Expected exactly 1 model when searching for "~source_identifier~" used in '"~source_type~"' macro. Found "~models|length~" models/sources.") }}
        {% endif %}
    {% endif %}

{%- endmacro -%}
