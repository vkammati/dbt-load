# Databricks notebook source
# MAGIC %md
# MAGIC #Setup benchmarking DBT vs DLT
# MAGIC We setup this (automated) benchmark to get a fair comparison of both cost and performance between DLT pipelines and DBT. For a detailed explanation and the results of the test we did with it, we encourage you to read appendix A of this [whitepaper](https://my.shell.com/:w:/g/personal/sertac_oruc_shell_com/ET24WS-HICNEjgxQU6EvrmkBgPM9hJmBHyrXxtQB_oVciQ?e=xrtMkl) first.
# MAGIC
# MAGIC This notebook is meant to create the tables to define the test scenario's and store its results. It also shows how to add configurations and explains what things needs to create manually. Make sure to create the manually resource as described and change the use cases below per your needs. Once everything is setup, the notebook '2_benchmark_run' can be used to actually run the benchmark.
# MAGIC
# MAGIC ###Required manual components
# MAGIC There are a couple of resources that needs to be created manually first. The identifiers of these components will be used as input to the sql scripts below.
# MAGIC
# MAGIC **All purpose compute**\
# MAGIC DBT needs some compute to run on. In production runs, job compute is typically used for this but these take time to start. To speed up the benchmark process, an all purpose compute can be used which will keep running between runs, saving about 3-4 minutes per run. Use 'shared' access mode and the latest LTS databrikcs runtime. Only 1 worker is required and the smallest compute (Standard_F4s) is enough.\
# MAGIC The variable '**dbt_job_cluster_id**' must be set to its cluster id.
# MAGIC
# MAGIC **SQL warehouse**\
# MAGIC The actual transformations done by DBT will be executed on a SQL warehouse instance. Create a dedicated one to make sure it is not used by any other processes during benchmark. All other settings will be set by the benchmark run so they can be left to the default settings.\
# MAGIC The variable '**dbt_sql_warehouse_id**' must be set to its sql warehouse id.
# MAGIC
# MAGIC **DBT workflow**\
# MAGIC To run DBT, a workflow is required. The benchmark run expects it to have two tasks. The first task should only run 'dbt debug'. This will make sure the sql warehouse is always started before running the actual benchmark. The seconds task should depend on the first task and run the model(s) you want to benchmark. The benchmark run will set the 'dbt command' and 'dbt vars' parameters so they can be left blank. Both tasks should use the wheel you want to benchmark and both task should use the same 'all purpose compute' mentioned above.\
# MAGIC The variable '**dbt_workflow_id**' should contain the id of the workflow.
# MAGIC
# MAGIC **DLT pipeline template**\
# MAGIC The DLT pipeline you want to use for the DLT runs need to created and tested manually. The benchmark proces will not run this pipeline but instead create copies, one for each of the different configurations. This is needed to test both full load and incremental scenarios. If all configurations would use the same pipeline, it would not be able to write to different tables without destroying the tables of the previous run. Each table in the template pipeline should use '${table_prefix}_' as prefix for its name. The 'table_prefix' configuration will be updated by the benchmark run based on its configuration.\
# MAGIC The variable '**dlt_pipeline_id**' must be set to the guid of this pipeline.
# MAGIC
# MAGIC **DLT workflow**\
# MAGIC The benchmark does not start the DLT pipeline directly but instead start a Databricks workflow that starts the pipeline. This makes it easier log the results/timings of run in the same way as the DBT workflow and also makes it easier to switch between incremental and full refresh runs. Simply create a workflow that only has one dlt pipeline task that points to the template pipeline above. The benchmark run will update the job when needed. \
# MAGIC The variable '**dlt_workflow_id**' should contain the id of the workflow.
# MAGIC

# COMMAND ----------

# Both the catalog and the schema must be set to be able to create the benchmark tables.
catalog = "dev_prod_dbt_poc_unitycatalog_dev"
schema = "performance_and_cost_benchmark"

# Update these variables based on the manually created resources explained above
dbt_job_cluster_id = "1234-123456-1a23bc45"
dbt_sql_warehouse_id = "9a548a3c453c1234"
dbt_workflow_id = "1234567890123456"
dlt_pipeline_id = "12af547b-64b5-56d2-5489-25fbac65124e"
dlt_workflow_id = "1234567890123456"

# To make sure the timings are not (much) impacted by "one-offs" or temporary hickups, it is
# advised to run the same configurations multiple times and take the average or median. By
# default this is set to 5 times but can be changed with the variable below.
nr_of_runs = 5


# COMMAND ----------

# The 'benchmark_use_case' table is meant to hold the use case that will be benchmarked. For
# instance, 'Full load with x mllion rows' or 'incremental run'.
spark.sql(
    f"create table if not exists {catalog}.{schema}.benchmark_use_case ("
    "    id bigint generated always as identity not null,"
    "    use_case string not null,"
    "    description string,"
    "    incremental boolean"
    ")"
)

# The 'benchmark_run_config' table contains all the different configuration the benchmark will
# run on and how often it needs to be run.
spark.sql(
    f"create table if not exists {catalog}.{schema}.benchmark_run_config ("
    "    id bigint generated always as identity not null,"
    "    use_case_id bigint not null,"
    "    run_type string not null,"
    "    nr_of_runs int not null,"
    "    incremental boolean,"
    "    job_id bigint not null,"
    "    dbt_job_cluster string,"
    "    dbt_sql_warehouse_id string,"
    "    dbt_sql_serverless boolean,"
    "    dbt_sql_cluster_size string,"
    "    dlt_pipeline_id string,"
    "    dlt_nr_of_workers int,"
    "    dlt_photon boolean"
    ")"
)

# The 'benchmark_run_log' table contains the results of a benchmark run, mostly to capture the
# duration.
spark.sql(
    f"create table if not exists {catalog}.{schema}.benchmark_run_log ("
    "    id bigint generated always as identity not null,"
    "    benchmark_run_config_id bigint not null,"
    "    start_time timestamp not null,"
    "    end_time timestamp not null,"
    "    duration int not null,"
    "    status string not null"
    ")"
)

# COMMAND ----------

# Insert (new) use cases
spark.sql(
    f"insert into {catalog}.{schema}.benchmark_use_case ("
    f"  use_case,"
    f"  description,"
    f"  incremental"
    f")"
    f"select use_case, description, incremental "
    f"from ("
    f"  select 'full_load_80m_rows' as use_case, 'Full load of example data model with 80 million orders.' as description, 0 as incremental union"
    f"  select 'incremental_2m_rows' as use_case, 'Incrementally add 2 millions orders to the current 80 million.' as description, 1 as incremental union"
    f"  select 'full_load_82m_rows' as use_case, 'Full load of example data model with the rows from \"full_load_80m_rows\" and \"incremental_2m_rows\" million orders.' as description, 0 as incremental union"
    f"  select 'incremental_2m_rows_again' as use_case, 'Incrementally add another 2 millions orders to the current 82 million.' as description, 1 as incremental"
    f") as new "
    f"where new.use_case not in (select use_case FROM {catalog}.{schema}.benchmark_use_case)"
)

# COMMAND ----------

# Insert any new dbt configuration per use case.
spark.sql(
    f"insert into {catalog}.{schema}.benchmark_run_config ("
    f"    use_case_id,"
    f"    run_type,"
    f"    job_id,"
    f"    incremental,"
    f"    dbt_job_cluster,"
    f"    dbt_sql_warehouse_id,"
    f"    dbt_sql_serverless,"
    f"    dbt_sql_cluster_size,"
    f"    nr_of_runs"
    f") "
    f"select "
    f"    new.use_case_id,"
    f"    new.run_type,"
    f"    new.job_id,"
    f"    new.incremental,"
    f"    new.dbt_job_cluster,"
    f"    new.dbt_sql_warehouse_id,"
    f"    new.dbt_sql_serverless,"
    f"    new.dbt_sql_cluster_size,"
    f"    new.nr_of_runs "
    f"from ("
    f"    select "
    f"        uc.use_case_id,"
    f"        uc.incremental,"
    f"        'DBT' as run_type,"
    f"        config.job_id,"
    f"        config.dbt_job_cluster,"
    f"        config.dbt_sql_warehouse_id,"
    f"        serverless.dbt_sql_serverless,"
    f"        dimensions.dbt_sql_cluster_size,"
    f"        config.nr_of_runs"
    f"    from "
    f"        (select id as use_case_id, use_case, incremental from {catalog}.{schema}.benchmark_use_case where use_case in ('full_load_80m_rows', 'incremental_2m_rows', 'full_load_82m_rows', 'incremental_2m_rows_again')) uc,"
    f"        (select {dbt_workflow_id} job_id, '{dbt_job_cluster_id}' dbt_job_cluster, '{dbt_sql_warehouse_id}' dbt_sql_warehouse_id, {nr_of_runs} as nr_of_runs) config,"
    f"        ("
    f"            select '2X-Small' as dbt_sql_cluster_size, array() as exclude_for_use_case union"
    f"            select 'X-Small', array() union"
    f"            select 'Small', array() union"
    f"            select 'Medium', array('incremental_2m_rows') union"
    f"            select 'Large', array('incremental_2m_rows', 'incremental_2m_rows_again') union"
    f"            select 'X-Large', array('incremental_2m_rows', 'incremental_2m_rows_again')"
    f"        ) dimensions,"
    f"        (select 1 dbt_sql_serverless union select 0 ) serverless"
    f"    where not array_contains(dimensions.exclude_for_use_case, uc.use_case)"
    f") as new "
    f"where not exists ("
    f"    select 1 "
    f"    from {catalog}.{schema}.benchmark_run_config cfg "
    f"    where cfg.use_case_id=new.use_case_id and cfg.run_type=new.run_type and cfg.dbt_sql_cluster_size=new.dbt_sql_cluster_size and cfg.dbt_sql_serverless=new.dbt_sql_serverless"
    f"    )"
)

# COMMAND ----------


# Insert any new dlt configuration per use case.
spark.sql(
    f"insert into {catalog}.{schema}.benchmark_run_config ("
    f"    use_case_id,"
    f"    run_type,"
    f"    incremental,"
    f"    job_id,"
    f"    dlt_pipeline_id,"
    f"    dlt_nr_of_workers,"
    f"    dlt_photon,"
    f"    nr_of_runs"
    f") "
    f"select"
    f"    new.use_case_id,"
    f"    new.run_type,"
    f"    new.incremental,"
    f"    new.job_id,"
    f"    new.dlt_pipeline_id,"
    f"    new.dlt_nr_of_workers,"
    f"    new.dlt_photon,"
    f"    new.nr_of_runs "
    f"from ("
    f"    select"
    f"        uc.use_case_id,"
    f"        uc.incremental,"
    f"        'DLT' as run_type,"
    f"        config.job_id,"
    f"        config.dlt_pipeline_id,"
    f"        dimensions.dlt_nr_of_workers,"
    f"        dimensions.dlt_photon,"
    f"        config.nr_of_runs"
    f"    from "
    f"        (select id as use_case_id, use_case, incremental from {catalog}.{schema}.benchmark_use_case where use_case in ('full_load_80m_rows', 'incremental_2m_rows', 'incremental_2m_rows_again')) uc,"
    f"        (select {dlt_workflow_id} job_id, '{dlt_pipeline_id}' dlt_pipeline_id, {nr_of_runs} as nr_of_runs) config,"
    f"        ("
    f"            select 1 dlt_nr_of_workers, false as dlt_photon, array() exclude_for_use_case union"
    f"            select 2, false, array() union"
    f"            select 3, false, array() union"
    f"            select 5, false, array() union"
    f"            select 7, false, array() union"
    f"            select 10, false, array() union"
    f"            select 15, false, array() union"
    f"            select 20, false, array('incremental_2m_rows', 'incremental_2m_rows_again') union"
    f""
    f"            select 1 dlt_nr_of_workers, true as dlt_photon, array() exclude_for_use_case union"
    f"            select 2, true, array() union"
    f"            select 3, true, array() union"
    f"            select 5, true, array() union"
    f"            select 7, true, array() union"
    f"            select 10, true, array('incremental_2m_rows', 'incremental_2m_rows_again') union"
    f"            select 15, true, array('incremental_2m_rows', 'incremental_2m_rows_again') union"
    f"            select 20, true, array('incremental_2m_rows', 'incremental_2m_rows_again')"
    f"        ) dimensions"
    f"    where not array_contains(dimensions.exclude_for_use_case, uc.use_case)"
    f") as new "
    f"where not exists ("
    f"    select 1 "
    f"    from {catalog}.{schema}.benchmark_run_config cfg "
    f"    where cfg.use_case_id=new.use_case_id and cfg.run_type=new.run_type and cfg.dlt_nr_of_workers=new.dlt_nr_of_workers and cfg.dlt_photon=new.dlt_photon"
    f"    )"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- This sql query can be used to report over the logged results. Make sure to select the correct catalog/schema first.
# MAGIC with
# MAGIC -- The 'config' CTE is used to filter out the use case you want to report on
# MAGIC config as (
# MAGIC     select
# MAGIC       case
# MAGIC         when use_case in ('full_load_80m_rows', 'full_load_82m_rows') then 'full_load_80m_rows'
# MAGIC         when use_case = 'incremental_2m_rows_again' then 'incremental_2m_rows'
# MAGIC         else use_case end as use_case, -- treat dbt 82M use case the same as dlt 80M.
# MAGIC       cfg.*
# MAGIC     from benchmark_run_config cfg
# MAGIC       inner join benchmark_use_case uc on cfg.use_case_id = uc.id
# MAGIC     where uc.use_case not in ('full_load_40m_rows', 'incremental_2m_rows') -- <- 40M was not enough rows to see any difference between cluster sizes and first incremental run had unfair issues with both dbt and dlt
# MAGIC       and not (uc.use_case = 'full_load_80m_rows' and run_type = 'DBT') -- <-- 80M dbt run was done signle threaded. Re-run with 82M should be used for dbt. No re-run done for dlt as the last 2M rows should not have a relevant impact.
# MAGIC ),
# MAGIC
# MAGIC -- The 'run_detail' CTE groups (sub)technologies together and add DBU calculation
# MAGIC run_detail as  (
# MAGIC     select
# MAGIC       use_case,
# MAGIC       lower(run_type) as tech,
# MAGIC       case
# MAGIC           when run_type = 'DLT' then concat(lower(run_type),'-',if(dlt_photon, 'photon', 'no-photon'))
# MAGIC           else concat(lower(run_type),'-',if(dbt_sql_serverless, 'serverless', 'server-based'))
# MAGIC       end as subtech,
# MAGIC       coalesce(lower(dbt_sql_cluster_size), '') as dbt_sql_cluster_size,
# MAGIC       cfg.id as config_id,
# MAGIC       case
# MAGIC           when run_type = 'DLT' then concat(cast(dlt_nr_of_workers as varchar(2)), ' worker(s)', if(dlt_photon, ' (photon)', ' (no photon)'))
# MAGIC           else concat(lower(dbt_sql_cluster_size), if(dbt_sql_serverless, ' (serverless)', ' (server-based)'))
# MAGIC       end as config,
# MAGIC       case
# MAGIC           when run_type = 'DLT' then (dlt_nr_of_workers + 1) * if(dlt_photon, 5, 1)
# MAGIC           when lower(dbt_sql_cluster_size) = '2x-small' then 4
# MAGIC           when lower(dbt_sql_cluster_size) = 'x-small' then 6
# MAGIC           when lower(dbt_sql_cluster_size) = 'small' then 12
# MAGIC           when lower(dbt_sql_cluster_size) = 'medium' then 24
# MAGIC           when lower(dbt_sql_cluster_size) = 'large' then 40
# MAGIC           when lower(dbt_sql_cluster_size) = 'x-large' then 80
# MAGIC           else 0
# MAGIC       end as dbu,
# MAGIC       log.start_time,
# MAGIC       log.end_time,
# MAGIC       log.duration,
# MAGIC       log.status
# MAGIC     from benchmark_run_log log
# MAGIC       inner join config cfg on log.benchmark_run_config_id=cfg.id
# MAGIC     order by use_case, subtech, config_id, start_time
# MAGIC )
# MAGIC
# MAGIC -- The 'run_average' CTE calculates the average run time per configuration
# MAGIC , run_average (
# MAGIC     select use_case, tech, subtech, dbt_sql_cluster_size, config_id, config, dbu, avg(duration*1.00) avg_duration, count(*) nr_of_runs
# MAGIC     from run_detail
# MAGIC     where status = 'SUCCESS'
# MAGIC     group by use_case, tech, subtech, dbt_sql_cluster_size, config_id, config, dbu
# MAGIC     order by use_case, subtech, dbt_sql_cluster_size, config_id, config, dbu
# MAGIC )
# MAGIC
# MAGIC -- The 'pivot_on_dbu' simply pivots all timings, creating one line per dbu with all the timing of the different technologies
# MAGIC , pivot_on_dbu (
# MAGIC     select
# MAGIC       use_case,
# MAGIC       dbu,
# MAGIC       array_join(collect_set(case when tech  = 'dlt' then config else dbt_sql_cluster_size end), ' / ') as config,
# MAGIC       avg(case when subtech = 'dbt-server-based' then avg_duration end) as dbt_server_based,
# MAGIC       avg(case when subtech = 'dbt-serverless' then avg_duration end) as dbt_serverless,
# MAGIC       avg(case when subtech = 'dlt-no-photon' then avg_duration end) as dlt_photon,
# MAGIC       avg(case when subtech = 'dlt-photon' then avg_duration end) as dlt_no_photon
# MAGIC     from run_average
# MAGIC     group by use_case, dbu
# MAGIC     order by use_case, dbu
# MAGIC )
# MAGIC
# MAGIC select *
# MAGIC from pivot_on_dbu
# MAGIC

# COMMAND ----------
