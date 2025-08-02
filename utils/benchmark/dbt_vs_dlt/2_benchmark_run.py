# Databricks notebook source
# MAGIC %md
# MAGIC #Running benchmarking DBT vs DLT
# MAGIC This notebook is used to run the benchmark logic setup using the '1_setup_benchmark' notebook. Please make sure the required resources, tables and configurations have been created using '1_setup_benchmark'. This notebook depends on it!
# MAGIC
# MAGIC ###Final configurations
# MAGIC The bulk of the setup is done using the '1_setup_benchmark' notebook. In this notebook, two last things need to be changed before it can be started to run the benchmark.
# MAGIC 1. The variables in the third cell need be reviewed and changed to the your setup. This contains the name of the catalog and schema and the use case you want to run.
# MAGIC 2. In the fourth cell, a functions is created to returns an AD token for authenticating to the Databricks API. You can have the function return a fixed PAT or have it generate a dynamic token for a SPN. You can also choose to store the credentials in a secret scope (recommended) or hardcode it into this notebook. For more details, read the comments in cell 4 and make the changes to fit your situation.
# MAGIC
# MAGIC Once done, click the 'Run all' button to run the benchmark for a given use case.

# COMMAND ----------

import concurrent.futures
import datetime
import os

# All needed imports
import sys
import time

sys.path.append(os.path.abspath(".."))

from helpers.azure.credentials import generate_spn_ad_token
from helpers.databricks.auth import get_dbx_http_header
from helpers.databricks.clusters import get_cluster, start_cluster, update_cluster
from helpers.databricks.jobs import (
    get_job_run,
    trigger_job_run,
    update_dbt_job,
    update_dlt_job,
)
from helpers.databricks.pipelines import (
    create_or_update_pipeline,
    get_pipeline,
    get_pipeline_update,
    validate_pipeline,
)
from helpers.databricks.sql_warehouses import (
    get_sql_warehouse,
    start_sql_warehouse,
    update_sql_warehouse,
)

# COMMAND ----------

# MAGIC %md
# MAGIC Review the two cells below and make the necessary changes

# COMMAND ----------

# Provide the same caatlog and schame names as used while setting up the benchmark logic
catalog = "dev_prod_dbt_poc_unitycatalog_dev"
schema = "performance_and_cost_benchmark"

# Each run can only benchmark one use case. Provide the name of the use case here.
use_case = "test_use_case"

# Leave this as is
workspace_url = spark.conf.get("spark.databricks.workspaceUrl")

# COMMAND ----------

# This function is use to get the token needed to call the databricks api. The token should provide the
# privelege to create, change and start jobs, dlt pipelines, clusters and sql warehouses.


# Change the function below to use a PAT or to fetch a token for an Azure SPN. The required information
# can come from a (key vault backed) secret scope or hardcode.
def get_http_header():

    # To use a hardcoded PAT, use this line and comment out the the rest except the return statement
    ad_token = "dapia5216....-2"

    # Alternatively, fetch tenant-id, client-id and and client-secret of a SPN that is allowed to
    # run manage and start workflows, sql warehosue and DLT pipelines from a secret scope and use those
    # to generate an ad token.
    # For more information about secret scopes: https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes
    tenant_id = dbutils.secrets.get(scope="keyvault", key="AZ-AL-SPN-TENANT-ID")
    spn_client_id = dbutils.secrets.get(scope="keyvault", key="AZ-AL-SPN-CLIENT-ID")
    spn_client_secret = dbutils.secrets.get(
        scope="keyvault", key="AZ-AL-SPN-CLIENT-SECRET"
    )

    ad_token = generate_spn_ad_token(
        tenant_id=tenant_id,
        spn_client_id=spn_client_id,
        spn_client_secret=spn_client_secret,
        scope="2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default",
    )

    return get_dbx_http_header(ad_token)


# COMMAND ----------

# MAGIC %md
# MAGIC All the cells below must be left unchanged

# COMMAND ----------


def execute_run(run_type: str, benchmark_run_config_id: int, job_id: int):
    # Execute all runs
    prefix = run_type.upper()
    if prefix == "DBT":
        prefix = "    DBT"

    run_success = False
    header = get_http_header()
    run_id = trigger_job_run(job_id=job_id, host=workspace_url, http_header=header)
    print(f"{prefix}: Started job '{job_id}' and got run id '{run_id}'")

    # Check if pipeline is runnning. Will timeout after 300 x 30s = 2,5h
    for x in range(300):
        run_status = get_job_run(run_id=run_id, host=workspace_url, http_header=header)
        if run_status.get("state").get("life_cycle_state") == "RUNNING":
            print(f"{prefix}: Job still running. Going to sleep. ({x+1}/250)")
            time.sleep(30)
        elif (
            run_status.get("state").get("life_cycle_state") == "TERMINATED"
            and run_status.get("state").get("result_state") == "SUCCESS"
        ):
            print(
                f"{prefix}: Job '{job_id}' with run id '{run_id}' FINISHED. Logging result. ({x+1}/250)"
            )
            log_run_result(run_type, benchmark_run_config_id, run_status)
            run_success = True
            break
        else:
            print(run_status)
            log_run_result(run_type, benchmark_run_config_id, run_status)
            raise Exception(f"{prefix}: Unexpected result from get run status")

    if not run_success:
        raise Exception(
            f"{prefix}: Timeout reached when running job with id {benchmark_run_config_id}"
        )


def log_run_result(run_type: str, benchmark_run_config_id: int, run_result: dict):
    prefix = run_type.upper()
    if prefix == "DBT":
        prefix = "    DBT"

    task = [t for t in run_result.get("tasks") if t.get("task_key") == "run"][0]
    start_time = datetime.datetime.fromtimestamp(task.get("start_time") / 1e3)
    end_time = datetime.datetime.fromtimestamp(task.get("end_time") / 1e3)
    duration = int((end_time - start_time).total_seconds())
    status = task.get("state").get("result_state")

    print(
        f"{prefix}: Run with config id {benchmark_run_config_id} finished in {duration} seconds."
    )

    # log_result
    spark.sql(
        f"INSERT INTO {catalog}.{schema}.benchmark_run_log(benchmark_run_config_id, start_time, end_time, duration, status)"
        f"SELECT {benchmark_run_config_id}, '{start_time}', '{end_time}', {duration}, '{status}'"
    )


# COMMAND ----------


def benchmark_dbt(run_config: dict):
    print(
        f"    DBT: Starting dbt run with id {run_config.id}. {run_config.nr_runs_to_go} of {run_config.nr_of_runs} runs left to go on cluster size '{run_config.dbt_sql_cluster_size}' ({'serverless' if run_config.dbt_sql_serverless else 'server-based'})"
    )

    # For each run left to be done
    for run in range(
        run_config.nr_of_runs - run_config.nr_runs_to_go + 1, run_config.nr_of_runs + 1
    ):
        print(f"    DBT: Starting run '{run}'")

        # Updating sql warehouse and tags
        header = get_http_header()
        sql_warehouse = get_sql_warehouse(
            warehouse_id=run_config.dbt_sql_warehouse_id,
            host=workspace_url,
            http_header=header,
        )
        if (
            sql_warehouse.get("cluster_size").lower()
            != run_config.dbt_sql_cluster_size.lower()
            or sql_warehouse.get("enable_serverless_compute")
            != run_config.dbt_sql_serverless
        ):
            print(
                f'    DBT: Cluster size is {sql_warehouse.get("cluster_size").lower()} ({"serverless" if sql_warehouse.get("enable_serverless_compute") else "server-based"}), changing to {run_config.dbt_sql_cluster_size} ({"serverless" if run_config.dbt_sql_serverless else "server-based"})'
            )

        update_sql_warehouse(
            warehouse_id=run_config.dbt_sql_warehouse_id,
            use_case=run_config.use_case,
            cluster_size=run_config.dbt_sql_cluster_size,
            serverless=run_config.dbt_sql_serverless,
            run=run,
            host=workspace_url,
            http_header=header,
        )

        # Also update the cluster, just for the tags.
        job_cluster = get_cluster(
            cluster_id=run_config.dbt_job_cluster, host=workspace_url, http_header=header
        )
        update_cluster(
            cluster=job_cluster,
            use_case=run_config.use_case,
            cluster_size=run_config.dbt_sql_cluster_size,
            serverless=run_config.dbt_sql_serverless,
            run=run,
            host=workspace_url,
            http_header=header,
        )
        if job_cluster.get("state") == "TERMINATED":
            # No need to poll the status after starting, will automatically be started (or waited on) when job starts. Starting it just to speed up the process.
            print(f"    DBT: Starting terminated cluster ({run_config.dbt_job_cluster})")
            start_cluster(
                cluster_id=run_config.dbt_job_cluster,
                host=workspace_url,
                http_header=header,
            )

        # Update the job. Needed for jobs but also to set the run command to incremental or not and set the vars for the proper table prefixes.
        print(f"    DBT: updating job ({run_config.job_id})")
        update_dbt_job(
            job_id=run_config.job_id,
            job_cluster_id=run_config.dbt_job_cluster,
            use_case=run_config.use_case,
            cluster_size=run_config.dbt_sql_cluster_size,
            serverless=run_config.dbt_sql_serverless,
            incremental=run_config.incremental,
            run=run,
            host=workspace_url,
            http_header=header,
        )

        # Check if warehouse is runnning. Timeout at 15m.
        for x in range(30):
            sleep_time = 3 if run_config.dbt_sql_serverless else 30
            sql_warehouse = get_sql_warehouse(
                warehouse_id=run_config.dbt_sql_warehouse_id,
                host=workspace_url,
                http_header=header,
            )
            if sql_warehouse.get("state") == "STARTING":
                print(f"    DBT: Warehouse still starting. Going to sleep. ({x+1}/30)")
                time.sleep(sleep_time)
            elif sql_warehouse.get("state") == "STOPPED":
                print(
                    f"    DBT: Warehouse is stopped. Restarting and going to sleep. ({x+1}/30)"
                )
                sql_warehouse = start_sql_warehouse(
                    warehouse_id=run_config.dbt_sql_warehouse_id,
                    host=workspace_url,
                    http_header=header,
                )
                time.sleep(sleep_time)
            elif sql_warehouse.get("state") == "RUNNING":
                print("    DBT: Warehouse ready.")
                break
            else:
                print(sql_warehouse)
                raise Exception("   DBT: Unexpected result from sql warehouse status")

        # execute run
        execute_run("dbt", run_config.id, run_config.job_id)


def benchmark_dlt(run_config: dict):
    print(
        f"DLT: Starting dlt run with id {run_config.id}. {run_config.nr_runs_to_go} of {run_config.nr_of_runs} runs left to go with '{run_config.dlt_nr_of_workers}' workers and photon set to '{run_config.dlt_photon}'"
    )

    # For each run left to be done
    for run in range(
        run_config.nr_of_runs - run_config.nr_runs_to_go + 1, run_config.nr_of_runs + 1
    ):
        print(f"DLT: Starting run '{run}'")

        # Check if warehouse has the right cluster size
        header = get_http_header()
        pipeline = get_pipeline(
            pipeline_id=run_config.dlt_pipeline_id, host=workspace_url, http_header=header
        )
        if (
            pipeline.get("spec").get("clusters")[0].get("autoscale").get("max_workers")
            != run_config.dlt_nr_of_workers
            or pipeline.get("spec").get("photon") != run_config.dlt_photon
        ):
            print(
                f"DLT: Changing pipeline (workers={pipeline.get('spec').get('clusters')[0].get('autoscale').get('max_workers')}, photon={pipeline.get('spec').get('photon')}) to '{run_config.dlt_nr_of_workers}' worker(s) and photon set to '{run_config.dlt_photon}'"
            )

        # Updating the pipeline
        pipeline_id = create_or_update_pipeline(
            template_pipeline=pipeline,
            use_case=run_config.use_case,
            nr_of_workers=run_config.dlt_nr_of_workers,
            photon=run_config.dlt_photon,
            run=run,
            host=workspace_url,
            http_header=header,
        )

        # Update the tags on the job as well
        print(f"DLT: updating job ({run_config.job_id})")
        update_dlt_job(
            job_id=run_config.job_id,
            pipeline_id=pipeline_id,
            use_case=run_config.use_case,
            nr_of_workers=run_config.dlt_nr_of_workers,
            photon=run_config.dlt_photon,
            run=run,
            incremental=run_config.incremental,
            host=workspace_url,
            http_header=header,
        )

        # Create all resources before we run. We do this by executing the pipeline for a single table
        update_id = validate_pipeline(
            pipeline_id=pipeline_id, host=workspace_url, http_header=header
        )

        # Check if pipleine is runnning. Timeout at 15m.
        for x in range(30):
            pipeline_update = get_pipeline_update(
                pipeline_id=pipeline_id,
                update_id=update_id,
                host=workspace_url,
                http_header=header,
            )
            state = pipeline_update.get("update").get("state")
            if state in [
                "QUEUED",
                "CREATED",
                "INITIALIZING",
                "WAITING_FOR_RESOURCES",
                "SETTING_UP_TABLES",
                "RESETTING",
                "STOPPING",
            ]:
                print(f"DLT: Pipeline in state '{state}'. Going to sleep. ({x+1}/30)")
                time.sleep(30)
            elif state == "COMPLETED":
                print("DLT: Pipeline ready.")
                break
            else:
                print(pipeline_update)
                raise Exception(
                    f"DLT: Unexpected state '{state}' from pipeline update status"
                )

        # execute runs
        execute_run("dlt", run_config.id, run_config.job_id)


# COMMAND ----------


# Function that supplies all dbt configurations to run
def execute_benchmark_dbt():
    dbt_benchmark_runs = spark.sql(
        f"select cfg.*, cfg.nr_of_runs - coalesce(cnt.runs, 0) as nr_runs_to_go, uc.use_case "
        f"from {catalog}.{schema}.benchmark_run_config as cfg"
        f"   inner join {catalog}.{schema}.benchmark_use_case as uc on cfg.use_case_id = uc.id and uc.use_case = '{use_case}' "
        f"   left outer join (select benchmark_run_config_id, count(*) runs from {catalog}.{schema}.benchmark_run_log where status = 'SUCCESS' group by benchmark_run_config_id) as cnt on cfg.id = cnt.benchmark_run_config_id "
        f"where cfg.run_type = 'DBT' AND coalesce(cnt.runs, 0) < cfg.nr_of_runs "
    )

    for row in dbt_benchmark_runs.collect():
        benchmark_dbt(row)


# Function that supplies all dlt configurations to run
def execute_benchmark_dlt():
    dlt_benchmark_runs = spark.sql(
        f"select cfg.*, cfg.nr_of_runs - coalesce(cnt.runs, 0) as nr_runs_to_go, uc.use_case "
        f"from {catalog}.{schema}.benchmark_run_config as cfg"
        f"   inner join {catalog}.{schema}.benchmark_use_case as uc on cfg.use_case_id = uc.id and uc.use_case = '{use_case}' "
        f"   left outer join (select benchmark_run_config_id, count(*) runs from {catalog}.{schema}.benchmark_run_log where status = 'SUCCESS' group by benchmark_run_config_id) as cnt on cfg.id = cnt.benchmark_run_config_id "
        f"where cfg.run_type = 'DLT' AND coalesce(cnt.runs, 0) < cfg.nr_of_runs "
    )

    for row in dlt_benchmark_runs.collect():
        benchmark_dlt(row)


# This is where the benchmark is started!!
# Use ThreadPoolExecutor to run 1 dbt and 1 dlt thread concurrently
with concurrent.futures.ThreadPoolExecutor() as executor:
    future1 = executor.submit(execute_benchmark_dbt)
    future2 = executor.submit(execute_benchmark_dlt)

# Wait for tasks to complete
concurrent.futures.wait([future1, future2])

# COMMAND ----------
