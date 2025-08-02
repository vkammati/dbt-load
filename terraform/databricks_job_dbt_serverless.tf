
locals {
  job_file_path_serverless = "../config/${terraform.workspace}/databricks_dbt_job.yml"
}

data "local_file" "databricks_jobs_serverless" {
  count    = fileexists(local.job_file_path_serverless) ? 1 : 0
  filename = local.job_file_path_serverless
}

locals {
  job_config_data_serverless = fileexists(local.job_file_path_serverless) ? yamldecode(data.local_file.databricks_jobs_serverless[0].content).jobs : null
  job_config_serverless      = fileexists(local.job_file_path_serverless) ? local.job_config_data_serverless : coalesce(local.job_config_data_serverless, [])
}

data "databricks_current_config" "this_serverless" {}

data "databricks_node_type" "smallest_job_cluster_serverless" {
  local_disk = true
}

data "databricks_spark_version" "latest_job_lts_serverless" {
  long_term_support = true
}

locals {
  first_sql_endpoint_http_path_serverless = can(values(databricks_sql_endpoint.sql_endpoint)[0].odbc_params[0].path) ? coalesce(var.dbx_http_path, values(databricks_sql_endpoint.sql_endpoint)[0].odbc_params[0].path) : coalesce(var.dbx_http_path, "dbx_http_path is missing")
}

# This resource block defines a Databricks job named "job_dbt_run_all_serverless".
# The job is created for each entry in the local.job_config_serverless map where the cluster_type is "serverless".
# The job name is used as the key in the for_each map.
resource "databricks_job" "job_dbt_run_all_serverless" {
  for_each = {
    for index, job in local.job_config_serverless :
    job.name => job
    if job.cluster_type == "serverless"
  }

  name                = each.value.name
  description         = lookup(each.value, "description", null)
  max_concurrent_runs = lookup(each.value, "max_concurrent_runs", 1)
  timeout_seconds     = lookup(each.value, "timeout_seconds", null)
  tags                = lookup(each.value, "tags", {})

  run_as {
    service_principal_name = lookup(each.value, "run_as_service_principal_id", var.run_spn_client_id)
  }

  dynamic "schedule" {
    for_each = contains(keys(each.value), "schedule") ? [each.value.schedule] : []
    content {
      quartz_cron_expression = schedule.value.quartz_cron_expression
      timezone_id            = lookup(schedule.value, "timezone_id", "UTC")
      pause_status           = lookup(schedule.value, "pause_status", "UNPAUSED")
    }
  }

  dynamic "continuous" {
    for_each = contains(keys(each.value), "continuous") ? [each.value.continuous] : []
    content {
      pause_status = lookup(continuous.value, "pause_status", "UNPAUSED")
    }
  }

  dynamic "trigger" {
    for_each = contains(keys(each.value), "trigger") ? [each.value.trigger] : []
    content {
      pause_status = lookup(trigger.value, "pause_status", "UNPAUSED")

      file_arrival {
        url                               = lookup(trigger.value, "file_arrival_url", null)
        min_time_between_triggers_seconds = lookup(trigger.value, "file_arrival_min_time_between_triggers_seconds", null)
        wait_after_last_change_seconds    = lookup(trigger.value, "file_arrival_wait_after_last_change_seconds", null)
      }
    }
  }

  dynamic "queue" {
    for_each = contains(keys(each.value), "queue") ? [each.value.queue] : []
    content {
      enabled = queue.value
    }
  }

  environment {
    environment_key = "default"
    spec {
      dependencies = ["/Workspace/Shared/edp_dbt_runner/edp_dbt_runner-${var.edp_dbt_runner_wheel_version}-py3-none-any.whl"]
      client       = "1"
    }
  }

  dynamic "task" {
    for_each = { for index, task in each.value.tasks : task.task_key => task }

    content {
      task_key                  = task.value.task_key
      timeout_seconds           = lookup(task.value, "timeout_seconds", 10800)
      run_if                    = lookup(task.value, "run_if", "ALL_SUCCESS")
      max_retries               = lookup(task.value, "max_retries", 0)
      min_retry_interval_millis = lookup(task.value, "min_retry_interval_millis", 0)
      environment_key           = "default"

      dynamic "python_wheel_task" {
        for_each = contains(keys(task.value), "dbt_command") ? [task.value] : []

        content {
          package_name = lookup(task.value, "package_name", "edp_dbt_runner")
          entry_point  = lookup(task.value, "entry_point", "run")
          named_parameters = {
            dbt_command = "${lookup(task.value, "dbt_command", "")}"
            dbt_vars    = "${jsonencode(lookup(task.value, "dbt_vars", {}))}"
            environment_variables = "${jsonencode(tomap({
              DBX_HTTP_PATH = lookup(task.value, "http_path", local.first_sql_endpoint_http_path_serverless)
            }))}"
            log_level = "${lookup(task.value, "log_level", "INFO")}"
            context = "${jsonencode(tomap({
              environment         = terraform.workspace,
              job_name            = "{{job.name}}",
              task_name           = "{{task.name}}",
              github_organisation = var.github_organisation,
              github_repository   = var.github_repository,
              githubapp_id        = var.githubapp_id,
            }))}"
            elementary        = "${jsonencode(lookup(task.value, "elementary", tomap({ update_github_pages = false })))}"
            continuous_config = "${jsonencode(lookup(task.value, "continuous_config", {}))}"
            spark_env_vars = jsonencode(tomap({
              PYSPARK_PYTHON : "/databricks/python3/bin/python3"
              DBX_CLUSTER_ID : coalesce(var.dbx_cluster_id, " ")
              DBX_UNITY_CATALOG : var.dbx_unity_catalog
              EXTERNAL_LOCATION_URL : coalesce(var.external_location_url, "n/a")
              DBT_EXTERNAL_RAW : coalesce(var.dbt_external_raw, " ")
              DBT_EXTERNAL_EUH : coalesce(var.dbt_external_euh, " ")
              DBT_EXTERNAL_EH : coalesce(var.dbt_external_eh, " ")
              DBT_EXTERNAL_CUR : coalesce(var.dbt_external_cur, " ")
              DBT_EXTERNAL_ELEMENTARY : coalesce(var.dbt_external_elementary, " ")
              DBX_ELEMENTARY_SCHEMA : coalesce(var.dbx_elementary_schema, " ")
              DBT_LANDING_LOC : coalesce(var.dbt_landing_loc, " ")
              TEAMS_WEBHOOK_URL : coalesce(var.teams_webhook_url, " ")
            }))
          }
        }
      }

      dynamic "depends_on" {
        for_each = contains(keys(task.value), "depends_on") ? task.value.depends_on : []
        content {
          task_key = depends_on.value
        }
      }

      dynamic "health" {
        for_each = contains(keys(task.value), "task_runtime_warning_threshold_seconds") ? [1] : []
        content {
          rules {
            metric = "RUN_DURATION_SECONDS"
            op     = "GREATER_THAN"
            value  = lookup(task.value, "task_runtime_warning_threshold_seconds")
          }
        }
      }

      dynamic "email_notifications" {
        iterator = task_email_notifications
        for_each = contains(keys(task.value), "email_notifications") ? [task.value.email_notifications] : []
        content {
          on_start                               = lookup(task_email_notifications.value, "on_start", null)
          on_success                             = lookup(task_email_notifications.value, "on_success", null)
          on_failure                             = lookup(task_email_notifications.value, "on_failure", null)
          on_duration_warning_threshold_exceeded = lookup(task_email_notifications.value, "on_duration_warning_threshold_exceeded", null)
        }
      }

      dynamic "webhook_notifications" {
        for_each = contains(keys(task.value), "webhook_notifications") ? [task.value.webhook_notifications] : []
        content {
          dynamic "on_start" {
            for_each = contains(keys(webhook_notifications.value), "on_start") ? webhook_notifications.value.on_start : []
            content {
              id = on_start.value
            }
          }
          dynamic "on_success" {
            for_each = contains(keys(webhook_notifications.value), "on_success") ? webhook_notifications.value.on_success : []
            content {
              id = on_success.value
            }
          }
          dynamic "on_failure" {
            for_each = contains(keys(webhook_notifications.value), "on_failure") ? webhook_notifications.value.on_failure : []
            content {
              id = on_failure.value
            }
          }
          dynamic "on_duration_warning_threshold_exceeded" {
            for_each = contains(keys(webhook_notifications.value), "on_duration_warning_threshold_exceeded") ? webhook_notifications.value.on_duration_warning_threshold_exceeded : []
            content {
              id = on_duration_warning_threshold_exceeded.value
            }
          }
        }
      }
    }
  }
}
