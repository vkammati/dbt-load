
locals {
  job_file_path = "../config/${terraform.workspace}/databricks_dbt_job.yml"
}

data "local_file" "databricks_jobs" {
  count    = fileexists(local.job_file_path) ? 1 : 0
  filename = local.job_file_path
}

locals {
  job_config_data = fileexists(local.job_file_path) ? yamldecode(data.local_file.databricks_jobs[0].content).jobs : null
  job_config      = fileexists(local.job_file_path) ? local.job_config_data : coalesce(local.job_config_data, [])

  azure_availability_config_data = (data.databricks_current_config.this.cloud_type == "azure") && fileexists(local.job_file_path) ? yamldecode(data.local_file.databricks_jobs[0].content).job_cluster.azure_attributes : null
  azure_availability_config      = fileexists(local.job_file_path) ? local.azure_availability_config_data : coalesce(local.azure_availability_config_data, [])

  aws_availability_config_data = (data.databricks_current_config.this.cloud_type == "aws") && fileexists(local.job_file_path) ? yamldecode(data.local_file.databricks_jobs[0].content).job_cluster.aws_attributes : null
  aws_availability_config      = fileexists(local.job_file_path) ? local.aws_availability_config_data : coalesce(local.aws_availability_config_data, [])
}


data "databricks_current_config" "this" {}

data "databricks_node_type" "smallest_job_cluster" {
  local_disk = true
}

data "databricks_spark_version" "latest_job_lts" {
  long_term_support = true
}

locals {
  first_sql_endpoint_http_path = can(values(databricks_sql_endpoint.sql_endpoint)[0].odbc_params[0].path) ? coalesce(var.dbx_http_path, values(databricks_sql_endpoint.sql_endpoint)[0].odbc_params[0].path) : coalesce(var.dbx_http_path, "dbx_http_path is missing")
}

resource "databricks_job" "job_dbt_run_all" {
  for_each = {
    for index, job in local.job_config :
    job.name => job
    if job.cluster_type == "standard"
  }

  name                = each.value.name
  description         = lookup(each.value, "description", null)
  max_concurrent_runs = lookup(each.value, "max_concurrent_runs", 1)
  timeout_seconds     = lookup(each.value, "timeout_seconds", null)
  # all key-value pairs under the tags attribute will be applied as tags to the job
  tags = lookup(each.value, "tags", {})

  # Set the run_as service principal. By default the Run SPN client id stored in
  # github is passed in and used but it can be overruled from the config yaml. It is
  # not possible to set this to a user account.
  run_as {
    service_principal_name = lookup(each.value, "run_as_service_principal_id", var.run_spn_client_id)
  }

  # Set schedule if specified. This is mutually exclusive with 'continuous' and 'trigger'
  dynamic "schedule" {
    for_each = contains(keys(each.value), "schedule") ? [each.value.schedule] : []
    content {
      quartz_cron_expression = schedule.value.quartz_cron_expression
      timezone_id            = lookup(schedule.value, "timezone_id", "UTC")
      pause_status           = lookup(schedule.value, "pause_status", "UNPAUSED")
    }
  }

  # Set continuous if specified. This is mutually exclusive with 'schedule' and 'trigger'
  dynamic "continuous" {
    for_each = contains(keys(each.value), "continuous") ? [each.value.continuous] : []
    content {
      pause_status = lookup(continuous.value, "pause_status", "UNPAUSED")
    }
  }

  # Set (file) trigger if specified. This is mutually exclusive with 'schedule' and 'continuous'
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

  # Optionally, enable the options to have workflow runs queued if the job is already running
  dynamic "queue" {
    for_each = contains(keys(each.value), "queue") ? [each.value.queue] : []
    content {
      enabled = queue.value
    }
  }


  # This is the job cluster that will be used to run edp_dbt_runner tasks. A single
  # node cluster should be more than enough to run dbt. We add this one to each job
  # independently of the other clusters that might be defined in the config yaml.
  # If not use by any task, it will not incur cost anyway.
  job_cluster {
    job_cluster_key = "edp_dbt_runner_cluster"
    new_cluster {
      spark_version       = data.databricks_spark_version.latest_job_lts.id
      node_type_id        = data.databricks_node_type.smallest_job_cluster.id
      runtime_engine      = "STANDARD"
      data_security_mode  = "SINGLE_USER"
      enable_elastic_disk = true
      num_workers         = 0
      spark_conf = {
        spark_databricks_delta_preview_enabled                                                = true
        "spark.databricks.cluster.profile"                                                    = "singleNode"
        "spark.master"                                                                        = "local[*, 4]"
        "fs.azure.account.auth.type.${var.storage_account}.dfs.core.windows.net"              = "OAuth"
        "fs.azure.account.oauth.provider.type.${var.storage_account}.dfs.core.windows.net"    = "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
        "fs.azure.account.oauth2.client.id.${var.storage_account}.dfs.core.windows.net"       = var.run_spn_tenant_id
        "fs.azure.account.oauth2.client.secret.${var.storage_account}.dfs.core.windows.net"   = var.run_spn_client_secret
        "fs.azure.account.oauth2.client.endpoint.${var.storage_account}.dfs.core.windows.net" = "https://login.microsoftonline.com/${var.run_spn_client_id}/oauth2/token"
      }
      custom_tags = {
        ResourceClass = "SingleNode"
      }

      dynamic "azure_attributes" {
        for_each = data.databricks_current_config.this.cloud_type == "azure" ? { azure : true } : {}
        content {
          #Check the yaml file for the availability setting. If not set, set to SPOT_WITH_FALLBACK_AZURE
          #Currently the setting is used for 'prd' environment to set to ON_DEMAND_AZURE
          availability = lookup(local.azure_availability_config, "availability", "SPOT_WITH_FALLBACK_AZURE")
        }
      }
      dynamic "aws_attributes" {
        for_each = data.databricks_current_config.this.cloud_type == "aws" ? { aws : true } : {}
        content {
          #Check the yaml file for the availability setting. If not set, set to SPOT_WITH_FALLBACK
          #Currently the setting is used for 'prd' environment to set to ON_DEMAND
          availability = lookup(local.aws_availability_config, "availability", "SPOT_WITH_FALLBACK")
        }
      }
    }
  }

  # Here we add any job clusters that might be defined for the job in the config yaml.
  # In the task itself, one only needs to refer to the job_cluster_key to use the
  # shared job cluster. We do not support dedicated clusters create for a single task.
  dynamic "job_cluster" {
    for_each = lookup(each.value, "job_clusters", [])

    content {
      job_cluster_key = job_cluster.value.job_cluster_key

      new_cluster {
        spark_version      = lookup(job_cluster.value, "spark_version", data.databricks_spark_version.latest_cluster_lts.id)
        node_type_id       = lookup(job_cluster.value, "node_type_id", lookup(job_cluster.value, "runtime_engine", "STANDARD") == "STANDARD" ? data.databricks_node_type.smallest.id : data.databricks_node_type.smallest_with_photon.id)
        runtime_engine     = lookup(job_cluster.value, "runtime_engine", "STANDARD")
        data_security_mode = lookup(job_cluster.value, "data_security_mode", "USER_ISOLATION")
        # Set the envrionment variables. The PYSPARK_PYTHON must always be set but can be overwritten
        spark_env_vars = merge({ PYSPARK_PYTHON : "/databricks/python3/bin/python3" }, lookup(job_cluster.value, "spark_env_vars", {}))

        # Either specify the num_workers (fixed number of workers, can be 0 as well) or autoscale (min_workers and max_workers)
        num_workers = lookup(job_cluster.value, "num_workers", null)
        dynamic "autoscale" {
          for_each = !(contains(keys(job_cluster.value), "num_workers")) ? [1] : []
          content {
            min_workers = contains(keys(job_cluster.value), "autoscale") ? lookup(job_cluster.value.autoscale, "min_workers", 1) : 1
            max_workers = contains(keys(job_cluster.value), "autoscale") ? lookup(job_cluster.value.autoscale, "max_workers", 2) : 2
          }
        }
        #If num_workers = 0, this means it needs to be a single node cluster and we need to set spark_conf and custom_tags as well
        spark_conf = (lookup(job_cluster.value, "num_workers", null) != 0 ? null : tomap({
          "spark.databricks.cluster.profile" = "singleNode"
          "spark.master"                     = "local[*, 4]"
        }))
        custom_tags = lookup(job_cluster.value, "num_workers", null) != 0 ? null : tomap({ ResourceClass = "SingleNode" })


        dynamic "azure_attributes" {
          for_each = contains(keys(job_cluster.value), "azure_attributes") ? [job_cluster.value.azure_attributes] : []
          content {
            first_on_demand    = lookup(azure_attributes.value, "first_on_demand", 1)
            availability       = lookup(azure_attributes.value, "availability", "SPOT_WITH_FALLBACK_AZURE")
            spot_bid_max_price = lookup(azure_attributes.value, "spot_bid_max_price", -1)
          }
        }

        dynamic "aws_attributes" {
          for_each = contains(keys(job_cluster.value), "aws_attributes") ? [job_cluster.value.aws_attributes] : []
          content {
            first_on_demand        = lookup(aws_attributes.value, "first_on_demand", 0)
            availability           = lookup(aws_attributes.value, "availability", "SPOT_WITH_FALLBACK")
            spot_bid_price_percent = lookup(aws_attributes.value, "spot_bid_price_percent", 100)
          }
        }
      }
    }
  }


  dynamic "notification_settings" {
    for_each = contains(keys(each.value), "notification_settings") ? [each.value.notification_settings] : []
    content {
      no_alert_for_skipped_runs  = lookup(notification_settings.value, "no_alert_for_skipped_runs", false)
      no_alert_for_canceled_runs = lookup(notification_settings.value, "no_alert_for_canceled_runs", false)
    }
  }

  # Defining dynamic health block for jobs
  dynamic "health" {
    for_each = contains(keys(each.value), "job_runtime_warning_threshold_seconds") ? [1] : []
    content {
      rules {
        metric = "RUN_DURATION_SECONDS"
        op     = "GREATER_THAN"
        value  = lookup(each.value, "job_runtime_warning_threshold_seconds")
        # Alert if the job duration exceeds job_runtime_warning_threshold_seconds.
      }
    }
  }

  # Set job level email notifications, if specified
  dynamic "email_notifications" {
    for_each = contains(keys(each.value), "email_notifications") ? [each.value.email_notifications] : []
    content {
      on_start                               = lookup(email_notifications.value, "on_start", null)
      on_success                             = lookup(email_notifications.value, "on_success", null)
      on_failure                             = lookup(email_notifications.value, "on_failure", null)
      on_duration_warning_threshold_exceeded = lookup(email_notifications.value, "on_duration_warning_threshold_exceeded", null)
      no_alert_for_skipped_runs              = lookup(email_notifications.value, "no_alert_for_skipped_runs", null)
    }
  }

  # Set job level webhook notifications (teams, slack, etc), if specified
  dynamic "webhook_notifications" {
    for_each = contains(keys(each.value), "webhook_notifications") ? [each.value.webhook_notifications] : []
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


  # Here the task(s) will be added to the job
  dynamic "task" {
    for_each = { for index, task in each.value.tasks : task.task_key => task }

    content {
      task_key                  = task.value.task_key
      job_cluster_key           = contains(keys(task.value), "existing_cluster_id") || contains(keys(task.value), "existing_cluster_name") ? null : lookup(task.value, "job_cluster_key", "edp_dbt_runner_cluster")
      existing_cluster_id       = contains(keys(task.value), "existing_cluster_id") ? task.value.existing_cluster_id : contains(keys(task.value), "existing_cluster_name") ? lookup(databricks_cluster.shared_cluster, task.value.existing_cluster_name).id : null
      timeout_seconds           = lookup(task.value, "timeout_seconds", 10800)
      run_if                    = lookup(task.value, "run_if", "ALL_SUCCESS")
      max_retries               = lookup(task.value, "max_retries", 0)
      min_retry_interval_millis = lookup(task.value, "min_retry_interval_millis", 0)

      # If the task has a 'dbt_command' attribute, we create a python wheel task and set
      # the latest edp_dbt_runner wheel as its library.
      dynamic "python_wheel_task" {
        for_each = contains(keys(task.value), "dbt_command") ? [task.value] : []

        content {
          package_name = lookup(task.value, "package_name", "edp_dbt_runner")
          entry_point  = lookup(task.value, "entry_point", "run")
          named_parameters = {
            dbt_command = "${lookup(task.value, "dbt_command", "")}"          # Set all dbt command including flags except vars
            dbt_vars    = "${jsonencode(lookup(task.value, "dbt_vars", {}))}" # Set dbt vars
            environment_variables = "${jsonencode(tomap({
              DBX_HTTP_PATH = lookup(task.value, "http_path", local.first_sql_endpoint_http_path)
            }))}"                                                    # Set environment variables
            log_level = "${lookup(task.value, "log_level", "INFO")}" # Set the python log level (DEBUG, INFO, WARNING, etc)
            context = "${jsonencode(tomap({
              environment         = terraform.workspace,
              job_name            = "{{job.name}}",
              task_name           = "{{task.name}}",
              github_organisation = var.github_organisation,
              github_repository   = var.github_repository,
              githubapp_id        = var.githubapp_id,
            }))}"                                                                                                         # Set additional context information needed for governance processes
            elementary        = "${jsonencode(lookup(task.value, "elementary", tomap({ update_github_pages = false })))}" # Set Elementary config
            continuous_config = "${jsonencode(lookup(task.value, "continuous_config", {}))}"                              # Set continuous/streaming config
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
      dynamic "library" {
        for_each = contains(keys(task.value), "dbt_command") ? [task.value] : []
        content {
          whl = "/Workspace/Shared/${lookup(task.value, "package_name", "edp_dbt_runner")}/${lookup(task.value, "package_name", "edp_dbt_runner")}-${var.edp_dbt_runner_wheel_version}-py3-none-any.whl"
        }
      }

      # If the task has a 'notebook_path' attribute, we create a notebook task and set
      # its path. If specified, we also add additional PyPi libraries to the task.
      dynamic "notebook_task" {
        for_each = contains(keys(task.value), "notebook_path") ? [task.value] : []
        content {
          notebook_path   = databricks_notebook.notebooks[replace(task.value.notebook_path, "\\", "/")].path
          source          = "WORKSPACE"
          base_parameters = lookup(task.value, "parameters", null)
        }
      }
      dynamic "library" {
        for_each = contains(keys(task.value), "notebook_path") && contains(keys(task.value), "python_packages") ? task.value.python_packages : []
        content {
          pypi {
            package = library.value
          }
        }
      }


      # The rest of the settings are generic for all types of task
      # "depends_on" can be use to make a task dependent on another task.
      dynamic "depends_on" {
        for_each = contains(keys(task.value), "depends_on") ? task.value.depends_on : []
        content {
          task_key = depends_on.value
        }
      }

      # Defining dynamic health block for tasks
      dynamic "health" {
        for_each = contains(keys(task.value), "task_runtime_warning_threshold_seconds") ? [1] : []
        content {
          rules {
            metric = "RUN_DURATION_SECONDS"
            op     = "GREATER_THAN"
            value  = lookup(task.value, "task_runtime_warning_threshold_seconds")
            # Alert if the task duration exceeds task_runtime_warning_threshold_seconds.
          }
        }
      }

      # Set task level email notifications, if specified
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

      # Set task level webhook notifications (teams, slack, etc), if specified
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
