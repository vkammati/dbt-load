
locals {
  cluster_file_path = "../config/${terraform.workspace}/databricks_cluster.yml"
}

data "local_file" "databricks_cluster" {
  count    = fileexists(local.cluster_file_path) ? 1 : 0
  filename = local.cluster_file_path
}

locals {
  cluster_config_data = fileexists(local.cluster_file_path) ? yamldecode(data.local_file.databricks_cluster[0].content).cluster : null
  cluster_config = fileexists(local.cluster_file_path) ? local.cluster_config_data : coalesce(local.cluster_config_data,[])
}

data "databricks_node_type" "smallest" {
  local_disk = true
}
data "databricks_node_type" "smallest_with_photon" {
  local_disk = true
  photon_driver_capable = true
  photon_worker_capable = true
}
data "databricks_spark_version" "latest_cluster_lts" {
  long_term_support = true
}

resource "databricks_cluster" "shared_cluster" {
  for_each = {
    for index, cluster in local.cluster_config :
    cluster.name => cluster
  }
  cluster_name            = each.value.name
  spark_version           = lookup(each.value, "spark_version", data.databricks_spark_version.latest_cluster_lts.id)
  node_type_id            = lookup(each.value, "node_type_id", lookup(each.value, "runtime_engine", "STANDARD") == "STANDARD" ? data.databricks_node_type.smallest.id : data.databricks_node_type.smallest_with_photon.id)
  runtime_engine          = lookup(each.value, "runtime_engine", "STANDARD")
  data_security_mode      = lookup(each.value, "data_security_mode", "USER_ISOLATION")
  autotermination_minutes = lookup(each.value, "autotermination_minutes", 20)
  # Set the envrionment variables. The PYSPARK_PYTHON must always be set but can be overwritten
  spark_env_vars          = merge({PYSPARK_PYTHON : "/databricks/python3/bin/python3"} , lookup(each.value, "spark_env_vars", {}))

  # Either specify the num_workers (fixed number of workers, can be 0 as well) or autoscale (min_workers and max_workers)
  num_workers = lookup(each.value, "num_workers", null)
  dynamic "autoscale" {
    for_each = !(contains(keys(each.value), "num_workers")) ? [1] : []
    content {
      min_workers = contains(keys(each.value), "autoscale") ? lookup(each.value.autoscale, "min_workers", 1) : 1
      max_workers = contains(keys(each.value), "autoscale") ? lookup(each.value.autoscale, "max_workers", 2) : 2
    }
  }
  #If num_workers = 0, this means it needs to be a single node cluster and we need to set spark_conf and custom_tags as well
    spark_conf = merge(
      lookup(each.value, "num_workers", null) != 0 ? null : tomap({
      "spark.databricks.cluster.profile"     = "singleNode"
      "spark.master"                         = "local[*, 4]"
    }),
     {
      "fs.azure.account.auth.type.${var.storage_account}.dfs.core.windows.net" = "OAuth"
      "fs.azure.account.oauth.provider.type.${var.storage_account}.dfs.core.windows.net" = "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
      "fs.azure.account.oauth2.client.id.${var.storage_account}.dfs.core.windows.net" = var.run_spn_client_id
      "fs.azure.account.oauth2.client.secret.${var.storage_account}.dfs.core.windows.net" = var.run_spn_client_secret
      "fs.azure.account.oauth2.client.endpoint.${var.storage_account}.dfs.core.windows.net" = "https://login.microsoftonline.com/${var.run_spn_tenant_id}/oauth2/token"
    })

  custom_tags = "${lookup(each.value, "num_workers", null) != 0 ? null : tomap({ResourceClass = "SingleNode"})}"


  dynamic "library" {
    for_each = contains(keys(each.value), "python_packages") ? each.value.python_packages : []
    content {
      pypi {
        package = library.value
      }
    }
  }

  dynamic "azure_attributes" {
    for_each = contains(keys(each.value), "azure_attributes") ? [each.value.azure_attributes] : []
    content {
      first_on_demand    = lookup(azure_attributes.value, "first_on_demand", 1)
      availability       = lookup(azure_attributes.value, "availability", "SPOT_WITH_FALLBACK_AZURE")
      spot_bid_max_price = lookup(azure_attributes.value, "spot_bid_max_price", -1)
    }
  }

  dynamic "aws_attributes" {
    for_each = contains(keys(each.value), "aws_attributes") ? [each.value.aws_attributes] : []
    content {
      first_on_demand         = lookup(aws_attributes.value, "first_on_demand", 0)
      availability            = lookup(aws_attributes.value, "availability", "SPOT_WITH_FALLBACK")
      spot_bid_price_percent  = lookup(aws_attributes.value, "spot_bid_price_percent", 100)
    }
  }
}

# Loop over the config again to add required and optional permissions
resource "databricks_permissions" "cluster_usage" {
  for_each = {
    for index, cluster in local.cluster_config :
    cluster.name => cluster
  }

  # Lookup the id of the created/existing cluster based on its name
  cluster_id = databricks_cluster.shared_cluster[each.key].id

  # Add required permission to the spn used to run workflows
  access_control {
    service_principal_name = var.run_spn_client_id
    permission_level       = "CAN_MANAGE"
  }

  # Add any optional access control specified in the yaml file
  dynamic "access_control" {
    for_each = lookup(each.value, "access_control", [])
    content {
        group_name             = lookup(access_control.value, "group_name", null)
        permission_level       = lookup(access_control.value, "permission_level", null)
      }
  }
}
