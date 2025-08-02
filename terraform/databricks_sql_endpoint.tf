
locals {
  sql_file_path = "../config/${terraform.workspace}/databricks_sql_warehouse.yml"
}

data "local_file" "databricks_sql_warehouse" {
  count    = fileexists(local.sql_file_path) ? 1 : 0
  filename = local.sql_file_path
}

locals {
  sql_wh_config_data = fileexists(local.sql_file_path) ? yamldecode(data.local_file.databricks_sql_warehouse[0].content).sql_warehouse : null
  sql_wh_config = fileexists(local.sql_file_path) ? local.sql_wh_config_data : coalesce(local.sql_wh_config_data,[])
}

resource "databricks_sql_endpoint" "sql_endpoint" {
  for_each = {
    for index, sql_wh in local.sql_wh_config :
    sql_wh.name => sql_wh
  }
  name                      = each.value.name
  cluster_size              = lookup(each.value, "cluster_size", "2X-Small")
  warehouse_type            = lookup(each.value, "warehouse_type", "CLASSIC")
  enable_serverless_compute = lookup(each.value, "enable_serverless_compute", "false")
  min_num_clusters          = lookup(each.value, "min_num_clusters", 1)
  max_num_clusters          = lookup(each.value, "max_num_clusters", 1)
  # Serverless compute has a minimum auto terminate of 5 min, PRO and CLASSIC has 10 min
  auto_stop_mins            = lookup(each.value, "auto_stop_mins", lookup(each.value, "enable_serverless_compute", false) ? 5 : 10)
  tags {
    custom_tags {
      key =contains(keys(each.value), "tags") ? lookup(each.value.tags[0], "key", "DEFAULT") : "DEFAULT"
      value =contains(keys(each.value), "tags") ? lookup(each.value.tags[0], "value", "DEFAULT") : "DEFAULT"
    }
  }
  timeouts {
    create = "30m"
  }
}

resource "databricks_permissions" "sql_usage" {
  for_each = databricks_sql_endpoint.sql_endpoint

  sql_endpoint_id = each.value.id

  access_control {
    service_principal_name = var.run_spn_client_id
    permission_level       = "CAN_USE"
  }
}

# Add any optional access control specified in the yaml file
resource "databricks_permissions" "sql_endpoint_permissions" {
  for_each = {
    for sql_wh in local.sql_wh_config :
    sql_wh.name => sql_wh
    if length(try(sql_wh.access_control, [])) > 0
  }
  sql_endpoint_id = databricks_sql_endpoint.sql_endpoint[each.key].id

  dynamic "access_control" {
    for_each = try(each.value.access_control, [])
    content {
      group_name       = lookup(access_control.value, "group_name", null)
      permission_level = lookup(access_control.value, "permission_level", null)
    }
  }
}
