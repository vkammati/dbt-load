locals {
  notebooks = fileset("../notebooks", "**/*.py")
}

resource "databricks_notebook" "notebooks" {
  for_each = local.notebooks
  source   = "../notebooks/${each.key}"
  path     = "/Workspace/Shared/edp_dbt_notebooks/${each.key}"
}
