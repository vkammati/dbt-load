# This resource creates a secret scope for EDP Auto Loader
resource "databricks_secret_scope" "this" {
  name                     = "${var.github_repository}_${terraform.workspace}_edp_dbt_runner"
  # initial_manage_principal = "users"
  backend_type             = "DATABRICKS"
}

# This resource grants READ permission to the Run SPN on the secret scope
resource "databricks_secret_acl" "secret_scope_read_run_spn" {
  count = var.run_spn_client_id != "" ? 1 : 0

  principal  = var.run_spn_client_id
  permission = "READ"
  scope      = databricks_secret_scope.this.id
}

# This resource creates a secret for the GitHub App Private Key
resource "databricks_secret" "githubapp_private_key" {
  count = var.githubapp_private_key != "" ? 1 : 0

  key          = "GITHUBAPP-PRIVATE-KEY"
  string_value = var.githubapp_private_key
  scope        = databricks_secret_scope.this.id
}

resource "databricks_secret" "run_spn_tenant_id" {
  count = var.run_spn_tenant_id != "" ? 1 : 0

  key = "RUN_SPN_TENANT_ID"
  string_value = var.run_spn_tenant_id
  scope = databricks_secret_scope.this.id
}

resource "databricks_secret" "run_spn_client_secret" {
  count = var.run_spn_client_secret != "" ? 1 : 0

  key = "RUN_SPN_CLIENT_SECRET"
  string_value = var.run_spn_client_secret
  scope = databricks_secret_scope.this.id
}

resource "databricks_secret" "storage_account" {
  count = var.storage_account != "" ? 1 : 0

  key = "AZ_SA_NAME"
  string_value = var.storage_account
  scope = databricks_secret_scope.this.id
}
