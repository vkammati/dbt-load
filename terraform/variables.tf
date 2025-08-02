variable "edp_dbt_runner_wheel_version" {
  description = "The version of the EDP DBT Runner wheel the workflow should use (x.x.x)"
  type        = string
}

variable "dbt_external_raw" {
  description = "URL to (folder in) 'deltalake' container (azure) or s3 bucket (aws) to store EXTERNAL tables of raw layer"
  type        = string
}

variable "dbt_external_euh" {
  description = "URL to (folder in) 'deltalake' container (azure) or s3 bucket (aws) to store EXTERNAL tables of enriched-unharmonized layer"
  type        = string
}

variable "dbt_external_eh" {
  description = "URL to (folder in) 'deltalake' container (azure) or s3 bucket (aws) to store EXTERNAL tables of enriched-harmonized layer"
  type        = string
}

variable "dbt_external_cur" {
  description = "URL to (folder in) 'deltalake' container (azure) or s3 bucket (aws) to store EXTERNAL tables of curated layer"
  type        = string
}

variable "dbt_external_elementary" {
  description = "URL to (folder in) 'deltalake' container (azure) or s3 bucket (aws) to store EXTERNAL tables of elementary package"
  type        = string
}

variable "external_location_url" {
  description = "URL to (folder in) 'deltalake' container (azure) or s3 bucket (aws) to store MANAGED tables"
  type        = string
}

variable "dbt_landing_loc" {
  description = "URL to s3 bucket used as landing location for incoming data. For now, only supported on AWS."
  type        = string
}

variable "dbx_unity_catalog" {
  description = "Databricks Unity catalog name"
  type        = string
}

variable "dbx_http_path" {
  description = "Databricks sql warehouse http path"
  type        = string
}

variable "dbx_cluster_id" {
  description = "Databricks cluster id"
  type        = string
}

variable "dbx_elementary_schema" {
  description = "Name of the schema used to write elementary tables"
  type        = string
}

variable "run_spn_client_id" {
  description = "client id of the spn used as 'run as' user of the workflow"
  type        = string
}

variable "github_organisation" {
  description = "Name of the GitHub organisation"
  type        = string
  default     = "sede-x"
}

variable "github_repository" {
  description = "Name of the GitHub repository"
  type        = string
}

variable "githubapp_id" {
  description = "Id of the GitHub App used to upload Elementary reports to GitHub pages"
  type        = string
}

variable "githubapp_private_key" {
  description = "Private Key of the GitHub App used to upload Elementary reports to GitHub pages"
  type        = string
}

variable "teams_webhook_url"{
  description = "Webhook URL generated for a dedicated teams channel to notify when some tests fail in the workflow"
  type = string
}

variable "run_spn_tenant_id"{
  description = "tenant id of the spn used as 'run as' user of the workflow"
  type        = string
}

variable "run_spn_client_secret"{
  description = "client secret of the spn used as 'run as' user of the workflow"
  type        = string
  default = ""
}

variable "storage_account"{
  description = "Name of the storage account"
  type        = string
  default = ""
}
