# Databricks notebook source
# MAGIC %md
# MAGIC # Example notebook
# MAGIC This is an example notebook to demonstrate how to deploy notebooks as part of the CI/CD pipeline.
# MAGIC All `.py` files in this `notebooks` directory and subdirectories will be deployed to Databricks workspace under the path `/Workspace/Shared/edp_dbt_notebooks`.
# MAGIC
# MAGIC **Note:** Be aware that all '.py' files will be created as 'Notebook files', not as 'Python files'.

# COMMAND ----------

print("Hello world!")
