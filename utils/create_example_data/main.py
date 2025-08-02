# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC This Notebook is meant to generate example data that can be used to quickly get started using DBT, without the need to ingest your own data. It create the tables needed for the small "sales" model explained below. It will create these tables as "managed tables" in a UC catalog. These tables represent the "raw" layer of the EDP architecture and the starting point for transformations into the "enriched unharmonized", "enriched harmonized" and "curated" data layers.
# MAGIC
# MAGIC ### Quick start
# MAGIC To run this Notebook, you will need:
# MAGIC - an 'all-purpose compute cluster' to run them on. The cluster must be Unity Catalog enabled.
# MAGIC - a UC Catalog and the permission to create a schema in it, **OR**
# MAGIC - a schema that is already created for you and the permissions to create managed tables inside that schema.
# MAGIC
# MAGIC When these conditions are met, attach this notebook to the cluster and go to the first cell below this one. In there, you need to specify general settings like 'catalog' and 'schema' name. Make sure to change their values and run the cell. If needed, it will automatically create the schema for you. \
# MAGIC
# MAGIC Now you're ready to generate the data! \
# MAGIC There are three different sections that can load the data for different purposes.
# MAGIC
# MAGIC **1. Initial load and table creation** \
# MAGIC This section will create the tables if they are not there yet. It will add just a few records to these tables, enough to run the DBT project and exactly see what rows are generated and how they "flow" through the different layers. Running this Notebook will **overwrite** any data that currently exists in the example data tables. \
# MAGIC Run this section if the tables are not created yet or if you want to "reload" them with just a few rows.
# MAGIC
# MAGIC **2. Add additional rows** \
# MAGIC This section is meant to add additonal data to the example data tables. It will only **append** data to the tables, not overwrite them. Therefor you can run it multiple times with different parameters if needed. This section can be usefull to experiment or test with bigger data sets and incremental loads. \
# MAGIC Make sure the 'Initial load and table creation' section has been run at least once before running this section.
# MAGIC
# MAGIC **3. Add streaming rows** \
# MAGIC This section is meant to add "streaming" data to the 'order' table. It will only **append** data to the table, not overwrite it. The data will be added in batches and you can set amount of orders in a batch and its duration. This section can be usefull to experiment or test with "streaming" sources. \
# MAGIC Make sure the 'Initial load and table creation' section has been run at least once before running this section.
# MAGIC
# MAGIC ### The model
# MAGIC Thi notebook will generate a small 'sales' model. It consist of a 'customer', a 'product', a 'product category' and an 'order' table.
# MAGIC - **Customer**: The 'customer' table contains a set of randomly generated "customers". Each customer will have a unique customer number and randomly generated attributes like name, address, email, etc. To simulate a real world example, some of these attributes can change over time. Each customer will have 1 record in the 'customer' table with a 'modified_at' of January 1st of the provided start year. Some customers will have additional rows with changed attributes at later dates.
# MAGIC - **Product**: The 'product' table contains a set of randomly generated "products". Each product will have a unique product number and randomly generated attributes like name, color, price, etc. Each 'product' also has a 'product category' it belongs to. To simulate a real world example, the color and price attributes can change over time. Each product will have 1 record in the 'product' table with a 'modified_at' of January 1st of the provided start year. Some products will have additional rows with changed attributes at later dates.
# MAGIC - **Product category**: The 'product_category' table contains a set of randomly generated "product categories". It has a parent-child structure. So there is 1 top-level 'product category' without a "parent category" and all other 'product categories' have a "parent category" that refers to another 'product category' in the same table. This hierarchy has no depth limit; it mainly depends on how many categories are generated. 'Products' however can only refer to so called "leaf level" 'product catagories' that do not have other 'product categories' refering to it.
# MAGIC - **Order**: The 'order' table contains a set of randomly generated "orders". Each order will have a unique 'order number' and randomly generated 'customer number', 'ordered at' timestamp and a 'payment method'. It will also have a nested array called 'order_details' that contains 1 or many order detail rows. Each of those detail row contains a random 'product number', the 'price' and the ordered 'amount'. This price of course corresponds to the price at that moment in the product table. For simplicity reasons, there is no relation between the 'order number' and  the 'ordered at' date. So, an order with a higher 'order number' can have an earlier 'orderded at' date.
# MAGIC
# MAGIC
# MAGIC ### General configuration
# MAGIC Configure the parameters below before running this notebook. This will configure the 'catalog' and 'schema' and create the schema if it does not exist yet.

# COMMAND ----------

# Both the catalog and the schema must be set to be able to create the demo data.
catalog = "dev_prod_dbt_poc_unitycatalog_dev"
schema = "dbt_example_raw"

# If the schema is not yet created, the path to the storage account used to store the managed tables must be
# provided. This paths must also be an External Location in your catalog.
storage_account_path = (
    "abfss://<container_name>@<storage_account_name>.dfs.core.windows.net"
)


# Make sure to create the schema if it does not exists yet
spark.sql(
    f"create schema if not exists {catalog}.{schema} managed location '{storage_account_path}';"
)

# COMMAND ----------

# MAGIC %md ## Initial load and table creation
# MAGIC This section will create the schema and tables if they are not there yet. In the next cell you can change the default settings if you wish. Run the next two cells to generate the data. \
# MAGIC **Note:** Running the cells below will overwrite any data that currently exists in the example data tables.

# COMMAND ----------

# The data will be generated for the year set here
year = 2022

# It is best to let the initial load generate just a few rows and then run the dbt model. Just enough to run the
# DBT project and exactly see what rows are generated and how they "flow" through the different data layers. After
# that, the 'Add additional rows' section can be used to add more data.
number_of_customers = 10
number_of_product_categories = 10
number_of_products = 10
number_of_orders = 20

# COMMAND ----------

# This will run the notebook to generate the data. There is no need to change anything here.
try:
    result = dbutils.notebook.run(
        "./core/1_initial_load_and_table_creation",
        600,
        {
            "catalog": catalog,
            "schema": schema,
            "year": year,
            "number_of_customers": number_of_customers,
            "number_of_product_categories": number_of_product_categories,
            "number_of_products": number_of_products,
            "number_of_orders": number_of_orders,
        },
    )
    print("Result: " + str(result))
except Exception as e:
    raise Exception(
        "Execution of the Notebook failed. Please click on the 'Notebook job' link above for more details"
    )

# COMMAND ----------

# MAGIC %md ## Add additional rows
# MAGIC This section can be used to add additional rows to existing tables. It should only be used after the 'Initial load and table creation' section has been run at least once to create the required tables. This section will only append rows to these tables; not create them. In the next cell, you can change the number of customers, products and orders to add. Run the next two cells to add the data to the tables. You can run this section as many times as needed.

# COMMAND ----------

# The data will be generated for the year set here
year = 2022

# Change the numbers below to generate more or less records. When set to 0, no records will be added to that table.
number_of_customers_to_add = 500
number_of_product_categories_to_add = 50
number_of_products_to_add = 50
number_of_orders_to_add = 100000

# COMMAND ----------

# This will run the notebook to generate the data. There is no need to change anything here.
try:
    result = dbutils.notebook.run(
        "./core/2_add_additional_rows",
        1800,
        {
            "catalog": catalog,
            "schema": schema,
            "year": year,
            "number_of_customers_to_add": number_of_customers_to_add,
            "number_of_product_categories_to_add": number_of_product_categories_to_add,
            "number_of_products_to_add": number_of_products_to_add,
            "number_of_orders_to_add": number_of_orders_to_add,
        },
    )
    print("Result: " + str(result))
except Exception as e:
    raise Exception(
        "Execution of the Notebook failed. Please click on the 'Notebook job' link above for more details"
    )

# COMMAND ----------

# MAGIC %md ## Add streaming rows
# MAGIC This section can be used to add additional rows to existing tables in a "streaming" fashion. It should only be used after the 'Initial load and table creation' section has been run at least once to create the required tables. This section will only append rows to the 'order' table; not create it. No rows will be added to the 'customer' or 'product' table. In the next cell, you can change the number of order to add per "batch". You must also set a limit on the number of batches and/or the number of seconds to run it. Run the next two cells to have the "batches" be added one by one to the 'order' table. You can run this section as many times as needed.

# COMMAND ----------

# The data will be generated for the year set here
year = 2022

# Change the numbers below to generate more or less records. Setting either
# number_of_batches or number_of_seconds to 0 will remove that limitation.
number_of_batches = 10
number_of_seconds = 0
number_of_orders_per_batch = 50

# COMMAND ----------

# This will run the notebook to generate the data. There is no need to change anything here.
try:
    result = dbutils.notebook.run(
        "./core/3_add_streaming_rows",
        1800 + number_of_seconds,
        {
            "catalog": catalog,
            "schema": schema,
            "year": year,
            "number_of_batches": number_of_batches,
            "number_of_seconds": number_of_seconds,
            "number_of_orders_per_batch": number_of_orders_per_batch,
        },
    )
    print("Result: " + str(result))
except Exception as e:
    raise Exception(
        "Execution of the Notebook failed. Please click on the 'Notebook job' link above for more details"
    )
