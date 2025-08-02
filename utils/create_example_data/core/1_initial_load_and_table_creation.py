# Databricks notebook source
# MAGIC %md
# MAGIC **Note:** This Notebook is meant to be run from the 'main' Notebook! Please check the 'main' notebook for further instructions.\
# MAGIC After including the 'shared' logic and fetching widget data, this Notebook will create the required tables in the given catalog and schema and add the requested amount of rows to them.

# COMMAND ----------

# MAGIC %run ./shared

# COMMAND ----------

# Add widgets so this notebook can be parameterized
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("year", "0")
dbutils.widgets.text("number_of_customers", "0")
dbutils.widgets.text("number_of_product_categories", "0")
dbutils.widgets.text("number_of_products", "0")
dbutils.widgets.text("number_of_orders", "0")

# Fetch all widgets values
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
year = int(dbutils.widgets.get("year"))
number_of_customers = int(dbutils.widgets.get("number_of_customers"))
number_of_product_categories = int(dbutils.widgets.get("number_of_product_categories"))
number_of_products = int(dbutils.widgets.get("number_of_products"))
number_of_orders = int(dbutils.widgets.get("number_of_orders"))


# COMMAND ----------

# MAGIC %md ### Customer
# MAGIC Create or overwrite 'customer' table

# COMMAND ----------


# Get a base list of customers
customer = get_customer_list(number_of_customers, year)

# Add some future mutations
customer = customer + get_mutations_to_customer_list(customer, year)

# Transform the list into a DataFrame ...
customer_df = get_customer_dataframe(customer)

# ... and then write it into a UC table
write_dataframe_to_table(customer_df, f"{catalog}.{schema}.customer", "Overwrite")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Product Category
# MAGIC Create or overwrite 'product_category' table

# COMMAND ----------

# Get a base list of product categories
product_category = get_product_category_list(number_of_product_categories, year)

# Transform the list into a DataFrame ...
product_category_df = get_product_category_dataframe(product_category)

# ... and then write it into a UC table
write_dataframe_to_table(product_category_df, f"{catalog}.{schema}.product_category", "Overwrite")


# COMMAND ----------

# MAGIC %md ### Product
# MAGIC Create or overwrite 'product' table

# COMMAND ----------

# Get a list of all the leaf-level product categories (categories that do not have child catagories)
product_category_df = read_table_to_dataframe(f"{catalog}.{schema}.product_category")
leaf_level_product_categories = get_leaf_level_product_categories(product_category_df)

# Get a base list of products
product = get_product_list(number_of_products, year, leaf_level_product_categories)

# Add some future mutations
product = product + get_mutations_to_product_list(product, year, 50)

# Transform the list into a DataFrame ...
product_df = get_product_dataframe(product)

# ... and then write it into a UC table
write_dataframe_to_table(product_df, f"{catalog}.{schema}.product", "Overwrite")


# COMMAND ----------

# MAGIC %md ### Order
# MAGIC Create or overwrite 'order' table

# COMMAND ----------

# First determine the last customer and product numbers in the schema
last_customer_number = get_last_business_key(
    "customer_nr", f"{catalog}.{schema}.customer"
)
last_product_number = get_last_business_key("product_nr", f"{catalog}.{schema}.product")

# Then use this to generate a list of order details.
order_detail = get_order_detail_list(
    number_of_orders, last_customer_number, last_product_number, year
)

# Transform the list into a DataFrame
order_detail_df = get_order_detail_dataframe(order_detail)

# Get an orders DataFrame by combining the 'order_details' with the 'products' DataFrame
# and then aggregating it to the 'order' level. All 'order details' will be stored in
# a nested array with the order record.
order_df = get_order_dataframe(order_detail_df, product_df)

# And then write it into a UC table
write_dataframe_to_table(order_df, f"{catalog}.{schema}.order", "Overwrite")

# COMMAND ----------

# Exit the notebook reporting "succes"
dbutils.notebook.exit("SUCCES")
