# Databricks notebook source
# MAGIC %md
# MAGIC **Note:** This Notebook is meant to be run from the 'main' Notebook! Please check the 'main' notebook for further instructions.\
# MAGIC After including the 'shared' logic and fetching widget data, this Notebook will generate the requested amount of rows and append them to the tables.

# COMMAND ----------

# MAGIC %run ./shared

# COMMAND ----------

# Add widgets so this notebook can be parameterized
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("year", "0")
dbutils.widgets.text("number_of_customers_to_add", "0")
dbutils.widgets.text("number_of_product_categories_to_add", "0")
dbutils.widgets.text("number_of_products_to_add", "0")
dbutils.widgets.text("number_of_orders_to_add", "0")

# Fetch all widgets values
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
year = int(dbutils.widgets.get("year"))
number_of_customers_to_add = int(dbutils.widgets.get("number_of_customers_to_add"))
number_of_product_categories_to_add = int(dbutils.widgets.get("number_of_product_categories_to_add"))
number_of_products_to_add = int(dbutils.widgets.get("number_of_products_to_add"))
number_of_orders_to_add = int(dbutils.widgets.get("number_of_orders_to_add"))


# COMMAND ----------

# MAGIC %md ### Customer
# MAGIC Add rows to 'customer' table

# COMMAND ----------

# First get the last customer number from the table
last_customer_number = get_last_business_key(
    "customer_nr", f"{catalog}.{schema}.customer"
)

if number_of_customers_to_add > 0:
    # Get a base list of customers
    customer = get_customer_list(
        number_of_customers_to_add, year, last_customer_number + 1
    )

    # Add some future mutations
    customer = customer + get_mutations_to_customer_list(customer, year)

    # Transform the list into a DataFrame ...
    customer_df = get_customer_dataframe(customer)

    # ... and then write it into a UC table
    write_dataframe_to_table(customer_df, f"{catalog}.{schema}.customer", "Append")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product category
# MAGIC Add rows to 'product_category' table

# COMMAND ----------

if number_of_product_categories_to_add > 0:
    # First get the last product category number from the table
    last_product_category_number = get_last_business_key("product_category_nr", f"{catalog}.{schema}.product_category")

    # Get a list of all the products and their categories to determine which categories already have a
    # product "attached". These will not get child categories. This way they remain "leaf-level" categories,
    # which simplifies the downstream processing.
    product_df = read_table_to_dataframe(f"{catalog}.{schema}.product")
    product_categories_to_exclude = [data[0] for data in product_df.select("product_category_nr").distinct().collect()]

    # Get a base list of product_catagories
    product_category = get_product_category_list(
        number_of_product_categories_to_add, year, product_categories_to_exclude, last_product_category_number + 1
    )

    # Transform the list into a DataFrame ...
    product_category_df = get_product_category_dataframe(product_category)

    # ... and then write it into a UC table
    write_dataframe_to_table(product_category_df, f"{catalog}.{schema}.product_category", "Append")

# COMMAND ----------

# MAGIC %md ### Product
# MAGIC Add rows to 'product' table

# COMMAND ----------

# First get the last product number from the table
last_product_number = get_last_business_key("product_nr", f"{catalog}.{schema}.product")

if number_of_products_to_add > 0:
    # Get a list of all the leaf-level product categories (categories that do not have child catagories)
    product_category_df = read_table_to_dataframe(f"{catalog}.{schema}.product_category")
    leaf_level_product_categories = get_leaf_level_product_categories(product_category_df)

    # Get a base list of products
    product = get_product_list(
        number_of_products_to_add, year, leaf_level_product_categories, last_product_number + 1
    )

    # Add some future mutations
    product = product + get_mutations_to_product_list(product, year)

    # Transform the list into a DataFrame ...
    product_df = get_product_dataframe(product)

    # ... and then write it into a UC table
    write_dataframe_to_table(product_df, f"{catalog}.{schema}.product", "Append")

# COMMAND ----------

# MAGIC %md ### Order
# MAGIC Add rows to 'order' table

# COMMAND ----------

if number_of_orders_to_add > 0:
    # First get the last order number from the table
    last_order_number = get_last_business_key("order_nr", f"{catalog}.{schema}.order")

    # Get a list of order details
    order_detail = get_order_detail_list(
        number_of_orders_to_add,
        last_customer_number + number_of_customers_to_add,
        last_product_number + number_of_products_to_add,
        year,
        last_order_number + 1,
    )

    # Transform the list into a DataFrame
    order_detail_df = get_order_detail_dataframe(order_detail)

    # To create the orders dataframe, a full table with all the products is needed. The currenct 'product_df'
    # Dataframe (if even created) only contains the products created in this increment. Therefore we read
    # the content of the UC product table into a dataframe
    product_df = read_table_to_dataframe(f"{catalog}.{schema}.product")

    # Get an 'order' DataFrame by combining the 'order_details' with the 'product' DataFrame
    # and then aggregating it to the 'order' level. All 'order details' will be stored in
    # a nested array with the order record.
    order_df = get_order_dataframe(order_detail_df, product_df)

    # And then write it into a UC table
    write_dataframe_to_table(order_df, f"{catalog}.{schema}.order", "Append")

# COMMAND ----------

# Exit the notebook reporting "succes"
dbutils.notebook.exit("SUCCES")
