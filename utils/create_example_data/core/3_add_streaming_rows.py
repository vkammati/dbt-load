# Databricks notebook source
# MAGIC %md
# MAGIC **Note:** This Notebook is meant to be run from the 'main' Notebook! Please check the 'main' notebook for further instructions.\
# MAGIC After including the 'shared' logic and fetching widget data, his Notebook will continuously add orders to to the order table in a loop, simulating a streaming source.

# COMMAND ----------

# MAGIC %run ./shared

# COMMAND ----------

# Add widgets so this notebook can be parameterized
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("year", "0")
dbutils.widgets.text("number_of_batches", "0")
dbutils.widgets.text("number_of_seconds", "0")
dbutils.widgets.text("number_of_orders_per_batch", "0")

# Fetch all widgets values
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
year = int(dbutils.widgets.get("year"))
number_of_batches = int(dbutils.widgets.get("number_of_batches"))
number_of_seconds = int(dbutils.widgets.get("number_of_seconds"))
number_of_orders_per_batch = int(dbutils.widgets.get("number_of_orders_per_batch"))


# COMMAND ----------

# Check if either one of number_of_batches or number_of_batches is set
if number_of_batches == 0 and number_of_seconds == 0:
    raise Exception(
        "Parameters 'number_of_batches' and 'number_of_seconds' are both 0 or empty."
        " At least one of the two must be set."
    )


# COMMAND ----------

# MAGIC %md ### Order
# MAGIC Add rows to 'order' table, one (mini) batch at a time

# COMMAND ----------

# First get the last customer, product and order number from the table
last_customer_number = get_last_business_key(
    "customer_nr", f"{catalog}.{schema}.customer"
)
last_product_number = get_last_business_key("product_nr", f"{catalog}.{schema}.product")
last_order_number = get_last_business_key("order_nr", f"{catalog}.{schema}.order")

# To create the orders dataframe, a full table with all the products is needed.
product_df = read_table_to_dataframe(f"{catalog}.{schema}.product")

# Initialize counters and timer
batch_counter = 0
start_time = datetime.datetime.now()
seconds_running = (datetime.datetime.now() - start_time).total_seconds()

while (number_of_batches == 0 or batch_counter < number_of_batches) and (
    number_of_seconds == 0 or (seconds_running < number_of_seconds)
):
    # Get a list of order details
    order_detail = get_order_detail_list(
        number_of_orders_per_batch,
        last_customer_number,
        last_product_number,
        year,
        last_order_number + (number_of_orders_per_batch * batch_counter) + 1,
    )

    # Transform the list into a DataFrame
    order_detail_df = get_order_detail_dataframe(order_detail)

    # Get an orders DataFrame by combining the 'order_details' with the 'product' DataFrame
    # and then aggregating it to the 'order' level. All 'order details' will be stored in
    # a nested array with the order record.
    order_df = get_order_dataframe(order_detail_df, product_df)

    # And then write it into a UC table
    write_dataframe_to_table(order_df, f"{catalog}.{schema}.order", "Append")

    # Update counters and timers
    batch_counter += 1
    seconds_running = (datetime.datetime.now() - start_time).total_seconds()

    print(
        f"Finished batch '{batch_counter}'"
        f", running for '{seconds_running:,.0f}' seconds now."
    )

# COMMAND ----------

# Exit the notebook reporting "succes"
dbutils.notebook.exit("SUCCES")
