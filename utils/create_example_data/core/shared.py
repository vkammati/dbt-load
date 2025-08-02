# Databricks notebook source
# MAGIC %md Installing packages and importing libraries...
# MAGIC

# COMMAND ----------

# Before doing anything else, first install the 'Faker' library to be able to generate some nice demo data and restart Python
%pip install Faker
dbutils.library.restartPython()

# COMMAND ----------

# Import all needed packages
import datetime
from decimal import Decimal

from faker import Faker
from faker.providers import DynamicProvider
from pyspark.sql import DataFrame

# COMMAND ----------

# MAGIC %md Setup 'faker' configuration to generate random data...

# COMMAND ----------

# Create a 'faker' object that will generate English, Indian and Dutch names and addresses.
Faker.seed(0)
faker = Faker(['en_US', 'en_IN', 'nl_NL'])

# Additionally, create another faker than is used for custom random generation of data
custom_faker = Faker()

# This will provider can be used to generate random payment methods
example_data_provider = DynamicProvider(
     provider_name="payment_method",
     elements=["Credit Card", "Debit Card", "Cash", "Other"],
)
custom_faker.add_provider(example_data_provider)

# COMMAND ----------

# MAGIC %md
# MAGIC Enabling Change Data Feed...

# COMMAND ----------

# Enable ChangeDataFeed (CDF) to support out-of-the-box incremental loads with DBT
spark.sql("set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;")

# COMMAND ----------

# MAGIC %md Importing 'generic' functions...

# COMMAND ----------

# Global variables
_first_customer_number = 1000001
_first_product_category_number = 1000001
_first_product_number = 1000001
_first_order_number = 1000001

# COMMAND ----------

# Generic functions
def write_dataframe_to_table(df: DataFrame, table: str, mode: str):
    df.write.mode(mode).option("mergeSchema", "true").saveAsTable(table)


def read_table_to_dataframe(table: str) -> DataFrame:
    return spark.table(table)


def get_last_business_key(business_key: str, table: str, leading_characters_to_strip: int = 1) -> int:
    # Get the max by stripping the leading character(s) first and then casting it to an int
    df = spark.sql(
        f"select max(cast(substring({business_key}, {leading_characters_to_strip+1}) as int)) as max_business_key"
        f" from {table}"
        )
    if df.count() == 0:
        return 0

    # Collect the value from the DataFrame
    max_business_key = df.collect()[0][0]

    # Using the max aggregate function can yield a row with a NULL value or an empty table
    return max_business_key if max_business_key else 0

# COMMAND ----------

# MAGIC %md Importing 'customer' functions...

# COMMAND ----------

# Generate a list of random customers.
def get_customer_list(nr_of_rows: int, year: int, start_customer_number = _first_customer_number) -> []:
    customer_list = []
    for c in range(nr_of_rows):
        customer = faker.simple_profile()
        customer_list.append(
            (
                f'C{start_customer_number+c}',
                customer['name'],
                customer['address'],
                faker.current_country_code(),
                customer['mail'],
                customer['sex'],
                customer['birthdate'],
                datetime.datetime(year, 1, 1) #for simplicity, they all start at the same date.
            )
        )

    return customer_list


def get_mutations_to_customer_list(customers: [], year: int, percent_of_rows_to_change: int = 20) -> []:
    # Add customers to list who attributes have changed. This make a good example for a SCD. 20% of the customer
    # will get a changed address

    customer_count = len(customers)
    nr_of_rows_to_add = int(customer_count * (percent_of_rows_to_change/100))

    new_customers = []
    for _ in range(nr_of_rows_to_add):
        random_customer = customers[faker.pyint(max_value=customer_count-1)]
        new_customers.append(
            (
                random_customer[0],
                random_customer[1],
                faker.address(), #only the address and country code will change
                faker.current_country_code(),
                random_customer[4],
                random_customer[5],
                random_customer[6],
                faker.date_time_between(start_date=random_customer[7], end_date=datetime.date(year, 12, 31))
            )
        )

    # Return the created list
    return new_customers


# Create a DataFrame out of the customer list and return it
def get_customer_dataframe(customers: []) -> DataFrame:
    df = spark.createDataFrame(customers, schema=["customer_nr", "name", "address", "country", "email", "gender", "birthdate", "modified_at"])
    df = df.sort("customer_nr", "modified_at")

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC %md Importing 'Product category' functions...

# COMMAND ----------

# Generate a list of random product categories.
def get_product_category_list(nr_of_rows: int, year: int, product_categories_to_exclude = [], start_product_category_number: int = _first_product_category_number) -> []:
    product_category_list = []
    for p in range(nr_of_rows):
        if p == 0 and start_product_category_number == _first_product_category_number:
            # top level category
            parent  = None
        else:
            possible_categories = [f'A{cat}' for cat in range(_first_product_category_number, start_product_category_number+p, 1)]
            possible_categories = list(set(possible_categories) - set(product_categories_to_exclude))
            parent = possible_categories[faker.pyint(max_value=len(possible_categories)-1)]

        product_category_list.append(
            (
                f'A{start_product_category_number+p}',
                ' '.join(faker.words(2, unique=True)),
                parent,
                datetime.datetime(year, 1, 1) #for simplicity, they all start at the same date.
            )
        )

    return product_category_list


# Create a DataFrame out of the product category list and return it
def get_product_category_dataframe(product_categories: []) -> DataFrame:
    df = spark.createDataFrame(product_categories, schema=["product_category_nr", "name", "parent_nr", "modified_at"])
    df = df.sort("product_category_nr")

    return df


# Get a list of leaf level product categories
def get_leaf_level_product_categories(product_categories: DataFrame) -> list[str]:
    # Create a dataframe with all the categories that are in the parent_nr column.
    df_parents = product_categories.select("parent_nr").distinct()

    # Create a dataframe with all categories that are NOT in the parent dataframe.
    # These are, by definition, leaf-level categories
    df = product_categories.join(
        df_parents,
        product_categories.product_category_nr == df_parents.parent_nr,
        'leftanti'
    )

    # Transform the dataframe into a list of strings
    return [data[0] for data in df.select("product_category_nr").collect()]


# COMMAND ----------

# MAGIC %md Importing 'Product' functions...

# COMMAND ----------

# Generate a list of random products.
def get_product_list(nr_of_rows: int, year: int, product_categories: list[str], start_product_number: int = _first_product_number) -> []:
    product_list = []
    for p in range(nr_of_rows):
        price = faker.pyint(min_value=10, max_value=400, step=5)
        product_list.append(
            (
                f'P{start_product_number+p}',
                faker.catch_phrase(),
                faker.color_name(),
                product_categories[faker.pyint(max_value=len(product_categories)-1)],
                Decimal("%.2f" % (price*1.21)),
                Decimal(price),
                datetime.datetime(year, 1, 1) #for simplicity, they all start at the same date.
            )
        )

    return product_list


# Add products to list whoms attributes have changed. This make a good example for a SCD. By
# default 20% of the products will get a changed color
def get_mutations_to_product_list(products: [], year: int, percent_of_rows_to_change: int = 20) -> []:
    product_count = len(products)
    nr_of_rows_to_add = int(product_count * (percent_of_rows_to_change/100))

    new_products = []
    for _ in range(nr_of_rows_to_add):
        random_product = products[faker.pyint(max_value=product_count-1)]
        new_price = int(random_product[4] * (1+Decimal(faker.pyfloat(left_digits=0, right_digits=2, min_value=-0.1, max_value=0.1))))
        new_products.append(
            (
                random_product[0],
                random_product[1],
                faker.color_name(), # new color
                random_product[3],
                Decimal("%.2f" % (new_price*1.21)), #new price with vat
                Decimal(new_price), #new price without vat
                faker.date_time_between(start_date=random_product[6], end_date=datetime.date(year, 11, 30))
            )
        )

    return new_products


# Create a DataFrame out of the product list and return it
def get_product_dataframe(products: []) -> DataFrame:
    df = spark.createDataFrame(products, schema=["product_nr", "name", "color", "product_category_nr", "price", "priceWithoutVAT", "modified_at"])
    df = df.sort("product_nr", "modified_at")

    return df

# COMMAND ----------

# MAGIC %md Importing 'Order' functions...

# COMMAND ----------

# Generate a list of random order numbers.
def get_order_detail_list(nr_of_rows: int, last_customer_number: int, last_product_number: int, year: int, order_start_number: int = _first_order_number ) -> []:

    order_detail_list = []
    for o in range(nr_of_rows):
        ordered_at = faker.date_time_between(start_date=datetime.date(year, 1, 1), end_date=datetime.date(year, 12, 31))
        customer = f'C{faker.pyint(min_value=_first_customer_number, max_value=last_customer_number)}'
        payment_method = custom_faker.payment_method()
        for d in range(faker.pyint(min_value=1, max_value=5)):
            order_detail_list.append(
                (
                    f'O{order_start_number+o}',
                    ordered_at,
                    customer,
                    payment_method,
                    d+1,
                    f'P{faker.pyint(min_value=_first_product_number, max_value=last_product_number)}',
                    faker.pyint(min_value=1, max_value=5),
                )
            )

    return order_detail_list

# Create a DataFrame out of the product list and return it
def get_order_detail_dataframe(orders: []) -> DataFrame:
    df = spark.createDataFrame(data=orders,schema=["order_nr", "ordered_at", "customer_number", "payment_method", "order_detail_nr", "product_nr", "amount"])

    return df


# Combine order details df with the products df. The order_details Contains all the 'order level'
# attributes as well as a product code. In this function we will:
# - add all the product details information like price and discount
# - group by 'order level' columns to get 1 row per order
# - aggregate all order details (at a product level) into a nested array structure
def get_order_dataframe(order_details: DataFrame, products: DataFrame) -> DataFrame:
    from pyspark.sql.functions import collect_list, count, lead, struct, sum
    from pyspark.sql.window import Window

    # First we chnage the products dataframe in such a way that each product will have a 'valid from' and 'valid to' timestamp.
    # This will make it eaier to make the join on orderdetails.
    products = products \
            .withColumnRenamed("modified_at","valid_from") \
            .withColumn("valid_to", lead("valid_from", default=datetime.datetime(9999,12,31)).over(Window.partitionBy("product_nr").orderBy("valid_from")))


    df = order_details \
        .join(products, [order_details.product_nr == products.product_nr, order_details.ordered_at >= products.valid_from, order_details.ordered_at < products.valid_to], 'inner') \
        .drop(products.product_nr) \
        .withColumn("orderTotalPayableAmount", products.price * order_details.amount) \
        .withColumn("orderTotalPayableAmountWithoutVAT", products.priceWithoutVAT * order_details.amount) \
        .groupBy("order_nr", "ordered_at", "customer_number", "payment_method") \
        .agg(
            count("product_nr").alias("OrderDistinctProductCount"),
            sum("amount").alias("OrderTotalAmount"),
            sum("orderTotalPayableAmount").alias("OrderTotalPayableAmount"),
            sum("orderTotalPayableAmountWithoutVAT").alias("OrderTotalPayableAmountWithoutVAT"),
            collect_list(struct(
                order_details.order_detail_nr,
                order_details.product_nr,
                order_details.amount,
                products.price,
                products.priceWithoutVAT
            )).alias("order_details"),
        )

    return df
