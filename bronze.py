# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Olist_Ecommerce_Project").getOrCreate()

print("Spark Session Created")

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/workspace/default/filestore"))

# COMMAND ----------

orders = spark.read.option("header", True).option("inferSchema", True).csv(
"/Volumes/workspace/default/filestore/olist_orders_dataset.csv"
)

order_items = spark.read.option("header", True).option("inferSchema", True).csv(
"/Volumes/workspace/default/filestore/olist_order_items_dataset.csv"
)

payments = spark.read.option("header", True).option("inferSchema", True).csv(
"/Volumes/workspace/default/filestore/olist_order_payments_dataset.csv"
)

reviews = spark.read.option("header", True).option("inferSchema", True).csv(
"/Volumes/workspace/default/filestore/olist_order_reviews_dataset.csv"
)

products = spark.read.option("header", True).option("inferSchema", True).csv(
"/Volumes/workspace/default/filestore/olist_products_dataset.csv"
)

customers = spark.read.option("header", True).option("inferSchema", True).csv(
"/Volumes/workspace/default/filestore/olist_customers_dataset.csv"
)

sellers = spark.read.option("header", True).option("inferSchema", True).csv(
"/Volumes/workspace/default/filestore/olist_sellers_dataset.csv"
)

category_translation = spark.read.option("header", True).option("inferSchema", True).csv(
"/Volumes/workspace/default/filestore/product_category_name_translation.csv"
)

# COMMAND ----------

orders.write.mode("overwrite").parquet(
"/Volumes/workspace/default/filestore/bronze/orders_raw"
)

customers.write.mode("overwrite").parquet(
"/Volumes/workspace/default/filestore/bronze/customers_raw"
)

products.write.mode("overwrite").parquet(
"/Volumes/workspace/default/filestore/bronze/products_raw"
)

order_items.write.mode("overwrite").parquet(
"/Volumes/workspace/default/filestore/bronze/order_items_raw"
)

payments.write.mode("overwrite").parquet(
"/Volumes/workspace/default/filestore/bronze/payments_raw"
)

reviews.write.mode("overwrite").parquet(
"/Volumes/workspace/default/filestore/bronze/reviews_raw"
)

sellers.write.mode("overwrite").parquet(
"/Volumes/workspace/default/filestore/bronze/sellers_raw"
)

category_translation.write.mode("overwrite").parquet(
"/Volumes/workspace/default/filestore/bronze/category_translation_raw"
)

# COMMAND ----------

