# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark=SparkSession.builder.appName("Gold_Layer").getOrCreate()

# COMMAND ----------

orders = spark.read.parquet("/Volumes/workspace/default/filestore/silver/orders_clean")
order_items = spark.read.parquet("/Volumes/workspace/default/filestore/silver/order_items_clean")
payments = spark.read.parquet("/Volumes/workspace/default/filestore/silver/payments_clean")
reviews = spark.read.parquet("/Volumes/workspace/default/filestore/silver/reviews_clean")
products = spark.read.parquet("/Volumes/workspace/default/filestore/silver/products_clean")
customers = spark.read.parquet("/Volumes/workspace/default/filestore/silver/customers_clean/")
sellers = spark.read.parquet("/Volumes/workspace/default/filestore/silver/sellers_clean")
category_translation = spark.read.parquet("/Volumes/workspace/default/filestore/silver/category_translation_clean")

# COMMAND ----------

print("orders:", orders.count())
print("order_items:", order_items.count())
print("payments:", payments.count())
print("reviews:", reviews.count())
print("products:", products.count())
print("customers:", customers.count())
print("sellers:", sellers.count())

# COMMAND ----------

payment_summary = payments.groupBy("order_id").agg(
    sum("payment_value").alias("total_payment_value"),
    max("payment_type").alias("payment_type"),
    max("payment_installments").alias("payment_installments")
)

# COMMAND ----------

review_summary = reviews.groupBy("order_id").agg(
    avg("review_score").alias("avg_review_score"),
    max("review_creation_date").alias("last_review_creation_date")
)

# COMMAND ----------

fact_sales = order_items.alias("oi") \
    .join(orders.alias("o"), col("oi.order_id") == col("o.order_id"), "left") \
    .join(products.alias("p"), col("oi.product_id") == col("p.product_id"), "left") \
    .join(customers.alias("c"), col("o.customer_id") == col("c.customer_id"), "left") \
    .join(sellers.alias("s"), col("oi.seller_id") == col("s.seller_id"), "left") \
    .join(payment_summary.alias("ps"), col("oi.order_id") == col("ps.order_id"), "left") \
    .join(review_summary.alias("rs"), col("oi.order_id") == col("rs.order_id"), "left")

# COMMAND ----------

fact_sales = fact_sales.select(
    col("oi.order_id").alias("order_id"),
    col("oi.order_item_id").alias("order_item_id"),
    col("oi.product_id").alias("product_id"),
    col("oi.seller_id").alias("seller_id"),
    col("o.customer_id").alias("customer_id"),

    col("o.order_status").alias("order_status"),
    col("o.order_purchase_timestamp").alias("order_purchase_timestamp"),
    col("o.order_year").alias("order_year"),
    col("o.order_month").alias("order_month"),
    col("o.order_day").alias("order_day"),
    col("o.delivery_days").alias("delivery_days"),
    col("o.estimated_delivery_days").alias("estimated_delivery_days"),
    col("o.late_delivery_flag").alias("late_delivery_flag"),

    col("oi.price").alias("price"),
    col("oi.freight_value").alias("freight_value"),
    col("oi.total_price").alias("total_price"),

    col("p.product_category_name").alias("product_category_name"),
    col("p.product_category_name_english").alias("product_category_name_english"),

    col("c.customer_city").alias("customer_city"),
    col("c.customer_state").alias("customer_state"),

    col("s.seller_city").alias("seller_city"),
    col("s.seller_state").alias("seller_state"),

    col("ps.total_payment_value").alias("total_payment_value"),
    col("ps.payment_type").alias("payment_type"),
    col("ps.payment_installments").alias("payment_installments"),

    col("rs.avg_review_score").alias("avg_review_score"),
    col("rs.last_review_creation_date").alias("last_review_creation_date")
)

# COMMAND ----------

fact_sales.printSchema()
fact_sales.show(5)

# COMMAND ----------

dim_customers = customers.select(
    "customer_id",
    "customer_unique_id",
    "customer_city",
    "customer_state"
).dropDuplicates()

# COMMAND ----------

dim_products = products.select(
    "product_id",
    "product_category_name",
    "product_category_name_english",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm"
).dropDuplicates()

# COMMAND ----------

dim_sellers = sellers.select(
    "seller_id",
    "seller_city",
    "seller_state"
).dropDuplicates()

# COMMAND ----------

dim_orders = orders.select(
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_year",
    "order_month",
    "order_day",
    "delivery_days",
    "estimated_delivery_days",
    "late_delivery_flag"
).dropDuplicates()

# COMMAND ----------

fact_sales.write.mode("overwrite").parquet("/Volumes/workspace/default/filestore/gold/fact_sales")
dim_customers.write.mode("overwrite").parquet("/Volumes/workspace/default/filestore/gold/dim_customers")
dim_products.write.mode("overwrite").parquet("/Volumes/workspace/default/filestore/gold/dim_products")
dim_sellers.write.mode("overwrite").parquet("/Volumes/workspace/default/filestore/gold/dim_sellers")
dim_orders.write.mode("overwrite").parquet("/Volumes/workspace/default/filestore/gold/dim_orders")

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/workspace/default/filestore/gold"))

# COMMAND ----------

fact_sales.createOrReplaceTempView("fact_sales")
dim_customers.createOrReplaceTempView("dim_customers")
dim_products.createOrReplaceTempView("dim_products")
dim_sellers.createOrReplaceTempView("dim_sellers")
dim_orders.createOrReplaceTempView("dim_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI with SQL

# COMMAND ----------

# MAGIC %md
# MAGIC Total revenue

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ROUND(SUM(total_price), 2) AS total_revenue
# MAGIC FROM fact_sales

# COMMAND ----------

# MAGIC %md
# MAGIC Total orders

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT order_id) AS total_orders
# MAGIC FROM fact_sales

# COMMAND ----------

# MAGIC %md
# MAGIC Average order value

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ROUND(SUM(total_price) / COUNT(DISTINCT order_id), 2) AS avg_order_value
# MAGIC FROM fact_sales

# COMMAND ----------

# MAGIC %md
# MAGIC Monthly revenue

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_year,
# MAGIC        order_month,
# MAGIC        ROUND(SUM(total_price), 2) AS monthly_revenue
# MAGIC FROM fact_sales
# MAGIC GROUP BY order_year, order_month
# MAGIC ORDER BY order_year, order_month

# COMMAND ----------

# MAGIC %md
# MAGIC Top 10 categories by revenue

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT seller_id,
# MAGIC        ROUND(SUM(total_price), 2) AS revenue
# MAGIC FROM fact_sales
# MAGIC GROUP BY seller_id
# MAGIC ORDER BY revenue DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC Top 10 categories by revenue

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_state,
# MAGIC        ROUND(SUM(total_price), 2) AS revenue
# MAGIC FROM fact_sales
# MAGIC GROUP BY customer_state
# MAGIC ORDER BY revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC Top 10 sellers by revenue

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_state,
# MAGIC        ROUND(SUM(total_price), 2) AS revenue
# MAGIC FROM fact_sales
# MAGIC GROUP BY customer_state
# MAGIC ORDER BY revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC Late delivery percentage

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ROUND(100.0 * SUM(late_delivery_flag) / COUNT(*), 2) AS late_delivery_pct
# MAGIC FROM fact_sales

# COMMAND ----------

# MAGIC %md
# MAGIC Average delivery days

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ROUND(AVG(delivery_days), 2) AS avg_delivery_days
# MAGIC FROM fact_sales
# MAGIC WHERE delivery_days IS NOT NULL

# COMMAND ----------

Average review score by category

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT product_category_name_english,
# MAGIC        ROUND(AVG(avg_review_score), 2) AS avg_review_score
# MAGIC FROM fact_sales
# MAGIC GROUP BY product_category_name_english
# MAGIC ORDER BY avg_review_score DESC
# MAGIC LIMIT 10

# COMMAND ----------

fact_sales.write.mode("overwrite").format("delta").save("/Volumes/workspace/default/filestore/gold_delta/fact_sales")
dim_customers.write.mode("overwrite").format("delta").save("/Volumes/workspace/default/filestore/gold_delta/dim_customers")
dim_products.write.mode("overwrite").format("delta").save("/Volumes/workspace/default/filestore/gold_delta/dim_products")
dim_sellers.write.mode("overwrite").format("delta").save("/Volumes/workspace/default/filestore/gold_delta/dim_sellers")
dim_orders.write.mode("overwrite").format("delta").save("/Volumes/workspace/default/filestore/gold_delta/dim_orders")

# COMMAND ----------

fact_sales.coalesce(1).write.mode("overwrite").option("header",True).csv(
"/Volumes/workspace/default/filestore/output/fact_sales_csv"
)

# COMMAND ----------

dim_customers = spark.read.format("delta").load(
"/Volumes/workspace/default/filestore/gold_delta/dim_customers"
)

dim_customers.coalesce(1).write.mode("overwrite").option("header",True).csv(
"/Volumes/workspace/default/filestore/output/dim_customers_csv"
)

# COMMAND ----------

dim_products = spark.read.format("delta").load(
"/Volumes/workspace/default/filestore/gold_delta/dim_products"
)

dim_products.coalesce(1).write.mode("overwrite").option("header",True).csv(
"/Volumes/workspace/default/filestore/output/dim_products_csv"
)

# COMMAND ----------

dim_sellers = spark.read.format("delta").load(
"/Volumes/workspace/default/filestore/gold_delta/dim_sellers"
)

dim_sellers.coalesce(1).write.mode("overwrite").option("header",True).csv(
"/Volumes/workspace/default/filestore/output/dim_sellers_csv"
)

# COMMAND ----------

dim_orders = spark.read.format("delta").load(
"/Volumes/workspace/default/filestore/gold_delta/dim_orders"
)

dim_orders.coalesce(1).write.mode("overwrite").option("header",True).csv(
"/Volumes/workspace/default/filestore/output/dim_orders_csv"
)

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/workspace/default/filestore/output"))
