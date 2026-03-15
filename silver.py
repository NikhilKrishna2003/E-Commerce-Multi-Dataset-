# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark=SparkSession.builder.appName("Silver_Layer").getOrCreate()

# COMMAND ----------

orders = spark.read.parquet("/Volumes/workspace/default/filestore/bronze/orders_raw")
order_items = spark.read.parquet("/Volumes/workspace/default/filestore/bronze/order_items_raw")
payments = spark.read.parquet("/Volumes/workspace/default/filestore/bronze/payments_raw")
reviews = spark.read.parquet("/Volumes/workspace/default/filestore/bronze/reviews_raw")
products = spark.read.parquet("/Volumes/workspace/default/filestore/bronze/products_raw")
customers = spark.read.parquet("/Volumes/workspace/default/filestore/bronze/customers_raw")
sellers = spark.read.parquet("/Volumes/workspace/default/filestore/bronze/sellers_raw")
category_translation = spark.read.parquet("/Volumes/workspace/default/filestore/bronze/category_translation_raw")

# COMMAND ----------

def clean_column_names(df):
    new_cols = [c.strip().lower().replace(" ", "_").replace("-", "_") for c in df.columns]
    return df.toDF(*new_cols)

orders = clean_column_names(orders)
order_items = clean_column_names(order_items)
payments = clean_column_names(payments)
reviews = clean_column_names(reviews)
products = clean_column_names(products)
customers = clean_column_names(customers)
sellers = clean_column_names(sellers)
category_translation = clean_column_names(category_translation)

# COMMAND ----------

orders = orders.dropDuplicates(["order_id"])
order_items = order_items.dropDuplicates()
payments = payments.dropDuplicates()
reviews = reviews.dropDuplicates(["review_id"])
products = products.dropDuplicates(["product_id"])
customers = customers.dropDuplicates(["customer_id"])
sellers = sellers.dropDuplicates(["seller_id"])

# COMMAND ----------

orders = clean_column_names(orders)

orders = orders.dropDuplicates(["order_id"])

orders = orders.withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"))) \
               .withColumn("order_approved_at", to_timestamp(col("order_approved_at"))) \
               .withColumn("order_delivered_carrier_date", to_timestamp(col("order_delivered_carrier_date"))) \
               .withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date"))) \
               .withColumn("order_estimated_delivery_date", to_timestamp(col("order_estimated_delivery_date")))

orders = orders.withColumn("order_status", trim(lower(col("order_status"))))

orders = orders.withColumn("order_year", year(col("order_purchase_timestamp"))) \
               .withColumn("order_month", month(col("order_purchase_timestamp"))) \
               .withColumn("order_day", dayofmonth(col("order_purchase_timestamp"))) \
               .withColumn("delivery_days", datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))) \
               .withColumn("estimated_delivery_days", datediff(col("order_estimated_delivery_date"), col("order_purchase_timestamp"))) \
               .withColumn(
                   "late_delivery_flag",
                   when(col("order_delivered_customer_date") > col("order_estimated_delivery_date"), 1).otherwise(0)
               )

# COMMAND ----------

orders.filter(col("delivery_days") < 0).show()
orders.printSchema()

# COMMAND ----------

order_items = clean_column_names(order_items)

order_items = order_items.dropDuplicates()

order_items = order_items.withColumn("shipping_limit_date", to_timestamp(col("shipping_limit_date"))) \
                         .withColumn("order_item_id", col("order_item_id").cast("int")) \
                         .withColumn("price", col("price").cast("double")) \
                         .withColumn("freight_value", col("freight_value").cast("double"))

order_items = order_items.withColumn(
    "total_price",
    col("price") + col("freight_value")
)

# COMMAND ----------

order_items.filter(
    (col("price") < 0) | (col("freight_value") < 0)
).show()

order_items.printSchema()

# COMMAND ----------

payments = clean_column_names(payments)

payments = payments.dropDuplicates()

payments = payments.withColumn("payment_sequential", col("payment_sequential").cast("int")) \
                   .withColumn("payment_installments", col("payment_installments").cast("int")) \
                   .withColumn("payment_value", col("payment_value").cast("double"))

payments = payments.withColumn("payment_type", trim(lower(col("payment_type"))))

payments = payments.fillna({"payment_installments": 1})

# COMMAND ----------

payments.filter(col("payment_value") < 0).show()
payments.printSchema()

# COMMAND ----------

reviews = clean_column_names(reviews)

reviews = reviews.dropDuplicates(["review_id"])

# COMMAND ----------

reviews.printSchema()
reviews.show(5, False)

# COMMAND ----------

reviews = reviews.withColumn("review_score", col("review_score").cast("int")) \
                 .withColumn("review_creation_date", to_timestamp(col("review_creation_date"))) \
                 .withColumn("review_answer_timestamp", to_timestamp(col("review_answer_timestamp")))

# COMMAND ----------

reviews = spark.read.parquet("/Volumes/workspace/default/filestore/bronze/reviews_raw")

reviews = clean_column_names(reviews)

reviews = reviews.dropDuplicates(["review_id"])

reviews = reviews.withColumn(
    "review_score",
    expr("try_cast(review_score as int)")
)

reviews = reviews.withColumn(
    "review_creation_date",
    expr("try_cast(review_creation_date as timestamp)")
)

reviews = reviews.withColumn(
    "review_answer_timestamp",
    expr("try_cast(review_answer_timestamp as timestamp)")
)

# COMMAND ----------

reviews.filter(
    col("review_creation_date").isNull()
).select(
    "review_id",
    "review_comment_title",
    "review_comment_message"
).show(20, False)

# COMMAND ----------

reviews.filter(
    col("review_score").isNotNull() & ((col("review_score") < 1) | (col("review_score") > 5))
).show()

reviews.printSchema()

# COMMAND ----------

products = clean_column_names(products)

products = products.dropDuplicates(["product_id"])

products = products.withColumn("product_name_lenght", col("product_name_lenght").cast("int")) \
                   .withColumn("product_description_lenght", col("product_description_lenght").cast("int")) \
                   .withColumn("product_photos_qty", col("product_photos_qty").cast("int")) \
                   .withColumn("product_weight_g", col("product_weight_g").cast("double")) \
                   .withColumn("product_length_cm", col("product_length_cm").cast("double")) \
                   .withColumn("product_height_cm", col("product_height_cm").cast("double")) \
                   .withColumn("product_width_cm", col("product_width_cm").cast("double"))

products = products.fillna({"product_category_name": "unknown"})

# COMMAND ----------

products.printSchema()
products.show(5, False)

# COMMAND ----------

category_translation = clean_column_names(category_translation)

category_translation = category_translation.dropDuplicates(["product_category_name"])

# COMMAND ----------

category_translation.printSchema()
category_translation.show(5, False)

# COMMAND ----------

products = products.join(
    category_translation,
    on="product_category_name",
    how="left"
)

products = products.fillna({"product_category_name_english": "unknown"})

# COMMAND ----------

products.select(
    "product_id",
    "product_category_name",
    "product_category_name_english"
).show(10, False)

# COMMAND ----------

customers = clean_column_names(customers)

customers = customers.dropDuplicates(["customer_id"])

customers = customers.withColumn("customer_city", trim(lower(col("customer_city")))) \
                     .withColumn("customer_state", upper(col("customer_state")))

# COMMAND ----------

customers.printSchema()
customers.show(5, False)

# COMMAND ----------

sellers = clean_column_names(sellers)

sellers = sellers.dropDuplicates(["seller_id"])

sellers = sellers.withColumn("seller_city", trim(lower(col("seller_city")))) \
                 .withColumn("seller_state", upper(col("seller_state")))

# COMMAND ----------

sellers.printSchema()
sellers.show(5, False)

# COMMAND ----------

products = spark.read.parquet("/Volumes/workspace/default/filestore/bronze/products_raw")
category_translation = spark.read.parquet("/Volumes/workspace/default/filestore/bronze/category_translation_raw")

products = clean_column_names(products)
category_translation = clean_column_names(category_translation)

products = products.dropDuplicates(["product_id"])
category_translation = category_translation.dropDuplicates(["product_category_name"])

products = products.withColumn("product_name_lenght", col("product_name_lenght").cast("int")) \
                   .withColumn("product_description_lenght", col("product_description_lenght").cast("int")) \
                   .withColumn("product_photos_qty", col("product_photos_qty").cast("int")) \
                   .withColumn("product_weight_g", col("product_weight_g").cast("double")) \
                   .withColumn("product_length_cm", col("product_length_cm").cast("double")) \
                   .withColumn("product_height_cm", col("product_height_cm").cast("double")) \
                   .withColumn("product_width_cm", col("product_width_cm").cast("double")) \
                   .fillna({"product_category_name": "unknown"})

# COMMAND ----------

products = products.join(
    category_translation.select("product_category_name", "product_category_name_english"),
    on="product_category_name",
    how="left"
)

products = products.fillna({"product_category_name_english": "unknown"})

# COMMAND ----------

reviews = spark.read.parquet("/Volumes/workspace/default/filestore/bronze/reviews_raw")
reviews = clean_column_names(reviews)
reviews = reviews.dropDuplicates(["review_id"])

# COMMAND ----------

orders.write.mode("overwrite").parquet("/Volumes/workspace/default/filestore/silver/orders_clean")
order_items.write.mode("overwrite").parquet("/Volumes/workspace/default/filestore/silver/order_items_clean")
payments.write.mode("overwrite").parquet("/Volumes/workspace/default/filestore/silver/payments_clean")
reviews.write.mode("overwrite").parquet("/Volumes/workspace/default/filestore/silver/reviews_clean")
products.write.mode("overwrite").parquet("/Volumes/workspace/default/filestore/silver/products_clean")
customers.write.mode("overwrite").parquet("/Volumes/workspace/default/filestore/silver/customers_clean")
sellers.write.mode("overwrite").parquet("/Volumes/workspace/default/filestore/silver/sellers_clean")
category_translation.write.mode("overwrite").parquet("/Volumes/workspace/default/filestore/silver/category_translation_clean")