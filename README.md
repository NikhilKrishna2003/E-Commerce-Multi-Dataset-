# E-Commerce Multi-Dataset Lakehouse Pipeline

## Overview

This project demonstrates an end-to-end **Data Engineering pipeline** built using **PySpark and Lakehouse architecture principles**.
It processes multiple e-commerce datasets and transforms raw transactional data into **analytics-ready business tables** using the **Medallion Architecture (Bronze → Silver → Gold)** pattern.

The pipeline simulates a real-world retail data platform where messy transactional datasets are ingested, cleaned, transformed, and aggregated for downstream analytics and reporting.

The goal of this project is to practice **large-scale data transformation, ETL design, and data modeling** using PySpark and Databricks-style architecture.

---

## Architecture

The project follows the **Medallion Architecture**, a common pattern used in modern data lakehouses.

Bronze → Raw ingestion layer
Silver → Cleaned and standardized data
Gold → Business-level analytics tables

Pipeline flow:

Raw CSV datasets
↓
Bronze Layer (Raw ingestion)
↓
Silver Layer (Data cleaning and transformations)
↓
Gold Layer (Fact tables, dimension tables, business KPIs)
↓
Analytics / BI dashboards

---

## Technology Stack

* Python
* PySpark
* SQL
* Parquet / Delta format
* Lakehouse Medallion Architecture
* Data Engineering ETL pipeline design

Future improvements may include:

* Apache Airflow orchestration
* Delta Lake optimizations
* Incremental pipelines (MERGE / UPSERT)
* Streaming ingestion
* Dashboard integration (Power BI / Tableau)

---

## Dataset Description

This project uses a **multi-dataset e-commerce retail dataset**, which includes multiple relational datasets such as:

Customers
Orders
Order Items
Payments
Products
Product Category Translation
Reviews
Sellers

These datasets simulate a real retail data warehouse scenario where multiple tables must be integrated and transformed for analytics.

---

## Project Structure

```
E-Commerce-Multi-Dataset
│
├── datasets
│   ├── customers.csv
│   ├── orders.csv
│   ├── order_items.csv
│   ├── payments.csv
│   ├── products.csv
│   ├── sellers.csv
│   └── reviews.csv
│
├── bronze
│   └── bronze.py
│
├── silver
│   └── silver.py
│
├── gold
│   └── gold.py
│
└── README.md
```

---

## Bronze Layer – Raw Data Ingestion

The **Bronze layer** ingests raw datasets into the data lake without heavy transformations.

Key operations performed:

* Load raw CSV datasets using PySpark
* Preserve original raw structure
* Store datasets in a distributed format (Parquet / Delta)
* Maintain a staging layer for downstream transformations

Example operation:

```python
df = spark.read.option("header", True).csv("orders.csv")
```

Bronze layer acts as the **single source of truth for raw data**.

---

## Silver Layer – Data Cleaning and Standardization

The **Silver layer** transforms messy raw data into structured and standardized datasets.

Data engineering tasks performed include:

* Column name normalization
* Data type casting
* Duplicate removal
* Missing value handling
* Text standardization
* Timestamp conversion
* Data validation checks
* Feature engineering

Example transformation:

```python
df = df.withColumn("order_purchase_timestamp",
                   col("order_purchase_timestamp").cast("timestamp"))
```

Additional derived features include:

* order_year
* order_month
* delivery_days
* late_delivery_flag

This layer ensures **clean and reliable data for analytics**.

---

## Gold Layer – Business Analytics Tables

The **Gold layer** prepares final analytical datasets using fact and dimension modeling.

Tables generated include:

Fact Tables

fact_sales

Dimension Tables

dim_customers
dim_products
dim_sellers
dim_orders

Business KPIs generated include:

* Total revenue
* Total orders
* Average order value
* Monthly revenue trends
* Late delivery percentage
* Category revenue analysis

Example KPI query:

```sql
SELECT
order_year,
order_month,
SUM(total_price) AS monthly_revenue
FROM fact_sales
GROUP BY order_year, order_month
ORDER BY order_year, order_month
```

These tables can be directly connected to **BI tools for reporting**.

---

## Data Engineering Concepts Demonstrated

This project demonstrates the following concepts:

* Medallion Architecture
* ETL pipeline design
* PySpark transformations
* Data cleaning strategies
* Data aggregation and analytics modeling
* Fact and dimension table design
* Business KPI computation
* Data lakehouse workflow

---

## Key PySpark Transformations Used

* withColumn
* groupBy
* agg
* join
* dropDuplicates
* fillna
* cast
* filter
* SQL transformations

Example:

```python
df.groupBy("category").agg(sum("sales"))
```

---
## Dashboard Visualizations

The processed Gold Layer datasets were used to build interactive dashboards
showing delivery performance, payment trends, and customer satisfaction metrics.

### Tableau-Dashboard
![E-Commerce Tableau Dashboard](Dashboards/Tableau-dashboard/tableau.png)

### PowerBi-Dashboard
![E-Commerce PowerBi Dashboard](Dashboards/PowerBi-Dashboard/powerbi.png)

### Excel-Dashboard
![E-Commerce Excel Dashboard](Dashboards/Excel-dashboard/excel.png)

## Future Improvements

The project will be continuously improved as new concepts are learned.

Planned improvements include:

* Delta Lake integration across all layers
* Incremental pipeline processing using MERGE
* Partitioning strategies for large datasets
* Data quality validation framework
* Workflow orchestration using Apache Airflow
* Streaming ingestion using Spark Structured Streaming
* Integration with BI dashboards
* Performance optimizations (OPTIMIZE / ZORDER)

More enhancements will be added as the project evolves.

---

## Learning Goals

This project is part of my learning journey toward becoming a **Data Engineer**.
The focus is on understanding:

* Large-scale data processing
* Data pipeline architecture
* Data modeling for analytics
* PySpark ETL pipelines
* Modern data lakehouse systems

---

## Author

Nikhil Krishna

GitHub
https://github.com/NikhilKrishna2003

---

## Note

This project will continue to evolve as new data engineering concepts are explored and implemented.
