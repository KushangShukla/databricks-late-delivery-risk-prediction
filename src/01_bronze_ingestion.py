# Databricks notebook source
# DBTITLE 1,Notebook Header (Markdown)
# 01_bronze_ingestion

**Purpose:**  
Generate realistic historical shipment data and store it as a Bronze Delta table.

**Layer:** Bronze  
**Output Table:** logistics_bronze.shipments_raw

# COMMAND ----------

# DBTITLE 1,Imports & Setup (Python)
from datetime import datetime, timedelta
import random

# COMMAND ----------

# DBTITLE 1,Imports & Setup (Python)
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS logistics_bronze;

# COMMAND ----------

# DBTITLE 1,Master Reference Data (Python)
cities = [
    ("Mumbai", "Maharashtra"),
    ("Delhi", "Delhi"),
    ("Bengaluru", "Karnataka"),
    ("Chennai", "Tamil Nadu"),
    ("Hyderabad", "Telangana"),
    ("Pune", "Maharashtra"),
    ("Ahmedabad", "Gujarat"),
    ("Jaipur", "Rajasthan"),
    ("Kolkata", "West Bengal"),
    ("Indore", "Madhya Pradesh")
]

carriers = [
    "BlueDart",
    "Delhivery",
    "EcomExpress",
    "DTDC",
    "XpressBees"
]

shipping_modes = ["Road", "Air"]

# COMMAND ----------

# DBTITLE 1,Date Logic (REALISTIC DELIVERY BEHAVIOR)
def generate_dates():
    order_date = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 364))

    expected_days = random.choice([2, 3, 4, 5, 6])
    expected_delivery = order_date + timedelta(days=expected_days)

    delay = random.choices(
        population=[-1, 0, 1, 2, 3],
        weights=[10, 55, 20, 10, 5]
    )[0]

    actual_delivery = expected_delivery + timedelta(days=delay)

    return (
        order_date.date(),
        expected_delivery.date(),
        actual_delivery.date()
    )

# COMMAND ----------

# DBTITLE 1,Shipment Record Generator
def generate_shipment(shipment_id):
    origin_city, origin_state = random.choice(cities)
    destination_city, destination_state = random.choice(cities)

    while destination_city == origin_city:
        destination_city, destination_state = random.choice(cities)

    order_date, expected_date, actual_date = generate_dates()

    distance_km = random.randint(100, 2200)
    weight = round(random.uniform(1, 50), 2)

    carrier = random.choice(carriers)
    shipping_mode = random.choices(
        shipping_modes,
        weights=[70, 30]
    )[0]

    return (
        f"SHP{shipment_id:06d}",
        order_date,
        expected_date,
        actual_date,
        origin_city,
        destination_city,
        origin_state,
        destination_state,
        distance_km,
        carrier,
        shipping_mode,
        weight,
        "Delivered"
    )

# COMMAND ----------

# DBTITLE 1,Generate Dataset (10,000 Rows)
data = [generate_shipment(i) for i in range(1, 10001)]

# COMMAND ----------

# DBTITLE 1,Create Spark DataFrame
columns = [
    "shipment_id",
    "order_date",
    "expected_delivery_date",
    "actual_delivery_date",
    "origin_city",
    "destination_city",
    "origin_state",
    "destination_state",
    "distance_km",
    "carrier",
    "shipping_mode",
    "shipment_weight_kg",
    "delivery_status"
]

shipments_df = spark.createDataFrame(data, columns)

# COMMAND ----------

# DBTITLE 1,Write Bronze Delta Table
shipments_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("logistics_bronze.shipments_raw")

# COMMAND ----------

# DBTITLE 1,Validation (Sanity Checks)
from pyspark.sql.functions import col

# COMMAND ----------

# DBTITLE 1,Row count
spark.sql("""
SELECT COUNT(*) AS total_shipments
FROM logistics_bronze.shipments_raw
""").show()

# COMMAND ----------

# DBTITLE 1,Late vs On-Time distribution
spark.sql("""
SELECT
  actual_delivery_date > expected_delivery_date AS is_late,
  COUNT(*) AS count
FROM logistics_bronze.shipments_raw
GROUP BY is_late
""").show()

# COMMAND ----------

