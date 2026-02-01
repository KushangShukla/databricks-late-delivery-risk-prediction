# Databricks notebook source
# DBTITLE 1,Notebook Header (Markdown)
# 02_silver_enrichment

**Purpose:**  
Enrich raw shipment data with operational intelligence (weather, traffic, carrier reliability)  
and generate feature-ready Silver tables.

**Layer:** Silver  
**Input Table:** logistics_bronze.shipments_raw  
**Output Table:** logistics_silver.shipment_features

# COMMAND ----------

# DBTITLE 1,Create Silver Schema (SQL)
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS logistics_silver;

# COMMAND ----------

# DBTITLE 1,Imports (Python)
import random
from pyspark.sql.functions import col, month, dayofweek

# COMMAND ----------

# DBTITLE 1,Load Bronze Data
shipments_df = spark.table("logistics_bronze.shipments_raw")

# COMMAND ----------

# DBTITLE 1,WEATHER RISK TABLE (Synthetic but Realistic)
cities = [
    "Mumbai","Delhi","Bengaluru","Chennai","Hyderabad",
    "Pune","Ahmedabad","Jaipur","Kolkata","Indore"
]

weather_data = [
    (
        city,
        round(random.uniform(0.0, 1.0), 2),
        random.choice(["Clear", "Rain", "Storm", "Fog"])
    )
    for city in cities
]

weather_df = spark.createDataFrame(
    weather_data,
    ["city", "weather_severity_score", "dominant_weather_type"]
)

weather_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("logistics_silver.weather_risk")

# COMMAND ----------

# DBTITLE 1,TRAFFIC RISK TABLE (Route-Level)
traffic_data = []

for origin in cities:
    for destination in cities:
        if origin != destination:
            traffic_data.append((
                origin,
                destination,
                round(random.uniform(0.2, 1.0), 2)
            ))

traffic_df = spark.createDataFrame(
    traffic_data,
    ["origin_city", "destination_city", "avg_traffic_index"]
)

traffic_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("logistics_silver.traffic_risk")

# COMMAND ----------

# DBTITLE 1,CARRIER RELIABILITY TABLE
carriers = ["BlueDart", "Delhivery", "EcomExpress", "DTDC", "XpressBees"]

carrier_data = [
    (
        carrier,
        round(random.uniform(0.05, 0.35), 2),
        round(random.uniform(0.5, 3.5), 2)
    )
    for carrier in carriers
]

carrier_df = spark.createDataFrame(
    carrier_data,
    ["carrier", "historical_delay_rate", "avg_delay_days"]
)

carrier_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("logistics_silver.carrier_reliability")

# COMMAND ----------

# DBTITLE 1,Load Enrichment Tables
weather_df = spark.table("logistics_silver.weather_risk")
traffic_df = spark.table("logistics_silver.traffic_risk")
carrier_df = spark.table("logistics_silver.carrier_reliability")

# COMMAND ----------

# DBTITLE 1,JOIN & FEATURE ENGINEERING (CORE SILVER LOGIC)
silver_df = (
    shipments_df
    .join(weather_df, shipments_df.origin_city == weather_df.city, "left")
    .join(traffic_df, ["origin_city", "destination_city"], "left")
    .join(carrier_df, "carrier", "left")
    .withColumn(
        "is_late",
        col("actual_delivery_date") > col("expected_delivery_date")
    )
    .withColumn(
        "is_weekend",
        dayofweek(col("expected_delivery_date")).isin([1, 7]).cast("int")
    )
    .withColumn(
        "is_peak_season",
        month(col("order_date")).isin([10, 11, 12]).cast("int")
    )
)

# COMMAND ----------

# DBTITLE 1,Write Silver Feature Table
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("logistics_silver.shipment_features")

# COMMAND ----------

# DBTITLE 1,Validation Queries
# MAGIC %sql
# MAGIC SELECT is_weekend, COUNT(*) 
# MAGIC FROM logistics_silver.shipment_features
# MAGIC GROUP BY is_weekend;