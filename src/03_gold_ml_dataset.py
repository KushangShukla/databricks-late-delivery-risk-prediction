# Databricks notebook source
# DBTITLE 1,Notebook Header (Markdown)
# 03_gold_ml_dataset

**Purpose:**  
Prepare a clean, machine-learning-ready Gold dataset by handling nulls,  
casting labels, and selecting final feature columns.

**Layer:** Gold  
**Input Table:** logistics_silver.shipment_features  
**Output Table:** logistics_gold.ml_delay_dataset

# COMMAND ----------

# DBTITLE 1,Create Gold Schema (SQL)
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS logistics_gold;

# COMMAND ----------

# DBTITLE 1,Imports (Python)
from pyspark.sql.functions import col

# COMMAND ----------

# DBTITLE 1,Load Silver Feature Table
silver_df = spark.table("logistics_silver.shipment_features")

# COMMAND ----------

# DBTITLE 1,Select ML Columns (EXPLICIT & CLEAN)
ml_df = silver_df.select(
    "shipment_id",
    "distance_km",
    "shipment_weight_kg",
    "avg_traffic_index",
    "weather_severity_score",
    "historical_delay_rate",
    "avg_delay_days",
    "is_weekend",
    "is_peak_season",
    "is_late"
)

# COMMAND ----------

# DBTITLE 1,Handle Missing Values (DOMAIN-AWARE)
ml_df_clean = ml_df.fillna({
    "avg_traffic_index": 0.5,
    "weather_severity_score": 0.3,
    "historical_delay_rate": 0.2,
    "avg_delay_days": 1.0
})

# COMMAND ----------

# DBTITLE 1,Cast Target Label (CRITICAL FOR SPARK ML)
ml_df_clean = ml_df_clean.withColumn(
    "is_late",
    col("is_late").cast("int")
)

# COMMAND ----------

# DBTITLE 1,Write ML-Ready Gold Table (SAFE OVERWRITE)
ml_df_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("logistics_gold.ml_delay_dataset")

# COMMAND ----------

# DBTITLE 1,Validation Queries
# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM logistics_gold.ml_delay_dataset;

# COMMAND ----------

# DBTITLE 1,Class Distribution
# MAGIC %sql
# MAGIC SELECT is_late, COUNT(*) 
# MAGIC FROM logistics_gold.ml_delay_dataset
# MAGIC GROUP BY is_late;

# COMMAND ----------

# DBTITLE 1,Null Check
# MAGIC %sql
# MAGIC SELECT
# MAGIC   SUM(CASE WHEN avg_traffic_index IS NULL THEN 1 ELSE 0 END) AS traffic_nulls,
# MAGIC   SUM(CASE WHEN weather_severity_score IS NULL THEN 1 ELSE 0 END) AS weather_nulls
# MAGIC FROM logistics_gold.ml_delay_dataset;