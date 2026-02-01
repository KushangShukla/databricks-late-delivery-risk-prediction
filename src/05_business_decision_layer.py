# Databricks notebook source
# DBTITLE 1,Notebook Header (Markdown)
# 05_business_decision_layer

**Purpose:**  
Transform ML predictions into actionable business decisions by generating  
delay risk scores, risk categories, and recommended actions.

**Layer:** Gold  
**Output Table:** logistics_gold.delivery_risk_decisions

# COMMAND ----------

# DBTITLE 1,Imports (Spark-Connect Safe)
from pyspark.sql.functions import col, when
from pyspark.ml.functions import vector_to_array
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassificationModel

# COMMAND ----------

# DBTITLE 1,Load ML Dataset
ml_df = spark.table("logistics_gold.ml_delay_dataset")

# COMMAND ----------

# DBTITLE 1,Rebuild Feature Vector (KEEP shipment_id)
feature_cols = [
    "distance_km",
    "shipment_weight_kg",
    "avg_traffic_index",
    "weather_severity_score",
    "historical_delay_rate",
    "avg_delay_days",
    "is_weekend",
    "is_peak_season"
]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features",
    handleInvalid="skip"
)

feature_df = assembler.transform(ml_df).select(
    "shipment_id",
    "features",
    "is_late"
)

# COMMAND ----------

# DBTITLE 1,Load Final Trained Model (MLflow Artifact)
rf_model = RandomForestClassificationModel.load(
    "/Volumes/workspace/default/ml_models/random_forest_delay_model"
)


# COMMAND ----------

# DBTITLE 1,Generate Predictions
predictions_df = rf_model.transform(feature_df)

# COMMAND ----------

# DBTITLE 1,Extract Delay Risk Score (Spark-Connect SAFE)
scored_df = predictions_df.withColumn(
    "delay_risk_score",
    vector_to_array(col("probability"))[1]
)

# COMMAND ----------

# DBTITLE 1,Create Risk Categories
decision_df = scored_df.withColumn(
    "risk_category",
    when(col("delay_risk_score") < 0.30, "Low")
    .when(col("delay_risk_score") < 0.70, "Medium")
    .otherwise("High")
)

# COMMAND ----------

# DBTITLE 1,Attach Business Actions
decision_df = decision_df.withColumn(
    "recommended_action",
    when(col("risk_category") == "High", "Reroute shipment / Alert operations")
    .when(col("risk_category") == "Medium", "Monitor shipment closely")
    .otherwise("No action required")
)

# COMMAND ----------

# DBTITLE 1,Final Business Output
final_business_df = decision_df.select(
    "shipment_id",
    "delay_risk_score",
    "risk_category",
    "recommended_action",
    "is_late"
)

# COMMAND ----------

# DBTITLE 1,Write Gold Decision Table
final_business_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("logistics_gold.delivery_risk_decisions")

# COMMAND ----------

# DBTITLE 1,Validation Queries
# MAGIC %sql
# MAGIC SELECT risk_category, COUNT(*) 
# MAGIC FROM logistics_gold.delivery_risk_decisions
# MAGIC GROUP BY risk_category;

# COMMAND ----------

# DBTITLE 1,Actual Delay Rate by Risk
# MAGIC %sql
# MAGIC SELECT
# MAGIC   risk_category,
# MAGIC   AVG(is_late) AS actual_delay_rate
# MAGIC FROM logistics_gold.delivery_risk_decisions
# MAGIC GROUP BY risk_category;

# COMMAND ----------

