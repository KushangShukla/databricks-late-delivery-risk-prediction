# Databricks notebook source
# DBTITLE 1,Notebook Header (Markdown)
# 04_ml_training_mlflow

**Purpose:**  
Train baseline and final ML models for late delivery risk prediction  
and track experiments using MLflow with Unity Catalog compliance.

**Input Table:** logistics_gold.ml_delay_dataset  
**Models:** Logistic Regression (baseline), Random Forest (final)

# COMMAND ----------

# DBTITLE 1,Imports & MLflow UC Setup
import os
import mlflow
import mlflow.spark

from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# COMMAND ----------

# DBTITLE 1,Imports & MLflow UC Setup
os.environ["MLFLOW_DFS_TMP"] = "/Volumes/workspace/default/mlflow_tmp"

# COMMAND ----------

# DBTITLE 1,Load Gold ML Dataset
ml_df = spark.table("logistics_gold.ml_delay_dataset")

# COMMAND ----------

# DBTITLE 1,Assemble Feature Vector (KEEP shipment_id)
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

final_df = assembler.transform(ml_df).select(
    "shipment_id",
    "features",
    "is_late"
)

# COMMAND ----------

# DBTITLE 1,Train / Test Split
train_df, test_df = final_df.randomSplit([0.8, 0.2], seed=42)

# COMMAND ----------

# DBTITLE 1,MLflow Experiment Setup
mlflow.set_experiment("/Shared/Late_Delivery_Risk_Prediction")

# COMMAND ----------

# DBTITLE 1,BASELINE MODEL: Logistic Regression
mlflow.end_run()  # safety in case a run is already active

# COMMAND ----------

with mlflow.start_run(run_name="Logistic_Regression_Baseline"):
    
    lr = LogisticRegression(
        featuresCol="features",
        labelCol="is_late",
        family="binomial"
    )
    
    lr_model = lr.fit(train_df)
    lr_preds = lr_model.transform(test_df)
    
    evaluator = BinaryClassificationEvaluator(
        labelCol="is_late",
        metricName="areaUnderROC"
    )
    
    auc_lr = evaluator.evaluate(lr_preds)
    
    mlflow.log_metric("ROC_AUC", auc_lr)
    mlflow.spark.log_model(lr_model, "logistic_regression_model")
    
    print("Logistic Regression ROC-AUC:", auc_lr)

# COMMAND ----------

# DBTITLE 1,FINAL MODEL: Random Forest
mlflow.end_run()

# COMMAND ----------

with mlflow.start_run(run_name="Random_Forest_Final_Model"):
    
    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="is_late",
        numTrees=100,
        maxDepth=8,
        featureSubsetStrategy="sqrt",
        seed=42
    )
    
    rf_model = rf.fit(train_df)
    rf_preds = rf_model.transform(test_df)
    
    evaluator = BinaryClassificationEvaluator(
        labelCol="is_late",
        metricName="areaUnderROC"
    )
    
    auc_rf = evaluator.evaluate(rf_preds)
    
    mlflow.log_metric("ROC_AUC", auc_rf)
    mlflow.log_params({
        "numTrees": 100,
        "maxDepth": 8,
        "featureSubsetStrategy": "sqrt"
    })
    
    mlflow.spark.log_model(rf_model, "random_forest_model")
    
    print("Random Forest ROC-AUC:", auc_rf)

# COMMAND ----------

# DBTITLE 1,Feature Importance (INSIGHT GENERATION)
import pandas as pd

importances = rf_model.featureImportances.toArray()

feature_importance_df = pd.DataFrame({
    "feature": feature_cols,
    "importance": importances
}).sort_values(by="importance", ascending=False)

feature_importance_df

# COMMAND ----------

# DBTITLE 1,Quick Model Comparison
print(f"Baseline LR AUC: {auc_lr}")
print(f"Final RF AUC: {auc_rf}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.default.ml_models;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES IN workspace.default;

# COMMAND ----------

rf_model.write().overwrite().save(
    "/Volumes/workspace/default/ml_models/random_forest_delay_model"
)

# COMMAND ----------

