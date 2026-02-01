# Databricks notebook source
# DBTITLE 1,Header (Markdown)
# 06_validation_and_sql

**Purpose:**  
Validate the end-to-end AI pipeline outputs and provide SQL queries  
used for dashboards and stakeholder analysis.

**Input Table:** logistics_gold.delivery_risk_decisions

# COMMAND ----------

# DBTITLE 1,Load Final Decision Table
final_df = spark.table("logistics_gold.delivery_risk_decisions")

# COMMAND ----------

# DBTITLE 1,Basic Sanity Checks
# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_shipments
# MAGIC FROM logistics_gold.delivery_risk_decisions;

# COMMAND ----------

# DBTITLE 1,Risk category distribution
# MAGIC %sql
# MAGIC SELECT risk_category, COUNT(*) AS shipments
# MAGIC FROM logistics_gold.delivery_risk_decisions
# MAGIC GROUP BY risk_category;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Actual delay rate by risk category
# MAGIC %sql
# MAGIC SELECT
# MAGIC   risk_category,
# MAGIC   ROUND(AVG(is_late), 3) AS actual_delay_rate
# MAGIC FROM logistics_gold.delivery_risk_decisions
# MAGIC GROUP BY risk_category;

# COMMAND ----------

# DBTITLE 1,Top high-risk shipments
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM logistics_gold.delivery_risk_decisions
# MAGIC WHERE risk_category = 'High'
# MAGIC ORDER BY delay_risk_score DESC
# MAGIC LIMIT 20;

# COMMAND ----------

