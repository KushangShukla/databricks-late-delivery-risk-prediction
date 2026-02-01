# Late Delivery Risk Prediction using Databricks Lakehouse

## Overview
This project implements an end-to-end AI-powered logistics decision intelligence system using the Databricks Lakehouse platform. It predicts late delivery risk for shipments and converts machine learning predictions into actionable business decisions.

The solution follows the Medallion (Bronze–Silver–Gold) data architecture and integrates Delta Lake, MLflow, Databricks Jobs, SQL Dashboards, and Genie for analytics and automation.

---

## Problem Statement
Late deliveries in logistics lead to SLA violations, customer dissatisfaction, and increased operational costs. Traditional rule-based systems are reactive and cannot model complex interactions between traffic, weather, carrier reliability, and seasonality.

This project addresses the problem by proactively predicting delivery delay risk and enabling operations teams to take preventive actions.

---

## Architecture
The system follows the Databricks Lakehouse Medallion Architecture:

Bronze (Raw Shipments)
↓
Silver (Enriched Features)
↓
Gold (ML Dataset & Decisions)
↓
MLflow Model Training
↓
Business Decisions
↓
Jobs + SQL Dashboard + Genie

---

## Data Architecture (Bronze → Silver → Gold)

### Bronze Layer
- Raw, source-like shipment data
- Synthetic but realistic logistics records
- Stored as Delta tables with ACID guarantees

**Table:** `logistics_bronze.shipments_raw`

---

### Silver Layer
- Business-aware transformations and feature engineering
- Enrichment using:
  - Weather severity
  - Traffic congestion
  - Carrier reliability
  - Temporal indicators (weekend, peak season)

**Table:** `logistics_silver.shipment_features`

---

### Gold Layer
- ML-ready dataset with cleaned and typed features
- Final business decision table with risk scores and actions

**Tables:**
- `logistics_gold.ml_delay_dataset`
- `logistics_gold.delivery_risk_decisions`

---

## Machine Learning
Two models were trained using Spark ML:

- Logistic Regression (baseline)
- Random Forest (final model)

Random Forest was selected due to its ability to model non-linear relationships in logistics data.

### MLflow
MLflow was used for:
- Experiment tracking
- Metric logging (ROC-AUC)
- Model comparison

Final models were materialized to Unity Catalog volumes for Spark-native inference.

---

## Decision Intelligence Layer
Instead of returning only predictions, the system converts model outputs into business decisions.

| Risk Category | Action |
|--------------|--------|
| Low | No action required |
| Medium | Monitor shipment closely |
| High | Reroute shipment / Alert operations |

This enables proactive operational decision-making.

---

## Orchestration with Databricks Jobs
The entire pipeline is automated using Databricks Jobs with task dependencies:

Bronze → Silver → Gold → ML Training → Decision Layer → Validation

This ensures reproducibility, automation, and production readiness.

---

## Analytics & Visualization
### Databricks SQL Dashboard
Provides real-time operational insights:
- Shipment risk distribution
- Actual delay rate by risk
- High-risk shipment table with actions

### Databricks Genie
Genie enables natural-language analytics for non-technical stakeholders, allowing them to ask questions like:
- "Which risk category has the highest delay rate?"
- "Show risky shipments with recommended actions"

---

## Governance
- Unity Catalog used for schema and volume management
- Delta Lake ensures ACID compliance and schema enforcement
- Clear separation of Bronze, Silver, and Gold layers

---

## Repository Structure
notebooks/ → Databricks pipeline notebooks
dashboards/ → SQL queries and dashboard screenshots
genie/ → Genie queries and screenshots
jobs/ → Databricks Jobs DAG
architecture/ → System architecture diagram

---

## Future Enhancements
- Real-time streaming ingestion
- Integration with external weather and traffic APIs
- Cost optimization and ETA prediction models
- Model monitoring and drift detection

---

## Conclusion
This project demonstrates a complete, production-style Databricks Lakehouse AI system that goes beyond prediction to deliver actionable decision intelligence for logistics operations.
