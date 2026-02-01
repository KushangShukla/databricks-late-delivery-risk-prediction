SELECT risk_category, COUNT(*) AS shipments
FROM logistics_gold.delivery_risk_decisions
GROUP BY risk_category;