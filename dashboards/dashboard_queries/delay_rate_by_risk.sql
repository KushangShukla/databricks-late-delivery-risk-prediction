SELECT
  risk_category,
  AVG(is_late) AS actual_delay_rate
FROM logistics_gold.delivery_risk_decisions
GROUP BY risk_category;