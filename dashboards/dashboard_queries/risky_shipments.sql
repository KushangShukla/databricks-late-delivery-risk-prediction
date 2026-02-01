SELECT shipment_id, delay_risk_score, recommended_action
FROM logistics_gold.delivery_risk_decisions
WHERE risk_category = 'High'
ORDER BY delay_risk_score DESC
LIMIT 15;