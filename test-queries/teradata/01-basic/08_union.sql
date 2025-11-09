-- UNION of two queries
SELECT
    customer_id,
    customer_name,
    'Premium' AS customer_tier
FROM SampleDB.Analytics.premium_customers
WHERE status = 'ACTIVE'

UNION ALL

SELECT
    customer_id,
    customer_name,
    'Standard' AS customer_tier
FROM SampleDB.Analytics.standard_customers
WHERE status = 'ACTIVE'
