-- EXISTS subquery
SELECT
    c.customer_id,
    c.customer_name,
    c.email
FROM SampleDB.Analytics.customers c
WHERE EXISTS (
    SELECT 1
    FROM SampleDB.Analytics.orders o
    WHERE o.customer_id = c.customer_id
    AND o.order_date >= CURRENT_DATE - 30
)
AND NOT EXISTS (
    SELECT 1
    FROM SampleDB.Analytics.customer_complaints cc
    WHERE cc.customer_id = c.customer_id
    AND cc.complaint_date >= CURRENT_DATE - 90
)
