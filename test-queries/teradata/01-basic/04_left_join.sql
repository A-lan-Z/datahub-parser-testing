-- LEFT JOIN with aggregation
SELECT
    c.customer_id,
    c.customer_name,
    COUNT(o.order_id) AS order_count,
    SUM(o.total_amount) AS total_spent
FROM SampleDB.Analytics.customers c
LEFT JOIN SampleDB.Analytics.orders o
    ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name
HAVING COUNT(o.order_id) > 0
