-- Basic INNER JOIN
SELECT
    o.order_id,
    o.order_date,
    c.customer_name,
    c.email
FROM SampleDB.Analytics.orders o
INNER JOIN SampleDB.Analytics.customers c
    ON o.customer_id = c.customer_id
WHERE o.order_date >= '2024-01-01'
