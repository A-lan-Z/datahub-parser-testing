-- Scalar subquery in SELECT clause
SELECT
    customer_id,
    customer_name,
    (SELECT COUNT(*)
     FROM SampleDB.Analytics.orders o
     WHERE o.customer_id = c.customer_id) AS total_orders,
    (SELECT MAX(order_date)
     FROM SampleDB.Analytics.orders o
     WHERE o.customer_id = c.customer_id) AS last_order_date
FROM SampleDB.Analytics.customers c
WHERE status = 'ACTIVE'
