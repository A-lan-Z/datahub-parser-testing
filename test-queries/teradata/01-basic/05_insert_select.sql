-- INSERT with SELECT
INSERT INTO SampleDB.Analytics.customer_summary
    (customer_id, customer_name, total_orders, last_order_date)
SELECT
    c.customer_id,
    c.customer_name,
    COUNT(o.order_id),
    MAX(o.order_date)
FROM SampleDB.Analytics.customers c
LEFT JOIN SampleDB.Analytics.orders o
    ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name
