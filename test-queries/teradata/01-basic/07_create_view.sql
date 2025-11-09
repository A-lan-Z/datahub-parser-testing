-- CREATE VIEW
CREATE VIEW SampleDB.Analytics.vw_recent_orders AS
SELECT
    o.order_id,
    o.order_date,
    c.customer_name,
    p.product_name,
    oi.quantity,
    oi.unit_price,
    oi.quantity * oi.unit_price AS line_total
FROM SampleDB.Analytics.orders o
INNER JOIN SampleDB.Analytics.customers c
    ON o.customer_id = c.customer_id
INNER JOIN SampleDB.Analytics.order_items oi
    ON o.order_id = oi.order_id
INNER JOIN SampleDB.Analytics.products p
    ON oi.product_id = p.product_id
WHERE o.order_date >= CURRENT_DATE - 90
