-- Tests INNER JOIN with explicit column references
-- Expected: Table-level lineage (customers, orders -> query result)
-- Expected: Column-level lineage tracking through join

SELECT
    c.customer_id,
    c.customer_name,
    o.order_id,
    o.order_date,
    o.total_amount
FROM SampleDB.Analytics.customers c
INNER JOIN SampleDB.Analytics.orders o
    ON c.customer_id = o.customer_id
WHERE o.order_date >= DATE '2024-01-01';
