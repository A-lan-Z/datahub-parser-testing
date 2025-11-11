-- Tests EXISTS subquery
-- Expected: Table-level lineage (customers, orders -> query result)
-- Expected: Column-level lineage for customer data

SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    c.customer_tier
FROM SampleDB.Analytics.customers c
WHERE EXISTS (
    SELECT 1
    FROM SampleDB.Analytics.orders o
    WHERE o.customer_id = c.customer_id
      AND o.order_date >= DATE '2024-01-01'
      AND o.total_amount > 5000
);
