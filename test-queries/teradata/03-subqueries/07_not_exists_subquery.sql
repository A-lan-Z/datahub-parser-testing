-- Tests NOT EXISTS subquery to find customers without recent orders
-- Expected: Table-level lineage (customers, orders -> query result)
-- Expected: Column-level lineage for customer data

SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    c.last_order_date
FROM SampleDB.Analytics.customers c
WHERE NOT EXISTS (
    SELECT 1
    FROM SampleDB.Analytics.orders o
    WHERE o.customer_id = c.customer_id
      AND o.order_date >= DATE '2024-06-01'
);
