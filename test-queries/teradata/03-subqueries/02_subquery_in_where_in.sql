-- Tests subquery with IN clause in WHERE
-- Expected: Table-level lineage (customers, orders -> query result)
-- Expected: Column-level lineage for customer data

SELECT
    customer_id,
    customer_name,
    email,
    customer_tier
FROM SampleDB.Analytics.customers
WHERE customer_id IN (
    SELECT customer_id
    FROM SampleDB.Analytics.orders
    WHERE order_date >= DATE '2024-01-01'
      AND total_amount > 1000
);
