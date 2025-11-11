-- Tests scalar subquery in SELECT clause
-- Expected: Table-level lineage (customers, orders -> query result)
-- Expected: Column-level lineage including calculated subquery column

SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    (SELECT COUNT(*)
     FROM SampleDB.Analytics.orders o
     WHERE o.customer_id = c.customer_id) AS order_count,
    (SELECT SUM(total_amount)
     FROM SampleDB.Analytics.orders o
     WHERE o.customer_id = c.customer_id) AS total_spent
FROM SampleDB.Analytics.customers c
WHERE c.status = 'ACTIVE';
