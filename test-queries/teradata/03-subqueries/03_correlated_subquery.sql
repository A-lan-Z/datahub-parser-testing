-- Tests correlated subquery with correlation to outer query
-- Expected: Table-level lineage (orders -> query result)
-- Expected: Column-level lineage through correlated reference

SELECT
    o1.order_id,
    o1.customer_id,
    o1.order_date,
    o1.total_amount
FROM SampleDB.Analytics.orders o1
WHERE o1.total_amount > (
    SELECT AVG(o2.total_amount)
    FROM SampleDB.Analytics.orders o2
    WHERE o2.customer_id = o1.customer_id
);
