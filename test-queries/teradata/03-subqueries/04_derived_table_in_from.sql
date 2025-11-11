-- Tests derived table (subquery in FROM clause)
-- Expected: Table-level lineage (orders, customers -> query result)
-- Expected: Column-level lineage through derived table

SELECT
    oa.customer_id,
    c.customer_name,
    oa.order_count,
    oa.total_revenue,
    oa.avg_order_value
FROM (
    SELECT
        customer_id,
        COUNT(*) AS order_count,
        SUM(total_amount) AS total_revenue,
        AVG(total_amount) AS avg_order_value
    FROM SampleDB.Analytics.orders
    WHERE order_date >= DATE '2024-01-01'
    GROUP BY customer_id
) AS oa
INNER JOIN SampleDB.Analytics.customers c
    ON oa.customer_id = c.customer_id;
