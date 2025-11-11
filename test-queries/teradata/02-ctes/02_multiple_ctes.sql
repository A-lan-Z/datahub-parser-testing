-- Tests multiple dependent CTEs
-- Expected: Table-level lineage (customers, orders -> query result)
-- Expected: Column-level lineage through multiple CTEs

WITH customer_totals AS (
    SELECT
        customer_id,
        SUM(total_amount) AS total_revenue,
        COUNT(*) AS order_count
    FROM SampleDB.Analytics.orders
    GROUP BY customer_id
),
high_value_customers AS (
    SELECT
        c.customer_id,
        c.customer_name,
        c.email,
        ct.total_revenue,
        ct.order_count
    FROM SampleDB.Analytics.customers c
    INNER JOIN customer_totals ct
        ON c.customer_id = ct.customer_id
    WHERE ct.total_revenue > 5000
)
SELECT
    customer_id,
    customer_name,
    email,
    total_revenue,
    order_count
FROM high_value_customers
ORDER BY total_revenue DESC;
