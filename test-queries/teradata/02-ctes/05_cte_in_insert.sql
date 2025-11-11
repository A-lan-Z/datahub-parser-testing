-- Tests CTE in INSERT statement
-- Expected: Table-level lineage (customers, orders -> customer_summary)
-- Expected: Column-level lineage through CTE into target table

WITH customer_metrics AS (
    SELECT
        c.customer_id,
        c.customer_name,
        COUNT(o.order_id) AS order_count,
        SUM(o.total_amount) AS total_spent,
        MAX(o.order_date) AS last_order_date
    FROM SampleDB.Analytics.customers c
    LEFT JOIN SampleDB.Analytics.orders o
        ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.customer_name
)
INSERT INTO SampleDB.Analytics.customer_summary
    (customer_id, customer_name, order_count, total_spent, last_order_date)
SELECT
    customer_id,
    customer_name,
    order_count,
    total_spent,
    last_order_date
FROM customer_metrics
WHERE order_count > 0;
