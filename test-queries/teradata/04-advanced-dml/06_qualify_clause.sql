-- Tests QUALIFY clause (Teradata-specific)
-- Expected: Table-level lineage (orders, customers -> query result)
-- Expected: Column-level lineage with QUALIFY filter

SELECT
    c.customer_id,
    c.customer_name,
    o.order_id,
    o.order_date,
    o.total_amount,
    ROW_NUMBER() OVER (
        PARTITION BY c.customer_id
        ORDER BY o.total_amount DESC
    ) AS order_rank
FROM SampleDB.Analytics.customers c
INNER JOIN SampleDB.Analytics.orders o
    ON c.customer_id = o.customer_id
QUALIFY order_rank = 1;
