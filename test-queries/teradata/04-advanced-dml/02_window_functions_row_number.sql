-- Tests window function ROW_NUMBER
-- Expected: Table-level lineage (orders -> query result)
-- Expected: Column-level lineage for all columns including window function result

SELECT
    order_id,
    customer_id,
    order_date,
    total_amount,
    ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY order_date DESC
    ) AS order_sequence
FROM SampleDB.Analytics.orders
QUALIFY order_sequence <= 5;
