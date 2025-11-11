-- Tests LAG and LEAD window functions
-- Expected: Table-level lineage (orders -> query result)
-- Expected: Column-level lineage including LAG/LEAD columns

SELECT
    order_id,
    customer_id,
    order_date,
    total_amount,
    LAG(total_amount, 1) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
    ) AS previous_order_amount,
    LEAD(total_amount, 1) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
    ) AS next_order_amount,
    total_amount - LAG(total_amount, 1, 0) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
    ) AS amount_change
FROM SampleDB.Analytics.orders;
