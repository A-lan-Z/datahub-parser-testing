-- Tests LATERAL join (may be complex for parser)
-- Expected: Partial lineage possible, lateral reference may complicate tracking
-- Expected: Column-level lineage through lateral join

SELECT
    c.customer_id,
    c.customer_name,
    recent_orders.order_id,
    recent_orders.order_date,
    recent_orders.total_amount
FROM SampleDB.Analytics.customers c
CROSS JOIN LATERAL (
    SELECT
        o.order_id,
        o.order_date,
        o.total_amount
    FROM SampleDB.Analytics.orders o
    WHERE o.customer_id = c.customer_id
    ORDER BY o.order_date DESC
    LIMIT 5
) AS recent_orders
WHERE c.status = 'ACTIVE';
