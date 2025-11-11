-- Tests mixed qualified and unqualified references
-- Expected: Table-level lineage using default schema for unqualified refs
-- Expected: Column-level lineage with proper schema resolution

SELECT
    c.customer_id,
    c.customer_name,
    o.order_id,
    o.total_amount,
    p.product_name
FROM customers c                                    -- Unqualified (uses default)
INNER JOIN SampleDB.Analytics.orders o              -- Fully qualified
    ON c.customer_id = o.customer_id
INNER JOIN SampleDB.Analytics.order_items oi
    ON o.order_id = oi.order_id
INNER JOIN products p                               -- Unqualified (uses default)
    ON oi.product_id = p.product_id;
