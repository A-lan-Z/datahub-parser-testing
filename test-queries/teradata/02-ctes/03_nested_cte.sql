-- Tests nested CTEs (3+ levels deep)
-- Expected: Table-level lineage (orders, order_items, products -> query result)
-- Expected: Column-level lineage through nested CTE chain

WITH order_lines AS (
    SELECT
        oi.order_id,
        oi.product_id,
        oi.quantity,
        oi.unit_price,
        p.product_name,
        p.category
    FROM SampleDB.Analytics.order_items oi
    INNER JOIN SampleDB.Analytics.products p
        ON oi.product_id = p.product_id
),
order_aggregates AS (
    SELECT
        order_id,
        COUNT(*) AS item_count,
        SUM(quantity * unit_price) AS order_total,
        STRING_AGG(product_name, ', ') AS products
    FROM order_lines
    GROUP BY order_id
),
enriched_orders AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_date,
        oa.item_count,
        oa.order_total,
        oa.products
    FROM SampleDB.Analytics.orders o
    INNER JOIN order_aggregates oa
        ON o.order_id = oa.order_id
)
SELECT
    order_id,
    customer_id,
    order_date,
    item_count,
    order_total,
    products
FROM enriched_orders
WHERE order_total > 500;
