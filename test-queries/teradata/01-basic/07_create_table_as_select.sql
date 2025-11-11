-- Tests CREATE TABLE AS SELECT (CTAS)
-- Expected: Table-level lineage (orders, order_items, products -> order_details)
-- Expected: Column-level lineage for all selected columns

CREATE TABLE SampleDB.Analytics.order_details AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_date,
        oi.order_item_id,
        oi.product_id,
        p.product_name,
        oi.quantity,
        oi.unit_price,
        oi.quantity * oi.unit_price AS line_total
    FROM SampleDB.Analytics.orders o
    INNER JOIN SampleDB.Analytics.order_items oi
        ON o.order_id = oi.order_id
    INNER JOIN SampleDB.Analytics.products p
        ON oi.product_id = p.product_id
) WITH DATA;
