-- Tests subquery with comparison operator (ALL/ANY)
-- Expected: Table-level lineage (products, order_items -> query result)
-- Expected: Column-level lineage for product data

SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.unit_price
FROM SampleDB.Analytics.products p
WHERE p.unit_price > ALL (
    SELECT AVG(oi.unit_price)
    FROM SampleDB.Analytics.order_items oi
    WHERE oi.product_id = p.product_id
    GROUP BY oi.order_id
);
