-- Correlated subquery
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.unit_price
FROM SampleDB.Analytics.products p
WHERE p.unit_price > (
    SELECT AVG(p2.unit_price)
    FROM SampleDB.Analytics.products p2
    WHERE p2.category = p.category
)
ORDER BY p.category, p.unit_price DESC
