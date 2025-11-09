-- Subquery in FROM clause (derived table)
SELECT
    category,
    product_count,
    avg_price,
    total_revenue
FROM (
    SELECT
        p.category,
        COUNT(DISTINCT p.product_id) AS product_count,
        AVG(p.unit_price) AS avg_price,
        SUM(oi.quantity * oi.unit_price) AS total_revenue
    FROM SampleDB.Analytics.products p
    INNER JOIN SampleDB.Analytics.order_items oi
        ON p.product_id = oi.product_id
    INNER JOIN SampleDB.Analytics.orders o
        ON oi.order_id = o.order_id
    WHERE o.order_date >= CURRENT_DATE - 90
    GROUP BY p.category
) AS category_stats
WHERE total_revenue > 50000
ORDER BY total_revenue DESC
