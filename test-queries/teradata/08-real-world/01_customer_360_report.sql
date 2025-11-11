-- Real-world example: Customer 360 degree view report
-- Business context: Comprehensive customer profile with lifetime metrics
-- Expected: Table-level lineage (customers, orders, order_items, products -> query result)
-- Expected: Column-level lineage through multiple CTEs and aggregations

WITH customer_order_history AS (
    SELECT
        o.customer_id,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(o.total_amount) AS lifetime_value,
        AVG(o.total_amount) AS avg_order_value,
        MIN(o.order_date) AS first_order_date,
        MAX(o.order_date) AS last_order_date,
        DATEDIFF(DAY, MIN(o.order_date), MAX(o.order_date)) AS customer_lifetime_days
    FROM SampleDB.Analytics.orders o
    WHERE o.status NOT IN ('CANCELLED', 'REFUNDED')
    GROUP BY o.customer_id
),
customer_product_preferences AS (
    SELECT
        o.customer_id,
        p.category AS preferred_category,
        SUM(oi.quantity * oi.unit_price) AS category_spend,
        ROW_NUMBER() OVER (
            PARTITION BY o.customer_id
            ORDER BY SUM(oi.quantity * oi.unit_price) DESC
        ) AS category_rank
    FROM SampleDB.Analytics.orders o
    INNER JOIN SampleDB.Analytics.order_items oi
        ON o.order_id = oi.order_id
    INNER JOIN SampleDB.Analytics.products p
        ON oi.product_id = p.product_id
    GROUP BY o.customer_id, p.category
),
customer_recent_activity AS (
    SELECT
        o.customer_id,
        COUNT(DISTINCT o.order_id) AS orders_last_90_days,
        SUM(o.total_amount) AS spend_last_90_days
    FROM SampleDB.Analytics.orders o
    WHERE o.order_date >= CURRENT_DATE - INTERVAL '90' DAY
    GROUP BY o.customer_id
)
SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    c.status,
    c.customer_tier,
    c.registration_date,
    coh.total_orders,
    coh.lifetime_value,
    coh.avg_order_value,
    coh.first_order_date,
    coh.last_order_date,
    coh.customer_lifetime_days,
    COALESCE(cra.orders_last_90_days, 0) AS orders_last_90_days,
    COALESCE(cra.spend_last_90_days, 0) AS spend_last_90_days,
    cpp.preferred_category,
    cpp.category_spend AS preferred_category_spend,
    CASE
        WHEN coh.last_order_date >= CURRENT_DATE - INTERVAL '30' DAY THEN 'ACTIVE'
        WHEN coh.last_order_date >= CURRENT_DATE - INTERVAL '90' DAY THEN 'AT_RISK'
        WHEN coh.last_order_date >= CURRENT_DATE - INTERVAL '180' DAY THEN 'DORMANT'
        ELSE 'LOST'
    END AS engagement_status,
    CASE
        WHEN coh.customer_lifetime_days > 0 THEN
            coh.lifetime_value / CAST(coh.customer_lifetime_days AS DECIMAL(15,2))
        ELSE 0
    END AS daily_value_rate
FROM SampleDB.Analytics.customers c
LEFT JOIN customer_order_history coh
    ON c.customer_id = coh.customer_id
LEFT JOIN customer_recent_activity cra
    ON c.customer_id = cra.customer_id
LEFT JOIN customer_product_preferences cpp
    ON c.customer_id = cpp.customer_id AND cpp.category_rank = 1
WHERE c.status = 'ACTIVE'
ORDER BY coh.lifetime_value DESC;
