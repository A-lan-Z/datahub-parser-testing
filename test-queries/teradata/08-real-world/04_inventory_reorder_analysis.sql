-- Real-world example: Inventory reorder point analysis
-- Business context: Calculate when to reorder products based on sales velocity
-- Expected: Table-level lineage (products, order_items, orders -> query result)
-- Expected: Column-level lineage with time-series calculations

WITH sales_velocity AS (
    SELECT
        oi.product_id,
        COUNT(DISTINCT o.order_id) AS order_frequency_30d,
        SUM(oi.quantity) AS units_sold_30d,
        AVG(oi.quantity) AS avg_units_per_order,
        STDDEV(oi.quantity) AS stddev_units_per_order,
        COUNT(DISTINCT CAST(o.order_date AS DATE)) AS days_with_sales
    FROM SampleDB.Analytics.order_items oi
    INNER JOIN SampleDB.Analytics.orders o
        ON oi.order_id = o.order_id
    WHERE o.order_date >= CURRENT_DATE - INTERVAL '30' DAY
      AND o.status = 'COMPLETED'
    GROUP BY oi.product_id
),
product_trends AS (
    SELECT
        oi.product_id,
        SUM(CASE WHEN o.order_date >= CURRENT_DATE - INTERVAL '7' DAY THEN oi.quantity ELSE 0 END) AS units_sold_7d,
        SUM(CASE WHEN o.order_date >= CURRENT_DATE - INTERVAL '14' DAY
                  AND o.order_date < CURRENT_DATE - INTERVAL '7' DAY THEN oi.quantity ELSE 0 END) AS units_sold_8_14d,
        SUM(CASE WHEN o.order_date >= CURRENT_DATE - INTERVAL '30' DAY
                  AND o.order_date < CURRENT_DATE - INTERVAL '14' DAY THEN oi.quantity ELSE 0 END) AS units_sold_15_30d
    FROM SampleDB.Analytics.order_items oi
    INNER JOIN SampleDB.Analytics.orders o
        ON oi.order_id = o.order_id
    WHERE o.order_date >= CURRENT_DATE - INTERVAL '30' DAY
      AND o.status = 'COMPLETED'
    GROUP BY oi.product_id
)
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.unit_price,
    sv.units_sold_30d,
    sv.order_frequency_30d,
    sv.avg_units_per_order,
    sv.stddev_units_per_order,
    sv.days_with_sales,
    pt.units_sold_7d,
    pt.units_sold_8_14d,
    pt.units_sold_15_30d,
    -- Daily sales velocity
    CASE
        WHEN sv.days_with_sales > 0 THEN
            sv.units_sold_30d / CAST(sv.days_with_sales AS DECIMAL(15,2))
        ELSE 0
    END AS daily_sales_velocity,
    -- Sales trend indicator
    CASE
        WHEN pt.units_sold_7d > pt.units_sold_8_14d THEN 'ACCELERATING'
        WHEN pt.units_sold_7d < pt.units_sold_8_14d * 0.8 THEN 'DECELERATING'
        ELSE 'STABLE'
    END AS trend_direction,
    -- Reorder point calculation (assuming 14-day lead time and 2x safety stock)
    CAST(
        (CASE
            WHEN sv.days_with_sales > 0 THEN
                sv.units_sold_30d / CAST(sv.days_with_sales AS DECIMAL(15,2))
            ELSE 0
        END * 14) +
        (2 * COALESCE(sv.stddev_units_per_order, 0))
        AS INTEGER
    ) AS recommended_reorder_point,
    -- Order quantity (30-day supply)
    CAST(
        CASE
            WHEN sv.days_with_sales > 0 THEN
                sv.units_sold_30d / CAST(sv.days_with_sales AS DECIMAL(15,2)) * 30
            ELSE 0
        END
        AS INTEGER
    ) AS recommended_order_quantity,
    CURRENT_DATE AS analysis_date
FROM SampleDB.Analytics.products p
INNER JOIN sales_velocity sv
    ON p.product_id = sv.product_id
INNER JOIN product_trends pt
    ON p.product_id = pt.product_id
WHERE sv.units_sold_30d > 0
ORDER BY sv.units_sold_30d DESC;
