-- Real-world example: Customer churn risk analysis
-- Business context: Identify at-risk customers for retention campaigns
-- Expected: Table-level lineage (customers, orders -> customer_churn_risk)
-- Expected: Column-level lineage with complex date calculations and scoring logic

CREATE TABLE SampleDB.Analytics.customer_churn_risk AS (
    WITH customer_metrics AS (
        SELECT
            c.customer_id,
            c.customer_name,
            c.email,
            c.customer_tier,
            c.registration_date,
            c.total_spent AS lifetime_value,
            c.last_order_date,
            c.order_count,
            DATEDIFF(DAY, c.last_order_date, CURRENT_DATE) AS days_since_last_order,
            DATEDIFF(DAY, c.registration_date, CURRENT_DATE) AS customer_age_days,
            CASE
                WHEN c.order_count > 0 THEN
                    DATEDIFF(DAY, c.registration_date, c.last_order_date) / CAST(c.order_count AS DECIMAL(15,2))
                ELSE NULL
            END AS avg_days_between_orders
        FROM SampleDB.Analytics.customers c
        WHERE c.status = 'ACTIVE'
    ),
    order_frequency_trend AS (
        SELECT
            customer_id,
            COUNT(CASE WHEN order_date >= CURRENT_DATE - INTERVAL '90' DAY THEN 1 END) AS orders_last_90_days,
            COUNT(CASE WHEN order_date >= CURRENT_DATE - INTERVAL '180' DAY
                        AND order_date < CURRENT_DATE - INTERVAL '90' DAY THEN 1 END) AS orders_90_to_180_days,
            SUM(CASE WHEN order_date >= CURRENT_DATE - INTERVAL '90' DAY THEN total_amount ELSE 0 END) AS spend_last_90_days,
            SUM(CASE WHEN order_date >= CURRENT_DATE - INTERVAL '180' DAY
                     AND order_date < CURRENT_DATE - INTERVAL '90' DAY THEN total_amount ELSE 0 END) AS spend_90_to_180_days
        FROM SampleDB.Analytics.orders
        WHERE status = 'COMPLETED'
        GROUP BY customer_id
    )
    SELECT
        cm.customer_id,
        cm.customer_name,
        cm.email,
        cm.customer_tier,
        cm.lifetime_value,
        cm.last_order_date,
        cm.days_since_last_order,
        cm.order_count,
        cm.avg_days_between_orders,
        oft.orders_last_90_days,
        oft.orders_90_to_180_days,
        oft.spend_last_90_days,
        oft.spend_90_to_180_days,
        -- Churn risk scoring logic
        CASE
            WHEN cm.days_since_last_order > 180 THEN 'HIGH'
            WHEN cm.days_since_last_order > 90 THEN 'MEDIUM'
            WHEN cm.days_since_last_order > 60 THEN 'LOW'
            ELSE 'MINIMAL'
        END AS recency_risk,
        CASE
            WHEN oft.orders_last_90_days = 0 AND oft.orders_90_to_180_days > 0 THEN 'HIGH'
            WHEN oft.orders_last_90_days < oft.orders_90_to_180_days THEN 'MEDIUM'
            WHEN oft.orders_last_90_days = oft.orders_90_to_180_days THEN 'LOW'
            ELSE 'MINIMAL'
        END AS frequency_risk,
        CASE
            WHEN oft.spend_last_90_days = 0 AND oft.spend_90_to_180_days > 0 THEN 'HIGH'
            WHEN oft.spend_last_90_days < oft.spend_90_to_180_days * 0.5 THEN 'MEDIUM'
            WHEN oft.spend_last_90_days < oft.spend_90_to_180_days THEN 'LOW'
            ELSE 'MINIMAL'
        END AS monetary_risk,
        -- Overall churn score (0-100)
        CAST(
            (CASE
                WHEN cm.days_since_last_order > 180 THEN 40
                WHEN cm.days_since_last_order > 90 THEN 25
                WHEN cm.days_since_last_order > 60 THEN 10
                ELSE 0
            END +
            CASE
                WHEN oft.orders_last_90_days = 0 AND oft.orders_90_to_180_days > 0 THEN 30
                WHEN oft.orders_last_90_days < oft.orders_90_to_180_days THEN 20
                WHEN oft.orders_last_90_days = oft.orders_90_to_180_days THEN 10
                ELSE 0
            END +
            CASE
                WHEN oft.spend_last_90_days = 0 AND oft.spend_90_to_180_days > 0 THEN 30
                WHEN oft.spend_last_90_days < oft.spend_90_to_180_days * 0.5 THEN 20
                WHEN oft.spend_last_90_days < oft.spend_90_to_180_days THEN 10
                ELSE 0
            END) AS INTEGER
        ) AS churn_risk_score,
        CURRENT_DATE AS analysis_date
    FROM customer_metrics cm
    LEFT JOIN order_frequency_trend oft
        ON cm.customer_id = oft.customer_id
    WHERE cm.lifetime_value > 1000  -- Focus on valuable customers
) WITH DATA;
