-- Nested CTEs (CTE referencing another CTE)
WITH daily_sales AS (
    SELECT
        CAST(order_date AS DATE) AS sale_date,
        SUM(total_amount) AS daily_total
    FROM SampleDB.Analytics.orders
    GROUP BY CAST(order_date AS DATE)
),
weekly_sales AS (
    SELECT
        DATE_TRUNC('week', sale_date) AS week_start,
        SUM(daily_total) AS weekly_total,
        AVG(daily_total) AS avg_daily_total
    FROM daily_sales
    GROUP BY DATE_TRUNC('week', sale_date)
),
monthly_sales AS (
    SELECT
        DATE_TRUNC('month', week_start) AS month_start,
        SUM(weekly_total) AS monthly_total,
        AVG(weekly_total) AS avg_weekly_total
    FROM weekly_sales
    GROUP BY DATE_TRUNC('month', week_start)
)
SELECT
    month_start,
    monthly_total,
    avg_weekly_total,
    LAG(monthly_total, 1) OVER (ORDER BY month_start) AS prev_month_total
FROM monthly_sales
ORDER BY month_start
