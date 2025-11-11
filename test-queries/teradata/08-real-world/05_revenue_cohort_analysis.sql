-- Real-world example: Customer cohort revenue analysis
-- Business context: Track revenue retention by customer acquisition cohort
-- Expected: Table-level lineage (customers, orders -> query result)
-- Expected: Column-level lineage with cohort grouping and period calculations

WITH customer_cohorts AS (
    SELECT
        customer_id,
        customer_name,
        DATE_TRUNC('month', registration_date) AS cohort_month,
        registration_date
    FROM SampleDB.Analytics.customers
    WHERE registration_date >= DATE '2023-01-01'
),
cohort_revenue AS (
    SELECT
        cc.cohort_month,
        DATE_TRUNC('month', o.order_date) AS revenue_month,
        DATEDIFF(MONTH, cc.cohort_month, DATE_TRUNC('month', o.order_date)) AS months_since_acquisition,
        COUNT(DISTINCT cc.customer_id) AS active_customers,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(o.total_amount) AS total_revenue,
        AVG(o.total_amount) AS avg_order_value
    FROM customer_cohorts cc
    INNER JOIN SampleDB.Analytics.orders o
        ON cc.customer_id = o.customer_id
    WHERE o.status = 'COMPLETED'
    GROUP BY cc.cohort_month, DATE_TRUNC('month', o.order_date)
),
cohort_sizes AS (
    SELECT
        cohort_month,
        COUNT(DISTINCT customer_id) AS cohort_size
    FROM customer_cohorts
    GROUP BY cohort_month
)
SELECT
    cr.cohort_month,
    cs.cohort_size,
    cr.months_since_acquisition,
    cr.revenue_month,
    cr.active_customers,
    cr.total_orders,
    cr.total_revenue,
    cr.avg_order_value,
    -- Retention metrics
    CAST((cr.active_customers * 100.0 / cs.cohort_size) AS DECIMAL(10,2)) AS customer_retention_pct,
    CAST((cr.total_revenue / cs.cohort_size) AS DECIMAL(15,2)) AS revenue_per_cohort_customer,
    -- Cumulative metrics by cohort
    SUM(cr.total_revenue) OVER (
        PARTITION BY cr.cohort_month
        ORDER BY cr.months_since_acquisition
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_cohort_revenue,
    SUM(cr.active_customers) OVER (
        PARTITION BY cr.cohort_month
        ORDER BY cr.months_since_acquisition
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_active_customers,
    -- Period-over-period growth
    LAG(cr.total_revenue, 1) OVER (
        PARTITION BY cr.cohort_month
        ORDER BY cr.months_since_acquisition
    ) AS prev_month_revenue,
    CASE
        WHEN LAG(cr.total_revenue, 1) OVER (
            PARTITION BY cr.cohort_month
            ORDER BY cr.months_since_acquisition
        ) > 0 THEN
            CAST(
                ((cr.total_revenue - LAG(cr.total_revenue, 1) OVER (
                    PARTITION BY cr.cohort_month
                    ORDER BY cr.months_since_acquisition
                )) * 100.0 / LAG(cr.total_revenue, 1) OVER (
                    PARTITION BY cr.cohort_month
                    ORDER BY cr.months_since_acquisition
                )) AS DECIMAL(10,2)
            )
        ELSE NULL
    END AS revenue_growth_pct
FROM cohort_revenue cr
INNER JOIN cohort_sizes cs
    ON cr.cohort_month = cs.cohort_month
ORDER BY cr.cohort_month, cr.months_since_acquisition;
