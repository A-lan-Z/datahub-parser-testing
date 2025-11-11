-- Real-world example: Sales performance dashboard ETL
-- Business context: Daily sales metrics aggregation for BI reporting
-- Expected: Table-level lineage (orders, order_items, products, employees -> sales_daily_metrics)
-- Expected: Column-level lineage with complex aggregations and window functions

INSERT INTO SampleDB.Analytics.sales_daily_metrics
    (report_date, product_category, total_orders, total_revenue, total_units_sold,
     avg_order_value, unique_customers, revenue_rank, revenue_pct_of_total)
WITH daily_category_sales AS (
    SELECT
        CAST(o.order_date AS DATE) AS report_date,
        p.category AS product_category,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(oi.quantity * oi.unit_price) AS total_revenue,
        SUM(oi.quantity) AS total_units_sold,
        AVG(o.total_amount) AS avg_order_value,
        COUNT(DISTINCT o.customer_id) AS unique_customers
    FROM SampleDB.Analytics.orders o
    INNER JOIN SampleDB.Analytics.order_items oi
        ON o.order_id = oi.order_id
    INNER JOIN SampleDB.Analytics.products p
        ON oi.product_id = p.product_id
    WHERE o.status = 'COMPLETED'
      AND o.order_date >= CURRENT_DATE - INTERVAL '7' DAY
    GROUP BY CAST(o.order_date AS DATE), p.category
),
daily_totals AS (
    SELECT
        report_date,
        SUM(total_revenue) AS day_total_revenue
    FROM daily_category_sales
    GROUP BY report_date
)
SELECT
    dcs.report_date,
    dcs.product_category,
    dcs.total_orders,
    dcs.total_revenue,
    dcs.total_units_sold,
    dcs.avg_order_value,
    dcs.unique_customers,
    RANK() OVER (
        PARTITION BY dcs.report_date
        ORDER BY dcs.total_revenue DESC
    ) AS revenue_rank,
    CASE
        WHEN dt.day_total_revenue > 0 THEN
            (dcs.total_revenue / dt.day_total_revenue) * 100
        ELSE 0
    END AS revenue_pct_of_total
FROM daily_category_sales dcs
INNER JOIN daily_totals dt
    ON dcs.report_date = dt.report_date;
