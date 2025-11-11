-- Tests very long query with many columns and complex expressions
-- Expected: Should handle but may have performance impact
-- Expected: Column-level lineage should track all columns

SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    c.status,
    c.customer_tier,
    c.registration_date,
    c.total_spent,
    c.last_order_date,
    c.order_count,
    o.order_id,
    o.order_date,
    o.total_amount AS order_total,
    o.status AS order_status,
    oi.order_item_id,
    oi.quantity,
    oi.unit_price,
    oi.quantity * oi.unit_price AS line_item_total,
    p.product_id,
    p.product_name,
    p.category,
    p.description,
    p.unit_price AS product_unit_price,
    CASE WHEN c.customer_tier = 'PLATINUM' THEN oi.quantity * oi.unit_price * 0.80
         WHEN c.customer_tier = 'GOLD' THEN oi.quantity * oi.unit_price * 0.85
         WHEN c.customer_tier = 'SILVER' THEN oi.quantity * oi.unit_price * 0.90
         ELSE oi.quantity * oi.unit_price END AS discounted_total,
    CURRENT_DATE AS report_date,
    CURRENT_TIMESTAMP AS report_timestamp,
    EXTRACT(YEAR FROM o.order_date) AS order_year,
    EXTRACT(MONTH FROM o.order_date) AS order_month,
    EXTRACT(DAY FROM o.order_date) AS order_day,
    DAYOFWEEK(o.order_date) AS order_day_of_week,
    DATEDIFF(DAY, o.order_date, CURRENT_DATE) AS days_since_order,
    ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY o.order_date DESC) AS customer_order_seq,
    RANK() OVER (PARTITION BY p.category ORDER BY oi.quantity * oi.unit_price DESC) AS category_sales_rank,
    SUM(oi.quantity * oi.unit_price) OVER (PARTITION BY c.customer_id) AS customer_total_value,
    AVG(oi.quantity * oi.unit_price) OVER (PARTITION BY c.customer_id) AS customer_avg_line_value,
    COUNT(*) OVER (PARTITION BY c.customer_id) AS customer_line_count
FROM SampleDB.Analytics.customers c
INNER JOIN SampleDB.Analytics.orders o
    ON c.customer_id = o.customer_id
INNER JOIN SampleDB.Analytics.order_items oi
    ON o.order_id = oi.order_id
INNER JOIN SampleDB.Analytics.products p
    ON oi.product_id = p.product_id
WHERE c.status = 'ACTIVE'
  AND o.order_date >= DATE '2024-01-01'
  AND p.category IN ('Electronics', 'Home & Garden', 'Clothing')
ORDER BY c.customer_id, o.order_date DESC;
