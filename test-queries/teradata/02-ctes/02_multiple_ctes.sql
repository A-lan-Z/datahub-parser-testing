-- Multiple CTEs with dependencies
WITH order_totals AS (
    SELECT
        customer_id,
        SUM(total_amount) AS total_spent
    FROM SampleDB.Analytics.orders
    WHERE order_date >= '2024-01-01'
    GROUP BY customer_id
),
product_preferences AS (
    SELECT
        o.customer_id,
        p.category,
        COUNT(*) AS purchase_count
    FROM SampleDB.Analytics.orders o
    INNER JOIN SampleDB.Analytics.order_items oi
        ON o.order_id = oi.order_id
    INNER JOIN SampleDB.Analytics.products p
        ON oi.product_id = p.product_id
    GROUP BY o.customer_id, p.category
),
top_customers AS (
    SELECT
        customer_id,
        total_spent
    FROM order_totals
    WHERE total_spent > 5000
)
SELECT
    c.customer_name,
    tc.total_spent,
    pp.category AS preferred_category,
    pp.purchase_count
FROM top_customers tc
INNER JOIN SampleDB.Analytics.customers c
    ON tc.customer_id = c.customer_id
LEFT JOIN product_preferences pp
    ON tc.customer_id = pp.customer_id
