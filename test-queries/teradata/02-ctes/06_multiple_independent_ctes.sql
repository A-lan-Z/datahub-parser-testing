-- Tests multiple independent CTEs joined in final query
-- Expected: Table-level lineage (customers, orders, products -> query result)
-- Expected: Column-level lineage from multiple independent CTEs

WITH customer_stats AS (
    SELECT
        customer_id,
        COUNT(*) AS lifetime_orders,
        SUM(total_amount) AS lifetime_value
    FROM SampleDB.Analytics.orders
    GROUP BY customer_id
),
product_stats AS (
    SELECT
        product_id,
        AVG(unit_price) AS avg_price,
        SUM(quantity) AS total_sold
    FROM SampleDB.Analytics.order_items
    GROUP BY product_id
),
customer_info AS (
    SELECT
        customer_id,
        customer_name,
        email,
        customer_tier
    FROM SampleDB.Analytics.customers
)
SELECT
    ci.customer_id,
    ci.customer_name,
    ci.customer_tier,
    cs.lifetime_orders,
    cs.lifetime_value
FROM customer_info ci
LEFT JOIN customer_stats cs
    ON ci.customer_id = cs.customer_id;
