-- Tests complex CASE expressions
-- Expected: Table-level lineage (customers, orders -> query result)
-- Expected: Column-level lineage including CASE results

SELECT
    c.customer_id,
    c.customer_name,
    c.total_spent,
    c.order_count,
    CASE
        WHEN c.total_spent > 100000 THEN 'PLATINUM'
        WHEN c.total_spent > 50000 THEN 'GOLD'
        WHEN c.total_spent > 10000 THEN 'SILVER'
        ELSE 'BRONZE'
    END AS customer_tier,
    CASE
        WHEN c.order_count = 0 THEN 'NEVER_ORDERED'
        WHEN c.last_order_date < CURRENT_DATE - INTERVAL '90' DAY THEN 'DORMANT'
        WHEN c.last_order_date < CURRENT_DATE - INTERVAL '30' DAY THEN 'AT_RISK'
        ELSE 'ACTIVE'
    END AS customer_status,
    CASE
        WHEN c.order_count > 0 THEN c.total_spent / c.order_count
        ELSE 0
    END AS avg_order_value
FROM SampleDB.Analytics.customers c;
