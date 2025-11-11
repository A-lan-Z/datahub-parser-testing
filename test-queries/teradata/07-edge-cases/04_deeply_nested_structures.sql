-- Tests deeply nested query structures
-- Expected: Should handle but may reach parser depth limits
-- Expected: Lineage through all nesting levels

SELECT
    outer_query.customer_id,
    outer_query.customer_name,
    outer_query.total_order_value
FROM (
    SELECT
        level3.customer_id,
        level3.customer_name,
        level3.total_order_value
    FROM (
        SELECT
            level2.customer_id,
            level2.customer_name,
            level2.total_order_value
        FROM (
            SELECT
                level1.customer_id,
                level1.customer_name,
                level1.total_order_value
            FROM (
                SELECT
                    c.customer_id,
                    c.customer_name,
                    SUM(o.total_amount) AS total_order_value
                FROM SampleDB.Analytics.customers c
                LEFT JOIN SampleDB.Analytics.orders o
                    ON c.customer_id = o.customer_id
                GROUP BY c.customer_id, c.customer_name
            ) AS level1
            WHERE level1.total_order_value > 1000
        ) AS level2
        WHERE level2.customer_name IS NOT NULL
    ) AS level3
    WHERE level3.total_order_value < 100000
) AS outer_query
ORDER BY outer_query.total_order_value DESC;
