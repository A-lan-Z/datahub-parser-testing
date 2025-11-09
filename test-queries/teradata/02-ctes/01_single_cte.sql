-- Single CTE
WITH customer_totals AS (
    SELECT
        customer_id,
        SUM(total_amount) AS total_spent,
        COUNT(*) AS order_count
    FROM SampleDB.Analytics.orders
    WHERE order_date >= '2024-01-01'
    GROUP BY customer_id
)
SELECT
    c.customer_name,
    ct.total_spent,
    ct.order_count
FROM customer_totals ct
INNER JOIN SampleDB.Analytics.customers c
    ON ct.customer_id = c.customer_id
WHERE ct.total_spent > 1000
