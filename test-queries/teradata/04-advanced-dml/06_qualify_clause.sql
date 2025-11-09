-- QUALIFY clause (Teradata-specific extension)
SELECT
    customer_id,
    order_id,
    order_date,
    total_amount,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn
FROM SampleDB.Analytics.orders
WHERE order_date >= CURRENT_DATE - 365
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) <= 5
