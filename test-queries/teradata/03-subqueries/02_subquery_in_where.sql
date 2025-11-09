-- Subquery in WHERE clause
SELECT
    order_id,
    order_date,
    customer_id,
    total_amount
FROM SampleDB.Analytics.orders
WHERE customer_id IN (
    SELECT customer_id
    FROM SampleDB.Analytics.customers
    WHERE customer_tier = 'Premium'
    AND status = 'ACTIVE'
)
AND total_amount > (
    SELECT AVG(total_amount)
    FROM SampleDB.Analytics.orders
    WHERE order_date >= CURRENT_DATE - 365
)
