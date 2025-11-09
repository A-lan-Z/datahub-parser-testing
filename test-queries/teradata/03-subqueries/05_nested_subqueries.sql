-- Multiple levels of nested subqueries
SELECT
    customer_id,
    customer_name,
    total_spent
FROM SampleDB.Analytics.customers
WHERE customer_id IN (
    SELECT customer_id
    FROM SampleDB.Analytics.orders
    WHERE order_id IN (
        SELECT order_id
        FROM SampleDB.Analytics.order_items
        WHERE product_id IN (
            SELECT product_id
            FROM SampleDB.Analytics.products
            WHERE category = 'Electronics'
            AND unit_price > 500
        )
    )
    AND order_date >= CURRENT_DATE - 180
)
