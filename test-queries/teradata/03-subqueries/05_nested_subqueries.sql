-- Tests nested subqueries (3+ levels deep)
-- Expected: Table-level lineage (customers, orders, order_items -> query result)
-- Expected: Column-level lineage through nested subquery chain

SELECT
    customer_id,
    customer_name,
    email
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
        )
    )
);
