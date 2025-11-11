-- Tests DELETE with subquery
-- Expected: Table-level lineage (orders, customers -> customers)
-- Expected: Tracks dependency on orders table

DELETE FROM SampleDB.Analytics.customers
WHERE customer_id IN (
    SELECT customer_id
    FROM SampleDB.Analytics.orders
    WHERE order_date < DATE '2020-01-01'
);
