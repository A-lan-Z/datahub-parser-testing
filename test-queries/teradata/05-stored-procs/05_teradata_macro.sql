-- Tests Teradata MACRO (similar to stored procedure)
-- Expected: Table-level lineage (orders, customers -> query result)
-- Expected: May work similar to parameterized query

CREATE MACRO SampleDB.Analytics.get_customer_orders(customer_id_param INT) AS (
    SELECT
        o.order_id,
        o.order_date,
        o.total_amount,
        o.status,
        c.customer_name,
        c.email
    FROM SampleDB.Analytics.orders o
    INNER JOIN SampleDB.Analytics.customers c
        ON o.customer_id = c.customer_id
    WHERE o.customer_id = :customer_id_param
    ORDER BY o.order_date DESC;
);
