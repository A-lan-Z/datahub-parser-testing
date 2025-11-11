-- Tests CREATE VIEW statement
-- Expected: Table-level lineage (customers, orders -> customer_order_view)
-- Expected: Column-level lineage for view definition

CREATE VIEW SampleDB.Analytics.customer_order_view AS
SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    o.order_id,
    o.order_date,
    o.total_amount,
    o.status
FROM SampleDB.Analytics.customers c
LEFT JOIN SampleDB.Analytics.orders o
    ON c.customer_id = o.customer_id;
