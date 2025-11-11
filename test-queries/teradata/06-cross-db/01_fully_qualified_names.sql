-- Tests fully qualified three-part names (database.schema.table)
-- Expected: Table-level lineage with correct database resolution
-- Expected: Column-level lineage with fully qualified references

SELECT
    c.customer_id,
    c.customer_name,
    o.order_id,
    o.order_date,
    o.total_amount
FROM SampleDB.Analytics.customers c
INNER JOIN SampleDB.Analytics.orders o
    ON c.customer_id = o.customer_id
WHERE o.order_date >= DATE '2024-01-01';
