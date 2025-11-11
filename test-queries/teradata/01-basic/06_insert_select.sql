-- Tests INSERT...SELECT statement
-- Expected: Table-level lineage (customers -> customer_summary)
-- Expected: Column-level lineage for inserted columns

INSERT INTO SampleDB.Analytics.customer_summary
    (customer_id, customer_name, total_spent, order_count, last_order_date)
SELECT
    customer_id,
    customer_name,
    total_spent,
    order_count,
    last_order_date
FROM SampleDB.Analytics.customers
WHERE status = 'ACTIVE';
