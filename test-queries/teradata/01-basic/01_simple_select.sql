-- Tests simple SELECT with explicit columns
-- Expected: Table-level lineage (customers -> query result)
-- Expected: Column-level lineage for customer_id, customer_name, email

SELECT
    customer_id,
    customer_name,
    email
FROM SampleDB.Analytics.customers
WHERE status = 'ACTIVE';
