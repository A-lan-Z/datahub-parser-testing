-- Simple SELECT with explicit columns
SELECT
    customer_id,
    customer_name,
    email
FROM SampleDB.Analytics.customers
WHERE status = 'ACTIVE'
