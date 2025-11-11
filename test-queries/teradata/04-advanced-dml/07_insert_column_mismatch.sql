-- Tests INSERT with mismatched column order (known limitation)
-- Expected: Table-level lineage (customers -> customer_archive)
-- Expected: Column-level lineage may be incorrect if order doesn't match

INSERT INTO SampleDB.Analytics.customer_archive
    (customer_id, customer_name, status, email, customer_tier)
SELECT
    customer_id,
    email,              -- Intentionally mismatched order
    customer_tier,      -- Intentionally mismatched order
    customer_name,      -- Intentionally mismatched order
    status              -- Intentionally mismatched order
FROM SampleDB.Analytics.customers
WHERE status = 'INACTIVE';
