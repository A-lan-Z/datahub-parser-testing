-- Tests dynamic identifier functions (known limitation)
-- Expected: Will likely fail - cannot resolve runtime table names
-- Expected: Parser cannot handle identifier() or similar dynamic functions

SELECT
    customer_id,
    customer_name,
    total_spent
FROM identifier('SampleDB.Analytics.customers')
WHERE status = 'ACTIVE';
