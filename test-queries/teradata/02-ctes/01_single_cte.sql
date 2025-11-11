-- Tests single CTE with simple query
-- Expected: Table-level lineage (customers -> query result)
-- Expected: Column-level lineage through CTE

WITH active_customers AS (
    SELECT
        customer_id,
        customer_name,
        email,
        total_spent
    FROM SampleDB.Analytics.customers
    WHERE status = 'ACTIVE'
)
SELECT
    customer_id,
    customer_name,
    email,
    total_spent
FROM active_customers
WHERE total_spent > 1000;
