-- Tests UNION (DISTINCT) to remove duplicates
-- Expected: Table-level lineage (customers from multiple filters -> query result)
-- Expected: Column-level lineage for deduplicated columns

SELECT customer_id, customer_name
FROM SampleDB.Analytics.customers
WHERE customer_tier = 'GOLD'
UNION
SELECT customer_id, customer_name
FROM SampleDB.Analytics.customers
WHERE total_spent > 50000;
