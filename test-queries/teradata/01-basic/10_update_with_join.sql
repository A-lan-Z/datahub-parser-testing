-- Tests UPDATE with JOIN
-- Expected: Table-level lineage (orders, customers -> customers)
-- Expected: Column-level lineage for updated columns

UPDATE SampleDB.Analytics.customers c
FROM SampleDB.Analytics.orders o
SET customer_tier = 'GOLD'
WHERE c.customer_id = o.customer_id
  AND o.total_amount > 10000;
