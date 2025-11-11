-- Tests default schema behavior with unqualified table names
-- Expected: Table-level lineage using default database and schema
-- Expected: Proper resolution based on --default-db and --default-schema params

SELECT
    customer_id,
    customer_name,
    email,
    total_spent,
    order_count
FROM customers
WHERE status = 'ACTIVE'
  AND total_spent > 10000;
