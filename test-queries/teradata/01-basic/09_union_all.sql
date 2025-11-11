-- Tests UNION ALL combining multiple queries
-- Expected: Table-level lineage (customers, employees -> query result)
-- Expected: Column-level lineage for unified columns

SELECT
    customer_id AS id,
    customer_name AS name,
    email,
    'CUSTOMER' AS source_type
FROM SampleDB.Analytics.customers
UNION ALL
SELECT
    employee_id AS id,
    employee_name AS name,
    CAST(employee_id AS VARCHAR(100)) AS email,
    'EMPLOYEE' AS source_type
FROM SampleDB.Analytics.employees;
