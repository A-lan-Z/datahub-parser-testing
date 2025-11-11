-- Tests SELECT * (requires schema registration in DataHub)
-- Expected: Table-level lineage (customers -> query result)
-- Expected: Column-level lineage for all columns in customers table

SELECT *
FROM SampleDB.Analytics.customers
WHERE registration_date >= DATE '2024-01-01';
