-- Tests cross-database query
-- Expected: Table-level lineage across different databases
-- Expected: Column-level lineage with database-qualified references

SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    r.region_name,
    r.region_code,
    s.store_name
FROM SampleDB.Analytics.customers c
INNER JOIN ReferenceDB.Geography.regions r
    ON c.region_id = r.region_id
INNER JOIN RetailDB.Stores.store_master s
    ON c.preferred_store_id = s.store_id
WHERE c.status = 'ACTIVE';
