-- Tests MERGE statement (known limitation: table-level lineage only)
-- Expected: Table-level lineage (customers_staging, customers -> customers)
-- Expected: No column-level lineage (known limitation)

MERGE INTO SampleDB.Analytics.customers AS target
USING SampleDB.Analytics.customers_staging AS source
    ON target.customer_id = source.customer_id
WHEN MATCHED THEN
    UPDATE SET
        customer_name = source.customer_name,
        email = source.email,
        customer_tier = source.customer_tier
WHEN NOT MATCHED THEN
    INSERT (customer_id, customer_name, email, status, customer_tier)
    VALUES (source.customer_id, source.customer_name, source.email, source.status, source.customer_tier);
