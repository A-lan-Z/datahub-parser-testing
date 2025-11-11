-- Tests JSON functions (known limitation area)
-- Expected: May fail or provide partial lineage
-- Expected: JSON path extraction may not be fully supported

SELECT
    customer_id,
    customer_name,
    JSON_EXTRACT(preferences, '$.email_opt_in') AS email_opt_in,
    JSON_EXTRACT(preferences, '$.favorite_category') AS favorite_category,
    JSON_VALUE(metadata, '$.source') AS data_source
FROM SampleDB.Analytics.customers
WHERE JSON_EXTRACT(preferences, '$.active') = 'true';
