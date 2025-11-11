-- Tests UNNEST construct (known limitation)
-- Expected: May fail or provide incomplete lineage
-- Expected: Array/collection operations may not be fully supported

SELECT
    c.customer_id,
    c.customer_name,
    tag_value
FROM SampleDB.Analytics.customers c
CROSS JOIN UNNEST(c.customer_tags) AS t(tag_value)
WHERE c.status = 'ACTIVE';
