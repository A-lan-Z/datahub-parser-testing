-- Tests cross-schema query within same database
-- Expected: Table-level lineage across different schemas
-- Expected: Column-level lineage with schema-qualified references

SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    s.sale_id,
    s.sales_amount,
    s.sale_date
FROM SampleDB.Analytics.customers c
INNER JOIN SampleDB.Sales.sales_transactions s
    ON c.customer_id = s.customer_id
WHERE s.sale_date >= DATE '2024-01-01';
