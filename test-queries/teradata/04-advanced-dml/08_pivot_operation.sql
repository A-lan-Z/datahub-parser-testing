-- Tests PIVOT operation for data transformation
-- Expected: Table-level lineage (orders -> query result)
-- Expected: Column-level lineage for pivoted columns

SELECT *
FROM (
    SELECT
        customer_id,
        EXTRACT(YEAR FROM order_date) AS order_year,
        total_amount
    FROM SampleDB.Analytics.orders
) AS source_data
PIVOT (
    SUM(total_amount)
    FOR order_year IN (2022, 2023, 2024)
) AS pivoted_data;
