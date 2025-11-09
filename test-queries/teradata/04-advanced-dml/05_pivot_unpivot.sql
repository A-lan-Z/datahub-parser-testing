-- PIVOT operation (Teradata syntax)
SELECT *
FROM (
    SELECT
        product_category,
        order_month,
        total_sales
    FROM SampleDB.Analytics.monthly_product_sales
) AS source_data
PIVOT (
    SUM(total_sales)
    FOR order_month IN ('2024-01', '2024-02', '2024-03', '2024-04', '2024-05', '2024-06')
) AS pivoted_data
