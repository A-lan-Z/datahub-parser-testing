-- Tests RIGHT JOIN
-- Expected: Table-level lineage (employees, sales -> query result)
-- Expected: Column-level lineage for all selected columns

SELECT
    e.employee_id,
    e.employee_name,
    s.sale_id,
    s.sales_amount,
    s.sale_date
FROM SampleDB.Analytics.employees e
RIGHT JOIN SampleDB.Analytics.sales s
    ON e.employee_id = s.employee_id;
