-- CTE used in INSERT statement
WITH high_performers AS (
    SELECT
        e.employee_id,
        e.employee_name,
        e.department_id,
        SUM(s.sales_amount) AS total_sales
    FROM SampleDB.Analytics.employees e
    INNER JOIN SampleDB.Analytics.sales s
        ON e.employee_id = s.employee_id
    WHERE s.sale_date >= CURRENT_DATE - 90
    GROUP BY e.employee_id, e.employee_name, e.department_id
    HAVING SUM(s.sales_amount) > 100000
)
INSERT INTO SampleDB.Analytics.bonus_eligible_employees
    (employee_id, employee_name, department_id, quarterly_sales, bonus_amount)
SELECT
    hp.employee_id,
    hp.employee_name,
    hp.department_id,
    hp.total_sales,
    hp.total_sales * 0.05 AS bonus_amount
FROM high_performers hp
