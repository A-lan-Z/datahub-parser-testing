-- Recursive CTE (organizational hierarchy)
WITH RECURSIVE employee_hierarchy AS (
    -- Anchor member: top-level managers
    SELECT
        employee_id,
        employee_name,
        manager_id,
        1 AS level
    FROM SampleDB.Analytics.employees
    WHERE manager_id IS NULL

    UNION ALL

    -- Recursive member: employees reporting to managers
    SELECT
        e.employee_id,
        e.employee_name,
        e.manager_id,
        eh.level + 1
    FROM SampleDB.Analytics.employees e
    INNER JOIN employee_hierarchy eh
        ON e.manager_id = eh.employee_id
    WHERE eh.level < 10  -- Prevent infinite recursion
)
SELECT
    employee_id,
    employee_name,
    manager_id,
    level
FROM employee_hierarchy
ORDER BY level, employee_name
