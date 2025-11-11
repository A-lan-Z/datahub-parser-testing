-- Tests recursive CTE for hierarchical data
-- Expected: Table-level lineage (employees -> query result)
-- Expected: Column-level lineage through recursive CTE

WITH RECURSIVE employee_hierarchy AS (
    -- Anchor member: top-level managers
    SELECT
        employee_id,
        employee_name,
        manager_id,
        1 AS level,
        CAST(employee_name AS VARCHAR(1000)) AS hierarchy_path
    FROM SampleDB.Analytics.employees
    WHERE manager_id IS NULL

    UNION ALL

    -- Recursive member: employees with managers
    SELECT
        e.employee_id,
        e.employee_name,
        e.manager_id,
        eh.level + 1,
        CAST(eh.hierarchy_path || ' > ' || e.employee_name AS VARCHAR(1000))
    FROM SampleDB.Analytics.employees e
    INNER JOIN employee_hierarchy eh
        ON e.manager_id = eh.employee_id
)
SELECT
    employee_id,
    employee_name,
    manager_id,
    level,
    hierarchy_path
FROM employee_hierarchy
ORDER BY level, employee_name;
