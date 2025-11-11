-- Tests cross-database query with CTE
-- Expected: Table-level lineage across databases through CTE
-- Expected: Column-level lineage with database-qualified references

WITH regional_customers AS (
    SELECT
        c.customer_id,
        c.customer_name,
        r.region_name,
        r.country_code
    FROM SampleDB.Analytics.customers c
    INNER JOIN ReferenceDB.Geography.regions r
        ON c.region_id = r.region_id
    WHERE r.country_code = 'US'
),
customer_orders AS (
    SELECT
        o.customer_id,
        COUNT(*) AS order_count,
        SUM(o.total_amount) AS total_revenue
    FROM SampleDB.Analytics.orders o
    GROUP BY o.customer_id
)
SELECT
    rc.customer_id,
    rc.customer_name,
    rc.region_name,
    co.order_count,
    co.total_revenue
FROM regional_customers rc
LEFT JOIN customer_orders co
    ON rc.customer_id = co.customer_id;
