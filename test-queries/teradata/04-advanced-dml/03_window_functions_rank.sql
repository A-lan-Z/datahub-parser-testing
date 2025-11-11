-- Tests window functions RANK and DENSE_RANK
-- Expected: Table-level lineage (customers -> query result)
-- Expected: Column-level lineage including window function results

SELECT
    customer_id,
    customer_name,
    total_spent,
    RANK() OVER (ORDER BY total_spent DESC) AS spending_rank,
    DENSE_RANK() OVER (ORDER BY total_spent DESC) AS dense_spending_rank,
    NTILE(10) OVER (ORDER BY total_spent DESC) AS spending_decile
FROM SampleDB.Analytics.customers
WHERE status = 'ACTIVE';
