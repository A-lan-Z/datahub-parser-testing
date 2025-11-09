-- INSERT with non-matching column list (known limitation)
-- This tests whether parser handles INSERT with different column orders
INSERT INTO SampleDB.Analytics.customer_backup
    (customer_name, customer_id, email)  -- Different order than SELECT
SELECT
    c.customer_id,      -- Position 1
    c.customer_name,    -- Position 2
    c.email            -- Position 3
FROM SampleDB.Analytics.customers c
WHERE c.status = 'ACTIVE'
