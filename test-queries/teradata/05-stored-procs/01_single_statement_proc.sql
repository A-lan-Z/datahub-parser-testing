-- Tests single-statement procedure
-- Expected: Table-level lineage (customers -> customer_summary)
-- Expected: Column-level lineage may work for simple single-statement procedures

CREATE PROCEDURE SampleDB.Analytics.update_customer_summary()
BEGIN
    INSERT INTO SampleDB.Analytics.customer_summary
        (customer_id, customer_name, total_spent, order_count, last_order_date)
    SELECT
        customer_id,
        customer_name,
        total_spent,
        order_count,
        last_order_date
    FROM SampleDB.Analytics.customers
    WHERE status = 'ACTIVE';
END;
