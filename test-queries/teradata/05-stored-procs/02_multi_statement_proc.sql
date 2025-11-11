-- Tests multi-statement procedure (known limitation area)
-- Expected: Partial or no lineage (known limitation)
-- Expected: Parser may struggle with multiple statements

CREATE PROCEDURE SampleDB.Analytics.process_orders()
BEGIN
    -- Delete old temporary data
    DELETE FROM SampleDB.Analytics.temp_orders;

    -- Insert fresh data
    INSERT INTO SampleDB.Analytics.temp_orders
        (order_id, customer_id, order_date, total_amount)
    SELECT
        order_id,
        customer_id,
        order_date,
        total_amount
    FROM SampleDB.Analytics.orders
    WHERE order_date >= CURRENT_DATE - INTERVAL '30' DAY;

    -- Update summary table
    UPDATE SampleDB.Analytics.customer_summary cs
    FROM SampleDB.Analytics.temp_orders t
    SET cs.order_count = cs.order_count + 1,
        cs.total_spent = cs.total_spent + t.total_amount
    WHERE cs.customer_id = t.customer_id;
END;
