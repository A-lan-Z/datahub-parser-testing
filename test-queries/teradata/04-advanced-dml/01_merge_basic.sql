-- MERGE statement (basic)
MERGE INTO SampleDB.Analytics.customer_summary AS target
USING (
    SELECT
        customer_id,
        COUNT(*) AS order_count,
        SUM(total_amount) AS total_spent,
        MAX(order_date) AS last_order_date
    FROM SampleDB.Analytics.orders
    WHERE order_date >= CURRENT_DATE - 30
    GROUP BY customer_id
) AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN
    UPDATE SET
        order_count = target.order_count + source.order_count,
        total_spent = target.total_spent + source.total_spent,
        last_order_date = source.last_order_date
WHEN NOT MATCHED THEN
    INSERT (customer_id, order_count, total_spent, last_order_date)
    VALUES (source.customer_id, source.order_count, source.total_spent, source.last_order_date)
