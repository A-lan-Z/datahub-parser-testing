-- UPDATE with JOIN
UPDATE SampleDB.Analytics.customers
FROM SampleDB.Analytics.customer_summary cs
SET
    total_orders = cs.total_orders,
    last_order_date = cs.last_order_date
WHERE customers.customer_id = cs.customer_id
