-- DELETE with subquery
DELETE FROM SampleDB.Analytics.old_orders
WHERE order_id IN (
    SELECT order_id
    FROM SampleDB.Analytics.archived_orders
    WHERE archive_date < CURRENT_DATE - 365
)
