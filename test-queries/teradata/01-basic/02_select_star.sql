-- SELECT * (requires schema registration in DataHub)
SELECT *
FROM SampleDB.Analytics.orders
WHERE order_date >= CURRENT_DATE - 30
