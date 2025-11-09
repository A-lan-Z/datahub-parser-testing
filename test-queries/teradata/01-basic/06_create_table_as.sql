-- CREATE TABLE AS SELECT (CTAS)
CREATE TABLE SampleDB.Analytics.high_value_customers AS
SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    c.registration_date,
    SUM(o.total_amount) AS lifetime_value
FROM SampleDB.Analytics.customers c
INNER JOIN SampleDB.Analytics.orders o
    ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name, c.email, c.registration_date
HAVING SUM(o.total_amount) > 10000
WITH DATA
