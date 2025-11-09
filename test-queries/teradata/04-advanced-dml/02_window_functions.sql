-- Window functions
SELECT
    order_id,
    customer_id,
    order_date,
    total_amount,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS order_rank,
    RANK() OVER (ORDER BY total_amount DESC) AS amount_rank,
    LAG(order_date, 1) OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_order_date,
    LEAD(total_amount, 1) OVER (PARTITION BY customer_id ORDER BY order_date) AS next_order_amount,
    SUM(total_amount) OVER (PARTITION BY customer_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
    AVG(total_amount) OVER (PARTITION BY customer_id ORDER BY order_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg_3
FROM SampleDB.Analytics.orders
WHERE order_date >= CURRENT_DATE - 365
