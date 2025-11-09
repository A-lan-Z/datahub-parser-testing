-- Complex CASE expressions
SELECT
    customer_id,
    customer_name,
    total_spent,
    CASE
        WHEN total_spent >= 50000 THEN 'Platinum'
        WHEN total_spent >= 20000 THEN 'Gold'
        WHEN total_spent >= 5000 THEN 'Silver'
        ELSE 'Bronze'
    END AS customer_tier,
    CASE
        WHEN last_order_date >= CURRENT_DATE - 30 THEN 'Active'
        WHEN last_order_date >= CURRENT_DATE - 90 THEN 'Recent'
        WHEN last_order_date >= CURRENT_DATE - 180 THEN 'Dormant'
        ELSE 'Inactive'
    END AS activity_status,
    CASE
        WHEN order_count = 0 THEN NULL
        ELSE total_spent / order_count
    END AS avg_order_value
FROM SampleDB.Analytics.customer_summary
