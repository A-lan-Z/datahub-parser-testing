-- Tests XML functions (known limitation area)
-- Expected: May fail or provide partial lineage
-- Expected: XML path extraction may not be fully supported

SELECT
    customer_id,
    customer_name,
    XMLEXTRACT(customer_data, '/customer/preferences/email_opt_in') AS email_opt_in,
    XMLEXTRACT(customer_data, '/customer/address/city') AS city,
    XMLEXTRACT(customer_data, '/customer/address/country') AS country
FROM SampleDB.Analytics.customers
WHERE XMLEXISTS('/customer/preferences[email_opt_in="true"]' PASSING customer_data);
