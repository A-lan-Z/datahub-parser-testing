-- Tests procedure with control flow (IF/ELSE)
-- Expected: Limited or no lineage (known limitation)
-- Expected: Control flow complicates lineage extraction

CREATE PROCEDURE SampleDB.Analytics.update_customer_tier(IN customer_id_param INT)
BEGIN
    DECLARE total_spent_var DECIMAL(15,2);

    SELECT total_spent
    INTO total_spent_var
    FROM SampleDB.Analytics.customers
    WHERE customer_id = customer_id_param;

    IF total_spent_var > 100000 THEN
        UPDATE SampleDB.Analytics.customers
        SET customer_tier = 'PLATINUM'
        WHERE customer_id = customer_id_param;
    ELSEIF total_spent_var > 50000 THEN
        UPDATE SampleDB.Analytics.customers
        SET customer_tier = 'GOLD'
        WHERE customer_id = customer_id_param;
    ELSE
        UPDATE SampleDB.Analytics.customers
        SET customer_tier = 'SILVER'
        WHERE customer_id = customer_id_param;
    END IF;
END;
