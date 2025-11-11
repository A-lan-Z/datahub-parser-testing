-- Tests procedure with LOOP/CURSOR (known limitation area)
-- Expected: Limited or no lineage
-- Expected: Procedural constructs complicate lineage tracking

CREATE PROCEDURE SampleDB.Analytics.process_customers_loop()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE v_customer_id INT;
    DECLARE customer_cursor CURSOR FOR
        SELECT customer_id
        FROM SampleDB.Analytics.customers
        WHERE status = 'ACTIVE';
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN customer_cursor;

    read_loop: LOOP
        FETCH customer_cursor INTO v_customer_id;
        IF done THEN
            LEAVE read_loop;
        END IF;

        UPDATE SampleDB.Analytics.customer_summary
        SET last_processed_date = CURRENT_DATE
        WHERE customer_id = v_customer_id;
    END LOOP;

    CLOSE customer_cursor;
END;
