-- Tests dynamic SQL with EXECUTE IMMEDIATE (known limitation)
-- Expected: No lineage (cannot parse runtime-evaluated references)
-- Expected: Parser cannot resolve dynamic table references

CREATE PROCEDURE SampleDB.Analytics.dynamic_query(IN table_name VARCHAR(100))
BEGIN
    DECLARE sql_stmt VARCHAR(1000);

    SET sql_stmt = 'SELECT customer_id, customer_name, total_spent FROM SampleDB.Analytics.' || table_name;

    EXECUTE IMMEDIATE sql_stmt;
END;
