# Lineage Report — 20251112_092911

* Source path: `lineage_outputs\20251112_092911`
* Folder flags: [ERR][GAP][LIN][SELF][COL]
* Queries analyzed: 84
* Total parser time: 784.780 ms
* Statement types via parser: 58
* Statement types via fallback: 26
* Parser returned UNKNOWN for 26 statement(s); fallback heuristics attempted.

### Legend

- `Parser` vs `Fallback`: whether the parser supplied the statement type or the regex fallback classifier did.
- Flags: `ERR`=parser/RPC error, `GAP`=missing upstream or downstream lineage, `LIN`=complete table lineage, `SELF`=self-referential lineage, `COL`=column-level lineage detected.

## DataFlow-Backed Emission

- Lineage emission now builds DataFlow/DataJob entities per [DataFlow](https://docs.datahub.com/docs/generated/metamodel/entities/dataflow) and [DataFlow & DataJob tutorials](https://docs.datahub.com/docs/api/tutorials/dataflow-datajob), so every SQL source path becomes its own flow and each statement becomes a job beneath it.
- The new CLI flags `--dataflow-orchestrator`, `--dataflow-cluster`, `--dataflow-prefix`, and `--datajob-type` control the URNs and job metadata that are created before pushing `dataJobInfo` + `dataJobInputOutput` aspects.
- Column-level lineage is attached through each job's `fineGrainedLineages`, while dataset scaffolds are still created automatically when missing so downstream DataJob references resolve cleanly.

## Statement Type Overview

_How many statements of each type we parsed, whether they succeeded, and how long they took._

| Statement Type | Queries | Success | Errors | Success % | Avg ms | P95 ms |
| --- | --- | --- | --- | --- | --- | --- |
| SELECT | 44 | 40 | 4 | 90.9% | 13.70 | 43.58 |
| END | 8 | 0 | 8 | 0.0% | 0.07 | 0.20 |
| INSERT | 5 | 5 | 0 | 100.0% | 12.01 | 28.87 |
| CREATE_PROCEDURE | 3 | 0 | 3 | 0.0% | 0.17 | 0.21 |
| DECLARE | 3 | 0 | 3 | 0.0% | 0.08 | 0.09 |
| UPDATE | 3 | 3 | 0 | 100.0% | 1.90 | 2.53 |
| CREATE_OTHER | 2 | 0 | 2 | 0.0% | 7.08 | 8.88 |
| CREATE_TABLE_AS_SELECT | 2 | 2 | 0 | 100.0% | 40.83 | 44.24 |
| IF | 2 | 0 | 2 | 0.0% | 0.18 | 0.23 |
| CALL | 1 | 0 | 1 | 0.0% | 0.15 | 0.15 |
| CLOSE | 1 | 0 | 1 | 0.0% | 0.15 | 0.15 |
| CREATE_MACRO | 1 | 0 | 1 | 0.0% | 0.22 | 0.22 |
| CREATE_VIEW | 1 | 1 | 0 | 100.0% | 8.97 | 8.97 |
| DELETE | 1 | 0 | 1 | 0.0% | 0.83 | 0.83 |
| ELSE | 1 | 0 | 1 | 0.0% | 0.06 | 0.06 |
| ELSEIF | 1 | 0 | 1 | 0.0% | 0.11 | 0.11 |
| MERGE | 1 | 1 | 0 | 100.0% | 7.62 | 7.62 |
| OPEN | 1 | 0 | 1 | 0.0% | 0.17 | 0.17 |
| READ_LOOP | 1 | 0 | 1 | 0.0% | 0.07 | 0.07 |
| SET | 1 | 0 | 1 | 0.0% | 0.32 | 0.32 |
| UNKNOWN | 1 | 0 | 1 | 0.0% | 0.06 | 0.06 |

## Parser Classification

_Breakdown of how each statement type was classified: parser-provided vs fallback vs still unknown, plus the raw parser labels returned._

| Statement Type | Parser | Fallback | Unresolved | Parser reported |
| --- | --- | --- | --- | --- |
| SELECT | 43 | 1 | 0 | SELECT (43), UNKNOWN (1) |
| END | 0 | 8 | 0 | UNKNOWN (8) |
| INSERT | 5 | 0 | 0 | INSERT (5) |
| CREATE_PROCEDURE | 0 | 3 | 0 | UNKNOWN (3) |
| DECLARE | 0 | 3 | 0 | UNKNOWN (3) |
| UPDATE | 3 | 0 | 0 | UPDATE (3) |
| CREATE_OTHER | 2 | 0 | 0 | CREATE_OTHER (2) |
| CREATE_TABLE_AS_SELECT | 2 | 0 | 0 | CREATE_TABLE_AS_SELECT (2) |
| IF | 0 | 2 | 0 | UNKNOWN (2) |
| CALL | 0 | 1 | 0 | UNKNOWN (1) |
| CLOSE | 0 | 1 | 0 | UNKNOWN (1) |
| CREATE_MACRO | 0 | 1 | 0 | UNKNOWN (1) |
| CREATE_VIEW | 1 | 0 | 0 | CREATE_VIEW (1) |
| DELETE | 1 | 0 | 0 | DELETE (1) |
| ELSE | 0 | 1 | 0 | UNKNOWN (1) |
| ELSEIF | 0 | 1 | 0 | UNKNOWN (1) |
| MERGE | 1 | 0 | 0 | MERGE (1) |
| OPEN | 0 | 1 | 0 | UNKNOWN (1) |
| READ_LOOP | 0 | 1 | 0 | UNKNOWN (1) |
| SET | 0 | 1 | 0 | UNKNOWN (1) |
| UNKNOWN | 0 | 1 | 0 | UNKNOWN (1) |

## Flag Distribution by Statement Type

_This highlights quality signals per statement type. Use it to spot types that systematically have errors, missing lineage, or column-level coverage._

| Statement Type | ERR | GAP | LIN | SELF | COL |
| --- | --- | --- | --- | --- | --- |
| SELECT | 4 | 42 | 1 | 0 | 39 |
| END | 8 | 0 | 0 | 0 | 0 |
| INSERT | 0 | 0 | 5 | 0 | 5 |
| CREATE_PROCEDURE | 3 | 0 | 0 | 0 | 0 |
| DECLARE | 3 | 0 | 0 | 0 | 0 |
| UPDATE | 0 | 0 | 0 | 3 | 3 |
| CREATE_OTHER | 2 | 0 | 2 | 0 | 0 |
| CREATE_TABLE_AS_SELECT | 0 | 0 | 2 | 0 | 2 |
| IF | 2 | 0 | 0 | 0 | 0 |
| CALL | 1 | 0 | 0 | 0 | 0 |
| CLOSE | 1 | 0 | 0 | 0 | 0 |
| CREATE_MACRO | 1 | 0 | 0 | 0 | 0 |
| CREATE_VIEW | 0 | 0 | 1 | 0 | 1 |
| DELETE | 1 | 0 | 1 | 0 | 0 |
| ELSE | 1 | 0 | 0 | 0 | 0 |
| ELSEIF | 1 | 0 | 0 | 0 | 0 |
| MERGE | 0 | 0 | 1 | 0 | 0 |
| OPEN | 1 | 0 | 0 | 0 | 0 |
| READ_LOOP | 1 | 0 | 0 | 0 | 0 |
| SET | 1 | 0 | 0 | 0 | 0 |
| UNKNOWN | 1 | 0 | 0 | 0 | 0 |

## Error Class Distribution by Statement Type

_Summaries of the parser's `debugInfoError` messages. These usually explain why lineage was incomplete for a statement type._

| Statement Type | Error Class | Count |
| --- | --- | --- |
| SELECT | <none> | 40 |
| SELECT | Expecting ) | 1 |
| SELECT | _TableName(database=None, db_schema=None, table='c') | 1 |
| SELECT | _TableName(database=None, db_schema=None, table='employees') | 1 |
| SELECT | _TableName(database=None, db_schema=None, table='o') | 1 |
| END | Can only generate column-level lineage for select-like inner statements, not | 8 |
| INSERT | <none> | 5 |
| CREATE_PROCEDURE | Expecting ) | 2 |
| CREATE_PROCEDURE | Got unsupported syntax for statement | 1 |
| DECLARE | Invalid expression / Unexpected token | 3 |
| UPDATE | <none> | 3 |
| CREATE_OTHER | Can only generate column-level lineage for select-like inner statements, not | 2 |
| CREATE_TABLE_AS_SELECT | <none> | 2 |
| IF | Got unsupported syntax for statement | 2 |
| CALL | Got unsupported syntax for statement | 1 |
| CLOSE | Can only generate column-level lineage for select-like inner statements, not | 1 |
| CREATE_MACRO | Got unsupported syntax for statement | 1 |
| CREATE_VIEW | <none> | 1 |
| DELETE | Can only generate column-level lineage for select-like inner statements, not | 1 |
| ELSE | Invalid expression / Unexpected token | 1 |
| ELSEIF | Invalid expression / Unexpected token | 1 |
| MERGE | <none> | 1 |
| OPEN | Can only generate column-level lineage for select-like inner statements, not | 1 |
| READ_LOOP | Invalid expression / Unexpected token | 1 |
| SET | Can only generate column-level lineage for select-like inner statements, not | 1 |
| UNKNOWN | Invalid expression / Unexpected token | 1 |

## Parser Error Breakdown

_The exact parser error strings returned when a statement failed entirely. Use this to trace concrete failures back to the source SQL._

| Statement Type | Parser Error | Count |
| --- | --- | --- |
| SELECT | <none> | 40 |
| SELECT | Expecting ). Line 12, Col: 68.<br>  ntry<br>FROM SampleDB.Analytics.customers<br>WHERE XMLEXISTS('/customer/preferences[email_opt_in="true"]' PASSING customer_data) | 1 |
| SELECT | _TableName(database=None, db_schema=None, table='c') | 1 |
| SELECT | _TableName(database=None, db_schema=None, table='employees') | 1 |
| SELECT | _TableName(database=None, db_schema=None, table='o') | 1 |
| END | Can only generate column-level lineage for select-like inner statements, not <class 'sqlglot.expressions.Column'> (outer statement type: <class 'sqlglot.expressions.Column'>) | 5 |
| END | Can only generate column-level lineage for select-like inner statements, not <class 'sqlglot.expressions.Alias'> (outer statement type: <class 'sqlglot.expressions.Alias'>) | 3 |
| INSERT | <none> | 5 |
| CREATE_PROCEDURE | Expecting ). Line 5, Col: 71.<br>  t resolve dynamic table references<br><br>CREATE PROCEDURE SampleDB.Analytics.dynamic_query(IN table_name VARCHAR(100))<br>BEGIN<br>    DECLARE sql_stmt VARCHAR(1000) | 1 |
| CREATE_PROCEDURE | Expecting ). Line 5, Col: 81.<br>  s lineage extraction<br><br>CREATE PROCEDURE SampleDB.Analytics.update_customer_tier(IN customer_id_param INT)<br>BEGIN<br>    DECLARE total_spent_var DECIMAL(15,2) | 1 |
| CREATE_PROCEDURE | Got unsupported syntax for statement: -- Tests procedure with LOOP/CURSOR (known limitation area)<br>-- Expected: Limited or no lineage<br>-- Expected: Procedural constructs complicate lineage tracking<br><br>CREATE PROCEDURE SampleDB.Analytics.process_customers_loop()<br>BEGIN<br>    DECLARE done INT DEFAULT FALSE | 1 |
| DECLARE | Invalid expression / Unexpected token. Line 1, Col: 24.<br>  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE | 1 |
| DECLARE | Invalid expression / Unexpected token. Line 1, Col: 25.<br>  DECLARE v_customer_id INT | 1 |
| DECLARE | Invalid expression / Unexpected token. Line 1, Col: 30.<br>  DECLARE customer_cursor CURSOR FOR<br>        SELECT customer_id<br>        FROM SampleDB.Analytics.customers<br>        WHERE status = 'AC | 1 |
| UPDATE | <none> | 3 |
| CREATE_OTHER | Can only generate column-level lineage for select-like inner statements, not <class 'sqlglot.expressions.Delete'> (outer statement type: <class 'sqlglot.expressions.Create'>) | 1 |
| CREATE_OTHER | Can only generate column-level lineage for select-like inner statements, not <class 'sqlglot.expressions.Insert'> (outer statement type: <class 'sqlglot.expressions.Create'>) | 1 |
| CREATE_TABLE_AS_SELECT | <none> | 2 |
| IF | Got unsupported syntax for statement: IF done THEN<br>            LEAVE read_loop | 1 |
| IF | Got unsupported syntax for statement: IF total_spent_var > 100000 THEN<br>        UPDATE SampleDB.Analytics.customers<br>        SET customer_tier = 'PLATINUM'<br>        WHERE customer_id = customer_id_param | 1 |
| CALL | Got unsupported syntax for statement: EXECUTE IMMEDIATE sql_stmt | 1 |
| CLOSE | Can only generate column-level lineage for select-like inner statements, not <class 'sqlglot.expressions.Alias'> (outer statement type: <class 'sqlglot.expressions.Alias'>) | 1 |
| CREATE_MACRO | Got unsupported syntax for statement: -- Tests Teradata MACRO (similar to stored procedure)<br>-- Expected: Table-level lineage (orders, customers -> query result)<br>-- Expected: May work similar to parameterized query<br><br>CREATE MACRO SampleDB.Analytics.get_customer_orders(customer_id_param INT) AS (<br>    SELECT<br>        o.order_id,<br>        o.order_date,<br>        o.total_amount,<br>        o.status,<br>        c.customer_name,<br>        c.email<br>    FROM SampleDB.Analytics.orders o<br>    INNER JOIN SampleDB.Analytics.customers c<br>        ON o.customer_id = c.customer_id<br>    WHERE o.customer_id = :customer_id_param<br>    ORDER BY o.order_date DESC | 1 |
| CREATE_VIEW | <none> | 1 |
| DELETE | Can only generate column-level lineage for select-like inner statements, not <class 'sqlglot.expressions.Delete'> (outer statement type: <class 'sqlglot.expressions.Delete'>) | 1 |
| ELSE | Invalid expression / Unexpected token. Line 1, Col: 4.<br>  ELSE<br>        UPDATE SampleDB.Analytics.customers<br>        SET customer_tier = 'SILVER'<br>        WHERE cust | 1 |
| ELSEIF | Invalid expression / Unexpected token. Line 1, Col: 24.<br>  ELSEIF total_spent_var > 50000 THEN<br>        UPDATE SampleDB.Analytics.customers<br>        SET customer_tier = 'GOLD'<br>        W | 1 |
| MERGE | <none> | 1 |
| OPEN | Can only generate column-level lineage for select-like inner statements, not <class 'sqlglot.expressions.Alias'> (outer statement type: <class 'sqlglot.expressions.Alias'>) | 1 |
| READ_LOOP | Invalid expression / Unexpected token. Line 2, Col: 13.<br>  read_loop: LOOP<br>        FETCH customer_cursor INTO v_customer_id | 1 |
| SET | Can only generate column-level lineage for select-like inner statements, not <class 'sqlglot.expressions.Set'> (outer statement type: <class 'sqlglot.expressions.Set'>) | 1 |
| UNKNOWN | Invalid expression / Unexpected token. Line 1, Col: 1.<br>  ) | 1 |
