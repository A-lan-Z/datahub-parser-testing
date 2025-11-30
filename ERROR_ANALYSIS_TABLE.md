# Error Analysis Report

**Purpose:** This document provides a detailed analysis of parser errors encountered during the processing of SQL statements. It maps unique error types to their root causes.

**Date:** November 26, 2025

---

## 1. Summary

| ID | Error Category | Error Pattern Summary | Count |
|:---|:---|:---|:---|
| 1 | Column Lineage Limitation | `Can only generate column-level lineage for select-like inner statements...` | 259 |
| 2 | Teradata USING Clause | `Invalid expression / Unexpected token... USING...` | 66 |
| 3 | Teradata LOCKING Clause | `Expected SELECT statement after LOCKING clause...` | 23 |
| 4 | Abbreviated CREATE TABLE | `Invalid expression... CT [table_name]...` | 18 |
| 5 | INSERT VALUES Scope Error | `Failed to build scope for statement - scope was empty...` | 22 |
| 6 | Teradata NAMED Keyword | `Expecting )... (NAMED alias)...` | 3 |
| 7 | Teradata REPLACE PROCEDURE | `Required keyword: 'definer' missing... SQL SECURITY OWNER` | 35 |
| 8 | Unsupported DDL Syntax | `Got unsupported syntax for statement...` | 24 |
| 9 | Duplicate Alias | `Alias already used: [alias]` | 10 |
| 10 | Schema Nesting Level | `Table [name] must match the schema's nesting level: 3` | 7 |
| 11 | Expression Type Mismatch | `* is not <class 'sqlglot.expressions.Alias'>` | 9 |
| 12 | Syntax Error (Parentheses) | `Expecting )...` | 12 |
| 13 | Invalid Teradata Syntax | `Invalid expression... SELECT SESSION` | 1 |
| 14 | Invalid MODIFY USER | `Invalid expression... Modify User...` | 1 |

---

## 2. Detailed Error Analysis

### 1. Column Lineage Limitation
*   **Error Pattern:** `Can only generate column-level lineage for select-like inner statements, not <class 'sqlglot.expressions.[Type]'>`
*   **Root Cause:** DataHub's column-level lineage extraction requires SQL statements that explicitly read source columns and project destination columns. This error is raised for statement types that do not support this paradigm.
*   **Error Source:** This is a **DataHub validation error** (raised during lineage analysis), not a SQLglot parsing error. The underlying parser successfully parses the SQL; DataHub's lineage layer cannot extract column-level lineage from certain statement types.
*   **Technical Explanation:** DataHub's `_column_level_lineage()` function attempts to extract a `SELECT` statement from the parsed Abstract Syntax Tree (AST). This operation is invalid for:
    *   **DDL Operations:** Statements that define structure rather than move data (e.g., `CREATE TABLE` without `AS SELECT`, `DROP`, `ALTER`, `TRUNCATE`).
    *   **Session/Control Statements:** Metadata operations such as `USE`, `SET`, or `COMMIT`/`ROLLBACK`.
    *   **Delete Operations:** `DELETE` statements remove data and do not produce output columns. Note: Table-level lineage showing the deleted-from table is still generated, but column-level lineage is not possible.
    *   **Insert Literals:** `INSERT INTO ... VALUES` statements often lack source column references.
    *   **Locking Statements:** `LOCKING TABLE` statements used solely for concurrency control.
*   **Supported Statements:** `SELECT`, `INSERT INTO ... SELECT`, `CREATE TABLE AS SELECT`, `UPDATE ... FROM`, and CTEs.
*   **Note:** While `MERGE` statements are recognized by the parser, DataHub **does not generate column-level lineage** for MERGE INTO statements. Table-level lineage only.

### 2. Teradata USING Clause
*   **Error Pattern:** `Invalid expression / Unexpected token. Line 1, Col: 5. USING [parameter_declarations] [SQL_statement]`
*   **Root Cause:** The parser encounters a Teradata-specific `USING` clause, which declares parameters for dynamic SQL and stored procedures. This syntax is non-standard and not recognized by the parser.
*   **Technical Explanation:** The parser fails because:
    1.  **Token Recognition:** It expects standard SQL constructs (like a `JOIN` condition) after the `USING` keyword.
    2.  **Type Declarations:** It encounters syntax like `param_name(DATATYPE)` which is invalid in this context for standard SQL.
*   **Observed Parameter Patterns:** System parameters, timestamp variables, complex types, and quoted identifiers.

### 3. Teradata LOCKING Clause
*   **Error Pattern:** `Expected SELECT statement after LOCKING clause. Line X, Col: Y.`
*   **Root Cause:** The parser supports the Teradata `LOCKING` clause but enforces a strict grammar requiring an immediate `SELECT` statement following the lock definition.
*   **Technical Explanation:** The parsing error occurs in three scenarios:
    1.  **Chained Locks:** Multiple `LOCKING` statements appear sequentially (e.g., locking multiple tables). The parser expects a `SELECT` after the first one.
    2.  **Non-SELECT Operations:** The `LOCKING` clause is followed by `DELETE`, `INSERT`, or `UPDATE` instead of `SELECT`.
    3.  **Malformed Syntax:** The query is truncated or missing the subsequent statement entirely.

### 4. Abbreviated CREATE TABLE
*   **Error Pattern:** `Invalid expression / Unexpected token. Line 1, Col: 15. CT [table_name] ,FALLBACK (...)`
*   **Root Cause:** The query uses `CT` as a non-standard abbreviation for `CREATE TABLE`.
*   **Technical Explanation:** `CT` is not a valid SQL keyword. The parser interprets it as an identifier or unknown token. These abbreviations (including `AT` for `ALTER TABLE`, `CV` for `CREATE VIEW`) are typically client-side macros or specific to certain utility logs and are not valid SQL syntax.

### 5. INSERT VALUES Scope Error
*   **Error Pattern:** `Failed to build scope for statement - scope was empty: INSERT INTO [table] (...) VALUES (...)`
*   **Root Cause:** The scope builder cannot establish a lineage context because the `INSERT` statement relies solely on literal values rather than source tables.
*   **Technical Explanation:** Column-level lineage requires mapping a source column to a destination column. In `INSERT ... VALUES` statements containing only literals (e.g., `VALUES (1, 'text')`), there are no source table references. Consequently, the scope builder returns an empty set, raising an error. This is expected behavior for literal insertions.

### 6. Teradata NAMED Keyword
*   **Error Pattern:** `Expecting ). Line 1, Col: [X]. [context] (NAMED alias) [more context]`
*   **Root Cause:** The query uses the Teradata-specific `(NAMED alias)` syntax for column aliasing or character set conversion, which differs from the standard `AS alias`.
*   **Technical Explanation:** The parser interprets the parentheses following a column reference as the start of a function call. When it encounters the `NAMED` keyword instead of a valid function argument, it raises a syntax error expecting a closing parenthesis.
*   **Note:** Support for the NAMED keyword has been added to SQLglot (as of Issue #4380) for Teradata character set conversion syntax (e.g., `CONVERT(expr USING charset)`). However, older versions or specific usage patterns may still trigger this error.

### 7. Teradata REPLACE PROCEDURE
*   **Error Pattern:** `Required keyword: 'definer' missing for <class 'sqlglot.expressions.SqlSecurityProperty'>... SQL SECURITY OWNER`
*   **Root Cause:** A dialect incompatibility exists regarding stored procedure security definitions.
*   **Technical Explanation:** The parser adheres to MySQL grammar conventions for `SQL SECURITY`, which expect `DEFINER` or `INVOKER`. Teradata, however, allows `OWNER`, `CREATOR`, or `DEFINER`. The presence of `OWNER` causes a validation failure in the `SqlSecurityProperty` node.

### 8. Unsupported DDL Syntax
*   **Error Pattern:** `Got unsupported syntax for statement: [DDL_statement]`
*   **Root Cause:** The statement type (e.g., `ALTER PROCEDURE`, `CREATE DATABASE`) is recognized, but specific dialect options or directives are not supported.
*   **Technical Explanation:** This distinction differs from a "Syntax Error." The parser correctly identifies the high-level statement but fails to parse specific clauses, such as `COMPILE` directives in `ALTER PROCEDURE` or storage options in `CREATE DATABASE`. These are administrative commands and do not impact data lineage.

### 9. Duplicate Alias / Column Mapping Failures
*   **Error Pattern:** `sqlglot failed to map columns to their source tables; likely missing/outdated table definitions` or `Alias already used: [alias]`
*   **Root Cause:** The scope builder cannot establish complete column-to-table mappings due to either duplicate aliases, complex subqueries, or missing schema information.
*   **Technical Explanation:** The scope builder maintains a symbol table mapping aliases to tables and columns to their sources. When this mapping fails—whether due to duplicate aliases, complex nested subqueries, or missing schema metadata—the parser cannot deterministically resolve column references, resulting in a semantic error.
*   **Example from Real Data:**
    ```sql
    UPDATE source_table st
    FROM source_table st
    SET field1 = (SELECT date_col AS date_alias FROM temp_table WHERE condition = true)
    WHERE st.id > 0
    ```
    *Error Message:* `sqlglot failed to map columns to their source tables; likely missing/outdated table definitions`
    *Note:* The UPDATE statement with a complex nested subquery in the SET clause makes it difficult for the scope builder to establish unambiguous column mappings, particularly because the subquery's output columns (date_alias) don't clearly map to the target table's columns.

### 10. Schema Nesting Level
*   **Error Pattern:** `Table [name] must match the schema's nesting level: 3.`
*   **Root Cause:** The table reference provided has only 2 levels (e.g., `database.table`), but the system configuration requires fully qualified names with 3 levels (e.g., `catalog.database.table`).
*   **Technical Explanation:** DataHub enforces a standard 3-level naming convention for cross-platform consistency. Teradata queries often use a 2-level format (`database.table`). This error is triggered specifically when **CREATE VIEW or CREATE TABLE statements** define output objects with 2-level names. Unlike SELECT queries where output validation may be deferred, DDL statements immediately validate the fully qualified name of the persistent object being created.
*   **Example from Real Data:**
    ```sql
    CREATE VIEW db_alias.view_name AS
    SELECT col_a, col_b, col_c
    FROM source_db.table_source s1
    JOIN source_db.table_history s2
        ON s1.id = s2.id
    ```
    *Error Message:* `Table db_alias.view_name must match the schema's nesting level: 3`
    *Note:* The CREATE VIEW statement defines an output with 2-level qualification. The parser validates this immediately and requires 3-level naming (e.g., `catalog.db_alias.view_name`).
*   **Resolution:** Either specify a 3-level name in the CREATE VIEW/TABLE statement or configure `--default-catalog` parameter to auto-expand 2-level names.

### 11. Expression Type Mismatch
*   **Error Pattern:** `* is not <class 'sqlglot.expressions.Alias'>.`
*   **Root Cause:** The lineage analyzer expects all expressions in the projection list to be `Alias` nodes, but encounters an unexpanded wildcard (`*`).
*   **Technical Explanation:** Typically, `*` is expanded into individual columns during the optimization phase. In complex queries involving nested subqueries, unions, or joins where schema information is missing or ambiguous, this expansion may fail. The lineage analyzer then encounters a raw `Star` node instead of an `Alias`, causing a type check failure.
*   **Example from Real Data:**
    ```sql
    SELECT *
    FROM source_schema.error_tracking_table
    ORDER BY 1
    ```
    *Error Message:* `* is not <class 'sqlglot.expressions.Alias'>`
    *Note:* The parser cannot expand the wildcard into individual columns, possibly due to missing schema metadata for the source table. This prevents column-level lineage extraction.
*   **Resolution:** Either specify explicit columns (`SELECT col_a, col_b, col_c`) or ensure the source table schema is registered in DataHub metadata.

### 12. Syntax Error (Parentheses)
*   **Error Pattern:** `Expecting ). Line [X], Col: [Y].`
*   **Root Cause:** The SQL query contains unbalanced parentheses.
*   **Technical Explanation:** This is a standard syntax error. It typically occurs in deeply nested function calls, complex `CASE` expressions, or subqueries where a closing parenthesis is missing or misplaced.
*   **Example from Real Data:**
    ```sql
    UPDATE query_log
    SET duration_field = CASE WHEN first_timestamp
                        THEN 0 ELSE (EXTRACT(DAY FROM (first_timestamp - start_timestamp DAY TO SECOND))*86400
                        + EXTRACT(HOUR FROM (first_timestamp - start_timestamp))*3600
                        + EXTRACT(MINUTE FROM (first_timestamp - start_timestamp DAY TO SECOND))*60
                        + EXTRACT(SECOND FROM (first_timestamp - start_timestamp DAY TO SECOND))
                        END
    WHERE first_timestamp > ? AND first_timestamp < ?
    ```
    *Error Message:* `Expecting ) Line 3, Col: 18`
    *Note:* The CASE expression contains unbalanced parentheses in the EXTRACT function calls and condition evaluation. Line breaks and escape sequences may obscure the actual syntax structure.

### 13. Invalid Teradata Syntax
*   **Error Pattern:** `Invalid expression / Unexpected token... SELECT SESSION`
*   **Root Cause:** The statement `SELECT SESSION` appears incomplete or invalid in the provided context.
*   **Technical Explanation:** While `SELECT SESSION` might be valid in specific contexts (e.g., retrieving session info), the parser expects standard projection clauses (columns, functions, literals) following `SELECT`. Used in isolation or incorrectly, it is treated as an invalid token.

### 14. Invalid MODIFY USER
*   **Error Pattern:** `Invalid expression / Unexpected token... Modify User...`
*   **Root Cause:** The query uses `MODIFY USER`, which is non-standard syntax.
*   **Technical Explanation:** Standard SQL uses `ALTER USER` for user management. `MODIFY` is not a recognized DDL keyword, causing the parser to fail.

---

## 3. Categorization and Statistics

| Category | Description | Count | Percentage |
|:---|:---|:---|:---|
| **Expected Limitations** | DDL, DELETE, USE commands (No column lineage possible) | 259 | 52.9% |
| **Teradata Dialect** | `USING`, `LOCKING`, `REPLACE PROCEDURE`, `NAMED`, `CT` | 145 | 29.6% |
| **Configuration** | Schema nesting, Duplicate aliases | 17 | 3.5% |
| **Syntax/Invalid SQL** | Parentheses, Invalid keywords | 14 | 2.9% |
| **System Limits** | Insert Values scope, Wildcard expansion | 31 | 6.3% |
| **Unsupported DDL** | `ALTER PROCEDURE`, `CREATE DATABASE`, `CREATE USER` | 24 | 4.9% |
| **Total** | | **490** | **100%** |
