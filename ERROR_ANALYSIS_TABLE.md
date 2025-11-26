# Error Analysis Report

**Purpose:** This document provides a detailed analysis of parser errors encountered during the processing of SQL statements. It maps unique error types to their root causes and offers technical explanations to guide resolution efforts.

**Date:** November 26, 2025

---

## 1. Executive Summary

The following table provides a high-level overview of the identified error categories. Detailed analysis for each category follows in Section 2.

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
*   **Technical Explanation:** The `_prepare_query_columns()` function attempts to extract a `SELECT` statement from the parsed Abstract Syntax Tree (AST). This operation is invalid for:
    *   **DDL Operations:** Statements that define structure rather than move data (e.g., `CREATE TABLE` without `AS SELECT`, `DROP`, `ALTER`, `TRUNCATE`).
    *   **Session/Control Statements:** Metadata operations such as `USE`, `SET`, or `COMMIT`/`ROLLBACK`.
    *   **Delete Operations:** `DELETE` statements remove data and do not produce output columns.
    *   **Insert Literals:** `INSERT INTO ... VALUES` statements often lack source column references.
    *   **Locking Statements:** `LOCKING TABLE` statements used solely for concurrency control.
*   **Supported Statements:** `SELECT`, `INSERT INTO ... SELECT`, `CREATE TABLE AS SELECT`, `UPDATE ... FROM`, `MERGE`, and CTEs.

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
*   **Root Cause:** The query uses the Teradata-specific `(NAMED alias)` syntax for column aliasing, which differs from the standard `AS alias`.
*   **Technical Explanation:** The parser interprets the parentheses following a column reference as the start of a function call. When it encounters the `NAMED` keyword instead of a valid function argument, it raises a syntax error expecting a closing parenthesis.

### 7. Teradata REPLACE PROCEDURE
*   **Error Pattern:** `Required keyword: 'definer' missing for <class 'sqlglot.expressions.SqlSecurityProperty'>... SQL SECURITY OWNER`
*   **Root Cause:** A dialect incompatibility exists regarding stored procedure security definitions.
*   **Technical Explanation:** The parser adheres to MySQL grammar conventions for `SQL SECURITY`, which expect `DEFINER` or `INVOKER`. Teradata, however, allows `OWNER`, `CREATOR`, or `DEFINER`. The presence of `OWNER` causes a validation failure in the `SqlSecurityProperty` node.

### 8. Unsupported DDL Syntax
*   **Error Pattern:** `Got unsupported syntax for statement: [DDL_statement]`
*   **Root Cause:** The statement type (e.g., `ALTER PROCEDURE`, `CREATE DATABASE`) is recognized, but specific dialect options or directives are not supported.
*   **Technical Explanation:** This distinction differs from a "Syntax Error." The parser correctly identifies the high-level statement but fails to parse specific clauses, such as `COMPILE` directives in `ALTER PROCEDURE` or storage options in `CREATE DATABASE`. These are administrative commands and do not impact data lineage.

### 9. Duplicate Alias
*   **Error Pattern:** `sqlglot failed to map columns to their source tables... Alias already used: [alias]`
*   **Root Cause:** The query reuses the same table alias multiple times within the same scope, creating ambiguity for column resolution.
*   **Technical Explanation:** The scope builder maintains a symbol table mapping aliases to tables. When a duplicate alias is registered (e.g., in self-joins or nested subqueries without unique aliasing), the parser cannot deterministically resolve column references to their source, resulting in a semantic error.

### 10. Schema Nesting Level
*   **Error Pattern:** `Table [name] must match the schema's nesting level: 3.`
*   **Root Cause:** The table reference provided has only 2 levels (e.g., `database.table`), but the system configuration requires fully qualified names with 3 levels (e.g., `catalog.database.table`).
*   **Technical Explanation:** DataHub enforces a standard 3-level naming convention for cross-platform consistency. Teradata queries often use a 2-level format (`database.table`). This error indicates a need for a default catalog/database configuration to normalize these references.

### 11. Expression Type Mismatch
*   **Error Pattern:** `* is not <class 'sqlglot.expressions.Alias'>.`
*   **Root Cause:** The lineage analyzer expects all expressions in the projection list to be `Alias` nodes, but encounters an unexpanded wildcard (`*`).
*   **Technical Explanation:** Typically, `*` is expanded into individual columns during the optimization phase. In complex queries involving nested subqueries, unions, or joins where schema information is missing or ambiguous, this expansion may fail. The lineage analyzer then encounters a raw `Star` node instead of an `Alias`, causing a type check failure.

### 12. Syntax Error (Parentheses)
*   **Error Pattern:** `Expecting ). Line [X], Col: [Y].`
*   **Root Cause:** The SQL query contains unbalanced parentheses.
*   **Technical Explanation:** This is a standard syntax error. It typically occurs in deeply nested function calls, complex `CASE` expressions, or subqueries where a closing parenthesis is missing or misplaced.

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

---

## 4. Recommendations

### 4.1. Strategic Actions
1.  **Filter Expected Errors:** Explicitly exclude "Column Lineage Limitation" errors from failure reports, as these are design constraints, not bugs.
2.  **Pre-Processing Pipeline:** Implement a text-processing layer to standardize Teradata SQL before parsing. This should handle:
    *   Removal of `USING` clauses.
    *   Removal of `LOCKING` clauses.
    *   Expansion of `CT` to `CREATE TABLE`.
    *   Standardization of `(NAMED alias)` to `AS alias`.
3.  **Configuration Updates:** Ensure the parser configuration includes `default_db` and `default_schema` parameters to resolve nesting level errors.

### 4.2. Reporting Improvements
*   Implement granular error categorization to automatically classify errors into "Actionable," "Expected," or "Dialect-Unsupported" buckets.