# DataHub SQL Parser & SQLglot Error Catalogue

**Version:** 1.0
**Last Updated:** 2025-11-26
**Purpose:** Comprehensive reference guide for all DataHub SQL parser and sqlglot error messages, their causes, and resolutions.

---

## Table of Contents

1. [Overview](#overview)
2. [Error Classification System](#error-classification-system)
3. [SQLglot Core Errors](#sqlglot-core-errors)
4. [DataHub Parser Errors](#datahub-parser-errors)
5. [Statement-Specific Errors](#statement-specific-errors)
6. [Dialect-Specific Errors](#dialect-specific-errors)
7. [Troubleshooting Guide](#troubleshooting-guide)
8. [References](#references)

---

## Overview

The DataHub SQL parser is built on [sqlglot](https://github.com/tobymao/sqlglot), a Python SQL parser and transpiler. Errors can originate from:

1. **SQLglot Parser Layer** - Syntax parsing and tokenization
2. **DataHub Lineage Layer** - Column-level lineage generation
3. **Scope Building Layer** - Schema and column resolution
4. **Dialect Layer** - Database-specific syntax support

### Error Flow

```
SQL Query → SQLglot Parser → DataHub Lineage Analyzer → Result
              ↓                    ↓
         ParseError          SqlUnderstandingError
                              UnsupportedStatementTypeError
```

---

## Error Classification System

### Error Levels (SQLglot)

| Level | Behavior | Use Case |
|-------|----------|----------|
| `IGNORE` | Ignore all errors | Permissive parsing |
| `WARN` | Log errors, continue | Debugging |
| `RAISE` | Collect all errors, raise once | Batch processing |
| `IMMEDIATE` | Raise on first error | Fail-fast |

### Error Types (DataHub)

| Type | Base Class | Purpose |
|------|-----------|---------|
| `ParseError` | Exception | SQLglot syntax errors |
| `OptimizeError` | Exception | SQLglot optimization failures |
| `UnsupportedStatementTypeError` | TypeError | Statement doesn't support column lineage |
| `SqlUnderstandingError` | Exception | Schema/scope resolution failures |
| `CooperativeTimeoutError` | TimeoutError | Parsing exceeded 10-second limit |
| `PanicException` | BaseException | Rust tokenizer crashes |

---

## SQLglot Core Errors

### 1. Invalid Expression / Unexpected Token

**Error Pattern:**
```
Invalid expression / Unexpected token. Line X, Col: Y.
[SQL context showing error location]
```

**Causes:**

#### A. Unsupported Syntax for Dialect
**Why:** SQL syntax valid in one dialect but not recognized by the parser.

**Common Examples:**
- Teradata `USING` clause with parameter declarations
- MySQL `CONVERT ... USING` syntax
- Abbreviated keywords (e.g., `CT` for `CREATE TABLE`)
- Proprietary functions or constructs

**Real Example from Data:**
```sql
-- Error: Invalid expression / Unexpected token. Line 1, Col: 5.
USING STATUS(CHAR(4)), ROWCOUNT(INTEGER), POSITIONING(BYTE(500)) UPDATE ...
```

**Resolution:**
- Verify dialect is set correctly (e.g., `read="teradata"`)
- Check if syntax is supported in sqlglot for that dialect
- Consider rewriting query to standard SQL
- Report missing dialect support to sqlglot project

#### B. Template Variables or Placeholders
**Why:** Parser encounters non-SQL tokens like `{{variable}}` or `${placeholder}`.

**Real Example:**
```sql
SELECT * FROM table WHERE id = {{user_id}}  -- Parser doesn't recognize {{}}
```

**Resolution:**
- Replace placeholders with actual values before parsing
- Use standard SQL parameter markers (`?` or `:param`)
- Pre-process queries to substitute template variables

#### C. Malformed SQL Syntax
**Why:** Actual syntax error in the query (missing commas, unmatched parentheses, etc.).

**Real Example from Data:**
```sql
-- Error: Expecting ). Line 3, Col: 18.
SE WHEN FirstRespTime < StartTime THEN 0 ELSE ( EXTRACT(DAY FROM ...
```

**Resolution:**
- Validate SQL syntax using database's native parser first
- Check for missing/extra commas, parentheses, quotes
- Use SQL formatter to identify structural issues

#### D. Truncated Queries
**Why:** Query was cut off mid-statement, leaving incomplete syntax.

**Resolution:**
- Ensure complete queries are passed to parser
- Check query log extraction limits
- Verify no character encoding issues

---

### 2. Expecting [Token]

**Error Pattern:**
```
Expecting ). Line X, Col: Y.
[SQL context]
```

**Causes:**

#### A. Unmatched Parentheses
**Why:** Opening parenthesis without corresponding closing parenthesis, or vice versa.

**Real Example from Data:**
```sql
-- Error: Expecting ). Line 19, Col: 35.
) AS CurrentPerm
FROM DBC.DatabaseSpace a
WHERE a.TableID <> '00000000000'XB AND ...
```

**Resolution:**
- Count opening and closing parentheses
- Use IDE with bracket matching
- Format query to see nesting structure

#### B. Missing Commas in Lists
**Why:** Comma omitted between list items (columns, expressions, etc.).

**Resolution:**
- Check column lists, function arguments, value lists
- Ensure proper comma placement

#### C. Syntax Context Mismatch
**Why:** Token appears in wrong context (e.g., keyword where expression expected).

**Resolution:**
- Review SQL grammar for the statement type
- Check if reserved words need quoting

---

### 3. Required Keyword Missing

**Error Pattern:**
```
Required keyword: '[keyword]' missing for <class 'sqlglot.expressions.[Type]'>. Line X, Col: Y.
```

**Causes:**

#### A. Dialect-Specific Keyword Requirements
**Why:** SQLglot expects certain keywords that may be optional in specific dialects.

**Real Example from Data:**
```sql
-- Error: Required keyword: 'definer' missing for <class 'sqlglot.expressions.SqlSecurityProperty'>. Line 2, Col: 19.
REPLACE PROCEDURE EPTONDSSProc.SPLDLEPM_PWA_Sts_Ovrvw ()
SQL SECURITY OWNER
```

**Why:** Teradata's `SQL SECURITY OWNER` syntax differs from MySQL's `SQL SECURITY DEFINER`.

**Resolution:**
- Add missing keyword if syntactically valid
- Check dialect documentation for correct syntax
- May require sqlglot dialect enhancement

---

### 4. [Expression] is not <class 'sqlglot.expressions.[Type]'>

**Error Pattern:**
```
* is not <class 'sqlglot.expressions.Alias'>.
```

**Causes:**

#### A. Type Mismatch in Expression Parsing
**Why:** Parser expected a specific expression type but got another.

**Real Example from Data:**
```sql
-- Error: * is not <class 'sqlglot.expressions.Alias'>.
-- Occurs with: SELECT *, other_column FROM ...
```

**Why:** In complex queries, `*` expansion conflicting with alias requirements.

**Resolution:**
- Expand `*` to explicit column list
- Check for ambiguous column references
- Simplify query structure

---

### 5. Got Unsupported Syntax for Statement

**Error Pattern:**
```
Got unsupported syntax for statement: [STATEMENT]
```

**Causes:**

#### A. Statement Type Not Fully Implemented
**Why:** SQLglot recognizes statement type but doesn't support all variants.

**Real Examples from Data:**

```sql
-- ALTER PROCEDURE with compile directive
ALTER PROCEDURE DBM.SP_DB_Spool compile

-- CREATE DATABASE with Teradata-specific options
create database temp from sysspace
as permanent = 1e9
no before journal, no after journal

-- CREATE USER with complex parameters
CREATE USER UC76L
FROM USER_CPD
AS PASSWORD=****************
,Temporary=10E9
,Spool=10E9
...
```

**Resolution:**
- Check if statement type is needed for lineage
- Consider skipping DDL statements if only DML lineage needed
- Report to sqlglot for dialect enhancement

---

## DataHub Parser Errors

### 6. Can Only Generate Column-Level Lineage for Select-Like Inner Statements

**Error Pattern:**
```
Can only generate column-level lineage for select-like inner statements, not <class 'sqlglot.expressions.[Type]'> (outer statement type: <class 'sqlglot.expressions.[Type]'>)
```

**Causes:**

#### A. Non-SELECT Statement Type
**Why:** DataHub's column-level lineage requires extractable SELECT statements.

**Affected Statement Types (from real data):**
- `DATABASE` / `USE` - 70 occurrences
- `DROP_TABLE` / `DROP` - 70 occurrences
- `DELETE` - 61 occurrences
- `CREATE_TABLE_AS_SELECT` - 38 occurrences (when table reference, not SELECT)
- `DROP_PROCEDURE` - 7 occurrences
- `DROP_DATABASE` - 6 occurrences
- `DROP_VIEW` - 2 occurrences
- `LOCKING` statements - 5 occurrences

**Why This Happens:**
- DDL operations (CREATE, DROP, ALTER) don't read/transform data
- DELETE doesn't produce output columns
- Database switching (USE) has no data flow

**Resolution:**
- **Expected behavior** - These statements shouldn't generate column lineage
- Use table-level lineage only for DDL
- Filter these statement types if column lineage is required

**Example:**
```sql
-- Error: Can only generate column-level lineage for select-like inner statements
DROP TABLE my_table;  -- No SELECT, no columns produced
```

---

### 7. Failed to Build Scope for Statement

**Error Pattern:**
```
Failed to build scope for statement - scope was empty: INSERT INTO [details]
```

**Causes:**

#### A. INSERT with VALUES Only (No SELECT)
**Why:** Scope building requires source tables/columns, but VALUES clause has literals only.

**Real Example from Data:**
```sql
-- Error: Failed to build scope for statement - scope was empty
INSERT INTO test_alan.pdcrdata.pdcrload_hst
(logdate, pdcrmacro, pdcrdatabase, pdcrtable, starttime, runscript, status)
VALUES (CURRENT_DATE, ?, ?, ?, CURRENT_TIMESTAMP(0), ?, ?);
```

**Why:** No tables to build scope from, only literal values and parameters.

**Resolution:**
- **Expected behavior** - Cannot generate column lineage from literals
- Use table-level lineage showing INSERT target
- If source data exists, rewrite as `INSERT INTO ... SELECT`

#### B. Missing Schema Information
**Why:** Tables referenced in query not found in schema resolver.

**Resolution:**
- Ensure schema metadata is provided to DataHub
- Use `default_db` and `default_schema` parameters
- Populate DataHub with dataset metadata before parsing

---

### 8. SQLglot Failed to Map Columns to Their Source Tables

**Error Pattern:**
```
sqlglot failed to map columns to their source tables; likely missing/outdated table schema info:
[specific issue like "Alias already used: pdcr"]
```

**Causes:**

#### A. Duplicate Alias Names
**Why:** Same alias used multiple times in query, causing ambiguity.

**Real Example from Data:**
```sql
-- Error: Alias already used: pdcr
-- Query likely has multiple tables/subqueries with alias "pdcr"
```

**Resolution:**
- Use unique aliases throughout query
- Check for nested subqueries with conflicting names
- Rename aliases to be descriptive and unique

#### B. Schema Information Missing/Outdated
**Why:** Parser needs column lists for tables but they're not in DataHub.

**Resolution:**
- Ingest dataset schemas into DataHub first
- Use profiling/metadata ingestion
- Provide schema_resolver in API calls

---

### 9. Table Must Match the Schema's Nesting Level

**Error Pattern:**
```
Table test_Alan.a must match the schema's nesting level: 3.
```

**Causes:**

#### A. Incorrect Table Qualification
**Why:** DataHub expects fully qualified names: `catalog.schema.table` (3 levels).

**Real Example from Data:**
```sql
-- Error: Table test_Alan.a must match the schema's nesting level: 3.
-- Only 2 parts provided, needs: database.schema.table
UPDATE test_Alan.a SET ...  -- Should be: test_Alan.schema_name.a
```

**Resolution:**
- Use fully qualified table names
- Set `default_db` and `default_schema` for automatic qualification
- Check platform's naming convention (catalog.schema.table vs schema.table)

---

### 10. Expected SELECT Statement After LOCKING Clause

**Error Pattern:**
```
Expected SELECT statement after LOCKING clause. Line X, Col: Y.
LOCKING TABLE [table] FOR ACCESS [operation]
```

**Causes:**

#### A. Multiple LOCKING Statements Before SELECT
**Why:** Parser expects SELECT immediately after first LOCKING, but sees another LOCKING.

**Real Example from Data:**
```sql
-- Error: Expected SELECT statement after LOCKING clause. Line 3, Col: 14.
LOCKING TABLE PDCRDATA.DatabaseSpace_Hst FOR ACCESS

LOCKING TABLE PDCRDATA.TableSpace_Hst FOR ACCESS

LOCKING TABLE PDCRDATA.SpoolSpace_Hst FOR ACCESS

SELECT ...
```

**Resolution:**
- Combine locks if possible: `LOCKING TABLE t1, t2, t3 FOR ACCESS`
- Check Teradata LOCKING syntax support in sqlglot
- May need to skip LOCKING clauses for parsing

#### B. Non-SELECT Statement After LOCKING
**Why:** LOCKING followed by INSERT, DELETE, or UPDATE instead of SELECT.

**Real Example from Data:**
```sql
-- Error: Expected SELECT statement after LOCKING clause.
LOCKING TABLE TDStats.CommandsListTbl FOR WRITE
DELETE FROM TDStats.CommandsListTbl WHERE ...
```

**Resolution:**
- Check if dialect supports this pattern
- May need to parse statement without LOCKING clause

---

### 11. Timeout Errors

**Error Pattern:**
```
CooperativeTimeoutError: Parsing exceeded 10-second timeout limit
```

**Causes:**

#### A. Extremely Complex Queries
**Why:** Recursive CTEs, deeply nested subqueries, or massive UNION chains.

**Resolution:**
- Simplify query structure
- Break into smaller queries
- Increase timeout (environment variable: `SQL_LINEAGE_TIMEOUT_ENABLED`)
- May indicate performance issue in query itself

#### B. Very Large Statement Count
**Why:** Parsing many statements in single request.

**Resolution:**
- Batch statements into smaller groups
- Parse statements individually

---

## Statement-Specific Errors

### DDL Statements

#### CREATE TABLE / CT

**Common Issues:**
- Abbreviated syntax (`CT` instead of `CREATE TABLE`) - 18 occurrences in data
- Complex column definitions with constraints
- Teradata-specific options (FALLBACK, WITH DATA)

**Example Error:**
```sql
-- Error: Invalid expression / Unexpected token. Line 1, Col: 15.
CT EPTAUTILITY.SUSPENDD_SEC_STG_LOG ,FALLBACK (LogType int, Seq int, ...)
```

**Resolution:**
- Expand abbreviations to full keywords
- Check dialect support for CREATE TABLE options

#### ALTER PROCEDURE

**Common Issues:**
- COMPILE directive not recognized - 10 occurrences
- Dialect-specific procedure syntax

**Example Error:**
```sql
-- Error: Got unsupported syntax for statement
ALTER PROCEDURE DBM.SP_DB_Spool compile
```

**Resolution:**
- Remove unsupported directives
- Focus on procedure definition, not compilation directives

#### REPLACE PROCEDURE

**Common Issues:**
- Missing DEFINER keyword - 30+ occurrences
- SQL SECURITY OWNER syntax

**Example Error:**
```sql
-- Error: Required keyword: 'definer' missing for SqlSecurityProperty
REPLACE PROCEDURE EPTONDSSProc.SPLDLEPM_PWA_Sts_Ovrvw ()
SQL SECURITY OWNER
```

**Resolution:**
- Add DEFINER clause if supported
- Check if procedure syntax matches dialect requirements

### DML Statements

#### USING Clause (Teradata)

**Common Issues:**
- Parameter declarations - 25+ error variations
- Not standard SQL, specific to Teradata stored procedures

**Example Error:**
```sql
-- Error: Invalid expression / Unexpected token. Line 1, Col: 5.
USING
_spVV17 (TIMESTAMP(6)),
_spVV18 (TIMESTAMP(6)),
_spVV19 (DECIMAL(13,0)),
```

**Why:** USING clause declares parameters for parameterized queries/procedures. SQLglot doesn't recognize this Teradata syntax.

**Resolution:**
- Extract query body without USING declaration
- Parse as standard SQL with parameter placeholders
- Consider pre-processing to remove USING blocks

#### INSERT Statements

**Common Issues:**
- NAMED keyword in columns (Teradata)
- Scope building failures with VALUES

**Example Error:**
```sql
-- Error: Expecting ). Line 1, Col: 111.
INSERT INTO EPTAOpAn.CSMEM_Attribs
SELECT pop.SMSF_Clnt_IntrntLId (NAMED Clnt_IntrntLId), ...
```

**Why:** `(NAMED alias)` is Teradata syntax for aliasing in target list.

**Resolution:**
- Remove NAMED keyword and parentheses
- Use standard AS aliasing

---

## Dialect-Specific Errors

### Teradata

**Key Challenges:**
1. **USING clause** - Parameter declarations
2. **LOCKING TABLE** - Multi-table locking syntax
3. **NAMED keyword** - Column aliasing in INSERT
4. **CT abbreviation** - Short form of CREATE TABLE
5. **QUALIFY clause** - Window function filtering
6. **REPLACE PROCEDURE** - SQL SECURITY OWNER
7. **Multiline INS/DEL/UPD** - Non-standard formatting

**Dialect Setting:**
```python
# Set dialect explicitly
result = parse_sql("SELECT * FROM table", read="teradata")
```

**Compatibility Level:**
- Basic SELECT/JOIN: ✅ Good
- CTEs and Window Functions: ✅ Good
- Stored Procedures: ⚠️ Limited
- DDL (CREATE/ALTER): ⚠️ Limited
- USING clause: ❌ Not supported
- LOCKING: ⚠️ Partial

### MySQL/MariaDB

**Key Challenges:**
1. **CONVERT ... USING** - Character set conversion
2. **Backticks for identifiers** - May need conversion to double quotes
3. **Non-standard string escaping**

**Resolution:**
- Use `read="mysql"` or `read="mariadb"`
- Replace backticks with double quotes if needed
- Check for proprietary MySQL functions

### BigQuery

**Key Challenges:**
1. **DECLARE statements** - Variable declarations
2. **EXCEPTION WHEN ERROR** - Error handling blocks
3. **Runtime functions** - `identifier()`, `execute immediate`

**Example from DataHub Issues:**
```sql
-- Not supported
DECLARE is_full_refresh bool DEFAULT false;
EXCEPTION WHEN ERROR THEN
  RAISE USING message = FORMAT(@@error.message);
```

**Resolution:**
- Extract core SELECT/INSERT/UPDATE from procedural code
- Skip variable declarations
- Focus on data transformation queries

### Snowflake

**Key Challenges:**
1. **identifier() function** - Runtime table name resolution
2. **$$ string literals** - Alternative quoting
3. **Snowflake-specific functions**

**Example:**
```sql
-- Cannot generate lineage - table resolved at runtime
SELECT * FROM identifier('my_db.my_schema.my_table');
```

**Resolution:**
- Replace identifier() with actual table name
- Pre-process queries before parsing

---

## Troubleshooting Guide

### Step 1: Identify Error Category

```
Is it a ParseError? → SQLglot syntax issue
Is it UnsupportedStatementTypeError? → Expected for non-SELECT
Is it SqlUnderstandingError? → Schema/scope issue
Is it a timeout? → Query too complex
```

### Step 2: Check Basics

✅ **Correct dialect specified?**
```python
parse_sql(query, read="teradata")  # Not "postgres" for Teradata!
```

✅ **Complete query?**
- No truncation
- No template variables ({{}} or ${})
- Valid SQL syntax

✅ **Schema information available?**
- Tables exist in DataHub
- Columns defined for datasets

### Step 3: Simplify Query

1. **Remove extensions:**
   - LOCKING clauses
   - USING parameter declarations
   - QUALIFY clauses
   - Vendor-specific hints

2. **Test incrementally:**
   ```sql
   -- Start simple
   SELECT * FROM table;

   -- Add joins
   SELECT * FROM table1 JOIN table2 ON ...;

   -- Add complexity gradually
   ```

3. **Check each component:**
   - CTEs independently
   - Subqueries independently
   - Window functions

### Step 4: Validate SQL

Use native database parser first:
```sql
-- In Teradata SQL Assistant
EXPLAIN SELECT ...;

-- In psql
\d table_name  -- Check if table exists
EXPLAIN SELECT ...;
```

### Step 5: Review Real-World Patterns

Check `error_message/parser_errors.md` for:
- Similar error messages
- Common patterns in your SQL dialect
- Known workarounds

### Step 6: Enable Debug Logging

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# DataHub will log detailed parsing info
result = graph.parse_sql_lineage(query)
```

### Step 7: Check Confidence Score

```python
confidence = result.sql_parsing_result.debug_info.confidence_score
if confidence < 0.7:
    print("Low confidence - review parser output carefully")
```

### Step 8: Manual Lineage as Fallback

For unparseable queries:
```python
# Emit manual lineage
from datahub.emitter.mce_builder import make_lineage_mce

lineage = make_lineage_mce(
    upstream_urns=["urn:li:dataset:(...)"],
    downstream_urn="urn:li:dataset:(...)"
)
```

---

## Common Patterns and Solutions

### Pattern 1: Stored Procedure Parsing

**Problem:** Procedures have declarations, control flow, multiple statements.

**Solution:**
```python
# Extract just the data transformation queries
def extract_queries(procedure_body):
    # Skip DECLARE, BEGIN, END, EXCEPTION blocks
    # Return only SELECT, INSERT, UPDATE, DELETE
    pass
```

### Pattern 2: Dynamic SQL

**Problem:** Table names in variables, runtime-constructed queries.

**Solution:**
- Pre-process to substitute variables with actual values
- Use query logs from execution (where names are resolved)
- Manual lineage for highly dynamic queries

### Pattern 3: Parameterized Queries

**Problem:** Parameters (`?`, `:param`, `@param`) in WHERE clauses.

**Solution:**
- Replace with literal values for parsing
- Or leave as-is (parameters don't affect lineage typically)

### Pattern 4: Complex CTEs

**Problem:** Recursive CTEs, deeply nested WITH clauses.

**Solution:**
- Test if CTE alone parses correctly
- Simplify recursion depth
- Break into separate statements if possible

### Pattern 5: Vendor Functions

**Problem:** Proprietary functions not in sqlglot.

**Solution:**
- Check sqlglot documentation for function support
- Replace with standard SQL equivalents
- May need custom function registration in sqlglot

---

## Error Statistics from Real Data

Based on 600+ parsing errors from Teradata test queries:

### Top 5 Error Categories

1. **Column-level lineage unavailable (DDL)**: 400+ (67%)
   - Expected for DROP, CREATE, ALTER, USE statements

2. **Invalid expression / Unexpected token**: 100+ (17%)
   - Primarily USING clause (25 variations)
   - CT abbreviation (18 cases)
   - LOCKING syntax (18 cases)

3. **Scope building failures**: 25+ (4%)
   - INSERT with VALUES only

4. **Unsupported stored procedure syntax**: 35+ (6%)
   - REPLACE PROCEDURE (30 cases)
   - ALTER PROCEDURE COMPILE (10 cases)

5. **Schema mismatches**: 5+ (1%)
   - Nesting level issues
   - Missing schema info

### Success Rates by Statement Type

| Statement Type | Success Rate | Notes |
|---------------|--------------|-------|
| SELECT | ~92% | High success |
| INSERT | ~70% | VALUES clauses fail |
| UPDATE | ~36% | Complex updates struggle |
| DELETE | 0% | Cannot generate column lineage |
| CREATE_TABLE_AS_SELECT | ~50% | Works when SELECT is clear |
| DROP/ALTER | 0% | Expected - DDL |
| Stored Procedures | ~10% | Dialect-specific syntax |

---

## Quick Reference: Error Message Lookup

| Error Message Prefix | Category | Section |
|---------------------|----------|---------|
| `Invalid expression / Unexpected token` | SQLglot Parse | [Section 3.1](#1-invalid-expression--unexpected-token) |
| `Expecting )` | SQLglot Parse | [Section 3.2](#2-expecting-token) |
| `Required keyword: 'X' missing` | SQLglot Parse | [Section 3.3](#3-required-keyword-missing) |
| `* is not <class` | SQLglot Type | [Section 3.4](#4-expression-is-not-class-sqlglotexpressionstype) |
| `Got unsupported syntax` | SQLglot Support | [Section 3.5](#5-got-unsupported-syntax-for-statement) |
| `Can only generate column-level lineage` | DataHub Lineage | [Section 4.6](#6-can-only-generate-column-level-lineage-for-select-like-inner-statements) |
| `Failed to build scope` | DataHub Scope | [Section 4.7](#7-failed-to-build-scope-for-statement) |
| `sqlglot failed to map columns` | DataHub Mapping | [Section 4.8](#8-sqlglot-failed-to-map-columns-to-their-source-tables) |
| `Table X must match the schema's nesting level` | DataHub Schema | [Section 4.9](#9-table-must-match-the-schemas-nesting-level) |
| `Expected SELECT statement after LOCKING` | SQLglot Dialect | [Section 4.10](#10-expected-select-statement-after-locking-clause) |

---

## References

### Official Documentation

- **SQLglot Official Docs**: [https://sqlglot.com/sqlglot.html](https://sqlglot.com/sqlglot.html)
- **SQLglot Parser API**: [https://sqlglot.com/sqlglot/parser.html](https://sqlglot.com/sqlglot/parser.html)
- **SQLglot Errors API**: [https://sqlglot.com/sqlglot/errors.html](https://sqlglot.com/sqlglot/errors.html)
- **SQLglot GitHub**: [https://github.com/tobymao/sqlglot](https://github.com/tobymao/sqlglot)
- **DataHub SQL Parser Docs**: [https://docs.datahub.com/docs/lineage/sql_parsing](https://docs.datahub.com/docs/lineage/sql_parsing)

### DataHub Support Articles

- **Query Translation and SQL Lineage Parser Errors**: [DataHub Support Article](https://support.datahub.com/hc/en-us/articles/41912146532763-Query-Translation-and-SQL-Lineage-Parser-Errors)
- **Query-Based Lineage Not Extracting Correctly**: [DataHub Support Article](https://support.datahub.com/hc/en-us/articles/41912003908891-Query-Based-Lineage-Not-Extracting-Correctly)

### GitHub Issues (Known Problems)

- **BigQuery Ingestion Lineage Issues**: [datahub-project/datahub#11654](https://github.com/datahub-project/datahub/issues/11654)
- **SQLglot ParseError with IS operator**: [tobymao/sqlglot#3978](https://github.com/tobymao/sqlglot/issues/3978)
- **MySQL CONVERT USING support**: [tobymao/sqlglot#384](https://github.com/tobymao/sqlglot/issues/384)
- **Unexpected token on alias without AS**: [tobymao/sqlglot#2788](https://github.com/tobymao/sqlglot/issues/2788)

### Community Resources

- **Stack Overflow - sqlglot tags**: [stackoverflow.com/questions/tagged/sqlglot](https://stackoverflow.com/questions/tagged/sqlglot)
- **DataHub Forum - Lineage Discussion**: [DataHub Forum](https://forum.datahubproject.io/t/troubleshooting-lineage-extraction-for-sql-queries-in-datahub/1253)

### Local Project Files

- `error_message/parser_errors.md` - Real-world error examples from 600+ test queries
- `parse_sql_minimal.py` - DataHub parser wrapper implementation
- `report_utils.py` - Error aggregation and reporting utilities

---

## Appendix: Error Code Quick Fix Guide

### Quick Fix 1: Remove USING Clause (Teradata)

```python
import re

def strip_using_clause(query):
    """Remove Teradata USING parameter declarations."""
    # Pattern: USING ... (params) followed by actual statement
    pattern = r'USING\s+[^;]+?\s+(SELECT|INSERT|UPDATE|DELETE|WITH)'
    return re.sub(pattern, r'\1', query, flags=re.IGNORECASE | re.DOTALL)
```

### Quick Fix 2: Expand CT to CREATE TABLE

```python
def expand_ct_abbreviation(query):
    """Expand CT to CREATE TABLE."""
    return re.sub(r'\bCT\b', 'CREATE TABLE', query, flags=re.IGNORECASE)
```

### Quick Fix 3: Remove LOCKING Clauses

```python
def remove_locking_clauses(query):
    """Remove Teradata LOCKING TABLE statements."""
    pattern = r'LOCKING\s+TABLE\s+[\w.]+\s+FOR\s+(ACCESS|WRITE|READ)\s+'
    return re.sub(pattern, '', query, flags=re.IGNORECASE)
```

### Quick Fix 4: Replace NAMED Keyword

```python
def remove_named_keyword(query):
    """Remove Teradata NAMED keyword from INSERT columns."""
    # Pattern: (NAMED alias)
    pattern = r'\(\s*NAMED\s+(\w+)\s*\)'
    return re.sub(pattern, r'AS \1', query, flags=re.IGNORECASE)
```

### Quick Fix 5: Fully Qualify Table Names

```python
def qualify_table_names(query, default_db, default_schema):
    """Add database.schema prefix to unqualified tables."""
    # This is simplified - production needs proper SQL parsing
    # Use sqlglot itself to transform the AST
    from sqlglot import parse_one, exp

    tree = parse_one(query, read="teradata")

    for table in tree.find_all(exp.Table):
        if len(table.parts) == 1:
            table.set("db", f"{default_db}.{default_schema}")

    return tree.sql(dialect="teradata")
```

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2025-11-26 | Initial comprehensive catalogue based on 600+ real parsing errors |

---

**End of Error Catalogue**

For questions or contributions, please update this document with new error patterns as they're discovered.
