# DataHub SQL Parser Testing Framework

This testing framework provides comprehensive capabilities for evaluating the DataHub SQL parser's performance, accuracy, and limitations with focus on Teradata SQL.

## Overview

The framework consists of:

1. **Enhanced Testing Script** (`parse_sql_test.py`) - Batch testing with detailed metrics
2. **Test Suite** (`test-queries/`) - Organized collection of SQL test cases
3. **Analysis Script** (`analyze_results.py`) - Statistical analysis and reporting
4. **Expected Results** (`expected-results/`) - Ground truth for validation

## Quick Start

### Prerequisites

```bash
# Install DataHub client
pip install acryl-datahub

# Set environment variables
export DATAHUB_SERVER="http://localhost:8080"
export DATAHUB_TOKEN="your-token-here"
```

### Running Basic Tests

```bash
# Test a single SQL file
python parse_sql_test.py \
  --sql-file test-queries/teradata/01-basic/01_simple_select.sql \
  --verbose

# Test all queries in a directory (batch mode)
python parse_sql_test.py \
  --sql-dir test-queries/teradata/01-basic \
  --output-csv test-results/basic-tests.csv \
  --output-json test-results/basic-tests.json

# Test with expected results validation
python parse_sql_test.py \
  --sql-dir test-queries/teradata/01-basic \
  --expected-dir expected-results \
  --output-csv test-results/basic-tests.csv
```

### Analyzing Results

```bash
# Generate analysis report
python analyze_results.py test-results/basic-tests.csv

# Save detailed analysis as JSON
python analyze_results.py test-results/basic-tests.csv \
  --output-json test-results/analysis.json
```

## Test Suite Organization

```
test-queries/teradata/
├── 01-basic/              # Basic SQL constructs (SELECT, INSERT, JOIN, etc.)
├── 02-ctes/               # Common Table Expressions (single, multiple, nested, recursive)
├── 03-subqueries/         # Subqueries (scalar, correlated, nested, EXISTS)
├── 04-advanced-dml/       # Advanced DML (MERGE, window functions, CASE, QUALIFY)
├── 05-stored-procs/       # Stored procedures and multi-statement scripts
├── 06-cross-db/           # Cross-database and cross-schema queries
├── 07-edge-cases/         # Known limitations and edge cases
└── 08-real-world/         # Sanitized production queries
```

### Test Categories

#### 01-basic
Tests fundamental SQL operations to establish baseline parser capabilities:
- Simple SELECT with explicit columns
- SELECT * (requires schema registration)
- INNER/LEFT/RIGHT JOINs
- INSERT...SELECT
- CREATE TABLE AS SELECT (CTAS)
- CREATE VIEW
- UNION/UNION ALL
- UPDATE with JOIN
- DELETE with subquery

**Expected Success Rate:** 95%+

#### 02-ctes
Tests Common Table Expression support at various complexity levels:
- Single CTE
- Multiple dependent CTEs
- Nested CTEs (3+ levels)
- Recursive CTEs
- CTEs in INSERT statements

**Expected Success Rate:** 90%+

#### 03-subqueries
Tests subquery handling in various contexts:
- Scalar subqueries in SELECT
- Subqueries in WHERE (IN, comparison)
- Correlated subqueries
- Derived tables (subquery in FROM)
- Nested subqueries (3+ levels)
- EXISTS/NOT EXISTS

**Expected Success Rate:** 85%+

#### 04-advanced-dml
Tests advanced SQL features:
- MERGE statements (table-level lineage only)
- Window functions (ROW_NUMBER, RANK, LAG, LEAD)
- INSERT with mismatched column lists (known limitation)
- Complex CASE expressions
- PIVOT operations
- QUALIFY clause (Teradata-specific)

**Expected Success Rate:** 70-80% (some known limitations)

#### 05-stored-procs
Tests stored procedure and multi-statement support:
- Single-statement procedures
- Multi-statement procedures
- Procedures with control flow
- Dynamic SQL (EXECUTE IMMEDIATE)
- Teradata macros

**Expected Success Rate:** 40-60% (known limitation area)

#### 06-cross-db
Tests cross-database query handling:
- Fully qualified names (db.schema.table)
- Mixed qualified/unqualified references
- Three-part naming
- Default schema behavior

**Expected Success Rate:** 85%+

#### 07-edge-cases
Tests known parser limitations:
- Dynamic references (identifier functions)
- JSON/XML functions
- UNNEST constructs
- Very long queries
- Deeply nested structures

**Expected Success Rate:** Variable (documents boundaries)

#### 08-real-world
Sanitized production queries for realistic testing:
- Collected from actual Teradata workloads
- Annotated with business context
- Variety of complexity levels

**Expected Success Rate:** Target 80%+

## Enhanced Testing Script Features

### parse_sql_test.py

#### Key Features

1. **Batch Testing Mode**
   - Process entire directories of SQL files
   - Glob pattern matching for file selection
   - Parallel-friendly architecture

2. **Comprehensive Metrics**
   - Parse success/failure
   - Confidence scores
   - Table and column lineage counts
   - Query complexity metrics (CTEs, JOINs, subqueries, etc.)
   - Parse timing with millisecond precision

3. **Expected Results Validation**
   - Compare actual vs. expected lineage
   - Calculate precision/recall metrics
   - Identify missing and extra tables/columns

4. **Structured Output**
   - CSV format for spreadsheet analysis
   - JSON format for programmatic processing
   - Real-time verbose console output

#### Command-Line Options

```
Input:
  --sql-file FILE         Single SQL file to test
  --sql-dir DIR           Directory of SQL files (batch mode)
  --pattern PATTERN       File pattern for batch mode (default: *.sql)

DataHub Connection:
  --server URL            DataHub GMS endpoint (default: http://localhost:8080)
  --platform PLATFORM     Dataset platform (default: teradata)
  --env ENV               Dataset environment (default: PROD)
  --default-db DB         Default database name
  --default-schema SCHEMA Default schema name
  --default-dialect DIALECT SQL dialect (default: teradata)

Validation:
  --expected-dir DIR      Directory with expected results JSON files

Output:
  --output-csv FILE       Write CSV results to file
  --output-json FILE      Write JSON results to file
  --verbose               Enable verbose debug output
```

#### Output Schema

**CSV Columns:**
- `query_id`: Unique identifier (filename stem)
- `query_file`: Full path to SQL file
- `statement_index`: Statement number within file
- `statement_type`: Inferred type (SELECT, INSERT, MERGE, etc.)
- `dialect`: SQL dialect used
- `success`: Parse succeeded (true/false)
- `confidence`: Confidence score (0.0-1.0)
- `parse_time_ms`: Parse duration in milliseconds
- `in_tables_count`: Number of upstream tables
- `out_tables_count`: Number of downstream tables
- `column_lineage_count`: Number of column lineage mappings
- `error_message`: Error text if failed
- `table_error`: Debug flag indicating table resolution issues
- Complexity metrics: `cte_count`, `max_cte_depth`, `subquery_count`, `join_count`, `union_count`, `window_function_count`, `line_count`, `char_count`
- Validation metrics: `validation_status`, `tables_precision`, `tables_recall`, `columns_precision`, `columns_recall`

**JSON Structure:**
Detailed array of test result objects with full nested data including debug info and validation details.

## Expected Results Format

Create JSON files in `expected-results/` matching your SQL file names:

```json
{
  "in_tables": [
    "urn:li:dataset:(urn:li:dataPlatform:teradata,SampleDB.Analytics.orders,PROD)",
    "urn:li:dataset:(urn:li:dataPlatform:teradata,SampleDB.Analytics.customers,PROD)"
  ],
  "out_tables": [
    "urn:li:dataset:(urn:li:dataPlatform:teradata,SampleDB.Analytics.customer_summary,PROD)"
  ],
  "column_lineage": [
    {
      "downstream": {
        "table": "urn:li:dataset:(urn:li:dataPlatform:teradata,SampleDB.Analytics.customer_summary,PROD)",
        "column": "customer_id"
      },
      "upstreams": [
        {
          "table": "urn:li:dataset:(urn:li:dataPlatform:teradata,SampleDB.Analytics.customers,PROD)",
          "column": "customer_id"
        }
      ]
    }
  ]
}
```

For multi-statement SQL files, use an array:

```json
[
  { "in_tables": [...], "out_tables": [...] },
  { "in_tables": [...], "out_tables": [...] }
]
```

## Analysis Script Features

### analyze_results.py

Generates comprehensive statistical analysis from CSV results:

#### Analysis Categories

1. **Overall Statistics**
   - Total queries, success rate
   - Confidence score distribution
   - Lineage extraction rates
   - Performance metrics

2. **By Statement Type**
   - Success rates per SQL statement type
   - Confidence and lineage rates
   - Identifies which statement types work best

3. **By Test Category**
   - Success rates per test directory
   - Tracks progression through complexity levels
   - Identifies problem areas

4. **Complexity Impact**
   - CTE complexity vs. success rate
   - JOIN complexity vs. success rate
   - Subquery complexity vs. success rate
   - Identifies where parser struggles

5. **Validation Results**
   - Precision/recall for table lineage
   - Precision/recall for column lineage
   - Identifies accuracy issues

6. **Error Analysis**
   - Error frequency and types
   - Common failure patterns
   - Example queries for each error type

## DataHub Test Environment Setup

### Schema Registration

For accurate column-level lineage testing, register test schemas in DataHub:

```python
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import SchemaMetadataClass, SchemaFieldClass

# Define your test table schema
schema = SchemaMetadataClass(
    schemaName="customers",
    platform="urn:li:dataPlatform:teradata",
    version=0,
    fields=[
        SchemaFieldClass(
            fieldPath="customer_id",
            nativeDataType="INTEGER",
            type=...,
        ),
        # ... more fields
    ],
)

# Emit to DataHub
graph = DataHubGraph(...)
graph.emit_mcp(MetadataChangeProposalWrapper(
    entityUrn="urn:li:dataset:(urn:li:dataPlatform:teradata,SampleDB.Analytics.customers,PROD)",
    aspect=schema,
))
```

### Sample Test Databases

Create the following test database structure in DataHub:

```
SampleDB.Analytics:
  - customers (customer_id, customer_name, email, status, customer_tier, registration_date, total_spent, last_order_date, order_count)
  - orders (order_id, customer_id, order_date, total_amount, status)
  - order_items (order_item_id, order_id, product_id, quantity, unit_price)
  - products (product_id, product_name, category, unit_price, description)
  - employees (employee_id, employee_name, manager_id, department_id)
  - sales (sale_id, employee_id, sales_amount, sale_date)
```

## Best Practices

### Writing Test Queries

1. **Use fully qualified table names** in test queries:
   ```sql
   -- Good
   SELECT * FROM SampleDB.Analytics.customers

   -- Avoid (unless testing default schema)
   SELECT * FROM customers
   ```

2. **Include comments** describing what each test validates:
   ```sql
   -- Tests parser handling of correlated subquery with aggregate
   SELECT ...
   ```

3. **One concept per file** when possible for clearer results

4. **Name files descriptively**: `01_simple_select.sql`, `05_recursive_cte.sql`

### Creating Expected Results

1. **Start with simple queries** to establish baseline
2. **Manually verify** lineage before creating expected results
3. **Use the parser output** as a starting point, then validate
4. **Document assumptions** in comments within JSON files

### Running Tests

1. **Start with basic tests** to ensure environment is working
2. **Progress through complexity levels** systematically
3. **Run full suite regularly** to catch regressions
4. **Use verbose mode** when debugging specific failures
5. **Compare results over time** to track improvements or regressions

## Known Limitations & Workarounds

Based on DataHub documentation and research:

### MERGE Statements
- **Limitation**: No column-level lineage for MERGE INTO
- **Workaround**: Extract underlying SELECT and test separately
- **Impact**: Table-level lineage works fine

### INSERT with Mismatched Columns
- **Limitation**: Column list must match SELECT clause order
- **Workaround**: Rewrite queries to match column order
- **Impact**: See test `04-advanced-dml/03_insert_column_mismatch.sql`

### Stored Procedures
- **Limitation**: Multi-statement scripts not fully supported
- **Workaround**: Extract individual statements for testing
- **Impact**: Single-statement procedures may work

### Dynamic SQL
- **Limitation**: Cannot parse runtime-evaluated references
- **Workaround**: Manual lineage annotation required
- **Impact**: identifier() functions, EXECUTE IMMEDIATE fail

### Window Function Partitions
- **Limitation**: PARTITION BY and ORDER BY columns not tracked
- **Workaround**: None needed (by design)
- **Impact**: May miss some dependencies

### SELECT * Expansion
- **Limitation**: Requires accurate schema in DataHub
- **Workaround**: Keep schemas updated
- **Impact**: Outdated schemas cause incorrect column lineage

## Interpreting Results

### Confidence Scores

- **0.9-1.0**: High confidence, parser understands query well
- **0.7-0.9**: Medium confidence, some ambiguity
- **0.0-0.7**: Low confidence, significant parsing challenges
- **0.0**: Parse failed completely

### Success Criteria by Category

| Category | Target Success Rate | Target High Confidence Rate |
|----------|--------------------|-----------------------------|
| 01-basic | 95%+ | 90%+ |
| 02-ctes | 90%+ | 85%+ |
| 03-subqueries | 85%+ | 80%+ |
| 04-advanced-dml | 75%+ | 70%+ |
| 05-stored-procs | 50%+ | 40%+ |
| 06-cross-db | 85%+ | 80%+ |
| 07-edge-cases | Variable | Variable |
| 08-real-world | 80%+ | 75%+ |

### Red Flags

- Success rate below 50% for basic queries
- Column lineage rate below 30% (indicates schema issues)
- Many errors of same type (indicates systematic problem)
- Confidence scores consistently below 0.7

## Troubleshooting

### Common Issues

**Problem**: All queries fail with "401 Unauthorized"
- **Solution**: Check `DATAHUB_TOKEN` environment variable or hardcoded TOKEN in script

**Problem**: Column lineage always empty
- **Solution**: Register table schemas in DataHub (required for column lineage)

**Problem**: Parse time extremely slow
- **Solution**: Check DataHub server performance, network latency

**Problem**: "Unknown dialect" errors
- **Solution**: Verify `--default-dialect` matches SQLGlot supported dialects

**Problem**: Validation always fails
- **Solution**: Check expected results JSON format, ensure URNs match exactly

## Next Steps

### Phase 1: Baseline Assessment
1. Run all basic tests (01-basic)
2. Verify test environment is configured correctly
3. Establish baseline success rates

### Phase 2: Complexity Testing
1. Run CTE tests (02-ctes)
2. Run subquery tests (03-subqueries)
3. Run advanced DML tests (04-advanced-dml)
4. Document where complexity impacts success

### Phase 3: Real-World Testing
1. Collect sanitized production queries
2. Categorize by complexity and business function
3. Run through parser
4. Calculate production readiness metrics

### Phase 4: Validation & Accuracy
1. Create expected results for representative queries
2. Measure precision/recall
3. Identify systematic accuracy issues
4. Document quality metrics

### Phase 5: Documentation & Recommendations
1. Generate comprehensive capability matrix
2. Document limitations and workarounds
3. Create best practices guide
4. Make go/no-go recommendation for large project

## Integration Recommendations

For integrating into larger projects:

1. **Use batch mode** for efficiency
2. **Implement confidence thresholds** (e.g., reject < 0.7)
3. **Monitor error rates** and alert on anomalies
4. **Keep schemas updated** in DataHub
5. **Have fallback strategy** for unsupported queries
6. **Track metrics over time** for quality monitoring
7. **Consider query rewriting** for known problematic patterns

## Contributing

When adding new test queries:

1. Place in appropriate category directory
2. Use descriptive filenames with numeric prefix
3. Include comment header describing test purpose
4. Add expected results JSON if validating
5. Update this README if adding new categories
6. Run full suite to ensure no regressions

## References

- [DataHub SQL Parsing Documentation](https://datahub.io/docs/lineage/sql_parsing)
- [SQLGlot Documentation](https://github.com/tobymao/sqlglot)
- [DataHub Python SDK](https://datahubproject.io/docs/python-sdk/)
