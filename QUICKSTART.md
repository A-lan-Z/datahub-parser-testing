# Quick Start Guide - DataHub SQL Parser Testing

## 5-Minute Setup

### 1. Prerequisites

```bash
# Install DataHub Python client
pip install acryl-datahub

# Verify DataHub is running
curl http://localhost:8080/health
```

### 2. Configure Authentication

```bash
# Set your DataHub token
export DATAHUB_TOKEN="your-token-here"

# Or edit parse_sql_test.py and parse_sql_minimal.py to use environment variable
```

### 3. Run Your First Test

```bash
# Test a single query
python parse_sql_test.py \
  --sql-file test-queries/teradata/01-basic/01_simple_select.sql \
  --verbose
```

### 4. Run a Test Category

```bash
# Test all basic queries
python parse_sql_test.py \
  --sql-dir test-queries/teradata/01-basic \
  --output-csv test-results/basic.csv \
  --output-json test-results/basic.json
```

### 5. Analyze Results

```bash
# Generate analysis report
python analyze_results.py test-results/basic.csv
```

## Run Complete Test Suite

```bash
# Run all tests (requires DataHub with test schemas)
./run_tests.sh

# View combined results
cat test-results/all-results.csv
```

## What You'll See

### Successful Parse Output

```
================================================================================
File: 01_simple_select.sql | Statement 1/1
Type: SELECT | Success: True | Confidence: 0.950
Parse time: 45.234 ms
Tables IN: 1 | OUT: 0 | Columns: 0
Complexity: CTEs=0, Subqueries=0, JOINs=0
================================================================================
```

### Analysis Report

```
================================================================================
SQL PARSER TEST RESULTS ANALYSIS
================================================================================

OVERALL STATISTICS:
  Total queries tested: 10
  Successful parses: 9 (90.0%)
  High confidence (>=0.9): 8 (80.0%)
  With column lineage: 5 (50.0%)

BY STATEMENT TYPE:
  SELECT:
    Success rate: 100.0%
    High confidence rate: 90.0%
```

## Next Steps

1. **Review TESTING_FRAMEWORK.md** for complete documentation
2. **Add your own test queries** to appropriate category directories
3. **Create expected results** for validation (optional)
4. **Run tests regularly** as you develop your project

## Common Commands

```bash
# Test with verbose output
python parse_sql_test.py --sql-file query.sql --verbose

# Test with custom dialect
python parse_sql_test.py --sql-dir queries/ --default-dialect snowflake

# Test with validation
python parse_sql_test.py \
  --sql-dir queries/ \
  --expected-dir expected/ \
  --output-csv results.csv

# Analyze results with JSON output
python analyze_results.py results.csv --output-json analysis.json
```

## Verify Setup

Before running tests, verify your DataHub connection:

```bash
# Test connection and API compatibility
python test_datahub_connection.py
```

This will check:
- DataHub server is reachable
- Authentication token is valid
- API version compatibility
- Basic parsing works

## Troubleshooting

**"unexpected keyword argument 'default_dialect'" error**
```bash
# Your DataHub version may not support this parameter
# The script automatically falls back to compatible mode
# Run the connection test to verify:
python test_datahub_connection.py
```

**All tests fail with 401 error**
```bash
# Check your token is valid
echo $DATAHUB_TOKEN

# Or set it in the script
export DATAHUB_TOKEN="your-valid-token"
```

**No column lineage in results**
```bash
# Column lineage requires schemas registered in DataHub
# See TESTING_FRAMEWORK.md > "DataHub Test Environment Setup"
```

**Tests run slowly**
```bash
# Check DataHub server response time
time curl http://localhost:8080/health

# Reduce test set if needed
python parse_sql_test.py --sql-dir queries/ --pattern "0[1-2]*.sql"
```

## File Structure

```
datahub-parser-testing/
├── parse_sql_minimal.py       # Original simple parser script
├── parse_sql_test.py          # Enhanced testing script
├── analyze_results.py         # Results analysis script
├── run_tests.sh              # Full test suite runner
├── test-queries/             # Test SQL files
│   └── teradata/
│       ├── 01-basic/         # 10 basic tests
│       ├── 02-ctes/          # 5 CTE tests
│       ├── 03-subqueries/    # 6 subquery tests
│       ├── 04-advanced-dml/  # 6 advanced tests
│       └── ...
├── expected-results/         # Ground truth for validation
├── test-results/            # Generated test outputs
├── TESTING_FRAMEWORK.md     # Complete documentation
└── QUICKSTART.md           # This file
```

## Getting Help

- **Full documentation**: See TESTING_FRAMEWORK.md
- **DataHub docs**: https://datahub.io/docs/lineage/sql_parsing
- **Report issues**: Create an issue in your repository
