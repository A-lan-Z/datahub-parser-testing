# Expected Results Directory

This directory is for storing expected lineage results in JSON format for validation testing.

## Purpose

Expected results files are used by the testing framework to validate that the DataHub SQL parser produces correct lineage output. By comparing actual parser results against these expected results, you can:

- Calculate precision and recall metrics
- Identify missing or extra tables/columns in lineage
- Track parser accuracy over time
- Detect regressions in parser behavior

## File Format

Create JSON files with the same name as your SQL test files (with `.json` extension instead of `.sql`).

### Single Statement Example

For a SQL file like `01_simple_select.sql`, create `01_simple_select.json`:

```json
{
  "in_tables": [
    "urn:li:dataset:(urn:li:dataPlatform:teradata,SampleDB.Analytics.customers,PROD)"
  ],
  "out_tables": [],
  "column_lineage": [
    {
      "downstream": {
        "table": "urn:li:dataset:(urn:li:dataPlatform:teradata,<query-result>,PROD)",
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

### Multi-Statement Example

For SQL files with multiple statements separated by `;`, use an array:

```json
[
  {
    "in_tables": [...],
    "out_tables": [...],
    "column_lineage": [...]
  },
  {
    "in_tables": [...],
    "out_tables": [...],
    "column_lineage": [...]
  }
]
```

## URN Format

DataHub uses URN (Uniform Resource Name) format for dataset identifiers:

```
urn:li:dataset:(urn:li:dataPlatform:{platform},{database}.{schema}.{table},{env})
```

Example:
```
urn:li:dataset:(urn:li:dataPlatform:teradata,SampleDB.Analytics.customers,PROD)
```

## Usage

Run tests with expected results validation:

```bash
python parse_sql_test.py \
  --sql-dir test-queries/teradata/01-basic \
  --expected-dir expected-results \
  --output-csv results.csv
```

The testing framework will:
1. Look for matching JSON files in this directory
2. Compare actual parser output against expected results
3. Calculate validation metrics (precision, recall)
4. Report differences in the output CSV/JSON

## Creating Expected Results

You can create expected results by:

1. **Running the parser first** and examining the output
2. **Manually verifying** the lineage is correct
3. **Saving the verified output** as the expected result
4. **Adding comments** to document assumptions

## Notes

- This directory is initially empty; populate it as needed for validation testing
- Not all test queries need expected results - use them for critical test cases
- Keep expected results updated if you modify the corresponding SQL files
