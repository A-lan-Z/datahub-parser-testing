# DataHub SQL Parser API Reference

## What DataHub Returns

Based on actual API testing, here's what `graph.parse_sql_lineage()` returns:

### Core Fields

```python
result = graph.parse_sql_lineage(sql=query, platform="teradata", env="PROD")

# DataHub returns:
result.query_type              # Statement type: "SELECT", "INSERT", "UPDATE", etc.
result.query_type_props        # Additional properties (usually empty dict)
result.query_fingerprint       # SHA256 hash of normalized query
result.in_tables               # List of upstream table URNs
result.out_tables              # List of downstream table URNs
result.column_lineage          # List of column-level lineage mappings
result.joins                   # Join information (usually empty for simple queries)
result.debug_info              # Debugging information
```

### Example Response (JSON format)

```json
{
  "query_type": "SELECT",
  "query_type_props": {},
  "query_fingerprint": "a55be9c7cb818e26dfe804a24c9d64b2828c745648ec77e5e88f20162c4e1810",
  "in_tables": [
    "urn:li:dataset:(urn:li:dataPlatform:teradata,sampledb.analytics.customers,DEV)"
  ],
  "out_tables": [],
  "column_lineage": [
    {
      "downstream": {
        "table": null,
        "column": "customer_id",
        "column_type": null,
        "native_column_type": null
      },
      "upstreams": [
        {
          "table": "urn:li:dataset:(urn:li:dataPlatform:teradata,sampledb.analytics.customers,DEV)",
          "column": "customer_id"
        }
      ],
      "logic": {
        "is_direct_copy": true,
        "column_logic": "\"customers\".\"customer_id\" AS \"customer_id\""
      }
    }
  ],
  "joins": [],
  "debug_info": {
    "confidence": 0.2,
    "generalized_statement": "SELECT customer_id, customer_name, email FROM SampleDB.Analytics.customers WHERE status = ?"
  }
}
```

## Field Details

### query_type
**Type:** String
**Source:** DataHub parser
**Values:** "SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "DROP", etc.

The type of SQL statement detected by the parser.

### query_fingerprint
**Type:** String
**Source:** DataHub parser
**Purpose:** Unique identifier for the normalized query

Useful for:
- Deduplicating similar queries
- Tracking query patterns
- Caching results

### in_tables
**Type:** List[String]
**Source:** DataHub parser
**Format:** DataHub URNs

Upstream/source tables that are read from in the query.

Example:
```python
["urn:li:dataset:(urn:li:dataPlatform:teradata,sampledb.analytics.customers,DEV)"]
```

### out_tables
**Type:** List[String]
**Source:** DataHub parser
**Format:** DataHub URNs

Downstream/target tables that are written to in the query.

For SELECT queries, this is typically empty.
For INSERT/UPDATE/MERGE, this contains the target table(s).

### column_lineage
**Type:** List[Object]
**Source:** DataHub parser

Column-level lineage mappings showing how columns flow from source to target.

**Structure:**
```python
{
  "downstream": {
    "table": "urn:li:dataset:(...)" or null,  # null for SELECT queries
    "column": "column_name",
    "column_type": "datatype" or null,
    "native_column_type": "native_type" or null
  },
  "upstreams": [
    {
      "table": "urn:li:dataset:(...)",
      "column": "source_column"
    }
  ],
  "logic": {
    "is_direct_copy": true/false,
    "column_logic": "SQL expression used"
  }
}
```

**Key points:**
- For SELECT queries, `downstream.table` is `null` (columns go to result set, not a table)
- `is_direct_copy: true` means it's a simple `SELECT column` without transformation
- `column_logic` shows the SQL expression that produces this column

### debug_info
**Type:** Object
**Source:** DataHub parser

Debugging and quality information.

**Structure:**
```python
{
  "confidence": 0.0 to 1.0,              # Parser confidence score
  "table_error": true/false or null,     # Whether table resolution failed
  "error": "error message" or null,      # Parse error if any
  "generalized_statement": "..."         # Query with literals replaced by ?
}
```

**Confidence score interpretation:**
- **0.9-1.0:** High confidence - tables exist in DataHub, schemas validated
- **0.7-0.9:** Medium confidence - partial validation
- **0.2-0.6:** Low confidence - parsed but tables not in DataHub
- **0.0:** Parse failed

### joins
**Type:** List[Object]
**Source:** DataHub parser

Join information (typically empty for simple queries, may be populated for complex joins).

## What parse_sql_test.py Adds

The testing script enhances DataHub's output with:

| Field | Source | Purpose |
|-------|--------|---------|
| **query_type** | DataHub | ✓ From DataHub's parser |
| **parse_time_ms** | Test script | Measure API call latency |
| **complexity metrics** | Test script | CTE count, JOIN count, etc. from regex |
| **validation** | Test script | Compare against expected results |
| **success flag** | Test script | Whether API call succeeded |

## API Parameters

### Required Parameters
```python
graph.parse_sql_lineage(
    sql="SELECT ...",           # SQL query string
    platform="teradata",        # Platform name
)
```

### Optional Parameters
```python
graph.parse_sql_lineage(
    sql="SELECT ...",
    platform="teradata",
    env="PROD",                 # Environment (PROD, DEV, QA, etc.)
    default_db="SampleDB",      # Default database for unqualified tables
    default_schema="Analytics", # Default schema for unqualified tables
    default_dialect="teradata"  # SQL dialect hint (newer versions only)
)
```

**Note:** `default_dialect` is only available in newer DataHub versions. The test script handles this gracefully.

## Error Handling

### Parse Errors
If parsing fails, `debug_info.error` contains the error message:

```python
if result.debug_info.error:
    print(f"Parse error: {result.debug_info.error}")
```

### API Errors
API-level errors (authentication, network, etc.) raise exceptions:

```python
try:
    result = graph.parse_sql_lineage(...)
except Exception as e:
    print(f"API error: {e}")
```

Common errors:
- `401 Unauthorized` - Invalid token
- `Connection refused` - DataHub not running
- `Could not find match for hunk context` - Version mismatch

## URN Format

DataHub uses URNs (Uniform Resource Names) for dataset identification:

```
urn:li:dataset:(urn:li:dataPlatform:<platform>,<database>.<schema>.<table>,<env>)
```

Examples:
```
urn:li:dataset:(urn:li:dataPlatform:teradata,sampledb.analytics.customers,PROD)
urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.public.events,DEV)
urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)
```

## Response Time

Typical parse times:
- **First query:** 100-200ms (includes connection setup)
- **Subsequent queries:** 50-150ms
- **Complex queries (10+ tables):** 200-500ms

Factors affecting performance:
- Network latency to DataHub server
- Query complexity
- Whether tables exist in DataHub (schema lookups)

## Version Compatibility

| DataHub Version | query_type | default_dialect | Notes |
|-----------------|------------|-----------------|-------|
| 0.13.x+ | ✓ | ✓ | Latest features |
| 0.12.x | ✓ | ✗ | No dialect parameter |
| 0.11.x | ✓ | ✗ | Older API |

The test script handles version differences automatically.
