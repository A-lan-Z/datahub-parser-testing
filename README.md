# DataHub SQL Parser Testing

This repository provides comprehensive testing tools for the DataHub SQL lineage parser, with a focus on evaluating parser capabilities, limitations, and accuracy for Teradata SQL (and other dialects).

## Contents

- **parse_sql_minimal.py** - Simple command-line wrapper for basic SQL parsing
- **parse_sql_test.py** - Enhanced testing script with batch mode, metrics, and validation
- **analyze_results.py** - Statistical analysis and reporting tool
- **test-queries/** - Organized test suite with 27+ sample queries
- **run_tests.sh** - Automated test runner for complete suite

## Quick Start

```bash
# Install dependencies
pip install acryl-datahub

# Run a single test
python parse_sql_test.py --sql-file test-queries/teradata/01-basic/01_simple_select.sql --verbose

# Run complete test suite
./run_tests.sh

# Analyze results
python analyze_results.py test-results/all-results.csv
```

See **[QUICKSTART.md](QUICKSTART.md)** for a 5-minute tutorial.

See **[TESTING_FRAMEWORK.md](TESTING_FRAMEWORK.md)** for complete documentation.

See **[TROUBLESHOOTING.md](TROUBLESHOOTING.md)** if you encounter errors.

---

## parse_sql_minimal.py

`parse_sql_minimal.py` is a thin command-line wrapper around the DataHub lineage
parser. It reads SQL statements from a local file, asks the DataHub GMS server
to parse them, prints the raw JSON response for every statement, and (optionally)
emits the parsed lineage back into DataHub.

## Prerequisites

- Python 3.8+ with the `datahub` ingestion client and its dependencies installed.
- Access to a running DataHub GMS endpoint (`http://localhost:8080` by default).
- A personal access token with `Metadata Write` privileges.

> **Security note:** The sample script currently hard-codes a `TOKEN` constant.
  Replace it with a secure mechanism (e.g., `os.environ["DATAHUB_TOKEN"]`) before
  running this in a real environment.

## Usage

```bash
python parse_sql_minimal.py path/to/query.sql \
  --server http://localhost:8080 \
  --platform teradata \
  --env PROD \
  --default-db SampleDB \
  --default-schema Analytics \
  --default-dialect teradata \
  --output parsed.json \
  --emit-lineage
```

Arguments:

| Flag | Description |
| --- | --- |
| `sql_file` (positional) | Path to a file whose contents are split on `;` and parsed statement-by-statement. |
| `--server` | DataHub GMS base URL; defaults to `DATAHUB_SERVER` env var or `http://localhost:8080`. |
| `--platform` | Logical platform of the datasets (e.g., `hive`, `snowflake`, `teradata`). |
| `--env` | Dataset environment (`PROD`, `CORP`, etc.). |
| `--default-db` / `--default-schema` | Optional hints used by the parser when SQL statements reference unqualified tables. |
| `--default-dialect` | Parser dialect hint (default `teradata`). |
| `--output` | When provided, writes the JSON response (single result or list) to this path. |
| `--emit-lineage` | If set, builds and emits Metadata Change Proposals (MCPs) for the parsed lineage. |

Stdout receives the JSON payload for each statement; stderr carries timing logs
and any emission warnings so that stdout stays valid JSON.

## Emitting lineage

When `--emit-lineage` is supplied and the parser reports no errors:

1. `_generate_lineage_mcps` converts table- and column-level lineage into MCP
   objects (one per downstream dataset).
2. `_accumulate_dataset_columns` tracks the columns mentioned by the lineage.
3. If a dataset referenced in the lineage does not yet exist in DataHub,
   `_ensure_datasets_exist` auto-creates a basic scaffold (properties + schema)
   before lineage emission.
4. MCPs are printed for inspection and then sent via `DataHubGraph.emit_mcps`.

You will see a summary of every downstream dataset that received lineage after a
successful emit.

## Output and timing

- Each statement's parser response is printed as pretty JSON and includes any
  parser-side error string under `debugInfoError`.
- High-resolution timings for the `parse_sql_lineage` RPC are logged to stderr.
  Multi-statement runs include a total parse-time summary.
- When `--output` is supplied, the first (or aggregated) response is also written
  to disk as formatted JSON.

## Notes and troubleshooting

- Ensure your DataHub token matches the target server; otherwise every RPC will
  fail with `401 Unauthorized`.
- The script assumes statements are separated by `;`. Remove trailing semicolons
  from dialects that treat them as part of a string literal.
- If you only need to inspect the parser output, omit `--emit-lineage` to avoid
  making any changes to your DataHub instance.
- Failures while auto-creating dataset scaffolds or emitting lineage are logged
  but do not stop the rest of the run; inspect stderr for `[emit]` messages.
