# SQL Lineage Utilities

## Requirements

- Python 3.9+ with the `acryl-datahub` package (for `DataHubGraph`, MCP builders, and schema classes).
- Network access to a DataHub GMS endpoint plus a user token. `parse_sql_minimal.py` currently keeps the token in the `TOKEN` constant.

## Workflow

1. Collect SQL to test. You can mix and match these inputs—every statement is split on semicolons and recorded with the file/row it came from:
   - `--sql-file`: point to a `.sql` file **or** directory; directories are searched recursively while individual files are split into multiple statements.
   - `--sql-dir`: flag for directories only; all `.sql` files beneath each directory are parsed.
   - `--csv-spec PATH:COLUMN`: read a single CSV file and extract SQL from the given column. Each row/statement pair becomes its own query.
   - `--csv-dir`: recurse through directories of CSVs (must pair with `--csv-dir-column` so the script knows which column to use).
2. Run the parser:

   ```bash
   python3 parse_sql_minimal.py \
     --sql-dir test-queries \
     --csv-spec reports.csv:sql_text \
     --server http://localhost:8080 \
     --platform teradata \
     --env PROD \
     --default-db analytics \
     --default-schema staging \
     --override-dialect teradata \
     --emit-lineage
   ```

   Key switches:

   - `--server`, `--platform`, `--env`, `--default-db`, `--default-schema`, `--override-dialect` mirror the parser RPC inputs.
   - `--emit-lineage` enables `LineageEmitter.collect/emit`, pushing the parsed lineage back into DataHub once the batch completes.

3. Inspect results. Each source file (or CSV) gets a folder named `[FLAGS]<source>--<hash>` containing:
   - One JSON file per statement with the raw parser payload, a terminal transcript, flags, and a preview of the SQL.
   - `[[]]report.json` with high‑level stats (counts, timing aggregates, statement‑type breakdowns, parser error classes).
   - `[[]]report.md` rendered via `report_utils.render_report_markdown()` for quick sharing.
   A run‑wide `[[]]report.json`/`.md` pair sits at the root; `report_utils.print_overview()` also writes a terse console summary.

4. (Optional) Emit lineage. When enabled, `LineageEmitter`:
   - Tracks every dataset URN and referenced columns while parsing.
   - Generates dataset scaffolds (properties + schema with placeholder field types) for any missing URN before emitting lineage MCPs.
   - Produces both table‑level and fine‑grained column lineage (when available) and logs each payload prior to sending.

## Flags & Reports

- Flags (`ERR`, `GAP`, `LIN`, `SELF`, `COL`, `OK`) are derived by `parse_sql_minimal.py` and summarized per statement type by `report_utils.compute_statement_type_metrics()`. They highlight parser/RPC errors, missing upstream/downstream tables, self‑joins, and column lineage coverage.
- Markdown tables list timing statistics, parser vs. fallback classification sources, flag distributions, error classes, and raw parser error strings.
