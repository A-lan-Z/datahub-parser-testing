# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository contains `parse_sql_minimal.py`, a command-line tool that wraps the DataHub lineage parser. It parses SQL statements from a file, retrieves lineage information from a DataHub GMS server, and optionally emits the parsed lineage back into DataHub.

## Prerequisites

- Python 3.8+ with the `datahub` ingestion client installed
- A running DataHub GMS endpoint (default: `http://localhost:8080`)
- A personal access token with Metadata Write privileges

## Running the Script

Basic usage:
```bash
python parse_sql_minimal.py path/to/query.sql \
  --server http://localhost:8080 \
  --platform teradata \
  --env PROD \
  --default-db SampleDB \
  --default-schema Analytics \
  --default-dialect teradata
```

With lineage emission:
```bash
python parse_sql_minimal.py path/to/query.sql \
  --platform teradata \
  --emit-lineage \
  --output parsed.json
```

Key parameters:
- `sql_file` (positional): Path to SQL file; statements are split on `;`
- `--server`: DataHub GMS URL (defaults to `DATAHUB_SERVER` env var or `http://localhost:8080`)
- `--platform`: Dataset platform (e.g., `hive`, `snowflake`, `teradata`)
- `--env`: Dataset environment (`PROD`, `CORP`, etc.)
- `--default-db` / `--default-schema`: Hints for unqualified table names
- `--default-dialect`: Parser dialect hint (default: `teradata`)
- `--output`: Write JSON response to this path
- `--emit-lineage`: Build and emit Metadata Change Proposals (MCPs) for parsed lineage

## Architecture

### Core Workflow

1. **SQL Parsing**: SQL file is split on `;` and each statement is sent to DataHub's `parse_sql_lineage` API
2. **Lineage Generation**: Parser returns table-level (`in_tables`, `out_tables`) and column-level lineage (`column_lineage`)
3. **MCP Construction**: `_generate_lineage_mcps` converts lineage into DataHub Metadata Change Proposals
4. **Dataset Scaffolding**: `_ensure_datasets_exist` auto-creates missing datasets before emitting lineage
5. **Emission**: MCPs are sent via `DataHubGraph.emit_mcps`

### Key Functions

- `_build_fine_grained_lineage`: Converts column-level lineage into `FineGrainedLineage` objects, filtering out self-referential relationships
- `_build_upstream_lineage_aspect`: Creates `UpstreamLineage` aspect with table-level and column-level lineage; conditionally includes confidence score if supported
- `_generate_lineage_mcps`: Generates one MCP per downstream dataset with all upstream relationships
- `_accumulate_dataset_columns`: Tracks all datasets and their columns mentioned in lineage for scaffolding
- `_build_dataset_scaffold_mcps`: Creates minimal dataset properties and schema aspects for auto-creation
- `_ensure_datasets_exist`: Checks dataset existence and creates scaffolds as needed before lineage emission

### Authentication

The script currently uses a hard-coded `TOKEN` constant (line 33). For production use, replace with:
```python
TOKEN = os.environ["DATAHUB_TOKEN"]
```

### Output Behavior

- **stdout**: Pretty-printed JSON for each statement's parser response
- **stderr**: Timing logs and emission warnings (keeps stdout as valid JSON)
- High-resolution timing for each `parse_sql_lineage` RPC is logged with millisecond precision
- Multi-statement runs include total parse time summary

### Self-Referential Lineage Handling

The script filters out self-referential lineage at two levels:
- **Column-level** (line 65-67): Skips column lineage where upstream dataset equals target dataset
- **Table-level** (line 124-131): Filters upstream tables matching downstream dataset; skips MCPs with empty upstreams and no fine-grained lineage

This prevents DataHub from receiving lineage that it cannot render meaningfully.

### Version Compatibility

The script checks for `confidenceScore` parameter support in `UpstreamLineage` (line 35-36) to maintain compatibility across DataHub versions.
