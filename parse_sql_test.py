"""
Enhanced SQL Parser Testing Script for DataHub

This script extends parse_sql_minimal.py with comprehensive testing capabilities:
- Batch testing mode for multiple SQL files
- Verbose debug output with confidence scores and parser warnings
- Structured error reporting and test metrics
- Query complexity analysis
- Expected vs. actual lineage validation
- Performance profiling and detailed timing
- CSV/JSON test report generation
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, DefaultDict, Dict, Iterable, List, Optional, Set

from datahub.emitter.mce_builder import dataset_urn_to_key, make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph, DatahubClientConfig
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage,
)


TOKEN = os.environ.get(
    "DATAHUB_TOKEN",
    "eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6ImRhdGFodWIiLCJ0eXBlIjoiUEVSU09OQUwiLCJ2ZXJzaW9uIjoiMiIsImp0aSI6IjI1OGRjMWNmLWYwNTItNDlmZS05M2YxLWFlNDc2NTdlZTNkNiIsInN1YiI6ImRhdGFodWIiLCJleHAiOjE3NTQ0MTczMjIsImlzcyI6ImRhdGFodWItbWV0YWRhdGEtc2VydmljZSJ9.qc3sjJamZBlOEQzlogyq20MnXrqP3kLsgCaAhRJ46_E",
)


@dataclass
class QueryComplexity:
    """Metrics describing SQL query complexity"""

    cte_count: int = 0
    max_cte_depth: int = 0
    subquery_count: int = 0
    join_count: int = 0
    union_count: int = 0
    window_function_count: int = 0
    line_count: int = 0
    char_count: int = 0


@dataclass
class TestResult:
    """Results from parsing a single SQL statement"""

    query_id: str
    query_file: str
    statement_index: int
    statement_type: str
    dialect: str
    success: bool
    confidence: float
    parse_time_ms: float

    # Lineage results
    in_tables: List[str]
    out_tables: List[str]
    column_lineage_count: int

    # Error information
    error_message: Optional[str]
    debug_info: Optional[str]
    table_error: bool

    # Complexity metrics
    complexity: QueryComplexity

    # Validation (if expected results provided)
    validation_status: Optional[str]  # "passed", "failed", "no_expected"
    validation_details: Optional[Dict[str, Any]]


def analyze_query_complexity(sql: str) -> QueryComplexity:
    """Analyze SQL query to extract complexity metrics"""
    complexity = QueryComplexity()

    # Basic metrics
    complexity.line_count = len(sql.splitlines())
    complexity.char_count = len(sql)

    # Case-insensitive pattern matching
    sql_upper = sql.upper()

    # Count CTEs
    with_matches = re.finditer(r'\bWITH\b', sql_upper)
    cte_matches = re.finditer(r'\bAS\s*\(', sql_upper)
    complexity.cte_count = len(list(cte_matches))

    # Estimate max CTE depth (simplified heuristic)
    if complexity.cte_count > 0:
        # Count max nested parentheses depth as proxy
        max_depth = 0
        current_depth = 0
        for char in sql:
            if char == '(':
                current_depth += 1
                max_depth = max(max_depth, current_depth)
            elif char == ')':
                current_depth -= 1
        complexity.max_cte_depth = min(max_depth // 2, complexity.cte_count)

    # Count subqueries (SELECT in SELECT)
    select_count = len(re.findall(r'\bSELECT\b', sql_upper))
    complexity.subquery_count = max(0, select_count - 1)

    # Count JOINs
    join_patterns = [r'\bINNER\s+JOIN\b', r'\bLEFT\s+JOIN\b', r'\bRIGHT\s+JOIN\b',
                     r'\bFULL\s+JOIN\b', r'\bCROSS\s+JOIN\b', r'\bJOIN\b']
    for pattern in join_patterns:
        complexity.join_count += len(re.findall(pattern, sql_upper))

    # Count UNIONs
    complexity.union_count = len(re.findall(r'\bUNION\s+(ALL\s+)?', sql_upper))

    # Count window functions
    window_patterns = [r'\bROW_NUMBER\s*\(', r'\bRANK\s*\(', r'\bDENSE_RANK\s*\(',
                       r'\bLEAD\s*\(', r'\bLAG\s*\(', r'\bFIRST_VALUE\s*\(',
                       r'\bLAST_VALUE\s*\(', r'\bOVER\s*\(']
    for pattern in window_patterns:
        complexity.window_function_count += len(re.findall(pattern, sql_upper))

    return complexity


def infer_statement_type(sql: str) -> str:
    """Infer the SQL statement type from the query"""
    sql_upper = sql.strip().upper()

    if sql_upper.startswith('SELECT'):
        return 'SELECT'
    elif sql_upper.startswith('INSERT'):
        return 'INSERT'
    elif sql_upper.startswith('UPDATE'):
        return 'UPDATE'
    elif sql_upper.startswith('DELETE'):
        return 'DELETE'
    elif sql_upper.startswith('MERGE'):
        return 'MERGE'
    elif sql_upper.startswith('CREATE VIEW'):
        return 'CREATE_VIEW'
    elif sql_upper.startswith('CREATE TABLE'):
        if 'AS SELECT' in sql_upper or 'AS\nSELECT' in sql_upper:
            return 'CTAS'
        return 'CREATE_TABLE'
    elif sql_upper.startswith('CREATE'):
        return 'CREATE_OTHER'
    elif sql_upper.startswith('WITH'):
        return 'CTE'
    else:
        return 'UNKNOWN'


def load_expected_results(expected_file: Path) -> Optional[Dict[str, Any]]:
    """Load expected lineage results from JSON file"""
    if not expected_file.exists():
        return None
    try:
        with open(expected_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"[warn] Failed to load expected results from {expected_file}: {e}", file=sys.stderr)
        return None


def validate_results(actual: Any, expected: Dict[str, Any]) -> Dict[str, Any]:
    """Compare actual parser results against expected results"""
    validation = {
        "tables_match": False,
        "columns_match": False,
        "tables_precision": 0.0,
        "tables_recall": 0.0,
        "columns_precision": 0.0,
        "columns_recall": 0.0,
        "missing_tables": [],
        "extra_tables": [],
        "missing_columns": [],
        "extra_columns": [],
    }

    # Validate table-level lineage
    actual_in = set(getattr(actual, "in_tables", []) or [])
    actual_out = set(getattr(actual, "out_tables", []) or [])
    expected_in = set(expected.get("in_tables", []))
    expected_out = set(expected.get("out_tables", []))

    actual_all = actual_in | actual_out
    expected_all = expected_in | expected_out

    if expected_all:
        true_positives = len(actual_all & expected_all)
        validation["tables_precision"] = true_positives / len(actual_all) if actual_all else 0.0
        validation["tables_recall"] = true_positives / len(expected_all) if expected_all else 0.0
        validation["tables_match"] = actual_all == expected_all
        validation["missing_tables"] = list(expected_all - actual_all)
        validation["extra_tables"] = list(actual_all - expected_all)

    # Validate column-level lineage
    actual_col_lineage = getattr(actual, "column_lineage", []) or []
    expected_col_lineage = expected.get("column_lineage", [])

    # Convert to comparable format (simplified)
    actual_col_pairs = set()
    for entry in actual_col_lineage:
        downstream_info = getattr(entry, "downstream", None)
        if downstream_info:
            downstream_col = f"{getattr(downstream_info, 'table', '')}:{getattr(downstream_info, 'column', '')}"
            for upstream in getattr(entry, "upstreams", []):
                upstream_col = f"{getattr(upstream, 'table', '')}:{getattr(upstream, 'column', '')}"
                actual_col_pairs.add((upstream_col, downstream_col))

    expected_col_pairs = set()
    for entry in expected_col_lineage:
        downstream = entry.get("downstream", {})
        downstream_col = f"{downstream.get('table', '')}:{downstream.get('column', '')}"
        for upstream in entry.get("upstreams", []):
            upstream_col = f"{upstream.get('table', '')}:{upstream.get('column', '')}"
            expected_col_pairs.add((upstream_col, downstream_col))

    if expected_col_pairs:
        true_positives = len(actual_col_pairs & expected_col_pairs)
        validation["columns_precision"] = true_positives / len(actual_col_pairs) if actual_col_pairs else 0.0
        validation["columns_recall"] = true_positives / len(expected_col_pairs) if expected_col_pairs else 0.0
        validation["columns_match"] = actual_col_pairs == expected_col_pairs
        validation["missing_columns"] = [f"{up}->{down}" for up, down in (expected_col_pairs - actual_col_pairs)]
        validation["extra_columns"] = [f"{up}->{down}" for up, down in (actual_col_pairs - expected_col_pairs)]

    return validation


def parse_single_statement(
    graph: DataHubGraph,
    statement: str,
    query_id: str,
    query_file: str,
    statement_index: int,
    args: argparse.Namespace,
    expected_results: Optional[Dict[str, Any]] = None,
) -> TestResult:
    """Parse a single SQL statement and return structured test results"""

    # Analyze complexity
    complexity = analyze_query_complexity(statement)
    statement_type = infer_statement_type(statement)

    # Parse with timing
    start_ns = time.perf_counter_ns()
    error_message = None
    result = None

    try:
        # Build kwargs for parse_sql_lineage (handle version compatibility)
        parse_kwargs = {
            "sql": statement,
            "platform": args.platform,
            "env": args.env,
        }

        # Add optional parameters if provided
        if args.default_db:
            parse_kwargs["default_db"] = args.default_db
        if args.default_schema:
            parse_kwargs["default_schema"] = args.default_schema

        # Try to use default_dialect if supported (newer DataHub versions)
        try:
            result = graph.parse_sql_lineage(
                **parse_kwargs,
                default_dialect=args.default_dialect,
            )
        except TypeError as te:
            # If default_dialect not supported, try without it
            if "default_dialect" in str(te):
                result = graph.parse_sql_lineage(**parse_kwargs)
            else:
                raise

        success = True
    except Exception as e:
        error_message = str(e)
        success = False
    finally:
        elapsed_ns = time.perf_counter_ns() - start_ns
        parse_time_ms = elapsed_ns / 1_000_000

    # Extract results
    confidence = 0.0
    in_tables = []
    out_tables = []
    column_lineage_count = 0
    debug_info_str = None
    table_error = False

    if result:
        debug_info = getattr(result, "debug_info", None)
        if debug_info:
            confidence = getattr(debug_info, "confidence", 0.0)
            table_error = getattr(debug_info, "table_error", False)
            error = getattr(debug_info, "error", None)
            if error:
                error_message = str(error)
                success = False

            # Capture full debug info in verbose mode
            if args.verbose:
                debug_info_str = json.dumps({
                    "confidence": confidence,
                    "table_error": table_error,
                    "error": str(error) if error else None,
                }, indent=2)

        in_tables = list(getattr(result, "in_tables", []) or [])
        out_tables = list(getattr(result, "out_tables", []) or [])
        column_lineage = getattr(result, "column_lineage", []) or []
        column_lineage_count = len(column_lineage)

    # Validate against expected results if provided
    validation_status = "no_expected"
    validation_details = None

    if expected_results and result:
        validation_details = validate_results(result, expected_results)
        if validation_details["tables_match"] and validation_details["columns_match"]:
            validation_status = "passed"
        else:
            validation_status = "failed"

    return TestResult(
        query_id=query_id,
        query_file=query_file,
        statement_index=statement_index,
        statement_type=statement_type,
        dialect=args.default_dialect,
        success=success,
        confidence=confidence,
        parse_time_ms=parse_time_ms,
        in_tables=in_tables,
        out_tables=out_tables,
        column_lineage_count=column_lineage_count,
        error_message=error_message,
        debug_info=debug_info_str,
        table_error=table_error,
        complexity=complexity,
        validation_status=validation_status,
        validation_details=validation_details,
    )


def process_sql_file(
    graph: DataHubGraph,
    sql_file: Path,
    args: argparse.Namespace,
    expected_dir: Optional[Path] = None,
) -> List[TestResult]:
    """Process a single SQL file and return test results for all statements"""

    sql_text = sql_file.read_text(encoding='utf-8')
    statements = [stmt.strip() for stmt in sql_text.split(";") if stmt.strip()]

    query_id = sql_file.stem
    results = []

    # Load expected results if available
    expected_results = None
    if expected_dir:
        expected_file = expected_dir / f"{query_id}.json"
        expected_results = load_expected_results(expected_file)

    for idx, statement in enumerate(statements, start=1):
        # For multi-statement files, check if expected results has per-statement expectations
        stmt_expected = None
        if expected_results:
            if isinstance(expected_results, list) and len(expected_results) >= idx:
                stmt_expected = expected_results[idx - 1]
            elif isinstance(expected_results, dict) and len(statements) == 1:
                stmt_expected = expected_results

        result = parse_single_statement(
            graph, statement, query_id, str(sql_file), idx, args, stmt_expected
        )
        results.append(result)

        # Print results in real-time if verbose
        if args.verbose:
            print(f"\n{'='*80}", file=sys.stderr)
            print(f"File: {sql_file.name} | Statement {idx}/{len(statements)}", file=sys.stderr)
            print(f"Type: {result.statement_type} | Success: {result.success} | Confidence: {result.confidence:.3f}", file=sys.stderr)
            print(f"Parse time: {result.parse_time_ms:.3f} ms", file=sys.stderr)
            print(f"Tables IN: {len(result.in_tables)} | OUT: {len(result.out_tables)} | Columns: {result.column_lineage_count}", file=sys.stderr)
            print(f"Complexity: CTEs={result.complexity.cte_count}, Subqueries={result.complexity.subquery_count}, JOINs={result.complexity.join_count}", file=sys.stderr)
            if result.error_message:
                print(f"ERROR: {result.error_message}", file=sys.stderr)
            if result.debug_info:
                print(f"Debug Info: {result.debug_info}", file=sys.stderr)
            if result.validation_status != "no_expected":
                print(f"Validation: {result.validation_status}", file=sys.stderr)
                if result.validation_details:
                    print(f"  Tables - Precision: {result.validation_details['tables_precision']:.2f}, Recall: {result.validation_details['tables_recall']:.2f}", file=sys.stderr)
                    print(f"  Columns - Precision: {result.validation_details['columns_precision']:.2f}, Recall: {result.validation_details['columns_recall']:.2f}", file=sys.stderr)

    return results


def write_results_csv(results: List[TestResult], output_file: Path) -> None:
    """Write test results to CSV file"""

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)

        # Header
        writer.writerow([
            'query_id', 'query_file', 'statement_index', 'statement_type', 'dialect',
            'success', 'confidence', 'parse_time_ms',
            'in_tables_count', 'out_tables_count', 'column_lineage_count',
            'error_message', 'table_error',
            'cte_count', 'max_cte_depth', 'subquery_count', 'join_count', 'union_count',
            'window_function_count', 'line_count', 'char_count',
            'validation_status', 'tables_precision', 'tables_recall',
            'columns_precision', 'columns_recall',
        ])

        # Data rows
        for result in results:
            val_details = result.validation_details or {}
            writer.writerow([
                result.query_id,
                result.query_file,
                result.statement_index,
                result.statement_type,
                result.dialect,
                result.success,
                f"{result.confidence:.4f}",
                f"{result.parse_time_ms:.3f}",
                len(result.in_tables),
                len(result.out_tables),
                result.column_lineage_count,
                result.error_message or '',
                result.table_error,
                result.complexity.cte_count,
                result.complexity.max_cte_depth,
                result.complexity.subquery_count,
                result.complexity.join_count,
                result.complexity.union_count,
                result.complexity.window_function_count,
                result.complexity.line_count,
                result.complexity.char_count,
                result.validation_status,
                f"{val_details.get('tables_precision', 0):.4f}",
                f"{val_details.get('tables_recall', 0):.4f}",
                f"{val_details.get('columns_precision', 0):.4f}",
                f"{val_details.get('columns_recall', 0):.4f}",
            ])


def write_results_json(results: List[TestResult], output_file: Path) -> None:
    """Write detailed test results to JSON file"""

    results_dict = [
        {
            **{k: v for k, v in asdict(result).items() if k != 'complexity'},
            'complexity': asdict(result.complexity),
        }
        for result in results
    ]

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results_dict, f, indent=2)


def print_summary(results: List[TestResult]) -> None:
    """Print summary statistics"""

    total = len(results)
    successful = sum(1 for r in results if r.success)
    high_confidence = sum(1 for r in results if r.confidence >= 0.9)
    with_column_lineage = sum(1 for r in results if r.column_lineage_count > 0)

    # Validation stats
    validated = [r for r in results if r.validation_status != "no_expected"]
    validation_passed = sum(1 for r in validated if r.validation_status == "passed")

    # Performance stats
    avg_parse_time = sum(r.parse_time_ms for r in results) / total if total > 0 else 0
    max_parse_time = max((r.parse_time_ms for r in results), default=0)

    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    print(f"Total statements: {total}")
    print(f"Successful parses: {successful} ({successful/total*100:.1f}%)")
    print(f"High confidence (>=0.9): {high_confidence} ({high_confidence/total*100:.1f}%)")
    print(f"With column lineage: {with_column_lineage} ({with_column_lineage/total*100:.1f}%)")

    if validated:
        print(f"\nValidation (against expected results):")
        print(f"  Validated: {len(validated)}")
        print(f"  Passed: {validation_passed} ({validation_passed/len(validated)*100:.1f}%)")

    print(f"\nPerformance:")
    print(f"  Average parse time: {avg_parse_time:.3f} ms")
    print(f"  Max parse time: {max_parse_time:.3f} ms")

    # Break down by statement type
    by_type = defaultdict(list)
    for r in results:
        by_type[r.statement_type].append(r)

    print(f"\nBy statement type:")
    for stmt_type, type_results in sorted(by_type.items()):
        type_success = sum(1 for r in type_results if r.success)
        print(f"  {stmt_type}: {type_success}/{len(type_results)} ({type_success/len(type_results)*100:.1f}%)")

    # Error analysis
    errors = [r for r in results if r.error_message]
    if errors:
        print(f"\nErrors encountered: {len(errors)}")
        error_types = defaultdict(int)
        for r in errors:
            # Simplify error message to type
            error_type = r.error_message.split(':')[0] if r.error_message else "Unknown"
            error_types[error_type] += 1
        for error_type, count in sorted(error_types.items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"  {error_type}: {count}")

    print("="*80 + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="parse_sql_test.py",
        description="Enhanced SQL parser testing with batch mode, validation, and detailed reporting.",
    )

    # Input
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--sql-file", help="Single SQL file to test.")
    input_group.add_argument("--sql-dir", help="Directory containing SQL files to test (batch mode).")

    # DataHub connection
    parser.add_argument(
        "--server",
        default=os.getenv("DATAHUB_SERVER", "http://localhost:8080"),
        help="DataHub GMS endpoint (default: %(default)s or DATAHUB_SERVER).",
    )

    # Parser configuration
    parser.add_argument(
        "--platform",
        default=os.getenv("DATAHUB_PLATFORM", "teradata"),
        help="Dataset platform (default: %(default)s or DATAHUB_PLATFORM).",
    )
    parser.add_argument(
        "--env",
        default=os.getenv("DATAHUB_ENV", "PROD"),
        help="Dataset environment (default: %(default)s or DATAHUB_ENV).",
    )
    parser.add_argument("--default-db", help="Default database name.")
    parser.add_argument("--default-schema", help="Default schema name.")
    parser.add_argument(
        "--default-dialect",
        default="teradata",
        help="SQL dialect hint (default: %(default)s).",
    )

    # Validation
    parser.add_argument(
        "--expected-dir",
        help="Directory containing expected results JSON files for validation.",
    )

    # Output
    parser.add_argument(
        "--output-csv",
        help="Path to write CSV test results.",
    )
    parser.add_argument(
        "--output-json",
        help="Path to write detailed JSON test results.",
    )

    # Options
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output with detailed debug information.",
    )
    parser.add_argument(
        "--pattern",
        default="*.sql",
        help="File pattern for batch mode (default: *.sql).",
    )

    args = parser.parse_args()

    # Initialize DataHub client
    graph = DataHubGraph(DatahubClientConfig(server=args.server, token=TOKEN))

    # Collect SQL files to process
    sql_files = []
    if args.sql_file:
        sql_files = [Path(args.sql_file)]
    elif args.sql_dir:
        sql_dir = Path(args.sql_dir)
        sql_files = sorted(sql_dir.rglob(args.pattern))
        print(f"Found {len(sql_files)} SQL files matching pattern '{args.pattern}'", file=sys.stderr)

    if not sql_files:
        print("No SQL files found to process.", file=sys.stderr)
        sys.exit(1)

    # Process files
    expected_dir = Path(args.expected_dir) if args.expected_dir else None
    all_results = []

    for sql_file in sql_files:
        print(f"\nProcessing: {sql_file.relative_to(Path.cwd()) if sql_file.is_relative_to(Path.cwd()) else sql_file}", file=sys.stderr)
        results = process_sql_file(graph, sql_file, args, expected_dir)
        all_results.extend(results)

    # Write outputs
    if args.output_csv:
        csv_path = Path(args.output_csv)
        write_results_csv(all_results, csv_path)
        print(f"\nCSV results written to: {csv_path}", file=sys.stderr)

    if args.output_json:
        json_path = Path(args.output_json)
        write_results_json(all_results, json_path)
        print(f"JSON results written to: {json_path}", file=sys.stderr)

    # Print summary
    print_summary(all_results)


if __name__ == "__main__":
    main()
