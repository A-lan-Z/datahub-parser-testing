from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from datahub.ingestion.graph.client import DataHubGraph, DatahubClientConfig

from emit_lineage import LineageEmitter
from report_utils import (
    build_debug_error_summary,
    compute_overview,
    compute_statement_type_metrics,
    print_overview,
    render_report_markdown,
)

TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6ImRhdGFodWIiLCJ0eXBlIjoiUEVSU09OQUwiLCJ2ZXJzaW9uIjoiMiIsImp0aSI6IjI1OGRjMWNmLWYwNTItNDlmZS05M2YxLWFlNDc2NTdlZTNkNiIsInN1YiI6ImRhdGFodWIiLCJleHAiOjE3NTQ0MTczMjIsImlzcyI6ImRhdGFodWItbWV0YWRhdGEtc2VydmljZSJ9.qc3sjJamZBlOEQzlogyq20MnXrqP3kLsgCaAhRJ46_E"
@dataclass
class QueryTask:
    identifier: str
    query_text: str
    origin: str
    context: str
    source_path: Path


@dataclass
class QueryOutcome:
    task: QueryTask
    upstreams: List[str]
    downstreams: List[str]
    column_edges: List[str]
    timing_ms: float
    parser_error: Optional[str]
    rpc_error: Optional[str]
    self_referential: bool
    raw_payload: Dict[str, Any]
    raw_json_path: Optional[Path] = None
    terminal_output: Optional[str] = None
    flags: List[str] = field(default_factory=list)
    statement_type: str = "UNKNOWN"
    statement_type_source: str = "unknown"
    parser_statement_type: Optional[str] = None

    @property
    def succeeded(self) -> bool:
        return not (self.rpc_error or self.parser_error)


def _split_statements(sql_text: str) -> List[str]:
    return [stmt.strip() for stmt in sql_text.split(";") if stmt.strip()]


def _safe_filename(label: str) -> str:
    sanitized = [c if c.isalnum() or c in {"-", "_"} else "_" for c in label]
    return "".join(sanitized)[:128] or "result"


def _load_tasks_from_file(path: Path) -> List[QueryTask]:
    text = path.read_text(encoding="utf-8")
    statements = _split_statements(text)
    tasks: List[QueryTask] = []
    for idx, statement in enumerate(statements, start=1):
        identifier = f"{path}:{idx}"
        tasks.append(
            QueryTask(
                identifier=identifier,
                query_text=statement,
                origin="file",
                context=f"{path} (statement {idx})",
                source_path=path,
            )
        )
    return tasks


def _load_tasks_from_directory(path: Path) -> List[QueryTask]:
    tasks: List[QueryTask] = []
    for sql_file in sorted(path.rglob("*.sql")):
        tasks.extend(_load_tasks_from_file(sql_file))
    return tasks


def _load_tasks_from_csv(spec: str, delimiter: str) -> List[QueryTask]:
    try:
        csv_path_str, column = spec.split(":", 1)
    except ValueError as exc:
        raise ValueError(
            "CSV spec must be in the form PATH:COLUMN (e.g. queries.csv:sql_text)."
        ) from exc

    csv_path = Path(csv_path_str)
    with csv_path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=delimiter)
        if not reader.fieldnames or column not in reader.fieldnames:
            available = ", ".join(reader.fieldnames or [])
            raise ValueError(
                f"Column '{column}' not found in {csv_path}. Available columns: {available}"
            )
        tasks: List[QueryTask] = []
        for row_idx, row in enumerate(reader, start=2):
            cell_value = (row.get(column) or "").strip()
            if not cell_value:
                continue
            statements = _split_statements(cell_value)
            for stmt_idx, statement in enumerate(statements, start=1):
                identifier = f"{csv_path}:row{row_idx}:stmt{stmt_idx}"
                tasks.append(
                    QueryTask(
                        identifier=identifier,
                        query_text=statement,
                        origin="csv",
                        context=(
                            f"{csv_path} row {row_idx} column '{column}' (statement {stmt_idx})"
                        ),
                        source_path=csv_path,
                    )
                )
        return tasks


def _collect_tasks(
    sql_files: List[str],
    sql_dirs: List[str],
    csv_specs: List[str],
    csv_delimiter: str,
    csv_dirs: List[str],
    csv_dir_column: Optional[str],
) -> List[QueryTask]:
    tasks: List[QueryTask] = []

    for file_path in sql_files:
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"SQL file not found: {path}")
        if path.is_dir():
            tasks.extend(_load_tasks_from_directory(path))
        else:
            tasks.extend(_load_tasks_from_file(path))

    for dir_path in sql_dirs:
        path = Path(dir_path)
        if not path.exists() or not path.is_dir():
            raise FileNotFoundError(f"SQL directory not found: {path}")
        tasks.extend(_load_tasks_from_directory(path))

    for csv_spec in csv_specs:
        tasks.extend(_load_tasks_from_csv(csv_spec, csv_delimiter))

    if csv_dirs:
        if not csv_dir_column:
            raise ValueError("--csv-dir-column is required when using --csv-dir.")
        for dir_path in csv_dirs:
            path = Path(dir_path)
            if not path.exists() or not path.is_dir():
                raise FileNotFoundError(f"CSV directory not found: {path}")
            for csv_file in sorted(path.rglob("*.csv")):
                tasks.extend(
                    _load_tasks_from_csv(f"{csv_file}:{csv_dir_column}", csv_delimiter)
                )

    return tasks


def _column_lineage_edges(column_lineage: Optional[Iterable[object]]) -> List[str]:
    edges: List[str] = []
    if not column_lineage:
        return edges
    for entry in column_lineage:
        downstream = getattr(entry, "downstream", None)
        downstream_table = getattr(downstream, "table", None)
        downstream_column = getattr(downstream, "column", None)
        downstream_label = (
            f"{downstream_table}.{downstream_column}"
            if downstream_table and downstream_column
            else None
        )
        for upstream in getattr(entry, "upstreams", []) or []:
            upstream_table = getattr(upstream, "table", None)
            upstream_column = getattr(upstream, "column", None)
            if upstream_table and upstream_column and downstream_label:
                edges.append(f"{upstream_table}.{upstream_column} -> {downstream_label}")
    return edges


def _sanitize_component(label: str) -> str:
    allowed = {"-", "_", ".", "#"}
    sanitized = [c if c.isalnum() or c in allowed else "_" for c in label]
    cleaned = "".join(sanitized).strip("_")
    return cleaned[:128] or "result"


def _query_preview_lines(task: QueryTask, max_lines: Optional[int] = None) -> List[str]:
    source_label = str(task.source_path) if task.source_path else task.identifier
    lines = [line.rstrip() for line in task.query_text.strip().splitlines()]
    if max_lines is not None:
        lines = lines[:max_lines]
    formatted: List[str] = [f"{source_label} ::"]
    if not lines:
        formatted.append("    <empty>")
        return formatted
    for line in lines:
        formatted.append(f"    {line}" if line else "    ")
    return formatted


def _query_preview(task: QueryTask, max_lines: Optional[int] = None) -> str:
    return "\n".join(_query_preview_lines(task, max_lines=max_lines))


def _normalize_statement_type_label(label: Optional[Any]) -> Optional[str]:
    if label is None:
        return None
    text = str(label).strip()
    if not text:
        return None
    if "." in text:
        text = text.split(".")[-1]
    text = text.replace("/", "_").replace("-", "_")
    text = re.sub(r"\s+", "_", text)
    text = text.upper()
    return text or None


def _extract_parser_statement_type(result_obj: Any, payload: Dict[str, Any]) -> Optional[str]:
    for key in ("queryType", "query_type", "querytype"):
        value = payload.get(key)
        normalized = _normalize_statement_type_label(value)
        if normalized:
            return normalized
    result_value = getattr(result_obj, "query_type", None)
    if result_value is not None:
        if hasattr(result_value, "name"):
            result_value = result_value.name
        normalized = _normalize_statement_type_label(result_value)
        if normalized:
            return normalized
    return None


def _clean_sql_for_classification(sql_text: str) -> str:
    text = re.sub(r"/\*.*?\*/", " ", sql_text, flags=re.S)
    text = re.sub(r"(?m)--.*?$", " ", text)
    text = re.sub(r"(?m)//.*?$", " ", text)
    text = re.sub(r"(?m)#.*?$", " ", text)
    return text.strip()


def _leading_sql_tokens(sql_text: str, limit: int = 6) -> List[str]:
    cleaned = _clean_sql_for_classification(sql_text)
    if not cleaned:
        return []
    tokens = re.findall(r"[A-Za-z_#]+", cleaned)
    return [token.upper() for token in tokens[:limit]]


def _infer_statement_type_from_sql(sql_text: str) -> str:
    tokens = _leading_sql_tokens(sql_text)
    if not tokens:
        return "UNKNOWN"
    first = tokens[0]
    second = tokens[1] if len(tokens) > 1 else ""
    third = tokens[2] if len(tokens) > 2 else ""

    if first == "SEL":
        return "SELECT"
    if first == "WITH":
        for token in tokens[1:]:
            if token in {"SELECT", "INSERT", "UPDATE", "DELETE", "MERGE"}:
                return token
        return "SELECT"
    if first in {"BT", "ET"}:
        return first
    if first == "DATABASE":
        return "DATABASE"
    if first == "USING":
        return "USING"
    if first in {"CALL", "EXEC", "EXECUTE"}:
        return "CALL"
    if first == "COLLECT" and second == "STATISTICS":
        return "COLLECT_STATISTICS"
    if first == "LOCKING":
        return "LOCKING"
    if first == "REPLACE":
        if second in {"PROCEDURE", "FUNCTION", "TABLE", "VIEW", "MACRO"}:
            return f"REPLACE_{second}"
        if second in {"SET", "MULTISET"} and third == "TABLE":
            return "REPLACE_TABLE"
    if first == "CREATE":
        if second in {"SET", "MULTISET"} and third == "TABLE":
            return "CREATE_TABLE"
        if second == "TABLE":
            return "CREATE_TABLE"
        if second == "VIEW":
            return "CREATE_VIEW"
        if second == "DATABASE":
            return "CREATE_DATABASE"
        if second in {"PROCEDURE", "MACRO", "FUNCTION"}:
            return f"CREATE_{second}"
    if first == "ALTER":
        if second in {"TABLE", "DATABASE", "PROCEDURE", "FUNCTION"}:
            return f"ALTER_{second}"
    if first == "DROP":
        if second in {"TABLE", "DATABASE", "VIEW", "PROCEDURE", "MACRO", "FUNCTION"}:
            return f"DROP_{second}"
    if first in {"INSERT", "UPDATE", "DELETE", "MERGE", "SELECT"}:
        return first
    if first == "GRANT":
        return "GRANT"
    if first == "REVOKE":
        return "REVOKE"
    if first == "DATABASE":
        return "DATABASE"
    if first == "LOGON":
        return "LOGON"
    if first == "LOGOFF":
        return "LOGOFF"
    if first == "BEGIN":
        if second == "TRANSACTION":
            return "BEGIN_TRANSACTION"
        return "BEGIN"
    return first or "UNKNOWN"


def _resolve_statement_type(parser_type: Optional[str], sql_text: str) -> Tuple[str, str]:
    normalized_parser_type = _normalize_statement_type_label(parser_type)
    if normalized_parser_type and normalized_parser_type != "UNKNOWN":
        return normalized_parser_type, "parser"
    inferred = _infer_statement_type_from_sql(sql_text)
    if inferred and inferred != "UNKNOWN":
        return inferred, "fallback"
    if inferred == "UNKNOWN":
        return "UNKNOWN", "fallback"
    if normalized_parser_type:
        return normalized_parser_type, "parser"
    return "UNKNOWN", "unknown"


FLAG_PRIORITY: Sequence[str] = ("ERR", "GAP", "LIN", "SELF", "COL", "OK")


def _build_flag_prefix(flags: Sequence[str]) -> str:
    ordered = [flag for flag in FLAG_PRIORITY if flag in flags]
    extras = [flag for flag in flags if flag not in FLAG_PRIORITY]
    ordered += sorted(extras)
    return "".join(f"[{flag}]" for flag in ordered)


def _compute_query_flags(outcome: QueryOutcome) -> List[str]:
    flags: List[str] = []
    has_upstreams = bool(outcome.upstreams)
    has_downstreams = bool(outcome.downstreams)
    complete_lineage = has_upstreams and has_downstreams
    partial_lineage = has_upstreams ^ has_downstreams

    if outcome.rpc_error or outcome.parser_error:
        flags.append("ERR")

    if partial_lineage:
        flags.append("GAP")

    if complete_lineage and not outcome.self_referential:
        flags.append("LIN")
    if outcome.self_referential:
        flags.append("SELF")
    has_column_lineage = bool(outcome.column_edges)
    if not has_column_lineage:
        column_lineage_payload = outcome.raw_payload.get("column_lineage")
        has_column_lineage = bool(column_lineage_payload)
    if has_column_lineage:
        flags.append("COL")
    if outcome.succeeded and not flags:
        flags.append("OK")
    return flags or ["OK"]


def _aggregate_folder_flags(outcomes: Sequence[QueryOutcome]) -> List[str]:
    flag_set = {flag for outcome in outcomes for flag in outcome.flags}
    non_ok = flag_set - {"OK"}
    if non_ok:
        flag_set = non_ok
    if not flag_set:
        flag_set = {"OK"}
    ordered = [flag for flag in FLAG_PRIORITY if flag in flag_set]
    extras = sorted(flag_set - set(FLAG_PRIORITY))
    return ordered + extras


def _build_source_folder_name(source_path: Path, flags: Sequence[str]) -> str:
    flag_prefix = _build_flag_prefix(flags)
    label = source_path.name or _sanitize_component(str(source_path))
    safe_label = _sanitize_component(label)
    hash_suffix = hashlib.sha1(str(source_path).encode("utf-8")).hexdigest()[:6]
    return f"{flag_prefix}{safe_label}--{hash_suffix}"


def _build_query_filename(outcome: QueryOutcome) -> str:
    flag_prefix = _build_flag_prefix(outcome.flags)
    identifier_label = _sanitize_component(outcome.task.identifier)
    hash_suffix = hashlib.sha1(outcome.task.identifier.encode("utf-8")).hexdigest()[:6]
    return f"{flag_prefix}{identifier_label}--{hash_suffix}.json"


def _render_query_outcome(index: int, total: int, outcome: QueryOutcome) -> str:
    status = "OK"
    if outcome.rpc_error:
        status = "RPC_ERROR"
    elif outcome.parser_error:
        status = "PARSE_ERROR"
    header = f"[{status}] Query {index}/{total}: {outcome.task.identifier}"
    lines = [
        "",
        header,
        "-" * len(header),
        f"Source: {outcome.task.context}",
        f"Flags: {_build_flag_prefix(outcome.flags)}" if outcome.flags else "Flags: []",
        f"Parse time: {outcome.timing_ms:.3f} ms",
        f"Self-referential lineage: {'YES' if outcome.self_referential else 'NO'}",
    ]
    if outcome.rpc_error:
        lines.append(f"RPC error: {outcome.rpc_error}")
    if outcome.parser_error:
        lines.append(f"Parser error: {outcome.parser_error}")

    lines.append("Query preview:")
    lines.append(_query_preview(outcome.task))

    lines.append("Lineage summary:")
    if not outcome.downstreams and not outcome.upstreams:
        lines.append("  (no upstream or downstream datasets detected)")
    else:
        if outcome.downstreams:
            lines.append("  Downstreams:")
            for dataset in outcome.downstreams:
                lines.append(f"    - {dataset}")
        if outcome.upstreams:
            lines.append("  Upstreams:")
            for dataset in outcome.upstreams:
                lines.append(f"    - {dataset}")
    if outcome.column_edges:
        lines.append("  Column lineage:")
        for edge in outcome.column_edges:
            lines.append(f"    - {edge}")

    raw_path_display = outcome.raw_json_path if outcome.raw_json_path else "<not written>"
    lines.append(f"Raw parser output: {raw_path_display}")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="parse_sql_minimal.py",
        description=(
            "Bulk-parse SQL via DataHub's lineage parser from files, directories, or CSV columns, "
            "and print human-readable lineage plus a run summary."
        ),
    )
    parser.add_argument(
        "--sql-file",
        action="append",
        default=[],
        help="Path to a .sql file. Repeat to add more or pass a directory to recurse for .sql files.",
    )
    parser.add_argument(
        "--sql-dir",
        action="append",
        default=[],
        help="Directory to search recursively for .sql files.",
    )
    parser.add_argument(
        "--csv-spec",
        action="append",
        default=[],
        metavar="PATH:COLUMN",
        help="CSV file and column name that contains SQL text (e.g. reports.csv:query).",
    )
    parser.add_argument(
        "--csv-dir",
        action="append",
        default=[],
        help="Directory containing CSV files (searched recursively). Use with --csv-dir-column.",
    )
    parser.add_argument(
        "--csv-dir-column",
        default=None,
        help="Column name containing SQL text for all files supplied via --csv-dir.",
    )
    parser.add_argument(
        "--csv-delimiter",
        default=",",
        help="Delimiter to use when reading CSV files (default: ',').",
    )
    parser.add_argument(
        "--server",
        default=os.getenv("DATAHUB_SERVER", "http://localhost:8080"),
        help="DataHub GMS endpoint (default: %(default)s or DATAHUB_SERVER).",
    )
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
    parser.add_argument(
        "--default-db",
        default=None,
        help="Optional default database name to apply when qualifying tables.",
    )
    parser.add_argument(
        "--default-schema",
        default=None,
        help="Optional default schema name to apply when qualifying tables.",
    )
    parser.add_argument(
        "--override-dialect",
        default="teradata",
        help="Optional SQL dialect hint for the parser (default: %(default)s).",
    )
    parser.add_argument(
        "--raw-output-dir",
        help="Directory where raw parser JSON should be stored (default: lineage_outputs/<timestamp>).",
    )
    parser.add_argument(
        "--emit-lineage",
        action="store_true",
        help="If set, emit the parsed lineage to DataHub using lineage utilities.",
    )
    args = parser.parse_args()

    if not (args.sql_file or args.sql_dir or args.csv_spec or args.csv_dir):
        parser.error("Provide at least one --sql-file/--sql-dir/--csv-spec/--csv-dir input.")

    if args.csv_dir and not args.csv_dir_column:
        parser.error("--csv-dir-column is required when using --csv-dir.")

    try:
        tasks = _collect_tasks(
            args.sql_file,
            args.sql_dir,
            args.csv_spec,
            args.csv_delimiter,
            args.csv_dir,
            args.csv_dir_column,
        )
    except Exception as exc:  # pragma: no cover - defensive
        print(f"Failed to load SQL inputs: {exc}", file=sys.stderr)
        sys.exit(1)

    if not tasks:
        print("No SQL statements found in the provided inputs.", file=sys.stderr)
        sys.exit(1)

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    raw_dir = Path(args.raw_output_dir or (Path("lineage_outputs") / timestamp))
    raw_dir.mkdir(parents=True, exist_ok=True)

    graph = DataHubGraph(DatahubClientConfig(server=args.server, token=TOKEN))
    emitter: Optional[LineageEmitter] = LineageEmitter(graph) if args.emit_lineage else None

    outcomes: List[QueryOutcome] = []

    for task in tasks:
        start_ns = time.perf_counter_ns()
        try:
            result = graph.parse_sql_lineage(
                task.query_text,
                platform=args.platform,
                env=args.env,
                default_db=args.default_db,
                default_schema=args.default_schema,
                override_dialect=args.override_dialect,
            )
            elapsed_ms = (time.perf_counter_ns() - start_ns) / 1_000_000
            payload = json.loads(result.json())
            debug_error = getattr(getattr(result, "debug_info", None), "error", None)
            payload["debugInfoError"] = str(debug_error) if debug_error else None
            upstreams = list(getattr(result, "in_tables", None) or [])
            downstreams = list(getattr(result, "out_tables", None) or [])
            column_edges = _column_lineage_edges(getattr(result, "column_lineage", None))
            parser_statement_type = _extract_parser_statement_type(result, payload)
            statement_type, statement_type_source = _resolve_statement_type(
                parser_statement_type, task.query_text
            )
            self_ref = bool(
                downstreams
                and upstreams
                and any(ds == us for ds in downstreams for us in upstreams)
            )
            outcome = QueryOutcome(
                task=task,
                upstreams=upstreams,
                downstreams=downstreams,
                column_edges=column_edges,
                timing_ms=elapsed_ms,
                parser_error=str(debug_error) if debug_error else None,
                rpc_error=None,
                self_referential=self_ref,
                raw_payload=payload,
                statement_type=statement_type,
                statement_type_source=statement_type_source,
                parser_statement_type=parser_statement_type,
            )
            outcomes.append(outcome)
            if emitter:
                emitter.collect(result)
        except Exception as exc:  # pragma: no cover - network failure
            elapsed_ms = (time.perf_counter_ns() - start_ns) / 1_000_000
            payload = {"error": str(exc), "query": task.query_text}
            parser_statement_type = None
            statement_type, statement_type_source = _resolve_statement_type(
                parser_statement_type, task.query_text
            )
            outcome = QueryOutcome(
                task=task,
                upstreams=[],
                downstreams=[],
                column_edges=[],
                timing_ms=elapsed_ms,
                parser_error=None,
                rpc_error=str(exc),
                self_referential=False,
                raw_payload=payload,
                statement_type=statement_type,
                statement_type_source=statement_type_source,
                parser_statement_type=parser_statement_type,
            )
            outcomes.append(outcome)

    for outcome in outcomes:
        if not outcome.flags:
            outcome.flags = _compute_query_flags(outcome)

    grouped: Dict[Path, List[QueryOutcome]] = defaultdict(list)
    for outcome in outcomes:
        grouped[outcome.task.source_path].append(outcome)

    source_metadata: Dict[Path, Dict[str, Any]] = {}
    for source_path, group in grouped.items():
        folder_flags = _aggregate_folder_flags(group)
        folder_name = _build_source_folder_name(source_path, folder_flags)
        folder_dir = raw_dir / folder_name
        folder_dir.mkdir(parents=True, exist_ok=True)

        for outcome in group:
            if not outcome.flags:
                outcome.flags = _compute_query_flags(outcome)
            filename = _build_query_filename(outcome)
            outcome.raw_json_path = folder_dir / filename

        source_metadata[source_path] = {
            "folder_dir": folder_dir,
            "folder_flags": folder_flags,
            "outcomes": group,
        }

    total_outcomes = len(outcomes)
    for idx, outcome in enumerate(outcomes, start=1):
        outcome.terminal_output = _render_query_outcome(idx, total_outcomes, outcome)
        print(outcome.terminal_output)

    for outcome in outcomes:
        if not outcome.raw_json_path:
            continue
        content = {
            "flags": outcome.flags,
            "terminal_output": outcome.terminal_output,
            "raw_payload": outcome.raw_payload,
            "source_query": outcome.task.query_text,
            "preview": _query_preview_lines(outcome.task),
        }
        outcome.raw_json_path.write_text(json.dumps(content, indent=2), encoding="utf-8")

    for group_info in source_metadata.values():
        folder_dir = group_info["folder_dir"]
        folder_flags = group_info["folder_flags"]
        group_outcomes: List[QueryOutcome] = group_info["outcomes"]
        report_path = folder_dir / "[[]]report.json"
        report_md_path = folder_dir / "[[]]report.md"

        overview = compute_overview(group_outcomes)
        debug_error_summary = build_debug_error_summary(group_outcomes)

        statement_summary, flag_keys, error_class_keys = compute_statement_type_metrics(
            group_outcomes, FLAG_PRIORITY
        )

        report_data = {
            "source": str(group_outcomes[0].task.source_path) if group_outcomes else "",
            "flags": folder_flags,
            **overview,
            "debug_info_error_counts": debug_error_summary,
            "statement_type_summary": statement_summary,
            "statement_type_flag_keys": flag_keys,
            "statement_type_error_classes": error_class_keys,
            "queries": [
                {
                    "identifier": outcome.task.identifier,
                    "flags": outcome.flags,
                    "raw_output_file": outcome.raw_json_path.name
                    if outcome.raw_json_path
                    else None,
                    "succeeded": outcome.succeeded,
                    "timing_ms": outcome.timing_ms,
                    "preview": _query_preview_lines(outcome.task),
                    "statement_type": outcome.statement_type,
                    "statement_type_source": outcome.statement_type_source,
                    "parser_statement_type": outcome.parser_statement_type,
                }
                for outcome in group_outcomes
            ],
        }
        report_data.setdefault("column_lineage_count", overview.get("column_lineage_count", 0))
        report_path.write_text(json.dumps(report_data, indent=2), encoding="utf-8")
        source_path_for_report = group_outcomes[0].task.source_path if group_outcomes else Path("")
        report_markdown = render_report_markdown(
            source_path_for_report,
            _build_flag_prefix(folder_flags),
            group_outcomes,
            statement_summary,
            flag_keys,
            error_class_keys,
        )
        report_md_path.write_text(report_markdown, encoding="utf-8")

    run_overview = compute_overview(outcomes)
    run_flags = _aggregate_folder_flags(outcomes)
    run_statement_summary, run_flag_keys, run_error_class_keys = (
        compute_statement_type_metrics(outcomes, FLAG_PRIORITY)
    )
    run_debug_summary = build_debug_error_summary(outcomes)
    run_report_path = raw_dir / "[[]]report.json"
    run_report_md_path = raw_dir / "[[]]report.md"
    run_report_data = {
        "source": str(raw_dir),
        "flags": run_flags,
        **run_overview,
        "debug_info_error_counts": run_debug_summary,
        "statement_type_summary": run_statement_summary,
        "statement_type_flag_keys": run_flag_keys,
        "statement_type_error_classes": run_error_class_keys,
        "queries": [
            {
                "identifier": outcome.task.identifier,
                "flags": outcome.flags,
                "raw_output_file": outcome.raw_json_path.name if outcome.raw_json_path else None,
                "succeeded": outcome.succeeded,
                "timing_ms": outcome.timing_ms,
                "preview": _query_preview_lines(outcome.task),
                "statement_type": outcome.statement_type,
                "statement_type_source": outcome.statement_type_source,
                "parser_statement_type": outcome.parser_statement_type,
            }
            for outcome in outcomes
        ],
    }
    run_report_data.setdefault(
        "column_lineage_count", run_overview.get("column_lineage_count", 0)
    )
    run_report_path.write_text(json.dumps(run_report_data, indent=2), encoding="utf-8")
    run_report_markdown = render_report_markdown(
        raw_dir,
        _build_flag_prefix(run_flags),
        outcomes,
        run_statement_summary,
        run_flag_keys,
        run_error_class_keys,
    )
    run_report_md_path.write_text(run_report_markdown, encoding="utf-8")

    print_overview(run_overview, raw_dir)

    if emitter:
        emitter.emit()


if __name__ == "__main__":
    main()
