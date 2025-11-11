from __future__ import annotations

import math
import re
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, TYPE_CHECKING

ANSI_ESCAPE_PATTERN = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")

if TYPE_CHECKING:  # pragma: no cover - type checking only
    from parse_sql_minimal import QueryOutcome


def _normalize_error_label(error_value: Any) -> str:
    if error_value is None:
        return "<none>"
    if not isinstance(error_value, str):
        return str(error_value)
    text = error_value.strip()
    if not text:
        return "<none>"
    cut_idx: Optional[int] = None
    for delimiter in (":", ".", "<"):
        idx = text.find(delimiter)
        if idx != -1 and (cut_idx is None or idx < cut_idx):
            cut_idx = idx
    if cut_idx is not None:
        text = text[:cut_idx]
    return text.strip() or "<unknown>"


def _percentile(values: Sequence[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    k = (len(ordered) - 1) * quantile
    lower = math.floor(k)
    upper = math.ceil(k)
    if lower == upper:
        return ordered[int(k)]
    return ordered[lower] * (upper - k) + ordered[upper] * (k - lower)


def _build_timing_summary(timings: List[float]) -> Dict[str, float]:
    if not timings:
        return {"avg": 0.0, "median": 0.0, "p95": 0.0, "min": 0.0, "max": 0.0}
    return {
        "avg": sum(timings) / len(timings),
        "median": statistics.median(timings),
        "p95": _percentile(timings, 0.95),
        "min": min(timings),
        "max": max(timings),
    }


def build_debug_error_summary(
    outcomes: Sequence["QueryOutcome"],
) -> List[Dict[str, Any]]:
    debug_error_counts: Dict[str, int] = defaultdict(int)
    for outcome in outcomes:
        debug_error = outcome.raw_payload.get("debugInfoError")
        error_label = _normalize_error_label(debug_error)
        debug_error_counts[error_label] += 1
    sorted_debug_errors = sorted(
        debug_error_counts.items(), key=lambda item: (-item[1], item[0].lower())
    )
    return [{"message": label, "count": count} for label, count in sorted_debug_errors]


def compute_overview(outcomes: Sequence["QueryOutcome"]) -> Dict[str, Any]:
    return {
        "query_count": len(outcomes),
        "success_count": sum(1 for outcome in outcomes if outcome.succeeded),
        "parser_error_count": sum(1 for outcome in outcomes if outcome.parser_error),
        "rpc_error_count": sum(1 for outcome in outcomes if outcome.rpc_error),
        "error_count": sum(1 for outcome in outcomes if "ERR" in outcome.flags),
        "lineage_count": sum(1 for outcome in outcomes if "LIN" in outcome.flags),
        "gap_lineage_count": sum(1 for outcome in outcomes if "GAP" in outcome.flags),
        "self_referential_count": sum(1 for outcome in outcomes if "SELF" in outcome.flags),
        "column_lineage_count": sum(1 for outcome in outcomes if "COL" in outcome.flags),
        "timing_ms_total": sum(outcome.timing_ms for outcome in outcomes),
    }


def print_overview(overview: Dict[str, Any], raw_dir: Optional[Path] = None) -> None:
    print("\n=== Summary ===")
    print(f"Total queries: {overview['query_count']}")
    print(f"Successful parses: {overview['success_count']}")
    print(f"Parser errors: {overview['parser_error_count']}")
    print(f"RPC errors: {overview['rpc_error_count']}")
    print(f"Queries flagged with ERR: {overview['error_count']}")
    print(f"Queries with lineage: {overview['lineage_count']}")
    print(f"Queries missing lineage (GAP): {overview['gap_lineage_count']}")
    print(f"Self-referential lineage: {overview['self_referential_count']}")
    print(f"Queries with column lineage (COL): {overview['column_lineage_count']}")
    print(f"Total parser time (ms): {overview['timing_ms_total']:.3f}")
    if raw_dir is not None:
        print(f"Raw outputs stored in: {raw_dir}")


def _clean_markdown_cell(value: Any) -> str:
    text = str(value)
    text = ANSI_ESCAPE_PATTERN.sub("", text)
    text = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    if not text:
        return "—"
    text = text.replace("\n", "<br>")
    text = text.replace("|", r"\|")
    return text


def _format_markdown_table(
    headers: Sequence[str], rows: Sequence[Sequence[str]]
) -> List[str]:
    if not rows:
        return ["(no data)"]
    cleaned_headers = [_clean_markdown_cell(header) for header in headers]
    header_row = "| " + " | ".join(cleaned_headers) + " |"
    separator = "| " + " | ".join("---" for _ in headers) + " |"
    lines = [header_row, separator]
    for row in rows:
        cleaned_row = [_clean_markdown_cell(cell) for cell in row]
        lines.append("| " + " | ".join(cleaned_row) + " |")
    return lines


def _format_count_pairs(counts: Dict[str, int]) -> str:
    if not counts:
        return "—"
    items = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return ", ".join(f"{key} ({value})" for key, value in items)


def compute_statement_type_metrics(
    outcomes: Sequence["QueryOutcome"],
    flag_priority: Sequence[str],
) -> Tuple[Dict[str, Any], List[str], List[str]]:
    summary: Dict[str, Dict[str, Any]] = {}
    all_flags: Set[str] = set()
    all_error_classes: Set[str] = set()

    for outcome in outcomes:
        statement_type = outcome.statement_type or "UNKNOWN"
        stats = summary.setdefault(
            statement_type,
            {
                "total_queries": 0,
                "success_count": 0,
                "error_count": 0,
                "timings": [],
                "flag_counts": defaultdict(int),
                "error_class_counts": defaultdict(int),
                "parser_error_counts": defaultdict(int),
                "source_breakdown": defaultdict(int),
                "parser_reported_types": defaultdict(int),
            },
        )
        stats["total_queries"] += 1
        stats["timings"].append(outcome.timing_ms)
        if outcome.succeeded:
            stats["success_count"] += 1
        else:
            stats["error_count"] += 1

        stats["source_breakdown"][outcome.statement_type_source or "unknown"] += 1
        parser_label = outcome.parser_statement_type or "UNAVAILABLE"
        stats["parser_reported_types"][parser_label] += 1

        for flag in outcome.flags:
            stats["flag_counts"][flag] += 1
            all_flags.add(flag)

        error_label = _normalize_error_label(outcome.raw_payload.get("debugInfoError"))
        stats["error_class_counts"][error_label] += 1
        all_error_classes.add(error_label)

        parser_error_label = outcome.parser_error or "<none>"
        stats["parser_error_counts"][parser_error_label] += 1

    final_summary: Dict[str, Any] = {}
    for statement_type, stats in summary.items():
        timings = stats.pop("timings")
        timing_summary = _build_timing_summary(timings)
        success_count = stats["success_count"]
        total = stats["total_queries"]
        success_rate = (success_count / total * 100.0) if total else 0.0
        flag_counts = dict(stats["flag_counts"])
        error_class_counts = dict(stats["error_class_counts"])
        parser_error_counts = dict(stats["parser_error_counts"])
        source_breakdown = dict(stats["source_breakdown"])
        parser_reported_types = dict(stats["parser_reported_types"])
        final_summary[statement_type] = {
            "total_queries": total,
            "success_count": success_count,
            "error_count": stats["error_count"],
            "success_rate": success_rate,
            "error_rate": 100.0 - success_rate if total else 0.0,
            "timing_ms": timing_summary,
            "flag_counts": flag_counts,
            "error_class_counts": error_class_counts,
            "parser_error_counts": parser_error_counts,
            "source_breakdown": source_breakdown,
            "parser_reported_types": parser_reported_types,
        }

    flag_keys = [flag for flag in flag_priority if flag in all_flags]
    flag_keys += sorted(all_flags - set(flag_keys))
    error_class_keys = sorted(all_error_classes)

    return final_summary, flag_keys, error_class_keys


def render_report_markdown(
    source_path: Path,
    folder_flag_label: str,
    outcomes: Sequence["QueryOutcome"],
    statement_summary: Dict[str, Any],
    flag_keys: Sequence[str],
    error_class_keys: Sequence[str],
) -> str:
    total_queries = len(outcomes)
    total_timing_ms = sum(outcome.timing_ms for outcome in outcomes)

    parser_count = sum(
        stats["source_breakdown"].get("parser", 0) for stats in statement_summary.values()
    )
    fallback_count = sum(
        stats["source_breakdown"].get("fallback", 0) for stats in statement_summary.values()
    )
    unresolved_count = sum(
        stats["source_breakdown"].get("unknown", 0) for stats in statement_summary.values()
    )
    parser_unknown_total = sum(
        stats["parser_reported_types"].get("UNKNOWN", 0)
        for stats in statement_summary.values()
    )

    lines = [
        f"# Lineage Report — {source_path.name or source_path}",
        "",
        f"* Source path: `{source_path}`",
        f"* Folder flags: {folder_flag_label}",
        f"* Queries analyzed: {total_queries}",
        f"* Total parser time: {total_timing_ms:.3f} ms",
        f"* Statement types via parser: {parser_count}",
        f"* Statement types via fallback: {fallback_count}",
    ]
    if unresolved_count:
        lines.append(f"* Statement types still unknown: {unresolved_count}")
    if parser_unknown_total:
        lines.append(
            f"* Parser returned UNKNOWN for {parser_unknown_total} statement(s); "
            "fallback heuristics attempted."
        )
    lines.extend(
        [
            "",
            "### Legend",
            "",
            "- `Parser` vs `Fallback`: whether the parser supplied the statement type or the regex fallback classifier did.",
            "- Flags: `ERR`=parser/RPC error, `GAP`=missing upstream or downstream lineage, `LIN`=complete table lineage, "
            "`SELF`=self-referential lineage, `COL`=column-level lineage detected.",
        ]
    )

    if statement_summary:
        overview_rows: List[List[str]] = []
        for statement_type, stats in sorted(
            statement_summary.items(), key=lambda item: (-item[1]["total_queries"], item[0])
        ):
            timing = stats["timing_ms"]
            overview_rows.append(
                [
                    statement_type,
                    str(stats["total_queries"]),
                    str(stats["success_count"]),
                    str(stats["error_count"]),
                    f"{stats['success_rate']:.1f}%",
                    f"{timing['avg']:.2f}",
                    f"{timing['p95']:.2f}",
                ]
            )
        lines.extend(
            [
                "",
                "## Statement Type Overview",
                "",
                "_How many statements of each type we parsed, whether they succeeded, and how long they took._",
                "",
                *_format_markdown_table(
                    ["Statement Type", "Queries", "Success", "Errors", "Success %", "Avg ms", "P95 ms"],
                    overview_rows,
                ),
            ]
        )

        classification_rows: List[List[str]] = []
        for statement_type, stats in sorted(
            statement_summary.items(), key=lambda item: (-item[1]["total_queries"], item[0])
        ):
            breakdown = stats["source_breakdown"]
            parser_reported = _format_count_pairs(stats["parser_reported_types"])
            classification_rows.append(
                [
                    statement_type,
                    str(breakdown.get("parser", 0)),
                    str(breakdown.get("fallback", 0)),
                    str(breakdown.get("unknown", 0)),
                    parser_reported,
                ]
            )
        lines.extend(
            [
                "",
                "## Parser Classification",
                "",
                "_Breakdown of how each statement type was classified: parser-provided vs fallback vs still unknown, "
                "plus the raw parser labels returned._",
                "",
                *_format_markdown_table(
                    ["Statement Type", "Parser", "Fallback", "Unresolved", "Parser reported"],
                    classification_rows,
                ),
            ]
        )

        flag_rows: List[List[str]] = []
        for statement_type, stats in sorted(
            statement_summary.items(), key=lambda item: (-item[1]["total_queries"], item[0])
        ):
            row = [statement_type]
            for flag in flag_keys:
                row.append(str(stats["flag_counts"].get(flag, 0)))
            flag_rows.append(row)
        lines.extend(
            [
                "",
                "## Flag Distribution by Statement Type",
                "",
                "_This highlights quality signals per statement type. Use it to spot types that systematically have errors, "
                "missing lineage, or column-level coverage._",
                "",
            ]
        )
        if flag_keys:
            lines.extend(
                _format_markdown_table(["Statement Type", *flag_keys], flag_rows)
            )
        else:
            lines.append("No flags recorded for this source.")

        error_rows: List[List[str]] = []
        for statement_type, stats in sorted(
            statement_summary.items(), key=lambda item: (-item[1]["total_queries"], item[0])
        ):
            for error_label, count in sorted(
                stats["error_class_counts"].items(), key=lambda item: (-item[1], item[0])
            ):
                error_rows.append([statement_type, error_label, str(count)])
        lines.extend(
            [
                "",
                "## Error Class Distribution by Statement Type",
                "",
                "_Summaries of the parser's `debugInfoError` messages. These usually explain why lineage was incomplete for a statement type._",
                "",
            ]
        )
        if error_class_keys:
            lines.extend(
                _format_markdown_table(["Statement Type", "Error Class", "Count"], error_rows)
            )
        else:
            lines.append("No error classes detected for this source.")

        parser_error_rows: List[List[str]] = []
        for statement_type, stats in sorted(
            statement_summary.items(), key=lambda item: (-item[1]["total_queries"], item[0])
        ):
            for error_message, count in sorted(
                stats["parser_error_counts"].items(), key=lambda item: (-item[1], item[0])
            ):
                parser_error_rows.append([statement_type, error_message or "<none>", str(count)])
        lines.extend(
            [
                "",
                "## Parser Error Breakdown",
                "",
                "_The exact parser error strings returned. Use this to trace concrete failures back to the source SQL._",
                "",
                *_format_markdown_table(
                    ["Statement Type", "Parser Error", "Count"], parser_error_rows
                ),
            ]
        )
    else:
        lines.extend(
            [
                "",
                "No statement types were recorded for this source.",
            ]
        )

    lines.append("")
    return "\n".join(lines)


__all__ = [
    "build_debug_error_summary",
    "compute_overview",
    "compute_statement_type_metrics",
    "print_overview",
    "render_report_markdown",
]
