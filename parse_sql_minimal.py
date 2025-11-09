from __future__ import annotations

import argparse
import inspect
import json
import os
import sys
import time  # <-- added
from collections import defaultdict
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
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)

TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6ImRhdGFodWIiLCJ0eXBlIjoiUEVSU09OQUwiLCJ2ZXJzaW9uIjoiMiIsImp0aSI6IjI1OGRjMWNmLWYwNTItNDlmZS05M2YxLWFlNDc2NTdlZTNkNiIsInN1YiI6ImRhdGFodWIiLCJleHAiOjE3NTQ0MTczMjIsImlzcyI6ImRhdGFodWItbWV0YWRhdGEtc2VydmljZSJ9.qc3sjJamZBlOEQzlogyq20MnXrqP3kLsgCaAhRJ46_E"

_UPSTREAM_LINEAGE_SUPPORTS_CONFIDENCE = (
    "confidenceScore" in inspect.signature(UpstreamLineage).parameters
)


def _build_fine_grained_lineage(
    downstream_dataset: str, column_lineage: Optional[Iterable[Any]]
) -> List[FineGrainedLineage]:
    fine_grained: List[FineGrainedLineage] = []
    if not column_lineage:
        return fine_grained

    for entry in column_lineage:
        downstream_info = getattr(entry, "downstream", None)
        if downstream_info is None:
            continue
        target_dataset = getattr(downstream_info, "table", None) or downstream_dataset
        if target_dataset != downstream_dataset:
            continue

        downstream_column = getattr(downstream_info, "column", None)
        if not downstream_column:
            continue
        downstream_field = make_schema_field_urn(downstream_dataset, downstream_column)

        upstream_fields = []
        for upstream in getattr(entry, "upstreams", []):
            upstream_dataset = getattr(upstream, "table", None)
            upstream_column = getattr(upstream, "column", None)
            if upstream_dataset and upstream_column:
                if upstream_dataset == target_dataset:
                    # Skip self-pointing column lineage (DataHub does not render these)
                    continue
                upstream_fields.append(
                    make_schema_field_urn(upstream_dataset, upstream_column)
                )

        if not upstream_fields:
            continue

        fine_grained.append(
            FineGrainedLineage(
                upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                upstreams=upstream_fields,
                downstreamType=FineGrainedLineageDownstreamType.FIELD,
                downstreams=[downstream_field],
            )
        )

    return fine_grained


def _build_upstream_lineage_aspect(
    upstream_datasets: Iterable[str],
    confidence: float,
    *,
    fine_grained: Optional[List[FineGrainedLineage]] = None,
) -> UpstreamLineage:
    upstreams = [
        Upstream(dataset=dataset, type=DatasetLineageType.TRANSFORMED)
        for dataset in upstream_datasets
        if dataset
    ]

    kwargs: Dict[str, Any] = {"upstreams": upstreams}
    if fine_grained:
        kwargs["fineGrainedLineages"] = fine_grained
    if _UPSTREAM_LINEAGE_SUPPORTS_CONFIDENCE and confidence > 0:
        kwargs["confidenceScore"] = confidence

    return UpstreamLineage(**kwargs)


def _generate_lineage_mcps(
    result: Any,
) -> List[MetadataChangeProposalWrapper]:
    downstream_tables = getattr(result, "out_tables", None) or []
    if not downstream_tables:
        return []

    upstream_tables = getattr(result, "in_tables", None) or []
    column_lineage = getattr(result, "column_lineage", None)
    confidence = getattr(getattr(result, "debug_info", None), "confidence", 0.0)

    mcps: List[MetadataChangeProposalWrapper] = []
    for downstream_dataset in downstream_tables:
        fine_grained = _build_fine_grained_lineage(
            downstream_dataset, column_lineage
        )
        filtered_upstreams = [
            dataset
            for dataset in upstream_tables
            if dataset and dataset != downstream_dataset
        ]
        if not filtered_upstreams and not fine_grained:
            # Skip self-referential lineage that would produce empty upstream lists.
            continue
        aspect = _build_upstream_lineage_aspect(
            filtered_upstreams,
            confidence,
            fine_grained=fine_grained or None,
        )
        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=downstream_dataset,
                aspect=aspect,
            )
        )
    return mcps


def _accumulate_dataset_columns(
    dataset_columns: DefaultDict[str, Set[str]],
    upstream_datasets: Iterable[str],
    downstream_datasets: Iterable[str],
    column_lineage: Optional[Iterable[Any]],
) -> None:
    for dataset in list(upstream_datasets or []):
        if dataset:
            dataset_columns[dataset]  # initialize key
    for dataset in list(downstream_datasets or []):
        if dataset:
            dataset_columns[dataset]

    if not column_lineage:
        return

    for entry in column_lineage:
        downstream_info = getattr(entry, "downstream", None)
        if downstream_info is not None:
            table = getattr(downstream_info, "table", None)
            column = getattr(downstream_info, "column", None)
            if table:
                dataset_columns[table]
                if column:
                    dataset_columns[table].add(column)
        for upstream in getattr(entry, "upstreams", []):
            table = getattr(upstream, "table", None)
            column = getattr(upstream, "column", None)
            if table:
                dataset_columns[table]
                if column:
                    dataset_columns[table].add(column)


def _build_dataset_scaffold_mcps(
    dataset_urn: str, columns: Iterable[str]
) -> List[MetadataChangeProposalWrapper]:
    key = dataset_urn_to_key(dataset_urn)
    column_set = sorted({col for col in columns if col})
    schema_fields = [
        SchemaFieldClass(
            fieldPath=column,
            nativeDataType="UNKNOWN",
            type=SchemaFieldDataTypeClass(StringTypeClass()),
        )
        for column in column_set
    ]
    schema_aspect = SchemaMetadataClass(
        schemaName=key.name or dataset_urn,
        platform=key.platform,
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=schema_fields,
        dataset=dataset_urn,
        cluster=key.origin,
    )
    properties_aspect = DatasetPropertiesClass(
        name=key.name or dataset_urn,
        qualifiedName=key.name or dataset_urn,
        customProperties={"autoCreatedBy": "sqlparser_demo.parse_sql_minimal"},
    )
    return [
        MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=properties_aspect),
        MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=schema_aspect),
    ]


def _ensure_datasets_exist(
    graph: DataHubGraph, dataset_columns: Dict[str, Set[str]]
) -> None:
    for dataset_urn, columns in sorted(dataset_columns.items()):
        if not dataset_urn:
            continue
        try:
            if graph.exists(dataset_urn):
                continue
        except Exception as exc:  # pragma: no cover - network failure
            print(
                f"[emit] Skipping existence check for {dataset_urn}: {exc}",
                file=sys.stderr,
            )
            continue

        try:
            mcps = _build_dataset_scaffold_mcps(dataset_urn, columns)
        except Exception as exc:  # pragma: no cover - defensive
            print(
                f"[emit] Failed to prepare scaffold for {dataset_urn}: {exc}",
                file=sys.stderr,
            )
            continue

        for mcp in mcps:
            try:
                graph.emit_mcp(mcp)
            except Exception as exc:  # pragma: no cover - network failure
                print(
                    f"[emit] Failed to initialize dataset {dataset_urn}: {exc}",
                    file=sys.stderr,
                )
                break
        else:
            print(
                f"[emit] Auto-created dataset scaffold for {dataset_urn}",
                file=sys.stderr,
            )


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="sqlparser_runner.py",
        description="Parse SQL via DataHub's lineage parser and print the raw JSON response.",
    )
    parser.add_argument("sql_file", help="Path to the SQL text to parse.")
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
        "--default-dialect",
        default="teradata",
        help="Optional SQL dialect hint for the parser (default: %(default)s).",
    )
    parser.add_argument(
        "--output",
        help="Optional path to write the parsed lineage JSON.",
    )
    parser.add_argument(
        "--emit-lineage",
        action="store_true",
        help="If set, emit the parsed lineage to DataHub.",
    )
    args = parser.parse_args()

    sql_text = Path(args.sql_file).read_text(encoding="utf-8")
    statements = [stmt.strip() for stmt in sql_text.split(";") if stmt.strip()]

    graph = DataHubGraph(DatahubClientConfig(server=args.server, token=TOKEN))
    results = []
    mcps_to_emit: List[MetadataChangeProposalWrapper] = []
    dataset_columns: DefaultDict[str, Set[str]] = defaultdict(set)

    total_parse_ns = 0  # <-- accumulate pure parse time across statements

    for idx, statement in enumerate(statements, start=1):
        # --- measure only the parsing RPC ---
        start_ns = time.perf_counter_ns()
        try:
            result = graph.parse_sql_lineage(
                statement,
                platform=args.platform,
                env=args.env,
                default_db=args.default_db,
                default_schema=args.default_schema,
                default_dialect=args.default_dialect,
            )
        finally:
            elapsed_ns = time.perf_counter_ns() - start_ns
            total_parse_ns += elapsed_ns
            # Print to stderr so it doesn't mix with JSON on stdout
            print(
                f"[timing] statement {idx}: parse_sql_lineage took {elapsed_ns / 1_000_000:.3f} ms",
                file=sys.stderr,
            )
        # --- end timing region ---

        payload = json.loads(result.json())
        debug_error = getattr(getattr(result, "debug_info", None), "error", None)
        payload["debugInfoError"] = str(debug_error) if debug_error else None
        result_json = json.dumps(payload, indent=2)
        print(result_json)
        results.append(payload)

        upstream_tables = getattr(result, "in_tables", None) or []
        downstream_tables = getattr(result, "out_tables", None) or []
        column_lineage = getattr(result, "column_lineage", None)
        _accumulate_dataset_columns(
            dataset_columns,
            upstream_tables,
            downstream_tables,
            column_lineage,
        )

        if args.emit_lineage and not getattr(result.debug_info, "error", None):
            mcps_to_emit.extend(_generate_lineage_mcps(result))

    if len(statements) > 1:
        print(
            f"[timing] total parse time: {total_parse_ns / 1_000_000:.3f} ms "
            f"for {len(statements)} statements",
            file=sys.stderr,
        )

    if args.output and results:
        output_path = Path(args.output)
        payload = results[0] if len(results) == 1 else results
        output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    if args.emit_lineage:
        if mcps_to_emit:
            if dataset_columns:
                _ensure_datasets_exist(graph, dataset_columns)
            print("[emit] Lineage MCPs to be sent:")
            for mcp in mcps_to_emit:
                try:
                    payload = mcp.to_obj(simplified_structure=True)
                except Exception:
                    payload = {
                        "entityUrn": mcp.entityUrn,
                        "aspectName": mcp.aspectName,
                    }
                print(json.dumps(payload, indent=2))
            try:
                graph.emit_mcps(mcps_to_emit)
                emitted_datasets = sorted(
                    {mcp.entityUrn for mcp in mcps_to_emit if mcp.entityUrn}
                )
                print("[emit] Lineage emitted for:")
                for dataset in emitted_datasets:
                    print(f"  - {dataset}")
            except Exception as exc:
                print(f"[emit] Failed to emit lineage: {exc}", file=sys.stderr)
        else:
            print("[emit] No lineage to emit (no downstream datasets identified).")


if __name__ == "__main__":
    main()
