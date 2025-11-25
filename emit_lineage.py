from __future__ import annotations

import hashlib
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, DefaultDict, Dict, Iterable, List, Optional, Set

from datahub.emitter.mce_builder import (
    dataset_urn_to_key,
    make_data_flow_urn,
    make_data_job_urn_with_flow,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DatasetPropertiesClass,
    EdgeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)


@dataclass(frozen=True)
class LineageTaskContext:
    identifier: str
    context_label: str
    source_path: Path
    query_text: str

    def preview(self, max_chars: int = 240) -> str:
        stripped_lines = [line.strip() for line in self.query_text.splitlines()]
        condensed = " ".join(line for line in stripped_lines if line)
        if not condensed:
            condensed = "<empty>"
        if len(condensed) <= max_chars:
            return condensed
        return f"{condensed[: max_chars - 1]}…"

    @property
    def source_label(self) -> str:
        return str(self.source_path)


def _sanitize_identifier(value: str, *, fallback: str, max_length: int = 200) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.:/\\-]+", "_", value.strip())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    if not cleaned:
        cleaned = fallback
    if len(cleaned) <= max_length:
        return cleaned.lower()
    digest = hashlib.sha1(cleaned.encode("utf-8")).hexdigest()[:8]
    prefix = cleaned[: max_length - len(digest) - 1]
    return f"{prefix}_{digest}".lower()


def _relative_source_label(path: Path) -> str:
    try:
        relative = path.relative_to(Path.cwd())
        if str(relative):
            return relative.as_posix()
    except ValueError:
        pass
    return path.as_posix()


def _truncate_for_property(text: str, limit: int = 280) -> str:
    sanitized = re.sub(r"\s+", " ", text.strip())
    if not sanitized:
        return ""
    if len(sanitized) <= limit:
        return sanitized
    return f"{sanitized[: limit - 1]}…"


def _build_fine_grained_lineage(
    downstream_dataset: str,
    column_lineage: Optional[Iterable[Any]],
    confidence: Optional[float],
) -> List[FineGrainedLineageClass]:
    fine_grained: List[FineGrainedLineageClass] = []
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

        upstream_fields: List[str] = []
        for upstream in getattr(entry, "upstreams", []):
            upstream_dataset = getattr(upstream, "table", None)
            upstream_column = getattr(upstream, "column", None)
            if upstream_dataset and upstream_column:
                if upstream_dataset == target_dataset:
                    continue
                upstream_fields.append(
                    make_schema_field_urn(upstream_dataset, upstream_column)
                )

        if not upstream_fields:
            continue

        lineage_kwargs: Dict[str, Any] = {
            "upstreamType": FineGrainedLineageUpstreamTypeClass.FIELD_SET,
            "upstreams": upstream_fields,
            "downstreamType": FineGrainedLineageDownstreamTypeClass.FIELD,
            "downstreams": [downstream_field],
        }
        if confidence:
            lineage_kwargs["confidenceScore"] = confidence
        logic = getattr(entry, "logic", None)
        logic_text = getattr(logic, "column_logic", None)
        if logic_text:
            lineage_kwargs["transformOperation"] = logic_text
        fine_grained.append(FineGrainedLineageClass(**lineage_kwargs))

    return fine_grained


def _accumulate_dataset_columns(
    dataset_columns: DefaultDict[str, Set[str]],
    upstream_datasets: Iterable[str],
    downstream_datasets: Iterable[str],
    column_lineage: Optional[Iterable[Any]],
) -> None:
    for dataset in list(upstream_datasets or []):
        if dataset:
            dataset_columns[dataset]
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
        customProperties={"autoCreatedBy": "sqlparser_demo.emit_lineage"},
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


class LineageEmitter:
    def __init__(
        self,
        graph: DataHubGraph,
        *,
        orchestrator: str,
        cluster: str,
        env: str,
        job_type: str = "SQL_PARSER",
        flow_id_prefix: Optional[str] = None,
    ):
        self.graph = graph
        self.orchestrator = orchestrator
        self.cluster = cluster
        self.env = env
        self.job_type = job_type
        self.flow_id_prefix = flow_id_prefix or ""
        self.dataset_columns: DefaultDict[str, Set[str]] = defaultdict(set)
        self._flow_cache: Dict[Path, str] = {}
        self.flow_mcps: Dict[str, MetadataChangeProposalWrapper] = {}
        self.job_mcps: List[MetadataChangeProposalWrapper] = []

    def collect(self, context: LineageTaskContext, result: Any) -> None:
        upstream_tables = getattr(result, "in_tables", None) or []
        downstream_tables = getattr(result, "out_tables", None) or []
        column_lineage = getattr(result, "column_lineage", None)
        _accumulate_dataset_columns(
            self.dataset_columns,
            upstream_tables,
            downstream_tables,
            column_lineage,
        )
        if not downstream_tables:
            return

        flow_urn = self._ensure_flow(context)
        job_urn = self._build_job_urn(flow_urn, context)
        self.job_mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=self._build_job_info_aspect(flow_urn, context, result),
            )
        )
        self.job_mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=self._build_job_lineage_aspect(
                    upstream_tables,
                    downstream_tables,
                    column_lineage,
                    result,
                ),
            )
        )

    def emit(self) -> None:
        if not self.job_mcps:
            print("[emit] No lineage to emit (no downstream datasets identified).")
            return

        if self.dataset_columns:
            _ensure_datasets_exist(self.graph, self.dataset_columns)

        mcps_to_send = list(self.flow_mcps.values()) + self.job_mcps
        print("[emit] DataFlow/DataJob MCPs to be sent:")
        for mcp in mcps_to_send:
            try:
                payload = mcp.to_obj(simplified_structure=True)
            except Exception:
                payload = {"entityUrn": mcp.entityUrn, "aspectName": mcp.aspectName}
            print(json.dumps(payload, indent=2))

        try:
            self.graph.emit_mcps(mcps_to_send)
            emitted_entities = sorted(
                {mcp.entityUrn for mcp in mcps_to_send if mcp.entityUrn}
            )
            print("[emit] Lineage emitted for entities:")
            for entity in emitted_entities:
                print(f"  - {entity}")
        except Exception as exc:  # pragma: no cover - network failure
            print(f"[emit] Failed to emit lineage: {exc}", file=sys.stderr)

    # ------------------------------------------------------------------
    # Internal helpers

    def _ensure_flow(self, context: LineageTaskContext) -> str:
        source_path = context.source_path
        if source_path in self._flow_cache:
            return self._flow_cache[source_path]

        source_label = _relative_source_label(source_path)
        base_id = f"{self.flow_id_prefix}__{source_label}" if self.flow_id_prefix else source_label
        flow_id = _sanitize_identifier(base_id, fallback="sql_parser_flow")
        flow_name = source_path.name or flow_id
        flow_urn = make_data_flow_urn(self.orchestrator, flow_id, self.cluster)
        description = (
            f"SQL lineage extracted from {source_label}"
            if source_label
            else "SQL lineage extracted by parse_sql_minimal.py"
        )
        custom_props = {
            "sqlParserSourcePath": context.source_label,
            "sqlParserFlowId": flow_id,
        }
        if self.flow_id_prefix:
            custom_props["sqlParserFlowPrefix"] = self.flow_id_prefix
        flow_mcp = MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=DataFlowInfoClass(
                name=flow_name,
                description=description,
                project=str(source_path.parent),
                customProperties=custom_props,
                env=self.env,
            ),
        )
        self.flow_mcps[flow_urn] = flow_mcp
        self._flow_cache[source_path] = flow_urn
        return flow_urn

    def _build_job_urn(self, flow_urn: str, context: LineageTaskContext) -> str:
        job_id = _sanitize_identifier(context.identifier, fallback="sql_parser_job")
        return make_data_job_urn_with_flow(flow_urn, job_id)

    def _build_job_info_aspect(
        self, flow_urn: str, context: LineageTaskContext, result: Any
    ) -> DataJobInfoClass:
        description = _truncate_for_property(context.context_label, 512)
        preview = context.preview()
        custom_props: Dict[str, str] = {
            "sqlParserQueryIdentifier": context.identifier,
            "sqlParserSource": context.source_label,
            "sqlParserQueryPreview": preview,
        }
        fingerprint = getattr(result, "query_fingerprint", None)
        if fingerprint:
            custom_props["sqlParserQueryFingerprint"] = fingerprint
        parser_type = getattr(result, "query_type", None)
        if parser_type:
            custom_props["sqlParserQueryType"] = str(parser_type)
        confidence = getattr(getattr(result, "debug_info", None), "confidence", None)
        if confidence is not None:
            custom_props["sqlParserConfidence"] = f"{confidence:.6f}"
        return DataJobInfoClass(
            name=context.identifier,
            type=self.job_type,
            description=description or preview,
            flowUrn=flow_urn,
            customProperties=custom_props,
            env=self.env,
        )

    def _build_job_lineage_aspect(
        self,
        upstream_tables: Iterable[str],
        downstream_tables: Iterable[str],
        column_lineage: Optional[Iterable[Any]],
        result: Any,
    ) -> DataJobInputOutputClass:
        upstreams = [dataset for dataset in upstream_tables if dataset]
        downstreams = [dataset for dataset in downstream_tables if dataset]
        confidence = getattr(getattr(result, "debug_info", None), "confidence", None)
        fine_grained: List[FineGrainedLineageClass] = []
        for downstream_dataset in downstreams:
            fine_grained.extend(
                _build_fine_grained_lineage(
                    downstream_dataset,
                    column_lineage,
                    confidence=confidence,
                )
            )

        return DataJobInputOutputClass(
            inputDatasets=upstreams,
            outputDatasets=downstreams,
            inputDatasetEdges=[EdgeClass(destinationUrn=urn) for urn in upstreams],
            outputDatasetEdges=[EdgeClass(destinationUrn=urn) for urn in downstreams],
            fineGrainedLineages=fine_grained or None,
        )
