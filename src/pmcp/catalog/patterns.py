"""Architecture patterns and requirement fitness.

Fitness is computed only from the requirements and service behaviour. Whether pmcp can deploy a
pattern (adapter status) and whether that adapter has been proven against real AWS (integration
status) are reported next to it, never folded into the fitness level, so an implemented pattern
is not presented as the best fit just because it exists.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Literal

from pydantic import Field

from pmcp.domain.base import Model
from pmcp.domain.contract import DataContractBody

FitLevel = Literal["strong", "acceptable", "weak", "unsuitable"]
_RANK = {"strong": 3, "acceptable": 2, "weak": 1, "unsuitable": 0}
AdapterStatus = Literal["implemented", "catalog_only"]
IntegrationStatus = Literal["not_verified", "mock_tested", "aws_verified"]


class Requirements(Model):
    source_kinds: list[str]
    freshness_seconds: int
    mutation_mode: str
    duplicates: str
    keyed: bool
    expressible_in_sql: bool
    joins: int
    stateful_streaming: bool
    custom_code_needed: bool
    consumer_access: list[str]
    min_lookup_latency_ms: int | None
    records_per_day: int | None
    bytes_per_day: int | None
    peak_records_per_second: int | None
    reprocessing_window_days: int | None
    reuse: list[str]
    monthly_cost_cap_usd: float | None

    @classmethod
    def from_contract(cls, body: DataContractBody) -> Requirements:
        lat = [c.query_latency_ms for c in body.consumers if c.query_latency_ms]
        return cls(
            source_kinds=sorted({s.kind for s in body.sources}),
            freshness_seconds=body.freshness.max_lag_seconds,
            mutation_mode=body.mutation.mode,
            duplicates=body.mutation.duplicates,
            keyed=bool(body.mutation.key),
            expressible_in_sql=body.transform.expressible_in_sql,
            joins=body.transform.joins,
            stateful_streaming=body.transform.stateful_streaming,
            custom_code_needed=body.transform.custom_code_needed,
            consumer_access=sorted({c.access for c in body.consumers}),
            min_lookup_latency_ms=min(lat) if lat else None,
            records_per_day=body.volume.records_per_day,
            bytes_per_day=body.volume.bytes_per_day,
            peak_records_per_second=body.volume.peak_records_per_second,
            reprocessing_window_days=body.reprocessing_window_days,
            reuse=body.existing_resources_to_reuse,
            monthly_cost_cap_usd=body.monthly_cost_cap_usd,
        )


class Fitness(Model):
    level: FitLevel
    reasons: list[str] = Field(default_factory=list, description="Why the level is what it is (both + and -).")
    blockers: list[str] = Field(default_factory=list)
    unknowns: list[str] = Field(default_factory=list, description="Inputs that would change the verdict.")


class _Eval:
    def __init__(self) -> None:
        self.level: FitLevel = "strong"
        self.reasons: list[str] = []
        self.blockers: list[str] = []
        self.unknowns: list[str] = []

    def plus(self, reason: str) -> None:
        self.reasons.append(f"+ {reason}")

    def cap(self, level: FitLevel, reason: str) -> None:
        if _RANK[level] < _RANK[self.level]:
            self.level = level
        if level == "unsuitable":
            self.blockers.append(reason)
        else:
            self.reasons.append(f"- {reason}")

    def unknown(self, what: str) -> None:
        self.unknowns.append(what)

    def done(self) -> Fitness:
        return Fitness(level=self.level, reasons=self.reasons, blockers=self.blockers, unknowns=self.unknowns)


class Pattern(Model):
    pattern_id: str
    title: str
    summary: str
    components: dict[str, list[str]]
    adapter_status: AdapterStatus
    implementation_work: list[str] = Field(
        default_factory=list, description="What must be built before pmcp can deploy this pattern."
    )

    def evaluate(self, req: Requirements) -> Fitness:
        return _EVALUATORS[self.pattern_id](req)


def _volume_unknowns(e: _Eval, req: Requirements) -> None:
    if req.records_per_day is None and req.bytes_per_day is None:
        e.unknown("daily volume (records or bytes) - affects cost and sizing")


def _consumers_batch_sql(e: _Eval, req: Requirements) -> None:
    for access in req.consumer_access:
        if access in ("sql_analytics", "file_export"):
            e.plus(f"{access} consumers read Iceberg/Parquet through Athena or any Iceberg engine")
        elif access == "key_lookup":
            if req.min_lookup_latency_ms is not None and req.min_lookup_latency_ms < 1000:
                e.cap("weak", "key_lookup consumer needs <1 s reads; Athena query latency is seconds")
            else:
                e.cap("acceptable", "key_lookup served by SQL queries; latency is seconds")
                if req.min_lookup_latency_ms is None:
                    e.unknown("key_lookup latency requirement")
        elif access == "search":
            e.cap("weak", "search consumer needs an index (OpenSearch) as a second sink")
        elif access == "stream":
            e.cap("weak", "stream consumers need change events; a batch table does not emit them")


def _eval_athena(req: Requirements) -> Fitness:
    e = _Eval()
    kinds = set(req.source_kinds)
    if kinds <= {"s3", "glue"}:
        e.plus("source already lands in S3/Glue Catalog; SQL runs where the data is")
    else:
        e.cap("unsuitable", f"source kinds {sorted(kinds - {'s3', 'glue'})} must first land in S3 "
              "(e.g. kinesis_firehose_s3 or dms_cdc_s3 upstream)")
    f = req.freshness_seconds
    if f < 300:
        e.cap("unsuitable", f"freshness {f}s is below the ~5 min floor of scheduled Athena runs")
    elif f < 900:
        e.cap("acceptable", f"freshness {f}s needs runs every few minutes; each run scans the window and MERGE "
              "reads the target, so cost grows with frequency")
    else:
        e.plus(f"freshness {f}s fits scheduled SQL runs")
    if req.custom_code_needed or not req.expressible_in_sql:
        e.cap("unsuitable", "transform is not expressible in SQL")
    if req.stateful_streaming:
        e.cap("unsuitable", "stateful streaming semantics are not available in batch SQL")
    if req.joins > 3:
        e.cap("acceptable", f"{req.joins} joins per run in Athena; watch scanned bytes")
    elif req.joins:
        e.plus(f"{req.joins} join(s) are plain SQL")
    if req.mutation_mode != "append":
        e.plus("upserts/deletes via Iceberg MERGE keyed on the contract key, latest version by ordering column")
    if req.bytes_per_day and req.bytes_per_day > 5 * 1024**4:
        e.cap("acceptable", ">5 TB/day scanned per day; partition pruning and file compaction become critical")
    _consumers_batch_sql(e, req)
    _volume_unknowns(e, req)
    if req.reprocessing_window_days:
        e.plus("reprocessing re-runs the same SQL over a window as long as raw data is retained in S3")
    return e.done()


def _eval_glue(req: Requirements) -> Fitness:
    e = _Eval()
    kinds = set(req.source_kinds)
    if kinds <= {"s3", "glue"}:
        e.plus("source in S3/Glue Catalog")
    elif kinds <= {"s3", "glue", "rds", "redshift", "dynamodb"}:
        e.cap("acceptable", "non-S3 sources are read through Glue connections (network path required)")
    else:
        e.cap("unsuitable", f"source kinds {sorted(kinds)} are streams; use a streaming pattern or land in S3 first")
    f = req.freshness_seconds
    if f < 600:
        e.cap("unsuitable", f"freshness {f}s is below Glue job startup + run time")
    elif f < 3600:
        e.cap("acceptable", f"freshness {f}s means a job run at least every {f // 60} min; per-run minimum billing")
    else:
        e.plus(f"freshness {f}s fits scheduled Spark jobs")
    if req.custom_code_needed or not req.expressible_in_sql or req.joins > 3:
        e.plus("custom PySpark code and heavy joins are Glue's strength")
    else:
        small = req.bytes_per_day is not None and req.bytes_per_day < 10 * 1024**3
        if small:
            e.cap("acceptable", "transform is plain SQL on <10 GB/day; Athena SQL does the same with no job overhead")
    if req.stateful_streaming:
        e.cap("unsuitable", "stateful streaming is not provided by batch Glue jobs")
    if req.mutation_mode != "append":
        e.plus("upserts/deletes via Spark Iceberg MERGE")
    _consumers_batch_sql(e, req)
    _volume_unknowns(e, req)
    return e.done()


def _eval_firehose(req: Requirements) -> Fitness:
    e = _Eval()
    kinds = set(req.source_kinds)
    if kinds <= {"kinesis", "app"}:
        e.plus("events arrive as a stream (or the producer can write to one)")
    else:
        e.cap("unsuitable", f"source kinds {sorted(kinds)} are not streams")
    f = req.freshness_seconds
    if f < 5:
        e.cap("unsuitable", f"freshness {f}s is below Firehose's delivery floor")
    elif f < 60:
        e.cap("acceptable", f"freshness {f}s needs very small buffers; many small S3 objects")
    else:
        e.plus(f"freshness {f}s fits Firehose buffering")
    if req.mutation_mode != "append":
        e.cap("unsuitable", "this adapter delivers append-only objects; upserts/deletes need firehose_iceberg_upsert "
              "or a MERGE step downstream")
    if req.duplicates != "allowed":
        e.cap("weak", "Firehose is at-least-once; duplicates on retry must be removed downstream")
    if req.joins:
        e.cap("weak", "per-record Lambda transform cannot join datasets")
    if req.stateful_streaming:
        e.cap("unsuitable", "no state across records")
    if req.custom_code_needed:
        e.plus("custom per-record logic runs in the transform Lambda")
    for access in req.consumer_access:
        if access in ("file_export",):
            e.plus("file consumers read the delivered objects directly")
        elif access == "sql_analytics":
            e.cap("acceptable", "SQL consumers query raw delivered objects through Athena (no compaction)")
        elif access == "key_lookup":
            e.cap("unsuitable", "no key lookup store in this pattern")
        else:
            e.cap("weak", f"{access} consumers are not served by S3 objects")
    _volume_unknowns(e, req)
    return e.done()


def _eval_lambda_ddb(req: Requirements) -> Fitness:
    e = _Eval()
    kinds = set(req.source_kinds)
    if kinds <= {"kinesis", "app"}:
        e.plus("records arrive on a stream, processed per batch")
    else:
        e.cap("unsuitable", f"this adapter consumes Kinesis streams; sources {sorted(kinds)} need another ingest")
    if req.freshness_seconds < 1:
        e.cap("unsuitable", "sub-second freshness")
    else:
        e.plus("seconds-level delivery")
    if not req.keyed:
        e.cap("weak", "DynamoDB needs a key; contract has none")
    if req.mutation_mode != "append":
        e.plus("idempotent upserts/deletes with conditional writes on the ordering column")
    if req.joins:
        e.cap("weak", "joins require extra lookups per record")
    if req.stateful_streaming:
        e.cap("weak", "state across records would need its own table")
    for access in req.consumer_access:
        if access == "key_lookup":
            e.plus("key_lookup consumers get single-digit-ms reads")
        elif access == "sql_analytics":
            e.cap("weak", "SQL analytics on DynamoDB needs an export or a second sink")
        else:
            e.cap("weak", f"{access} consumers are not served by a key-value table")
    if req.peak_records_per_second and req.peak_records_per_second > 10_000:
        e.cap("acceptable", ">10k records/s needs many shards and Lambda concurrency; check quotas")
    elif req.peak_records_per_second is None:
        e.unknown("peak records per second - sizes shards and Lambda concurrency")
    return e.done()


def _eval_emr(req: Requirements) -> Fitness:
    fit = _eval_glue(req)
    fit.reasons.append("- same Spark semantics as Glue with finer capacity control; more setup for small jobs")
    return fit


def _eval_redshift(req: Requirements) -> Fitness:
    e = _Eval()
    has_wh = "redshift" in req.source_kinds or any("redshift" in r for r in req.reuse)
    if has_wh:
        e.plus("data/warehouse already in Redshift: in-warehouse SQL avoids moving data")
    else:
        e.cap("weak", "no existing warehouse; standing Redshift cost for this flow alone")
    if req.freshness_seconds < 300:
        e.cap("acceptable", "frequent ELT runs keep the warehouse busy")
    if not req.expressible_in_sql:
        e.cap("unsuitable", "transform is not SQL")
    for access in req.consumer_access:
        if access == "sql_analytics":
            e.plus("BI/SQL concurrency is Redshift's strength")
        elif access == "key_lookup" and (req.min_lookup_latency_ms or 10_000) < 200:
            e.cap("weak", "warehouse lookups are not single-digit-ms")
    return e.done()


def _eval_firehose_iceberg(req: Requirements) -> Fitness:
    e = _Eval()
    if set(req.source_kinds) <= {"kinesis", "app"}:
        e.plus("stream source")
    else:
        e.cap("unsuitable", "stream source required")
    if req.freshness_seconds < 60:
        e.cap("acceptable", "short buffers create small Iceberg files; compaction needed")
    if req.mutation_mode != "append":
        e.plus("Firehose Iceberg destination routes insert/update/delete per record")
    if req.joins or req.stateful_streaming:
        e.cap("weak", "no joins/state in Firehose")
    _consumers_batch_sql(e, req)
    return e.done()


def _eval_flink(req: Requirements) -> Fitness:
    e = _Eval()
    if set(req.source_kinds) <= {"kinesis", "kafka", "app"}:
        e.plus("stream source")
    else:
        e.cap("unsuitable", "stream source required")
    if req.stateful_streaming:
        e.plus("windows/sessions/joins over streams with exactly-once checkpoints")
    else:
        e.cap("weak", "no stateful streaming need; standing KPU cost is hard to justify")
    if req.freshness_seconds < 5:
        e.plus("sub-5s latency")
    return e.done()


def _eval_dms(req: Requirements) -> Fitness:
    e = _Eval()
    if "rds" in req.source_kinds:
        e.plus("relational source: CDC captures inserts/updates/deletes")
    else:
        e.cap("unsuitable", "DMS CDC needs a relational source")
    if req.mutation_mode == "append":
        e.cap("acceptable", "CDC change rows exceed an append-only need")
    return e.done()


def _eval_opensearch(req: Requirements) -> Fitness:
    e = _Eval()
    if "search" in req.consumer_access:
        e.plus("search consumers need an index")
    else:
        e.cap("unsuitable", "no search consumer")
    if set(req.consumer_access) - {"search"}:
        e.cap("acceptable", "other consumers still need a system-of-record sink")
    return e.done()


_EVALUATORS: dict[str, Callable[[Requirements], Fitness]] = {
    "s3_athena_iceberg_batch": _eval_athena,
    "s3_glue_spark_batch": _eval_glue,
    "kinesis_firehose_s3": _eval_firehose,
    "kinesis_lambda_dynamodb": _eval_lambda_ddb,
    "s3_emr_serverless_spark": _eval_emr,
    "redshift_sql_elt": _eval_redshift,
    "kinesis_firehose_iceberg_upsert": _eval_firehose_iceberg,
    "kinesis_flink_stateful": _eval_flink,
    "dms_cdc_s3": _eval_dms,
    "opensearch_ingestion": _eval_opensearch,
}

PATTERNS: dict[str, Pattern] = {
    p.pattern_id: p
    for p in [
        Pattern(
            pattern_id="s3_athena_iceberg_batch",
            title="S3 → Athena SQL (Iceberg MERGE) → S3 Iceberg table",
            summary="Scheduled SQL over files already in S3; upserts/deletes with MERGE; no cluster.",
            components={
                "ingest": ["s3"],
                "transform": ["athena"],
                "store": ["s3", "glue_catalog"],
                "orchestrate": ["eventbridge_scheduler", "step_functions"],
                "monitor": ["cloudwatch"],
            },
            adapter_status="implemented",
        ),
        Pattern(
            pattern_id="kinesis_firehose_s3",
            title="Kinesis Data Streams → Firehose (+Lambda transform) → S3",
            summary="Streaming delivery of events to S3 objects with an Athena-readable table.",
            components={
                "ingest": ["kinesis_streams", "firehose"],
                "transform": ["lambda"],
                "store": ["s3", "glue_catalog"],
                "orchestrate": [],
                "monitor": ["cloudwatch"],
            },
            adapter_status="implemented",
        ),
        Pattern(
            pattern_id="kinesis_lambda_dynamodb",
            title="Kinesis Data Streams → Lambda → DynamoDB",
            summary="Per-record processing into a key-value table for low-latency lookups.",
            components={
                "ingest": ["kinesis_streams"],
                "transform": ["lambda"],
                "store": ["dynamodb"],
                "orchestrate": [],
                "monitor": ["cloudwatch"],
            },
            adapter_status="implemented",
        ),
        Pattern(
            pattern_id="s3_glue_spark_batch",
            title="S3 → Glue Spark job → S3 Iceberg table",
            summary="Scheduled PySpark for heavy joins or custom code; Iceberg MERGE for upserts.",
            components={
                "ingest": ["s3"],
                "transform": ["glue_spark"],
                "store": ["s3", "glue_catalog"],
                "orchestrate": ["eventbridge_scheduler", "step_functions"],
                "monitor": ["cloudwatch"],
            },
            adapter_status="implemented",
        ),
        Pattern(
            pattern_id="s3_emr_serverless_spark",
            title="S3 → EMR Serverless Spark → S3 Iceberg",
            summary="Spark with fine-grained capacity control.",
            components={"ingest": ["s3"], "transform": ["emr_serverless"], "store": ["s3", "glue_catalog"],
                        "orchestrate": ["step_functions"], "monitor": ["cloudwatch"]},
            adapter_status="catalog_only",
            implementation_work=[
                "EMR Serverless application + execution role in the flow template",
                "Step Functions emr-serverless:startJobRun.sync task and job script packaging",
                "observe/diagnose readers for job run state and driver logs",
            ],
        ),
        Pattern(
            pattern_id="redshift_sql_elt",
            title="In-warehouse SQL ELT on Redshift",
            summary="Transform inside an existing Redshift warehouse with scheduled SQL (MERGE).",
            components={"ingest": ["redshift"], "transform": ["redshift"], "store": ["redshift"],
                        "orchestrate": ["eventbridge_scheduler"], "monitor": ["cloudwatch"]},
            adapter_status="catalog_only",
            implementation_work=[
                "Redshift Data API executor (async statement polling, as in awslabs redshift-mcp-server)",
                "Scheduler → Redshift Data API target and schema migration steps",
                "consumer-read verification with a Redshift database user/role",
            ],
        ),
        Pattern(
            pattern_id="kinesis_firehose_iceberg_upsert",
            title="Kinesis → Firehose → Iceberg (row-level upsert/delete)",
            summary="Streaming upserts into Iceberg tables via Firehose's Iceberg destination.",
            components={"ingest": ["kinesis_streams", "firehose"], "transform": ["lambda"],
                        "store": ["s3", "glue_catalog"], "orchestrate": [], "monitor": ["cloudwatch"]},
            adapter_status="catalog_only",
            implementation_work=[
                "Firehose IcebergDestinationConfiguration with unique keys and operation routing",
                "compaction schedule for small files",
            ],
        ),
        Pattern(
            pattern_id="kinesis_flink_stateful",
            title="Kinesis/MSK → Managed Service for Apache Flink",
            summary="Stateful streaming: windows, joins, sessions with exactly-once checkpoints.",
            components={"ingest": ["kinesis_streams", "msk_flink"], "transform": ["msk_flink"],
                        "store": ["s3"], "orchestrate": [], "monitor": ["cloudwatch"]},
            adapter_status="catalog_only",
            implementation_work=["Flink application packaging/deploy", "checkpoint/savepoint-based recovery"],
        ),
        Pattern(
            pattern_id="dms_cdc_s3",
            title="RDS/Aurora → DMS CDC → S3",
            summary="Change data capture from relational databases.",
            components={"ingest": ["dms"], "transform": [], "store": ["s3"], "orchestrate": [],
                        "monitor": ["cloudwatch"]},
            adapter_status="catalog_only",
            implementation_work=["DMS serverless replication config + endpoints", "VPC/network reachability checks"],
        ),
        Pattern(
            pattern_id="opensearch_ingestion",
            title="OpenSearch Ingestion → OpenSearch",
            summary="Index records for search consumers.",
            components={"ingest": ["opensearch"], "transform": [], "store": ["opensearch"], "orchestrate": [],
                        "monitor": ["cloudwatch"]},
            adapter_status="catalog_only",
            implementation_work=["OSI pipeline config", "index template from contract schema"],
        ),
    ]
}


def rank_key(level: FitLevel) -> int:
    return _RANK[level]
