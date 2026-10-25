"""Service facts used for constraint checks and candidate fitness.

Facts are documented defaults the planner reasons with, not live quotas. `get_service_constraints`
adds live data (region availability, Service Quotas) on top when an account is given, and every
static fact carries `kind="documented_default"` so callers can tell the two apart.
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field

from pmcp.domain.base import Model

Category = Literal["ingest", "transform", "store", "orchestrate", "monitor"]


class Fact(Model):
    name: str
    value: str
    kind: Literal["documented_default", "adjustable_quota", "behavior"] = "documented_default"
    quota_name: str | None = Field(
        None, description="Substring of the Service Quotas quota name used to read the live value, if one exists."
    )


class ServiceInfo(Model):
    service_id: str
    boto_service: str
    title: str
    categories: list[Category]
    latency_floor_seconds: int = Field(description="Practical lower bound on end-to-end delay this service adds.")
    delivery: Literal["exactly_once", "at_least_once", "transactional", "n/a"] = "n/a"
    strengths: list[str]
    weaknesses: list[str]
    cost_model: str
    facts: list[Fact] = Field(default_factory=list)
    quota_service_code: str | None = None


SERVICES: dict[str, ServiceInfo] = {
    s.service_id: s
    for s in [
        ServiceInfo(
            service_id="s3",
            boto_service="s3",
            title="Amazon S3",
            categories=["ingest", "store"],
            latency_floor_seconds=1,
            delivery="n/a",
            strengths=["cheap durable storage", "open formats (Parquet/Iceberg)", "strong read-after-write"],
            weaknesses=["no key lookups without a query engine", "small files hurt query engines"],
            cost_model="GB-month storage + per-request",
            facts=[
                Fact(name="request_rate_per_prefix", value="3,500 PUT/COPY/POST/DELETE and 5,500 GET/HEAD per second"),
                Fact(name="consistency", value="strong read-after-write for all objects", kind="behavior"),
            ],
        ),
        ServiceInfo(
            service_id="athena",
            boto_service="athena",
            title="Amazon Athena (SQL, engine v3, Iceberg)",
            categories=["transform", "store"],
            latency_floor_seconds=60,
            delivery="transactional",
            strengths=[
                "SQL transforms where data already lives (no cluster)",
                "Iceberg MERGE/UPDATE/DELETE for upserts and deletes",
                "pay per data scanned",
            ],
            weaknesses=[
                "not for sub-second key lookups",
                "per-query startup of seconds",
                "custom non-SQL logic needs UDFs (Lambda)",
            ],
            cost_model="USD per TB scanned (10 MB minimum per query)",
            facts=[
                Fact(name="dml_query_timeout", value="30 minutes default", kind="adjustable_quota"),
                Fact(
                    name="active_dml_queries",
                    value="account/region quota (commonly 20-25 by default)",
                    kind="adjustable_quota",
                    quota_name="Active DML queries",
                ),
                Fact(name="iceberg_dml", value="MERGE INTO / UPDATE / DELETE on Iceberg tables", kind="behavior"),
            ],
            quota_service_code="athena",
        ),
        ServiceInfo(
            service_id="glue_spark",
            boto_service="glue",
            title="AWS Glue Spark jobs",
            categories=["transform"],
            latency_floor_seconds=120,
            delivery="transactional",
            strengths=["large joins and heavy transforms", "custom PySpark code", "Iceberg support"],
            weaknesses=["job startup overhead", "minimum billing per run", "overkill for small data"],
            cost_model="USD per DPU-hour, billed per second with 1-minute minimum",
            facts=[
                Fact(name="worker_types", value="G.1X (1 DPU), G.2X (2 DPU), G.4X, G.8X; minimum 2 workers"),
                Fact(name="job_timeout", value="2880 minutes default", kind="adjustable_quota"),
            ],
            quota_service_code="glue",
        ),
        ServiceInfo(
            service_id="glue_catalog",
            boto_service="glue",
            title="AWS Glue Data Catalog",
            categories=["store"],
            latency_floor_seconds=0,
            strengths=["shared table metadata for Athena/Glue/EMR/Redshift Spectrum"],
            weaknesses=["metadata only"],
            cost_model="free tier then per object and per request",
        ),
        ServiceInfo(
            service_id="emr_serverless",
            boto_service="emr-serverless",
            title="Amazon EMR Serverless (Spark)",
            categories=["transform"],
            latency_floor_seconds=60,
            delivery="transactional",
            strengths=["large Spark/Hive workloads", "pre-initialized capacity", "fine-grained sizing"],
            weaknesses=["more moving parts than Glue for small jobs"],
            cost_model="USD per vCPU-hour, GB-hour and storage",
            quota_service_code="emr-serverless",
        ),
        ServiceInfo(
            service_id="lambda",
            boto_service="lambda",
            title="AWS Lambda",
            categories=["transform"],
            latency_floor_seconds=1,
            delivery="at_least_once",
            strengths=["per-record custom code", "stream processing with Kinesis/DynamoDB event sources"],
            weaknesses=["15-minute limit", "no large joins", "at-least-once delivery from streams"],
            cost_model="per request + GB-second",
            facts=[
                Fact(name="max_duration", value="900 seconds"),
                Fact(name="memory", value="128 MB - 10,240 MB"),
                Fact(name="kinesis_batch_size", value="1 - 10,000 records; parallelization factor 1-10 per shard"),
                Fact(
                    name="concurrent_executions",
                    value="account/region quota (1,000 default)",
                    kind="adjustable_quota",
                    quota_name="Concurrent executions",
                ),
            ],
            quota_service_code="lambda",
        ),
        ServiceInfo(
            service_id="kinesis_streams",
            boto_service="kinesis",
            title="Amazon Kinesis Data Streams",
            categories=["ingest"],
            latency_floor_seconds=1,
            delivery="at_least_once",
            strengths=["ordered per partition key", "replay within retention", "multiple consumers"],
            weaknesses=["consumers must handle duplicates", "shard/throughput planning in provisioned mode"],
            cost_model="on-demand: per stream-hour + per GB; provisioned: per shard-hour + PUT units",
            facts=[
                Fact(name="shard_write", value="1 MB/s or 1,000 records/s per shard"),
                Fact(name="retention", value="24 hours default, up to 365 days"),
                Fact(
                    name="shards_per_region",
                    value="account/region quota",
                    kind="adjustable_quota",
                    quota_name="Shards per Region",
                ),
            ],
            quota_service_code="kinesis",
        ),
        ServiceInfo(
            service_id="firehose",
            boto_service="firehose",
            title="Amazon Data Firehose",
            categories=["ingest", "transform"],
            latency_floor_seconds=5,
            delivery="at_least_once",
            strengths=["managed delivery to S3/Redshift/OpenSearch", "optional Lambda transform",
                       "Parquet conversion with a Glue schema"],
            weaknesses=["append-only delivery (no upsert/delete)", "duplicates possible on retry"],
            cost_model="per GB ingested (+ format conversion, + dynamic partitioning)",
            facts=[
                Fact(name="buffering", value="interval 0-900 s, size 1-128 MiB"),
                Fact(name="record_size", value="up to 1,000 KiB before base64"),
            ],
            quota_service_code="firehose",
        ),
        ServiceInfo(
            service_id="dynamodb",
            boto_service="dynamodb",
            title="Amazon DynamoDB",
            categories=["store"],
            latency_floor_seconds=0,
            delivery="transactional",
            strengths=["single-digit-ms key lookups", "conditional writes for idempotent upserts", "on-demand"],
            weaknesses=["no ad-hoc SQL analytics", "400 KB item limit", "access patterns fixed up front"],
            cost_model="on-demand per read/write request unit + GB-month",
            facts=[Fact(name="item_size", value="400 KB max")],
            quota_service_code="dynamodb",
        ),
        ServiceInfo(
            service_id="redshift",
            boto_service="redshift-serverless",
            title="Amazon Redshift (Serverless or provisioned)",
            categories=["transform", "store"],
            latency_floor_seconds=30,
            delivery="transactional",
            strengths=["in-warehouse SQL ELT and MERGE", "BI concurrency", "zero-ETL integrations"],
            weaknesses=["standing cost if no warehouse exists yet", "loading pipeline needed for S3 data"],
            cost_model="Serverless: RPU-hours (per second, 60 s minimum); provisioned: node-hours",
        ),
        ServiceInfo(
            service_id="msk_flink",
            boto_service="kinesisanalyticsv2",
            title="Amazon MSK / Managed Service for Apache Flink",
            categories=["ingest", "transform"],
            latency_floor_seconds=1,
            delivery="exactly_once",
            strengths=["stateful streaming (windows, joins, sessions)", "exactly-once with checkpoints"],
            weaknesses=["highest operational and standing cost", "application code required"],
            cost_model="Flink KPU-hours; MSK broker-hours or serverless partition-hours",
        ),
        ServiceInfo(
            service_id="dms",
            boto_service="dms",
            title="AWS DMS (CDC)",
            categories=["ingest"],
            latency_floor_seconds=5,
            delivery="at_least_once",
            strengths=["change data capture from relational databases"],
            weaknesses=["network path to the source DB", "replication instance or serverless capacity cost"],
            cost_model="replication instance hours or DMS Serverless DCU-hours",
        ),
        ServiceInfo(
            service_id="opensearch",
            boto_service="opensearch",
            title="Amazon OpenSearch Service / Ingestion",
            categories=["store", "ingest"],
            latency_floor_seconds=1,
            delivery="at_least_once",
            strengths=["full-text and vector search", "near-real-time indexing"],
            weaknesses=["standing cluster or OCU cost", "not a system of record"],
            cost_model="instance-hours or OCU-hours + storage",
        ),
        ServiceInfo(
            service_id="step_functions",
            boto_service="stepfunctions",
            title="AWS Step Functions (Standard)",
            categories=["orchestrate"],
            latency_floor_seconds=1,
            strengths=["durable multi-step runs", ".sync integrations with Athena/Glue/EMR", "execution history"],
            weaknesses=["25,000 events per execution history"],
            cost_model="per state transition",
        ),
        ServiceInfo(
            service_id="eventbridge_scheduler",
            boto_service="scheduler",
            title="Amazon EventBridge Scheduler",
            categories=["orchestrate"],
            latency_floor_seconds=60,
            strengths=["cron/rate schedules with flexible windows", "direct Step Functions targets"],
            weaknesses=["1-minute minimum granularity"],
            cost_model="per million invocations (large free tier)",
        ),
        ServiceInfo(
            service_id="cloudwatch",
            boto_service="cloudwatch",
            title="Amazon CloudWatch alarms + SNS",
            categories=["monitor"],
            latency_floor_seconds=60,
            strengths=["keeps watching after the agent session ends"],
            weaknesses=["metric periods of 1 minute or more for standard metrics"],
            cost_model="per alarm-month + SNS deliveries",
        ),
    ]
}
