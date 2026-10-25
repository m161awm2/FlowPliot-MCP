"""Data contract: the agreed description of what a flow must produce."""

from __future__ import annotations

import re
from typing import Literal

from pydantic import Field, field_validator, model_validator

from pmcp.domain.base import Model, Stored

ColumnType = Literal[
    "string", "int", "bigint", "double", "decimal(18,2)", "decimal(38,9)", "boolean", "date", "timestamp"
]
IDENT = r"^[a-z][a-z0-9_]{0,62}$"


class Column(Model):
    name: str = Field(pattern=IDENT)
    type: ColumnType
    nullable: bool = True
    description: str | None = None


class SourceDescriptor(Model):
    """Where the input lives. `uri` forms: s3://bucket/prefix/, kinesis:<stream-name>, glue:<db>.<table>,
    dynamodb:<table>, rds:<identifier>, redshift:<workgroup-or-cluster>.<db>.<schema>.<table>, kafka:<cluster>/<topic>,
    app:<free text, producer not yet emitting anywhere>."""

    name: str = Field(pattern=IDENT)
    uri: str
    format: Literal["jsonl", "json", "csv", "parquet", "avro", "records", "unknown"] = "unknown"
    columns: list[Column] = Field(default_factory=list, description="Source fields as profiled; may be empty.")
    event_time_column: str | None = None
    change_marker: str | None = Field(
        None, description="Column that marks a delete/tombstone record (boolean true = deleted)."
    )

    @property
    def kind(self) -> str:
        if self.uri.startswith("s3://"):
            return "s3"
        return self.uri.split(":", 1)[0] if ":" in self.uri else "unknown"


class QualityRule(Model):
    kind: Literal["not_null", "unique", "accepted_values", "range", "row_count_between", "custom_sql"]
    column: str | None = None
    values: list[str] | None = None
    min: float | None = None
    max: float | None = None
    sql: str | None = Field(
        None, description="custom_sql: a read-only SELECT returning one row with column `violations` (0 = pass)."
    )
    severity: Literal["error", "warn"] = "error"


class Freshness(Model):
    max_lag_seconds: int = Field(ge=1, description="Max age of the newest source record not yet visible to consumers.")
    measured_on: str | None = Field(None, description="Output column holding event time; defaults to source event time.")


class Mutation(Model):
    mode: Literal["append", "upsert", "upsert_with_deletes"] = "append"
    key: list[str] = Field(default_factory=list, description="Output primary key; required unless mode=append.")
    ordering_column: str | None = Field(
        None, description="Column that orders versions of the same key (latest wins). Required for upsert modes."
    )
    duplicates: Literal["allowed", "drop_exact", "latest_per_key"] = "latest_per_key"


class Consumer(Model):
    name: str
    access: Literal["sql_analytics", "key_lookup", "search", "file_export", "stream", "other"]
    principal_arn: str | None = Field(None, description="Existing IAM principal that must be able to read, if any.")
    query_latency_ms: int | None = Field(None, description="p95 read latency the consumer needs, if known.")
    notes: str | None = None


class Volume(Model):
    records_per_day: int | None = Field(None, ge=0)
    bytes_per_day: int | None = Field(None, ge=0)
    peak_records_per_second: int | None = Field(None, ge=0)
    growth_percent_per_month: float | None = None
    retention_days: int | None = Field(None, ge=1)


class TransformProfile(Model):
    """How hard the transformation is. The agent fills this from the requirements; the server uses it for
    candidate fitness only."""

    expressible_in_sql: bool = True
    joins: int = Field(0, ge=0)
    join_sources_outside_flow: bool = False
    stateful_streaming: bool = Field(False, description="Windows/sessions over unbounded streams.")
    custom_code_needed: bool = False
    rules: list[str] = Field(default_factory=list, description="Plain-language processing rules, in order.")


class OutputDataset(Model):
    name: str = Field(pattern=IDENT)
    columns: list[Column] = Field(min_length=1)
    partition_by: list[str] = Field(default_factory=list)


class DataContractBody(Model):
    title: str = Field(max_length=120)
    sources: list[SourceDescriptor] = Field(min_length=1)
    output: OutputDataset
    mutation: Mutation = Field(default_factory=Mutation)
    freshness: Freshness
    transform: TransformProfile = Field(default_factory=TransformProfile)
    volume: Volume = Field(default_factory=Volume)
    consumers: list[Consumer] = Field(min_length=1)
    quality: list[QualityRule] = Field(default_factory=list)
    reprocessing_window_days: int | None = Field(
        None, ge=1, description="How far back the flow must be able to rebuild output from source."
    )
    monthly_cost_cap_usd: float | None = Field(None, gt=0)
    existing_resources_to_reuse: list[str] = Field(
        default_factory=list, description="ARNs/URIs the user wants reused (e.g. an existing stream or warehouse)."
    )
    network_notes: str | None = None

    @model_validator(mode="after")
    def _consistency(self) -> DataContractBody:
        out_cols = {c.name for c in self.output.columns}
        m = self.mutation
        if m.mode != "append":
            if not m.key:
                raise ValueError("mutation.key is required for upsert modes")
            if not m.ordering_column:
                raise ValueError("mutation.ordering_column is required for upsert modes")
        for col in [*m.key, *([m.ordering_column] if m.ordering_column else []), *self.output.partition_by]:
            if col not in out_cols:
                raise ValueError(f"column '{col}' is referenced but not in output.columns")
        if self.freshness.measured_on and self.freshness.measured_on not in out_cols:
            raise ValueError("freshness.measured_on must be an output column")
        for rule in self.quality:
            if rule.column and rule.column not in out_cols:
                raise ValueError(f"quality rule column '{rule.column}' not in output.columns")
            if rule.kind == "custom_sql" and not rule.sql:
                raise ValueError("custom_sql quality rule needs sql")
        return self


class DataContract(Stored):
    KIND = "CONTRACT"

    contract_id: str
    contract_version: int
    body: DataContractBody
    previous_version: int | None = None

    @field_validator("contract_id")
    @classmethod
    def _cid(cls, v: str) -> str:
        if not re.match(r"^dc_[0-9a-z]+$", v):
            raise ValueError("bad contract_id")
        return v
