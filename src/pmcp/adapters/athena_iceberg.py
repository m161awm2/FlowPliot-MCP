"""Pattern s3_athena_iceberg_batch: scheduled Athena SQL that MERGEs a window of S3 source data into an
Iceberg table.

Runtime shape (all in the tenant account, no pmcp process involved):
  EventBridge Scheduler --(window_end=scheduled time)--> Step Functions --> Athena MERGE (.sync)
  CloudWatch alarms on failed / missing successful executions --> SNS.
Reruns of the same window are idempotent: MERGE keeps the latest version per key by the ordering
column, and append-only flows delete the window before inserting it.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from pydantic import Field, field_validator

from pmcp.adapters.base import (
    Adapter,
    DiagnosisParts,
    RecoveryOption,
    RenderContext,
    Rendered,
    RenderedArtifact,
    Runtime,
    alarm,
    alerts_topic,
    athena_query,
    data_bucket,
    iam_role,
    s3_parts,
    template,
    unverified,
)
from pmcp.aws.client import BOTO_CONFIG
from pmcp.aws.redaction import STEP_FUNCTIONS_EXECUTION_FIELDS, allow
from pmcp.catalog.pricing import PriceBook
from pmcp.domain.base import Model
from pmcp.domain.contract import Column, DataContractBody
from pmcp.domain.records import Check, CostEstimate, CostLine, Hypothesis, PlanStep
from pmcp.errors import ErrorCode, PmcpError
from pmcp.ids import iso, short
from pmcp.vendor.awslabs.sql_analyzer import SqlAnalyzer

PLACEHOLDERS = ("{{source}}", "{{window_start}}", "{{window_end}}")
DELETE_FLAG = "pmcp_deleted"


class SourcePartition(Model):
    column: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    kind: Literal["date"] = "date"
    format: str = Field("yyyy-MM-dd", description="Java date format of the partition value in the S3 path.")
    range_start: str = Field(description="Earliest partition value, e.g. 2026-01-01.")


class AthenaIcebergParams(Model):
    source_uri: str = Field(description="s3://bucket/prefix/ holding the source files.")
    source_format: Literal["jsonl", "csv", "parquet"]
    source_columns: list[Column] = Field(min_length=1, description="Raw source schema (from profile_source).")
    source_partition: SourcePartition | None = Field(
        None, description="Hive-style date partition in the source path (<column>=<value>/), read via projection."
    )
    csv_has_header: bool = True
    transform_sql: str = Field(
        description=(
            "A read-only SELECT producing the contract's output columns from {{source}}. Use {{window_start}} and "
            "{{window_end}} (timestamp with time zone expressions) to restrict input to the run window. For "
            "upsert_with_deletes also output a boolean column pmcp_deleted (true = delete the key)."
        )
    )
    schedule_expression: str = Field("rate(1 hour)", description="EventBridge Scheduler rate()/cron() expression.")
    window_minutes: int = Field(120, ge=5, le=60 * 24 * 7, description="Input window per scheduled run.")
    bytes_scanned_cutoff_gb: float = Field(10, gt=0, le=1024, description="Athena per-query scan limit (cost guard).")
    alert_email: str | None = None
    lake_formation_grants: bool = Field(False, description="Add Lake Formation grants (account uses LF permissions).")
    quarantined_paths: list[str] = Field(
        default_factory=list, max_length=200,
        description="Source files (s3:// paths under source_uri) excluded from processing. Compiled into the SQL as "
        "constant $path predicates so Athena never opens them.",
    )
    allow_destructive_rebuild: bool = Field(
        False, description="Permit drop-and-rebuild of the output table when a schema change cannot be applied in place."
    )

    @field_validator("schedule_expression")
    @classmethod
    def _sched(cls, v: str) -> str:
        if not re.fullmatch(r"(rate\(\d+ (minute|minutes|hour|hours|day|days)\)|cron\([^)]+\))", v):
            raise ValueError("schedule_expression must be rate(...) or cron(...)")
        return v


ATHENA_TYPES = {
    "string": "string",
    "int": "int",
    "bigint": "bigint",
    "double": "double",
    "decimal(18,2)": "decimal(18,2)",
    "decimal(38,9)": "decimal(38,9)",
    "boolean": "boolean",
    "date": "date",
    "timestamp": "timestamp",
}
SQL_CAST_TYPES = {**ATHENA_TYPES, "string": "varchar", "timestamp": "timestamp(6)"}


def _schedule_minutes(expr: str) -> int | None:
    m = re.fullmatch(r"rate\((\d+) (minute|minutes|hour|hours|day|days)\)", expr)
    if not m:
        return None
    n = int(m.group(1))
    return n * {"minute": 1, "minutes": 1, "hour": 60, "hours": 60, "day": 1440, "days": 1440}[m.group(2)]


def _q(ident: str) -> str:
    """Identifier quoting for DML (SELECT/MERGE/INSERT/DELETE), parsed by Athena's Trino engine."""
    return f'"{ident}"'


def _bq(ident: str) -> str:
    """Identifier quoting for DDL. Athena sends backtick-quoted DDL to its Hive parser, which is the one that
    accepts LOCATION/TBLPROPERTIES for Iceberg CREATE TABLE; double quotes route it to Trino and fail."""
    return f"`{ident}`"


def validate_transform_sql(sql: str) -> list[str]:
    errors = []
    probe = sql
    for p in PLACEHOLDERS:
        probe = probe.replace(p, "x")
    if not SqlAnalyzer.is_read_only_query(probe):
        errors.append("transform_sql must be a single read-only SELECT/WITH query")
    if ";" in SqlAnalyzer._preprocess_sql(probe).rstrip(";"):
        errors.append("transform_sql must not contain multiple statements")
    if "{{source}}" not in sql:
        errors.append("transform_sql must read from {{source}}")
    leftover = set(re.findall(r"\{\{[^}]*\}\}", sql)) - set(PLACEHOLDERS)
    if leftover:
        errors.append(f"unknown placeholders: {sorted(leftover)}")
    return errors


class Names:
    def __init__(self, ctx_or_short: RenderContext | str, account: str = "", region: str = "") -> None:
        if isinstance(ctx_or_short, RenderContext):
            s, account, region = ctx_or_short.short, ctx_or_short.account_id, ctx_or_short.region
        else:
            s = ctx_or_short
        self.short = s
        self.database = f"pmcp_{s}"
        self.raw_table = "raw_source"
        self.lines_table = "raw_lines"
        self.quarantine_table = "pmcp_quarantine"
        self.workgroup = f"pmcp-{s}"
        self.consumer_workgroup = f"pmcp-{s}-consumer"
        self.state_machine = f"pmcp-{s}-run"
        self.schedule = f"pmcp-{s}-schedule"
        self.bucket = f"pmcp-{s}-{account}-{region}"
        self.consumer_role = f"pmcp-{s}-consumer"


class SqlBuilder:
    def __init__(self, body: DataContractBody, p: AthenaIcebergParams, n: Names) -> None:
        self.body, self.p, self.n = body, p, n

    @property
    def target(self) -> str:
        return f"{self.n.database}.{_q(self.body.output.name)}"

    @property
    def target_ddl(self) -> str:
        return f"{_bq(self.n.database)}.{_bq(self.body.output.name)}"

    def source_ref(self) -> str:
        raw = f"{self.n.database}.{self.n.raw_table}"
        paths = sorted(set(self.p.quarantined_paths))
        if not paths:
            return raw
        # Constant predicates only: verified on Athena that `"$path" <> '<file>'` prunes the file before it is
        # opened, while `NOT IN (SELECT ...)` still reads it (and a malformed file then fails the query).
        conds = " AND ".join("\"$path\" <> '" + p.replace("'", "''") + "'" for p in paths)
        return f"(SELECT * FROM {raw} WHERE {conds})"

    def transform(self, start_expr: str, end_expr: str) -> str:
        # Comments stripped and whitespace collapsed: the SQL is embedded in a one-line States.Format.
        return (
            SqlAnalyzer._preprocess_sql(self.p.transform_sql)
            .replace("{{source}}", self.source_ref())
            .replace("{{window_start}}", start_expr)
            .replace("{{window_end}}", end_expr)
        )

    def _deduped(self, start_expr: str, end_expr: str) -> str:
        m = self.body.mutation
        keys = ", ".join(_q(k) for k in m.key)
        return (
            "SELECT * FROM (SELECT s.*, row_number() OVER (PARTITION BY "
            f"{keys} ORDER BY {_q(m.ordering_column or m.key[0])} DESC) AS pmcp_rn FROM ("
            f"{self.transform(start_expr, end_expr)}) s) WHERE pmcp_rn = 1"
        )

    def _cast(self, col: Column, alias: str = "s") -> str:
        return f"CAST({alias}.{_q(col.name)} AS {SQL_CAST_TYPES[col.type]})"

    def run_statements(self, start_expr: str, end_expr: str) -> list[str]:
        """Statements one scheduled/backfill run executes, in order."""
        m = self.body.mutation
        cols = self.body.output.columns
        col_list = ", ".join(_q(c.name) for c in cols)
        values = ", ".join(self._cast(c) for c in cols)
        if not m.key:
            time_col = self.body.freshness.measured_on
            assert time_col, "checked in check_supported"
            return [
                f"DELETE FROM {self.target} WHERE {_q(time_col)} >= {start_expr} AND {_q(time_col)} < {end_expr}",
                f"INSERT INTO {self.target} ({col_list}) SELECT {values} FROM ("
                f"{self.transform(start_expr, end_expr)}) s",
            ]
        on = " AND ".join(f"t.{_q(k)} = s.{_q(k)}" for k in m.key)
        ordering = _q(m.ordering_column or m.key[0])
        updates = ", ".join(f"{_q(c.name)} = {self._cast(c)}" for c in cols if c.name not in m.key)
        clauses = []
        if m.mode == "upsert_with_deletes":
            clauses.append(f"WHEN MATCHED AND s.{DELETE_FLAG} AND s.{ordering} >= t.{ordering} THEN DELETE")
        if m.mode in ("upsert", "upsert_with_deletes") and updates:
            clauses.append(f"WHEN MATCHED AND s.{ordering} >= t.{ordering} THEN UPDATE SET {updates}")
        not_deleted = f" AND coalesce(s.{DELETE_FLAG}, false) = false" if m.mode == "upsert_with_deletes" else ""
        clauses.append(f"WHEN NOT MATCHED{not_deleted} THEN INSERT ({col_list}) VALUES ({values})")
        merge = (
            f"MERGE INTO {self.target} t USING ({self._deduped(start_expr, end_expr)}) s ON ({on}) "
            + " ".join(clauses)
        )
        return [merge]

    def create_target(self) -> str:
        cols = ", ".join(f"{_bq(c.name)} {ATHENA_TYPES[c.type]}" for c in self.body.output.columns)
        types = {c.name: c.type for c in self.body.output.columns}
        parts = [f"day({_bq(c)})" if types[c] == "timestamp" else _bq(c) for c in self.body.output.partition_by]
        partitioned = f" PARTITIONED BY ({', '.join(parts)})" if parts else ""
        loc = f"s3://{self.n.bucket}/tables/{self.body.output.name}/"
        return (
            f"CREATE TABLE IF NOT EXISTS {self.target_ddl} ({cols}){partitioned} LOCATION '{loc}' "
            "TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet', 'write_compression'='zstd')"
        )

    def create_quarantine(self) -> str:
        loc = f"s3://{self.n.bucket}/tables/{self.n.quarantine_table}/"
        return (
            f"CREATE TABLE IF NOT EXISTS {_bq(self.n.database)}.{_bq(self.n.quarantine_table)} "
            f"(path string, reason string, added_at timestamp) LOCATION '{loc}' "
            "TBLPROPERTIES ('table_type'='ICEBERG')"
        )


def _sfn_format(sql: str) -> tuple[str, list[str]]:
    """Turn SQL with window placeholders into a States.Format template + ordered argument paths."""
    tokens = re.split(r"(\x00S\x00|\x00E\x00)", sql)
    out, args = [], []
    for t in tokens:
        if t == "\x00S\x00":
            out.append("{}")
            args.append("$.w.start_expr")
        elif t == "\x00E\x00":
            out.append("{}")
            args.append("$.w.end_expr")
        else:
            out.append(t.replace("\\", "\\\\").replace("'", "\\'").replace("{", "\\{").replace("}", "\\}"))
    return "".join(out), args


def _ts_literal(iso_ts: str) -> str:
    return f"from_iso8601_timestamp('{iso_ts}')"


class AthenaIcebergAdapter(Adapter):
    pattern_id = "s3_athena_iceberg_batch"
    Params = AthenaIcebergParams

    def check_supported(self, params: AthenaIcebergParams, contract: DataContractBody) -> None:
        problems = validate_transform_sql(params.transform_sql)
        if not params.source_uri.startswith("s3://") or not params.source_uri.endswith("/"):
            problems.append("source_uri must be an s3:// prefix ending with '/'")
        for qp in params.quarantined_paths:
            if not qp.startswith(params.source_uri) or "'" in qp:
                problems.append(f"quarantined path {qp} must be a file under source_uri")
        if not any(s.uri.rstrip("/") == params.source_uri.rstrip("/") for s in contract.sources):
            problems.append("source_uri must be one of the contract sources")
        m = contract.mutation
        if not m.key and not contract.freshness.measured_on:
            problems.append(
                "append without a key needs freshness.measured_on (an output time column) so reruns can replace the "
                "window idempotently"
            )
        if m.mode == "upsert_with_deletes" and DELETE_FLAG not in params.transform_sql:
            problems.append(f"upsert_with_deletes requires transform_sql to output a boolean '{DELETE_FLAG}' column")
        sched = _schedule_minutes(params.schedule_expression)
        if sched is not None and sched * 60 > contract.freshness.max_lag_seconds:
            problems.append(
                f"schedule every {sched} min cannot meet freshness.max_lag_seconds={contract.freshness.max_lag_seconds}"
            )
        if sched is not None and params.window_minutes < sched:
            problems.append("window_minutes must be >= the schedule interval or records between runs are skipped")
        if problems:
            raise PmcpError(
                ErrorCode.UNSUPPORTED_CONFIGURATION,
                "pattern_params cannot be deployed as given",
                details={"problems": problems},
            )

    # --- render -------------------------------------------------------------------------------------
    def _raw_table(self, ctx: RenderContext, p: AthenaIcebergParams, n: Names) -> dict[str, Any]:
        bucket, prefix = s3_parts(p.source_uri)
        cols = [{"Name": c.name, "Type": ATHENA_TYPES[c.type]} for c in p.source_columns]
        params: dict[str, str] = {"classification": p.source_format, "EXTERNAL": "TRUE"}
        if p.source_format == "jsonl":
            sd_extra = {
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe",
                    "Parameters": {"ignore.malformed.json": "FALSE", "case.insensitive": "TRUE"},
                },
            }
            params["classification"] = "json"
        elif p.source_format == "csv":
            sd_extra = {
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                    "Parameters": {"field.delim": ",", "serialization.format": ","},
                },
            }
            if p.csv_has_header:
                params["skip.header.line.count"] = "1"
        else:
            sd_extra = {
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"},
            }
        partition_keys = []
        if p.source_partition:
            sp = p.source_partition
            partition_keys = [{"Name": sp.column, "Type": "string"}]
            params.update(
                {
                    "projection.enabled": "true",
                    f"projection.{sp.column}.type": "date",
                    f"projection.{sp.column}.format": sp.format,
                    f"projection.{sp.column}.range": f"{sp.range_start},NOW",
                    f"projection.{sp.column}.interval": "1",
                    f"projection.{sp.column}.interval.unit": "DAYS",
                    "storage.location.template": f"s3://{bucket}/{prefix}{sp.column}=${{{sp.column}}}/",
                }
            )
        res = {
            "RawTable": {
                "Type": "AWS::Glue::Table",
                "Properties": {
                    "CatalogId": ctx.account_id,
                    "DatabaseName": {"Ref": "Database"},
                    "TableInput": {
                        "Name": n.raw_table,
                        "TableType": "EXTERNAL_TABLE",
                        "Parameters": params,
                        "PartitionKeys": partition_keys,
                        "StorageDescriptor": {"Columns": cols, "Location": p.source_uri, **sd_extra},
                    },
                },
            }
        }
        if p.source_format in ("jsonl", "csv"):
            # One-column view of the same files, used by diagnosis probes to find unparsable lines.
            res["LinesTable"] = {
                "Type": "AWS::Glue::Table",
                "Properties": {
                    "CatalogId": ctx.account_id,
                    "DatabaseName": {"Ref": "Database"},
                    "TableInput": {
                        "Name": n.lines_table,
                        "TableType": "EXTERNAL_TABLE",
                        "Parameters": {k: v for k, v in params.items() if k != "skip.header.line.count"}
                        | {"classification": "text"},
                        "PartitionKeys": partition_keys,
                        "StorageDescriptor": {
                            "Columns": [{"Name": "line", "Type": "string"}],
                            "Location": p.source_uri,
                            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                            # LazySimpleSerDe's default delimiter is \x01, so each line lands in one column.
                            # (CloudFormation rejects control characters in template strings.)
                            "SerdeInfo": {
                                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                                "Parameters": {"serialization.format": "1"},
                            },
                        },
                    },
                },
            }
        return res

    def _state_machine(self, sb: SqlBuilder, n: Names) -> dict[str, Any]:
        stmts = sb.run_statements("\x00S\x00", "\x00E\x00")
        states: dict[str, Any] = {
            "HasExplicitWindow": {
                "Type": "Choice",
                "Choices": [{"Variable": "$.window_start", "IsPresent": True, "Next": "ExplicitWindow"}],
                "Default": "ScheduledWindow",
            },
            "ScheduledWindow": {
                "Type": "Pass",
                "Parameters": {
                    "start_expr.$": "States.Format('(from_iso8601_timestamp(\\'{}\\') - interval \\'"
                    + str(sb.p.window_minutes)
                    + "\\' minute)', $.window_end)",
                    "end_expr.$": "States.Format('from_iso8601_timestamp(\\'{}\\')', $.window_end)",
                },
                "ResultPath": "$.w",
                "Next": "Statement0",
            },
            "ExplicitWindow": {
                "Type": "Pass",
                "Parameters": {
                    "start_expr.$": "States.Format('from_iso8601_timestamp(\\'{}\\')', $.window_start)",
                    "end_expr.$": "States.Format('from_iso8601_timestamp(\\'{}\\')', $.window_end)",
                },
                "ResultPath": "$.w",
                "Next": "Statement0",
            },
        }
        for i, sql in enumerate(stmts):
            fmt, args = _sfn_format(sql)
            nxt = f"Statement{i + 1}" if i + 1 < len(stmts) else "Done"
            states[f"Statement{i}"] = {
                "Type": "Task",
                "Resource": "arn:aws:states:::athena:startQueryExecution.sync",
                "Parameters": {
                    "QueryString.$": f"States.Format('{fmt}', {', '.join(args)})",
                    "WorkGroup": n.workgroup,
                    "QueryExecutionContext": {"Database": n.database},
                },
                "Retry": [
                    {
                        "ErrorEquals": ["Athena.TooManyRequestsException", "Athena.ThrottlingException"],
                        "IntervalSeconds": 10,
                        "MaxAttempts": 5,
                        "BackoffRate": 2,
                    },
                    {
                        # Iceberg commit conflicts surface as ICEBERG_COMMIT_ERROR; a retry re-reads the table.
                        "ErrorEquals": ["States.TaskFailed"],
                        "IntervalSeconds": 30,
                        "MaxAttempts": 1,
                    },
                ],
                "ResultPath": f"$.s{i}",
                "Next": nxt,
            }
        states["Done"] = {"Type": "Succeed"}
        return {"Comment": "pmcp s3_athena_iceberg_batch run", "StartAt": "HasExplicitWindow", "States": states}

    def render(self, ctx: RenderContext, p: AthenaIcebergParams) -> Rendered:
        body = ctx.contract.body
        n = Names(ctx)
        sb = SqlBuilder(body, p, n)
        src_bucket, src_prefix = s3_parts(p.source_uri)
        region, acct = ctx.region, ctx.account_id
        glue_arns = [
            f"arn:aws:glue:{region}:{acct}:catalog",
            f"arn:aws:glue:{region}:{acct}:database/{n.database}",
            f"arn:aws:glue:{region}:{acct}:table/{n.database}/*",
        ]
        wg_arn = f"arn:aws:athena:{region}:{acct}:workgroup/{n.workgroup}"
        cwg_arn = f"arn:aws:athena:{region}:{acct}:workgroup/{n.consumer_workgroup}"
        data_arn = f"arn:aws:s3:::{n.bucket}"
        athena_actions = [
            "athena:StartQueryExecution",
            "athena:GetQueryExecution",
            "athena:GetQueryResults",
            "athena:StopQueryExecution",
            "athena:GetWorkGroup",
        ]
        res: dict[str, Any] = {}
        res.update(data_bucket(ctx))
        res["Database"] = {
            "Type": "AWS::Glue::Database",
            "Properties": {"CatalogId": acct, "DatabaseInput": {"Name": n.database,
                                                                 "Description": f"pmcp flow {ctx.flow_id}"}},
        }
        res.update(self._raw_table(ctx, p, n))
        cutoff = int(p.bytes_scanned_cutoff_gb * 1024**3)
        for logical, name, prefix in (
            ("Workgroup", n.workgroup, "pipeline"),
            ("ConsumerWorkgroup", n.consumer_workgroup, "consumer"),
        ):
            res[logical] = {
                "Type": "AWS::Athena::WorkGroup",
                "Properties": {
                    "Name": name,
                    "RecursiveDeleteOption": True,
                    "State": "ENABLED",
                    "WorkGroupConfiguration": {
                        "EnforceWorkGroupConfiguration": True,
                        "PublishCloudWatchMetricsEnabled": True,
                        "BytesScannedCutoffPerQuery": max(cutoff, 10 * 1024**2),
                        "EngineVersion": {"SelectedEngineVersion": "Athena engine version 3"},
                        "ResultConfiguration": {"OutputLocation": f"s3://{n.bucket}/athena-results/{prefix}/"},
                    },
                    "Tags": [{"Key": k, "Value": v} for k, v in ctx.tags.items()],
                },
                "DependsOn": ["DataBucket"],
            }
        sfn_statements = [
            {"Effect": "Allow", "Action": athena_actions, "Resource": wg_arn},
            {
                "Effect": "Allow",
                "Action": ["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:GetPartitions",
                           "glue:UpdateTable", "glue:BatchGetPartition"],
                "Resource": glue_arns,
            },
            {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": f"arn:aws:s3:::{src_bucket}/{src_prefix}*"},
            {"Effect": "Allow", "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
             "Resource": f"arn:aws:s3:::{src_bucket}"},
            {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject",
                                           "s3:AbortMultipartUpload"], "Resource": f"{data_arn}/*"},
            {"Effect": "Allow", "Action": ["s3:ListBucket", "s3:GetBucketLocation"], "Resource": data_arn},
        ]
        if p.lake_formation_grants:
            sfn_statements.append({"Effect": "Allow", "Action": ["lakeformation:GetDataAccess"], "Resource": "*"})
        res.update(iam_role(ctx, "RunRole", ctx.name("run"), "states.amazonaws.com", sfn_statements))
        res["StateMachine"] = {
            "Type": "AWS::StepFunctions::StateMachine",
            "Properties": {
                "StateMachineName": n.state_machine,
                "StateMachineType": "STANDARD",
                "RoleArn": {"Fn::GetAtt": ["RunRole", "Arn"]},
                "DefinitionString": json.dumps(self._state_machine(sb, n)),
                "Tags": [{"Key": k, "Value": v} for k, v in ctx.tags.items()],
            },
        }
        res.update(
            iam_role(
                ctx,
                "SchedulerRole",
                ctx.name("sched"),
                "scheduler.amazonaws.com",
                [{"Effect": "Allow", "Action": "states:StartExecution", "Resource": {"Ref": "StateMachine"}}],
            )
        )
        res["Schedule"] = {
            "Type": "AWS::Scheduler::Schedule",
            "Properties": {
                "Name": n.schedule,
                "ScheduleExpression": p.schedule_expression,
                "ScheduleExpressionTimezone": "UTC",
                "FlexibleTimeWindow": {"Mode": "OFF"},
                "State": "ENABLED",
                "Target": {
                    "Arn": {"Ref": "StateMachine"},
                    "RoleArn": {"Fn::GetAtt": ["SchedulerRole", "Arn"]},
                    "Input": json.dumps({"window_end": "<aws.scheduler.scheduled-time>"}),
                    "RetryPolicy": {"MaximumRetryAttempts": 3, "MaximumEventAgeInSeconds": 3600},
                },
            },
        }
        consumer_statements = [
            {"Effect": "Allow", "Action": athena_actions, "Resource": cwg_arn},
            {
                "Effect": "Allow",
                "Action": ["glue:GetDatabase", "glue:GetTable", "glue:GetPartitions"],
                "Resource": [glue_arns[0], glue_arns[1],
                             f"arn:aws:glue:{region}:{acct}:table/{n.database}/{body.output.name}"],
            },
            {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": f"{data_arn}/tables/{body.output.name}/*"},
            {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject"],
             "Resource": f"{data_arn}/athena-results/consumer/*"},
            {"Effect": "Allow", "Action": ["s3:ListBucket", "s3:GetBucketLocation"], "Resource": data_arn},
        ]
        if p.lake_formation_grants:
            consumer_statements.append({"Effect": "Allow", "Action": ["lakeformation:GetDataAccess"], "Resource": "*"})
        res.update(iam_role(ctx, "ConsumerRole", n.consumer_role, None, consumer_statements, trust_platform=True))
        if p.lake_formation_grants:
            for logical, role in (("LfRunGrant", "RunRole"), ("LfConsumerGrant", "ConsumerRole")):
                res[logical] = {
                    "Type": "AWS::LakeFormation::PrincipalPermissions",
                    "Properties": {
                        "Principal": {"DataLakePrincipalIdentifier": {"Fn::GetAtt": [role, "Arn"]}},
                        "Resource": {"Table": {"CatalogId": acct, "DatabaseName": n.database, "TableWildcard": {}}},
                        "Permissions": ["SELECT", "DESCRIBE"] if role == "ConsumerRole" else ["ALL"],
                        "PermissionsWithGrantOption": [],
                    },
                    "DependsOn": ["Database"],
                }
        res.update(alerts_topic(ctx, p.alert_email))
        sm_dim = [{"Name": "StateMachineArn", "Value": {"Ref": "StateMachine"}}]
        res.update(alarm(ctx, "FailedRunsAlarm", "failed-runs", "AWS/States", "ExecutionsFailed", sm_dim,
                         description="A scheduled run failed"))
        # No successful run for max(schedule interval, freshness budget) => stale.
        interval_s = (_schedule_minutes(p.schedule_expression) or 60) * 60
        stale_period = min(86400, -(-max(interval_s, body.freshness.max_lag_seconds) // 60) * 60)
        res.update(
            alarm(ctx, "StaleAlarm", "no-successful-run", "AWS/States", "ExecutionsSucceeded", sm_dim,
                  period=stale_period, threshold=1, comparison="LessThanThreshold", missing="breaching",
                  description="No successful run within the freshness budget")
        )
        tmpl = template(
            f"pmcp flow {ctx.flow_id} ({self.pattern_id})",
            res,
            {
                "DataBucket": {"Ref": "DataBucket"},
                "Database": n.database,
                "OutputTable": body.output.name,
                "Workgroup": n.workgroup,
                "ConsumerWorkgroup": n.consumer_workgroup,
                "StateMachineArn": {"Ref": "StateMachine"},
                "ConsumerRoleArn": {"Fn::GetAtt": ["ConsumerRole", "Arn"]},
                "AlertsTopicArn": {"Ref": "AlertsTopic"},
            },
        )
        rendered = Rendered(template=tmpl)
        sample_sql = sb.run_statements(_ts_literal("<window_start>"), _ts_literal("<window_end>"))
        rendered.artifacts.append(RenderedArtifact("run.sql", "sql", ";\n\n".join(sample_sql) + ";\n"))
        rendered.artifacts.append(RenderedArtifact("transform.sql", "sql", p.transform_sql.strip() + "\n"))
        rendered.artifacts.append(RenderedArtifact("create_target.sql", "sql", sb.create_target() + ";\n"))

        ddl_steps = self._schema_steps(ctx, p, n, sb, rendered)
        rendered.post_deploy_steps.extend(ddl_steps)
        rendered.desired_summary = {
            "output_table": f"{n.database}.{body.output.name} (Iceberg, s3://{n.bucket}/tables/{body.output.name}/)",
            "schedule": p.schedule_expression,
            "window_minutes": p.window_minutes,
            "mutation": body.mutation.mode,
            "consumer_role": n.consumer_role,
            "alarms": [ctx.name("failed-runs"), ctx.name("no-successful-run")],
        }
        return rendered

    def _schema_steps(
        self, ctx: RenderContext, p: AthenaIcebergParams, n: Names, sb: SqlBuilder, rendered: Rendered
    ) -> list[PlanStep]:
        create = PlanStep(
            step_id="create_tables",
            action="athena.ddl",
            description="Create the Iceberg output and quarantine tables if absent",
            preconditions=["stack update complete"],
            params={"statements": [sb.create_target()]},
            reversible=True,
            compensation="Tables are dropped only by an explicit plan; leaving them is harmless.",
        )
        prev = ctx.previous_contract
        if prev is None:
            return [create]
        old = {c.name: c.type for c in prev.body.output.columns}
        new = {c.name: c.type for c in ctx.contract.body.output.columns}
        added = [c for c in ctx.contract.body.output.columns if c.name not in old]
        removed = [c for c in old if c not in new]
        changed = [c for c in new if c in old and old[c] != new[c]]
        key_changed = prev.body.mutation.key != ctx.contract.body.mutation.key
        part_changed = prev.body.output.partition_by != ctx.contract.body.output.partition_by
        rendered.schema_changes += [f"add column {c.name} {c.type}" for c in added]
        rendered.schema_changes += [f"remove column {c}" for c in removed]
        rendered.schema_changes += [f"change type of {c}: {old[c]} -> {new[c]}" for c in changed]
        if key_changed:
            rendered.schema_changes.append(f"key {prev.body.mutation.key} -> {ctx.contract.body.mutation.key}")
        if part_changed:
            rendered.schema_changes.append("partitioning changed")
        steps = [create]
        if removed or changed or key_changed or part_changed:
            rendered.breaking = True
            if not p.allow_destructive_rebuild:
                raise PmcpError(
                    ErrorCode.UNSUPPORTED_CONFIGURATION,
                    "schema change cannot be applied in place (column removed/retyped, key or partitioning changed)",
                    details={"schema_changes": rendered.schema_changes},
                    next_actions=["Re-run prepare_changes with pattern_params.allow_destructive_rebuild=true; the "
                                  "output table will be dropped and rebuilt from source over the reprocessing window."],
                )
            steps.insert(
                0,
                PlanStep(
                    step_id="drop_output",
                    action="athena.ddl",
                    description=f"Drop output table {ctx.contract.body.output.name} for rebuild",
                    params={"statements": [f"DROP TABLE IF EXISTS {sb.target_ddl}"]},
                    reversible=False,
                    on_failure="abort",
                    compensation="Data is rebuilt from source by the reprocess step; not restorable otherwise.",
                ),
            )
            rendered.reprocess_reason = "output table rebuilt after a breaking schema change"
        elif added:
            steps.append(
                PlanStep(
                    step_id="add_columns",
                    action="athena.add_columns",
                    description="Add new output columns in place",
                    params={"columns": [c.model_dump() for c in added]},
                    reversible=False,
                    compensation="Iceberg column additions are metadata-only; existing rows read NULL.",
                )
            )
        if ctx.previous_params and ctx.previous_params.get("transform_sql") != p.transform_sql:
            rendered.reprocess_reason = rendered.reprocess_reason or "transform_sql changed; existing rows were " \
                "produced by the old rule"
        if added and not rendered.reprocess_reason:
            rendered.reprocess_reason = "new columns are NULL for rows written before the change"
        return steps

    def reprocess_steps(self, ctx: RenderContext, p: AthenaIcebergParams, window: dict[str, str]) -> list[PlanStep]:
        return [
            PlanStep(
                step_id="reprocess",
                action="sfn.run_window",
                description=f"Re-run the flow over {window['start']} .. {window['end']}",
                preconditions=["schema steps succeeded"],
                params={"window_start": window["start"], "window_end": window["end"], "chunk_hours": 24},
                reversible=False,
                compensation="Re-running the previous rule over the same window restores prior values "
                "(rebuild_from_source); no snapshot rollback is attempted.",
            )
        ]

    # --- worker steps ---------------------------------------------------------------------------------
    def run_step(self, step: PlanStep, rt: Runtime, op_id: str) -> dict[str, Any]:
        n = _names(rt)
        session = rt.deployer or rt.reader
        if step.action == "athena.ddl":
            statements = step.params.get("statements")
            if statements is None:  # recovery: recreate from the current contract
                assert rt.contract is not None
                sb = SqlBuilder(rt.contract.body, AthenaIcebergParams.model_validate(rt.flow.pattern_params), n)
                statements = [sb.create_target()]
            results = []
            for sql in statements:
                r = athena_query(session, sql, workgroup=n.workgroup, database=n.database, timeout_s=300)
                if r["state"] != "SUCCEEDED":
                    raise PmcpError(ErrorCode.AWS_ERROR, f"DDL failed: {r['error']}", details={"query_id": r["query_id"]})
                results.append(r["query_id"])
            return {"query_ids": results}
        if step.action == "athena.add_columns":
            glue = session.client("glue", config=BOTO_CONFIG)
            table = rt.contract.body.output.name if rt.contract else rt.flow.outputs.get("OutputTable", "")
            existing = {c["Name"] for c in glue.get_table(DatabaseName=n.database, Name=table)["Table"][
                "StorageDescriptor"]["Columns"]}
            todo = [c for c in step.params["columns"] if c["name"] not in existing]
            if not todo:
                return {"skipped": "columns already present"}
            cols = ", ".join(f"{_bq(c['name'])} {ATHENA_TYPES[c['type']]}" for c in todo)
            r = athena_query(session, f"ALTER TABLE {_bq(n.database)}.{_bq(table)} ADD COLUMNS ({cols})",
                             workgroup=n.workgroup, database=n.database, timeout_s=300)
            if r["state"] != "SUCCEEDED":
                raise PmcpError(ErrorCode.AWS_ERROR, f"ALTER failed: {r['error']}")
            return {"added": [c["name"] for c in todo], "query_id": r["query_id"]}
        if step.action == "sfn.run_window":
            return self._run_windows(rt, session, step, op_id)
        if step.action == "scheduler.set_state":
            sched = session.client("scheduler", config=BOTO_CONFIG)
            cur = sched.get_schedule(Name=n.schedule)
            if cur["State"] == step.params["state"]:
                return {"skipped": "already " + cur["State"]}
            keep = {k: cur[k] for k in ("ScheduleExpression", "FlexibleTimeWindow", "Target",
                                        "ScheduleExpressionTimezone") if k in cur}
            sched.update_schedule(Name=n.schedule, State=step.params["state"], **keep)
            return {"state": step.params["state"]}
        return super().run_step(step, rt, op_id)

    def _run_windows(self, rt: Runtime, session: Any, step: PlanStep, op_id: str) -> dict[str, Any]:
        import time

        sfn = session.client("stepfunctions", config=BOTO_CONFIG)
        arn = rt.flow.outputs["StateMachineArn"]
        start = _parse(step.params["window_start"])
        end = _parse(step.params["window_end"])
        chunk = timedelta(hours=step.params.get("chunk_hours", 24))
        executions = []
        cur = start
        idx = 0
        while cur < end:
            nxt = min(cur + chunk, end)
            # Deterministic execution name: a resumed worker re-attaches instead of starting a duplicate run.
            name = f"{op_id.split('_', 1)[-1]}-{step.step_id}-{idx}"[:80]
            exec_arn = arn.replace(":stateMachine:", ":execution:") + ":" + name
            try:
                sfn.start_execution(stateMachineArn=arn, name=name,
                                    input=json.dumps({"window_start": iso(cur), "window_end": iso(nxt)}))
            except Exception as exc:  # ExecutionAlreadyExists = we started it before a crash
                if "ExecutionAlreadyExists" not in str(exc):
                    raise
            while True:
                d = sfn.describe_execution(executionArn=exec_arn)
                if d["status"] != "RUNNING":
                    break
                time.sleep(5)
            if d["status"] != "SUCCEEDED":
                raise PmcpError(
                    ErrorCode.AWS_ERROR,
                    f"reprocess window {iso(cur)}..{iso(nxt)} ended {d['status']}",
                    details={"execution": exec_arn, "error": d.get("error"), "cause": (d.get("cause") or "")[:500],
                             "cause_truncated": len(d.get("cause") or "") > 500,
                             "next_action": "get_run_failure with this flow_id and execution for full errors"},
                )
            executions.append({"execution": exec_arn, "window": [iso(cur), iso(nxt)]})
            cur = nxt
            idx += 1
        return {"executions": executions}

    # --- estimate -------------------------------------------------------------------------------------
    def estimate(self, body: DataContractBody, p: AthenaIcebergParams, region: str, prices: PriceBook) -> CostEstimate:
        assumptions = []
        runs_per_month = 30 * 24 * 60 / (_schedule_minutes(p.schedule_expression) or 60)
        if _schedule_minutes(p.schedule_expression) is None:
            assumptions.append("cron schedule: assumed hourly for cost")
        bytes_day = body.volume.bytes_per_day
        if bytes_day is None and body.volume.records_per_day:
            bytes_day = body.volume.records_per_day * 500
            assumptions.append("bytes/day estimated at 500 B/record")
        if bytes_day is None:
            bytes_day = 1024**3
            assumptions.append("no volume given: assumed 1 GB/day")
        window_bytes = bytes_day * p.window_minutes / 1440
        retention = body.volume.retention_days or body.reprocessing_window_days or 90
        target_bytes = bytes_day * retention * 0.3  # parquet+zstd vs raw json
        merge_read = target_bytes * 0.1 if body.mutation.key else 0  # MERGE reads matching target files
        scan_tb = (window_bytes + merge_read) * runs_per_month / 1024**4
        min_scan_tb = 10 * 1024**2 * runs_per_month / 1024**4
        athena = prices.get("athena_tb_scanned", region)
        sfn = prices.get("sfn_state_transition", region)
        s3 = prices.get("s3_gb_month", region)
        alarm_p = prices.get("cloudwatch_alarm_month", region)
        put = prices.get("s3_put_request", region)
        lines = [
            CostLine(item="Athena scans (window + MERGE read)", unit_price_usd=athena.usd, unit=athena.unit,
                     quantity=round(max(scan_tb, min_scan_tb), 6),
                     monthly_usd=round(max(scan_tb, min_scan_tb) * athena.usd, 4),
                     price_source=athena.source, price_as_of=athena.as_of),  # type: ignore[arg-type]
            CostLine(item="Step Functions transitions", unit_price_usd=sfn.usd, unit=sfn.unit,
                     quantity=runs_per_month * 6, monthly_usd=round(runs_per_month * 6 * sfn.usd, 4),
                     price_source=sfn.source, price_as_of=sfn.as_of),  # type: ignore[arg-type]
            CostLine(item="S3 storage for output (steady state)", unit_price_usd=s3.usd, unit=s3.unit,
                     quantity=round(target_bytes / 1024**3, 3), monthly_usd=round(target_bytes / 1024**3 * s3.usd, 4),
                     price_source=s3.source, price_as_of=s3.as_of),  # type: ignore[arg-type]
            CostLine(item="S3 PUTs (Iceberg data+metadata files)", unit_price_usd=put.usd, unit=put.unit,
                     quantity=runs_per_month * 20, monthly_usd=round(runs_per_month * 20 * put.usd, 4),
                     price_source=put.source, price_as_of=put.as_of),  # type: ignore[arg-type]
            CostLine(item="CloudWatch alarms (2)", unit_price_usd=alarm_p.usd, unit=alarm_p.unit, quantity=2,
                     monthly_usd=round(2 * alarm_p.usd, 4), price_source=alarm_p.source,  # type: ignore[arg-type]
                     price_as_of=alarm_p.as_of),
        ]
        assumptions += [
            f"{runs_per_month:.0f} runs/month, each scanning a {p.window_minutes}-minute window",
            f"retention {retention} days; output ~30% of raw size",
            "Athena bills at least 10 MB per query",
        ]
        return CostEstimate(
            monthly_usd=round(sum(line.monthly_usd for line in lines), 2),
            lines=lines,
            assumptions=assumptions,
            confidence="medium" if body.volume.bytes_per_day or body.volume.records_per_day else "low",
        )

    def reprocess_cost(self, body: DataContractBody, p: AthenaIcebergParams, window: dict[str, str],
                       region: str, prices: PriceBook) -> float:
        hours = (_parse(window["end"]) - _parse(window["start"])).total_seconds() / 3600
        bytes_day = body.volume.bytes_per_day or (body.volume.records_per_day or 2_000_000) * 500
        tb = bytes_day * hours / 24 / 1024**4
        return round(max(tb, 10 * 1024**2 / 1024**4) * prices.get("athena_tb_scanned", region).usd, 4)

    # --- verify ---------------------------------------------------------------------------------------
    def _default_window(self, rt: Runtime) -> dict[str, str] | None:
        sfn = rt.client("stepfunctions")
        arn = rt.flow.outputs.get("StateMachineArn")
        if not arn:
            return None
        for ex in sfn.list_executions(stateMachineArn=arn, statusFilter="SUCCEEDED", maxResults=5)["executions"]:
            inp = json.loads(sfn.describe_execution(executionArn=ex["executionArn"]).get("input") or "{}")
            end = inp.get("window_end")
            if not end or "<aws" in end:
                continue
            start = inp.get("window_start") or iso(_parse(end) - timedelta(
                minutes=int(rt.flow.pattern_params.get("window_minutes", 120))))
            return {"start": start, "end": end, "source": ex["executionArn"]}
        return None

    def verify(self, rt: Runtime, window: dict[str, str] | None) -> tuple[dict[str, str], list[Check]]:
        assert rt.contract is not None
        body = rt.contract.body
        p = AthenaIcebergParams.model_validate(rt.flow.pattern_params)
        n = _names(rt)
        sb = SqlBuilder(body, p, n)
        checks: list[Check] = []
        win = window or self._default_window(rt)
        if not win:
            return {}, [
                unverified("window", "row_count", "Pick a verification window",
                           "no successful run found and no window given; nothing to compare yet")
            ]
        start_expr, end_expr = _ts_literal(win["start"]), _ts_literal(win["end"])
        session = rt.reader
        glue = session.client("glue", config=BOTO_CONFIG)

        def q(sql: str) -> dict[str, Any]:
            return athena_query(session, sql, workgroup=n.workgroup, database=n.database, timeout_s=240)

        # existence + schema
        try:
            table = glue.get_table(DatabaseName=n.database, Name=body.output.name)["Table"]
            actual = {c["Name"]: c["Type"] for c in table["StorageDescriptor"]["Columns"]}
        except Exception as exc:
            checks.append(Check(check_id="exists", category="existence", status="failed",
                                description="Output table exists in the Glue Data Catalog",
                                observed=str(exc)[:200]))
            return win, checks
        cnt = q(f"SELECT count(*) AS n FROM {sb.target}")
        total = int(cnt["rows"][0]["n"]) if cnt["state"] == "SUCCEEDED" else None
        checks.append(Check(
            check_id="exists", category="existence",
            status="passed" if total else ("failed" if total == 0 else "error"),
            description="Output table exists and holds rows", expected="> 0 rows", observed=total,
            evidence=[f"athena:{cnt['query_id']}"], reason=cnt.get("error")))
        expected_types = {c.name: ATHENA_TYPES[c.type] for c in body.output.columns}
        mism = {k: {"expected": v, "actual": actual.get(k)} for k, v in expected_types.items()
                if not _type_match(v, actual.get(k))}
        checks.append(Check(
            check_id="schema", category="schema", status="failed" if mism else "passed",
            description="Output columns and types match the contract", expected=expected_types, observed=actual,
            reason=None if not mism else f"mismatched: {mism}"))

        m = body.mutation
        if m.key:
            keys = ", ".join(_q(k) for k in m.key)
            r = q(f"SELECT count(*) AS dup FROM (SELECT {keys} FROM {sb.target} GROUP BY {keys} HAVING count(*) > 1)")
            dup = int(r["rows"][0]["dup"]) if r["state"] == "SUCCEEDED" else None
            checks.append(Check(check_id="keys_unique", category="keys",
                                status="passed" if dup == 0 else ("failed" if dup else "error"),
                                description=f"No duplicate keys on {m.key}", expected=0, observed=dup,
                                evidence=[f"athena:{r['query_id']}"], reason=r.get("error")))
            # Expected state for the window, derived by running the contract's transform over source.
            on = " AND ".join(f"e.{_q(k)} = t.{_q(k)}" for k in m.key)
            ordering = _q(m.ordering_column or m.key[0])
            dedup = sb._deduped(start_expr, end_expr)
            del_filter = f"coalesce(e.{DELETE_FLAG}, false) = false" if m.mode == "upsert_with_deletes" else "true"
            sql = (
                f"WITH e AS ({dedup}) SELECT "
                f"(SELECT count(*) FROM e WHERE {del_filter}) AS expected_rows, "
                f"(SELECT count(*) FROM e JOIN {sb.target} t ON {on} WHERE {del_filter}) AS present, "
                f"(SELECT count(*) FROM e JOIN {sb.target} t ON {on} WHERE {del_filter} AND t.{ordering} < e.{ordering})"
                " AS stale_versions"
            )
            if m.mode == "upsert_with_deletes":
                sql += (f", (SELECT count(*) FROM e JOIN {sb.target} t ON {on} WHERE e.{DELETE_FLAG} AND "
                        f"t.{ordering} <= e.{ordering}) AS deletes_not_applied")
            r = q(sql)
            if r["state"] == "SUCCEEDED":
                row = {k: int(v) for k, v in r["rows"][0].items()}
                checks.append(Check(
                    check_id="row_count", category="row_count",
                    status="passed" if row["present"] == row["expected_rows"] else "failed",
                    description="Every key the transform produces for the window is in the output",
                    window=win, expected=row["expected_rows"], observed=row["present"],
                    evidence=[f"athena:{r['query_id']}"]))
                mut_ok = row["stale_versions"] == 0 and row.get("deletes_not_applied", 0) == 0
                checks.append(Check(
                    check_id="mutations", category="mutations", status="passed" if mut_ok else "failed",
                    description="Latest version per key applied; deletes applied" if m.mode == "upsert_with_deletes"
                    else "Latest version per key applied",
                    window=win, expected={"stale_versions": 0, "deletes_not_applied": 0},
                    observed=row, evidence=[f"athena:{r['query_id']}"]))
            else:
                checks.append(Check(check_id="row_count", category="row_count", status="error", window=win,
                                    description="Expected-vs-actual for the window", reason=r.get("error"),
                                    evidence=[f"athena:{r['query_id']}"]))
        else:
            tcol = _q(body.freshness.measured_on or "")
            r = q(
                f"SELECT (SELECT count(*) FROM ({sb.transform(start_expr, end_expr)}) s) AS expected_rows, "
                f"(SELECT count(*) FROM {sb.target} WHERE {tcol} >= {start_expr} AND {tcol} < {end_expr}) AS present"
            )
            if r["state"] == "SUCCEEDED":
                row = {k: int(v) for k, v in r["rows"][0].items()}
                checks.append(Check(check_id="row_count", category="row_count",
                                    status="passed" if row["present"] == row["expected_rows"] else "failed",
                                    description="Output rows in window equal transform output for the window",
                                    window=win, expected=row["expected_rows"], observed=row["present"],
                                    evidence=[f"athena:{r['query_id']}"]))
            checks.append(unverified("mutations", "mutations", "Updates/deletes", "contract is append-only"))

        # freshness and progress
        fcol = body.freshness.measured_on
        if fcol:
            src_time = next((s.event_time_column for s in body.sources if s.event_time_column), None)
            r = q(f"SELECT CAST(max({_q(fcol)}) AS varchar) AS out_max FROM {sb.target}")
            out_max = r["rows"][0]["out_max"] if r["state"] == "SUCCEEDED" else None
            src_max = None
            if src_time:
                # Same source view as the pipeline (quarantined files excluded), or one bad file breaks the check.
                r2 = q(f"SELECT CAST(max({_q(src_time)}) AS varchar) AS src_max FROM {sb.source_ref()}")
                src_max = r2["rows"][0]["src_max"] if r2["state"] == "SUCCEEDED" else None
            last_ok = self._last_success(rt)
            lag_s = (datetime.now(UTC) - last_ok).total_seconds() if last_ok else None
            checks.append(Check(
                check_id="freshness", category="freshness",
                status="unverified" if lag_s is None else ("passed" if lag_s <= body.freshness.max_lag_seconds
                                                           else "failed"),
                description="Last successful run finished within freshness.max_lag_seconds",
                expected=f"<= {body.freshness.max_lag_seconds}s", observed=None if lag_s is None else round(lag_s),
                reason=None if lag_s is not None else "no successful run yet"))
            if src_max and out_max:
                gap = (_parse_loose(src_max) - _parse_loose(out_max)).total_seconds()
                checks.append(Check(
                    check_id="progress", category="progress",
                    status="passed" if gap <= body.freshness.max_lag_seconds else "failed",
                    description="Newest output event time vs newest source event time",
                    expected=f"gap <= {body.freshness.max_lag_seconds}s", observed={"source_max": src_max,
                                                                                    "output_max": out_max,
                                                                                    "gap_seconds": round(gap)}))
            else:
                checks.append(unverified("progress", "progress", "Source vs output progress",
                                         "source event_time_column unknown or tables empty"))
        else:
            checks.append(unverified("freshness", "freshness", "Freshness", "contract has no freshness.measured_on"))

        checks.extend(self._quality_checks(body, sb, q))
        checks.append(self._consumer_check(rt, n, sb))
        return win, checks

    def _last_success(self, rt: Runtime) -> datetime | None:
        arn = rt.flow.outputs.get("StateMachineArn")
        if not arn:
            return None
        ex = rt.client("stepfunctions").list_executions(stateMachineArn=arn, statusFilter="SUCCEEDED",
                                                         maxResults=1)["executions"]
        return ex[0]["stopDate"].astimezone(UTC) if ex and ex[0].get("stopDate") else None

    def _quality_checks(self, body: DataContractBody, sb: SqlBuilder, q: Any) -> list[Check]:
        out = []
        for i, rule in enumerate(body.quality):
            col = _q(rule.column) if rule.column else None
            if rule.kind == "not_null":
                sql = f"SELECT count(*) AS violations FROM {sb.target} WHERE {col} IS NULL"
            elif rule.kind == "unique":
                sql = (f"SELECT count(*) AS violations FROM (SELECT {col} FROM {sb.target} GROUP BY {col} "
                       "HAVING count(*) > 1)")
            elif rule.kind == "accepted_values":
                vals = ", ".join("'" + v.replace("'", "''") + "'" for v in rule.values or [])
                sql = f"SELECT count(*) AS violations FROM {sb.target} WHERE {col} NOT IN ({vals})"
            elif rule.kind == "range":
                conds = []
                if rule.min is not None:
                    conds.append(f"{col} < {rule.min}")
                if rule.max is not None:
                    conds.append(f"{col} > {rule.max}")
                sql = f"SELECT count(*) AS violations FROM {sb.target} WHERE {' OR '.join(conds) or 'false'}"
            elif rule.kind == "row_count_between":
                sql = (f"SELECT CASE WHEN count(*) BETWEEN {rule.min or 0} AND {rule.max or 9e18} THEN 0 ELSE 1 END "
                       f"AS violations FROM {sb.target}")
            else:
                if not SqlAnalyzer.is_read_only_query(rule.sql):
                    out.append(Check(check_id=f"quality_{i}", category="quality", status="error",
                                     description="custom_sql", reason="custom_sql is not read-only; not executed"))
                    continue
                sql = (rule.sql or "").replace("{{output}}", sb.target)
            r = q(sql)
            if r["state"] != "SUCCEEDED":
                out.append(Check(check_id=f"quality_{i}", category="quality", status="error",
                                 description=f"{rule.kind} {rule.column or ''}".strip(), reason=r.get("error"),
                                 evidence=[f"athena:{r['query_id']}"]))
                continue
            v = int(float(r["rows"][0].get("violations") or 0))
            status = "passed" if v == 0 else ("failed" if rule.severity == "error" else "passed")
            out.append(Check(check_id=f"quality_{i}", category="quality", status=status,  # type: ignore[arg-type]
                             description=f"{rule.kind} {rule.column or ''}".strip(), expected=0, observed=v,
                             evidence=[f"athena:{r['query_id']}"],
                             reason="warn-level rule" if v and rule.severity == "warn" else None))
        return out

    def _consumer_check(self, rt: Runtime, n: Names, sb: SqlBuilder) -> Check:
        role_arn = rt.flow.outputs.get("ConsumerRoleArn")
        if not role_arn:
            return unverified("consumer_read", "consumer_read", "Read as the consumer role", "no consumer role output")
        try:
            sess = rt.consumer(role_arn)
            r = athena_query(sess, f"SELECT * FROM {sb.target} LIMIT 5", workgroup=n.consumer_workgroup,
                             database=n.database, timeout_s=120)
        except PmcpError as exc:
            return Check(check_id="consumer_read", category="consumer_read", status="failed",
                         description=f"SELECT as {role_arn}", reason=exc.message)
        ok = r["state"] == "SUCCEEDED" and len(r["rows"]) > 0
        return Check(check_id="consumer_read", category="consumer_read", status="passed" if ok else "failed",
                     description=f"SELECT ... LIMIT 5 as consumer role {role_arn} in {n.consumer_workgroup}",
                     expected=">= 1 row", observed=len(r["rows"]), evidence=[f"athena:{r['query_id']}"],
                     reason=r.get("error"))

    # --- observe / diagnose / recover -----------------------------------------------------------------
    def observe(self, rt: Runtime) -> dict[str, Any]:
        n = _names(rt)
        out: dict[str, Any] = {}
        arn = rt.flow.outputs.get("StateMachineArn")
        if arn:
            sfn = rt.client("stepfunctions")
            ex = sfn.list_executions(stateMachineArn=arn, maxResults=10)["executions"]
            out["recent_runs"] = [allow(e, STEP_FUNCTIONS_EXECUTION_FIELDS) for e in ex]
            counts: dict[str, int] = {}
            for e in ex:
                counts[e["status"]] = counts.get(e["status"], 0) + 1
            out["recent_run_counts"] = counts
        try:
            sched = rt.client("scheduler").get_schedule(Name=n.schedule)
            out["schedule"] = {"state": sched["State"], "expression": sched["ScheduleExpression"]}
        except Exception as exc:
            out["schedule"] = {"error": str(exc)[:200]}
        alarms = rt.client("cloudwatch").describe_alarms(AlarmNamePrefix=f"pmcp-{n.short}-")["MetricAlarms"]
        out["alarms"] = [{"name": a["AlarmName"], "state": a["StateValue"],
                          "since": str(a.get("StateUpdatedTimestamp"))} for a in alarms]
        last = self._last_success(rt)
        out["last_successful_run_end"] = iso(last) if last else None
        out["seconds_since_last_success"] = round((datetime.now(UTC) - last).total_seconds()) if last else None
        return out

    def diagnose(self, rt: Runtime, window: dict[str, str]) -> DiagnosisParts:
        facts: list[dict[str, Any]] = []
        hyps: list[Hypothesis] = []
        further: list[str] = []
        affected_windows: list[dict[str, str]] = []
        arn = rt.flow.outputs.get("StateMachineArn")
        start, end = _parse(window["start"]), _parse(window["end"])
        errors: list[str] = []
        if arn:
            sfn = rt.client("stepfunctions")
            for status in ("FAILED", "TIMED_OUT", "ABORTED"):
                for e in sfn.list_executions(stateMachineArn=arn, statusFilter=status, maxResults=20)["executions"]:
                    if not (start <= e["startDate"].astimezone(UTC) <= end):
                        continue
                    d = sfn.describe_execution(executionArn=e["executionArn"])
                    inp = json.loads(d.get("input") or "{}")
                    w_end = inp.get("window_end")
                    if w_end and "<aws" not in w_end:
                        w_start = inp.get("window_start") or iso(_parse(w_end) - timedelta(
                            minutes=int(rt.flow.pattern_params.get("window_minutes", 120))))
                        affected_windows.append({"start": w_start, "end": w_end})
                    cause = _failure_cause(sfn, e["executionArn"])
                    errors.append(cause)
                    facts.append({"fact_id": f"F{len(facts) + 1}", "kind": "failed_run",
                                  "execution": e["executionArn"], "status": status,
                                  "started": iso(e["startDate"]), "error": (d.get("error") or "")[:120],
                                  "cause_excerpt": cause[:600], "cause_truncated": len(cause) > 600,
                                  "details_tool": "get_run_failure"})
        obs = self.observe(rt)
        for a in obs.get("alarms", []):
            if a["state"] == "ALARM":
                facts.append({"fact_id": f"F{len(facts) + 1}", "kind": "alarm", **a})
        sched = obs.get("schedule", {})
        if sched.get("state") == "DISABLED":
            facts.append({"fact_id": f"F{len(facts) + 1}", "kind": "schedule_disabled", **sched})
        try:
            stack = rt.client("cloudformation").describe_stacks(StackName=rt.flow.stack_name)["Stacks"][0]
            facts.append({"fact_id": f"F{len(facts) + 1}", "kind": "stack_status",
                          "status": stack["StackStatus"]})
        except Exception as exc:
            facts.append({"fact_id": f"F{len(facts) + 1}", "kind": "stack_missing", "error": str(exc)[:200]})

        def ids(pred) -> list[str]:
            return [f["fact_id"] for f in facts if pred(f)]

        text = "\n".join(errors).lower()
        rules = [
            (r"hive_bad_data|malformed|json|cannot parse|unexpected character|row is not a valid",
             "A source file contains records that cannot be parsed with the declared source schema",
             ["quarantine_and_rerun"],
             "Run diagnose_failure with deep_probe=true to list the offending files"),
            (r"access ?denied|not authorized|insufficient (lake formation )?permission|forbidden",
             "A permission needed by the run is missing (IAM policy, bucket policy, KMS key or Lake Formation)",
             ["restore_template", "rerun_window"],
             "Compare the run role's current policy with the deployed template (CloudFormation drift)"),
            (r"table_not_found|does not exist|entity ?not ?found|schema_not_found",
             "The Glue database or a table was deleted or renamed outside pmcp",
             ["recreate_tables", "restore_template"],
             "Check the Glue catalog for the flow database and tables"),
            (r"column_not_found|cannot be resolved|type_mismatch|mismatched input|invalid_cast|cannot cast",
             "The source schema changed and no longer matches the transform/raw table definition",
             ["change_plan"],
             "Run profile_source on the source prefix and compare with the contract's source columns"),
            (r"exhausted resources|exceeded_time_limit|bytes scanned limit|query timeout|too many open",
             "The run exceeded Athena limits (scan cutoff, time limit, resources) for the window size",
             ["rerun_window_chunked", "change_plan"],
             "Check DataScannedInBytes of the failed query against bytes_scanned_cutoff_gb"),
            (r"iceberg_commit_error|commit failed|concurrent",
             "Two writers committed to the Iceberg table at the same time",
             ["rerun_window"],
             "List overlapping executions in the window"),
        ]
        for pattern, statement, recovery, check in rules:
            if errors and re.search(pattern, text):
                hyps.append(Hypothesis(hypothesis_id=f"H{len(hyps) + 1}", statement=statement, likelihood="high",
                                       supporting_evidence=ids(lambda f: f["kind"] == "failed_run"),
                                       suggested_recovery=recovery))
                further.append(check)
        if not errors:
            if sched.get("state") == "DISABLED":
                hyps.append(Hypothesis(hypothesis_id=f"H{len(hyps) + 1}", likelihood="high",
                                       statement="The schedule is disabled, so no runs happen",
                                       supporting_evidence=ids(lambda f: f["kind"] == "schedule_disabled"),
                                       suggested_recovery=["enable_schedule"]))
            elif any(f["kind"] == "alarm" for f in facts):
                hyps.append(Hypothesis(hypothesis_id=f"H{len(hyps) + 1}", likelihood="medium",
                                       statement="Runs are not completing on time but none failed in the window",
                                       supporting_evidence=ids(lambda f: f["kind"] == "alarm"),
                                       suggested_recovery=["rerun_window"]))
                further.append("Widen the diagnosis window; check scheduler target errors in CloudTrail")
        if errors and not hyps:
            hyps.append(Hypothesis(hypothesis_id="H1", likelihood="low",
                                   statement="Unclassified run failure; see cause excerpts",
                                   supporting_evidence=ids(lambda f: f["kind"] == "failed_run"),
                                   suggested_recovery=["rerun_window"]))
        if not facts:
            further.append("No failures, alarms or drift observed in the window")
        return DiagnosisParts(facts=facts, hypotheses=hyps, further_checks=further,
                              affected={"windows": _merge_windows(affected_windows)})

    def probe(self, rt: Runtime, window: dict[str, str]) -> dict[str, Any]:
        p = AthenaIcebergParams.model_validate(rt.flow.pattern_params)
        n = _names(rt)
        if p.source_format != "jsonl":
            return {"probes": [], "note": "file-level parse probe is implemented for jsonl sources"}
        sql = (
            f'SELECT "$path" AS path, count(*) AS bad_lines FROM {n.database}.{n.lines_table} '
            "WHERE length(trim(line)) > 0 AND try(json_parse(line)) IS NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 100"
        )
        r = athena_query(rt.reader, sql, workgroup=n.workgroup, database=n.database, timeout_s=600)
        typed = []
        if r["state"] == "SUCCEEDED":
            # Lines that are valid JSON but whose fields do not cast to the declared source types.
            casts = " OR ".join(
                f"(json_extract_scalar(line, '$.{c.name}') IS NOT NULL AND "
                f"try_cast(json_extract_scalar(line, '$.{c.name}') AS {SQL_CAST_TYPES[c.type]}) IS NULL)"
                for c in p.source_columns
                if c.type not in ("string", "timestamp")
            )
            if casts:
                r2 = athena_query(
                    rt.reader,
                    f'SELECT "$path" AS path, count(*) AS bad_lines FROM {n.database}.{n.lines_table} '
                    f"WHERE try(json_parse(line)) IS NOT NULL AND ({casts}) GROUP BY 1 ORDER BY 2 DESC LIMIT 100",
                    workgroup=n.workgroup, database=n.database, timeout_s=600,
                )
                typed = r2["rows"] if r2["state"] == "SUCCEEDED" else []
        return {
            "probes": [{"name": "unparsable_lines_by_file", "query_id": r["query_id"], "state": r["state"],
                        "rows": r["rows"], "error": r.get("error")},
                       {"name": "type_mismatch_lines_by_file", "rows": typed}],
            "bad_files": sorted({row["path"] for row in r["rows"]} | {row["path"] for row in typed}),
        }

    def recovery_options(self, rt: Runtime, incident: Any) -> list[RecoveryOption]:
        windows = incident.affected.get("windows") or [incident.window]
        bad_files = incident.affected.get("bad_files", [])
        rerun = [
            PlanStep(step_id=f"rerun_{i}", action="sfn.run_window",
                     description=f"Re-run window {w['start']} .. {w['end']}",
                     params={"window_start": w["start"], "window_end": w["end"], "chunk_hours": 24},
                     reversible=False, compensation="Reruns are idempotent (MERGE / window replace)")
            for i, w in enumerate(windows)
        ]
        verify = [PlanStep(step_id="verify", action="verify", description="Verify the affected window(s)",
                           params={"window": {"start": windows[0]["start"], "end": windows[-1]["end"]}})]
        opts = [
            RecoveryOption("rerun_window", "Re-run the failed window(s) and verify", rerun + verify),
            RecoveryOption("rerun_window_chunked", "Re-run in 1-hour chunks to stay under scan/time limits",
                           [s.model_copy(update={"params": {**s.params, "chunk_hours": 1}}) for s in rerun] + verify),
            RecoveryOption(
                "restore_template", "Re-deploy the last applied template (repairs drift), then re-run",
                rerun + verify, needs_template_restore=True,
                notes=["Restores infrastructure configuration only; data written by failed runs is not touched.",
                       "CloudFormation re-applies only resources whose template differs from the deployed one; an "
                       "out-of-band edit (drift) to an unchanged resource is NOT reverted by this option."]),
            RecoveryOption(
                "recreate_tables", "Re-create missing tables (IF NOT EXISTS), then re-run",
                [PlanStep(step_id="create_tables", action="athena.ddl", description="Re-create tables",
                          params={"statements": None})] + rerun + verify),
            RecoveryOption("enable_schedule", "Enable the schedule",
                           [PlanStep(step_id="enable", action="scheduler.set_state", description="Enable schedule",
                                     params={"state": "ENABLED"})]),
        ]
        if bad_files:
            opts.insert(0, RecoveryOption(
                "quarantine_and_rerun",
                f"Exclude {len(bad_files)} unparsable file(s) from the flow (template change), re-run, verify",
                rerun + verify,
                param_updates={"quarantined_paths": sorted(set(rt.flow.pattern_params.get("quarantined_paths", [])
                                                               + bad_files))},
                data_rollback="not_applicable",
                notes=["Source files are not modified; the flow's SQL skips the listed files until a change plan "
                       "removes them from quarantined_paths.",
                       "Records in quarantined files are missing from the output until then."],
            ))
        return opts


def _failure_cause(sfn: Any, execution_arn: str) -> str:
    events = sfn.get_execution_history(executionArn=execution_arn, reverseOrder=True, maxResults=25)["events"]
    for ev in events:
        for key in ("taskFailedEventDetails", "executionFailedEventDetails"):
            det = ev.get(key)
            if det and (det.get("cause") or det.get("error")):
                return f"{det.get('error', '')}: {det.get('cause', '')}"
    return ""


def _merge_windows(ws: list[dict[str, str]]) -> list[dict[str, str]]:
    if not ws:
        return []
    ws = sorted(ws, key=lambda w: w["start"])
    out = [dict(ws[0])]
    for w in ws[1:]:
        if w["start"] <= out[-1]["end"]:
            out[-1]["end"] = max(out[-1]["end"], w["end"])
        else:
            out.append(dict(w))
    return out


def _parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(UTC)


def _parse_loose(ts: str) -> datetime:
    ts = ts.strip().replace(" UTC", "+00:00").replace(" ", "T", 1)
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def _type_match(expected: str, actual: str | None) -> bool:
    if actual is None:
        return False
    a = actual.lower()
    e = expected.lower()
    return a == e or (e == "timestamp" and a.startswith("timestamp")) or (e == "string" and a in ("string", "varchar"))


def _names(rt: Runtime) -> Names:
    return Names(short(rt.flow.flow_id), rt.flow.account_id, rt.flow.region)


def validate_transform_code_or_sql(sql: str) -> list[str]:
    """Public alias used by tests and docs."""
    return validate_transform_sql(sql)
