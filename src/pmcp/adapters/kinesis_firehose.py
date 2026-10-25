"""Pattern kinesis_firehose_s3: Kinesis Data Streams → Firehose (Lambda transform) → S3, with a Glue table
that consumers query through Athena.

Delivery is at-least-once and append-only. The transform Lambda publishes Ok/Dropped/ProcessingFailed
counts as CloudWatch metrics (EMF) so verification can reconcile records in vs. records out without
reading the stream. Records the transform rejects land under errors/ with their raw payload and can be
replayed into the stream by a recovery plan.
"""

from __future__ import annotations

import base64
import json
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from pydantic import Field

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
    template,
    unverified,
)
from pmcp.adapters.codegen import (
    render_firehose_handler,
    validate_transform_code,
    zip_single,
)
from pmcp.adapters.common import (
    consumer_athena_check,
    consumer_s3_table_statements,
    default_window,
    flow_short,
    glue_columns,
    metric_sum,
    schema_check,
    workgroup,
)
from pmcp.aws.client import BOTO_CONFIG
from pmcp.catalog.pricing import PriceBook
from pmcp.domain.base import Model
from pmcp.domain.contract import DataContractBody
from pmcp.domain.records import Check, CostEstimate, CostLine, Hypothesis, PlanStep
from pmcp.errors import ErrorCode, PmcpError
from pmcp.ids import iso, sha256_hex


class FirehoseParams(Model):
    stream: str = Field(
        "create", description="'create' for a new pmcp stream, or the name of an existing Kinesis stream to read."
    )
    stream_mode: Literal["ON_DEMAND", "PROVISIONED"] = "ON_DEMAND"
    shards: int = Field(1, ge=1, le=50, description="PROVISIONED only.")
    retention_hours: int = Field(24, ge=24, le=8760)
    buffer_seconds: int = Field(60, ge=0, le=900)
    buffer_mb: int = Field(5, ge=1, le=128)
    output_format: Literal["json_gzip", "parquet"] = "json_gzip"
    transform_code: str | None = Field(
        None,
        description="Python defining `transform(record: dict) -> dict | None` (None drops the record). Only "
        "pure stdlib imports are allowed. Omit for pass-through.",
    )
    alert_email: str | None = None


def _n(s: str) -> dict[str, str]:
    return {
        "database": f"pmcp_{s}",
        "table": "events",
        "stream": f"pmcp-{s}-in",
        "delivery": f"pmcp-{s}-delivery",
        "function": f"pmcp-{s}-transform",
        "workgroup": f"pmcp-{s}",
        "consumer_workgroup": f"pmcp-{s}-consumer",
    }


class KinesisFirehoseAdapter(Adapter):
    pattern_id = "kinesis_firehose_s3"
    Params = FirehoseParams

    def check_supported(self, p: FirehoseParams, contract: DataContractBody) -> None:
        problems = []
        if contract.mutation.mode != "append":
            problems.append("kinesis_firehose_s3 delivers append-only objects; contract asks for "
                            f"{contract.mutation.mode}")
        if p.transform_code:
            problems += validate_transform_code(p.transform_code)
        if p.output_format == "parquet" and p.buffer_mb < 64:
            problems.append("parquet conversion requires buffer_mb >= 64")
        if p.buffer_seconds > contract.freshness.max_lag_seconds:
            problems.append(f"buffer_seconds {p.buffer_seconds} exceeds freshness {contract.freshness.max_lag_seconds}s")
        if p.stream != "create" and not any(s.uri == f"kinesis:{p.stream}" for s in contract.sources):
            problems.append(f"existing stream '{p.stream}' must be a contract source (kinesis:{p.stream})")
        if problems:
            raise PmcpError(ErrorCode.UNSUPPORTED_CONFIGURATION, "pattern_params cannot be deployed as given",
                            details={"problems": problems})

    def render(self, ctx: RenderContext, p: FirehoseParams) -> Rendered:
        body = ctx.contract.body
        n = _n(ctx.short)
        acct, region = ctx.account_id, ctx.region
        data_arn = f"arn:aws:s3:::{ctx.data_bucket}"
        res: dict[str, Any] = {}
        res.update(data_bucket(ctx))
        res.update(alerts_topic(ctx, p.alert_email))
        if p.stream == "create":
            stream_props: dict[str, Any] = {
                "Name": n["stream"],
                "RetentionPeriodHours": p.retention_hours,
                "StreamModeDetails": {"StreamMode": p.stream_mode},
                "StreamEncryption": {"EncryptionType": "KMS", "KeyId": "alias/aws/kinesis"},
                "Tags": [{"Key": k, "Value": v} for k, v in ctx.tags.items()],
            }
            if p.stream_mode == "PROVISIONED":
                stream_props["ShardCount"] = p.shards
            res["Stream"] = {"Type": "AWS::Kinesis::Stream", "Properties": stream_props}
            stream_arn: Any = {"Fn::GetAtt": ["Stream", "Arn"]}
            stream_name = n["stream"]
        else:
            stream_name = p.stream
            stream_arn = f"arn:aws:kinesis:{region}:{acct}:stream/{p.stream}"
        res["Database"] = {"Type": "AWS::Glue::Database",
                           "Properties": {"CatalogId": acct, "DatabaseInput": {"Name": n["database"]}}}
        if p.output_format == "parquet":
            sd = {"InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                  "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                  "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"}}
        else:
            sd = {"InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                  "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                  "SerdeInfo": {"SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe",
                                "Parameters": {"ignore.malformed.json": "TRUE"}}}
        res["OutputTable"] = {
            "Type": "AWS::Glue::Table",
            "Properties": {
                "CatalogId": acct,
                "DatabaseName": {"Ref": "Database"},
                "TableInput": {
                    "Name": n["table"],
                    "TableType": "EXTERNAL_TABLE",
                    "Parameters": {
                        "classification": "parquet" if p.output_format == "parquet" else "json",
                        "projection.enabled": "true",
                        "projection.dt.type": "date",
                        "projection.dt.format": "yyyy-MM-dd",
                        "projection.dt.range": "2024-01-01,NOW",
                        "projection.dt.interval": "1",
                        "projection.dt.interval.unit": "DAYS",
                        "storage.location.template": f"s3://{ctx.data_bucket}/data/dt=${{dt}}/",
                    },
                    "PartitionKeys": [{"Name": "dt", "Type": "string"}],
                    "StorageDescriptor": {"Columns": glue_columns(body), "Location": f"s3://{ctx.data_bucket}/data/",
                                          **sd},
                },
            },
        }
        code = render_firehose_handler(p.transform_code, ctx.flow_id)
        zip_bytes = zip_single("handler.py", code)
        zip_key = f"pmcp/artifacts/{sha256_hex(zip_bytes)}.zip"
        res.update(iam_role(ctx, "TransformRole", ctx.name("transform"), "lambda.amazonaws.com", [
            {"Effect": "Allow", "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
             "Resource": f"arn:aws:logs:{region}:{acct}:log-group:/aws/lambda/{n['function']}*"},
        ]))
        res["TransformLogs"] = {"Type": "AWS::Logs::LogGroup",
                                "Properties": {"LogGroupName": f"/aws/lambda/{n['function']}", "RetentionInDays": 14}}
        res["TransformFunction"] = {
            "Type": "AWS::Lambda::Function",
            "DependsOn": ["TransformLogs"],
            "Properties": {
                "FunctionName": n["function"],
                "Runtime": "python3.13",
                "Architectures": ["arm64"],
                "Handler": "handler.handler",
                "Timeout": 120,
                "MemorySize": 256,
                "Role": {"Fn::GetAtt": ["TransformRole", "Arn"]},
                "Code": {"S3Bucket": f"pmcp-artifacts-{acct}-{region}", "S3Key": zip_key},
                "Tags": [{"Key": k, "Value": v} for k, v in ctx.tags.items()],
            },
        }
        fh_statements: list[dict[str, Any]] = [
            {"Effect": "Allow", "Action": ["s3:AbortMultipartUpload", "s3:GetBucketLocation", "s3:GetObject",
                                           "s3:ListBucket", "s3:ListBucketMultipartUploads", "s3:PutObject"],
             "Resource": [data_arn, f"{data_arn}/*"]},
            {"Effect": "Allow", "Action": ["kinesis:DescribeStream", "kinesis:DescribeStreamSummary",
                                           "kinesis:GetShardIterator", "kinesis:GetRecords", "kinesis:ListShards"],
             "Resource": stream_arn},
            {"Effect": "Allow", "Action": ["kms:Decrypt"], "Resource": "*",
             "Condition": {"StringEquals": {"kms:ViaService": f"kinesis.{region}.amazonaws.com"}}},
            {"Effect": "Allow", "Action": ["lambda:InvokeFunction", "lambda:GetFunctionConfiguration"],
             "Resource": {"Fn::Sub": "${TransformFunction.Arn}*"}},
            {"Effect": "Allow", "Action": ["logs:PutLogEvents", "logs:CreateLogStream"],
             "Resource": f"arn:aws:logs:{region}:{acct}:log-group:/aws/kinesisfirehose/{n['delivery']}*"},
        ]
        if p.output_format == "parquet":
            fh_statements.append({"Effect": "Allow", "Action": ["glue:GetTable", "glue:GetTableVersion",
                                                                "glue:GetTableVersions"],
                                  "Resource": [f"arn:aws:glue:{region}:{acct}:catalog",
                                               f"arn:aws:glue:{region}:{acct}:database/{n['database']}",
                                               f"arn:aws:glue:{region}:{acct}:table/{n['database']}/{n['table']}"]})
        res.update(iam_role(ctx, "DeliveryRole", ctx.name("delivery"), "firehose.amazonaws.com", fh_statements))
        res["DeliveryLogs"] = {"Type": "AWS::Logs::LogGroup",
                               "Properties": {"LogGroupName": f"/aws/kinesisfirehose/{n['delivery']}",
                                              "RetentionInDays": 14}}
        res["DeliveryLogStream"] = {"Type": "AWS::Logs::LogStream",
                                    "Properties": {"LogGroupName": {"Ref": "DeliveryLogs"},
                                                   "LogStreamName": "DestinationDelivery"}}
        s3_conf: dict[str, Any] = {
            "BucketARN": data_arn,
            "RoleARN": {"Fn::GetAtt": ["DeliveryRole", "Arn"]},
            "Prefix": "data/dt=!{timestamp:yyyy-MM-dd}/",
            "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/dt=!{timestamp:yyyy-MM-dd}/",
            "BufferingHints": {"IntervalInSeconds": p.buffer_seconds, "SizeInMBs": p.buffer_mb},
            "CompressionFormat": "UNCOMPRESSED" if p.output_format == "parquet" else "GZIP",
            "CloudWatchLoggingOptions": {"Enabled": True, "LogGroupName": f"/aws/kinesisfirehose/{n['delivery']}",
                                         "LogStreamName": "DestinationDelivery"},
            "ProcessingConfiguration": {
                "Enabled": True,
                "Processors": [{
                    "Type": "Lambda",
                    "Parameters": [
                        {"ParameterName": "LambdaArn", "ParameterValue": {"Fn::GetAtt": ["TransformFunction", "Arn"]}},
                        {"ParameterName": "BufferSizeInMBs", "ParameterValue": "1"},
                        {"ParameterName": "BufferIntervalInSeconds", "ParameterValue": str(max(p.buffer_seconds, 1))},
                    ],
                }],
            },
        }
        if p.output_format == "parquet":
            s3_conf["DataFormatConversionConfiguration"] = {
                "Enabled": True,
                "InputFormatConfiguration": {"Deserializer": {"OpenXJsonSerDe": {}}},
                "OutputFormatConfiguration": {"Serializer": {"ParquetSerDe": {"Compression": "SNAPPY"}}},
                "SchemaConfiguration": {"DatabaseName": n["database"], "TableName": n["table"], "Region": region,
                                        "RoleARN": {"Fn::GetAtt": ["DeliveryRole", "Arn"]}, "VersionId": "LATEST"},
            }
        res["Delivery"] = {
            "Type": "AWS::KinesisFirehose::DeliveryStream",
            "DependsOn": ["DeliveryLogStream"],
            "Properties": {
                "DeliveryStreamName": n["delivery"],
                "DeliveryStreamType": "KinesisStreamAsSource",
                "KinesisStreamSourceConfiguration": {"KinesisStreamARN": stream_arn,
                                                     "RoleARN": {"Fn::GetAtt": ["DeliveryRole", "Arn"]}},
                "ExtendedS3DestinationConfiguration": s3_conf,
                "Tags": [{"Key": k, "Value": v} for k, v in ctx.tags.items()],
            },
        }
        res.update(workgroup(ctx, "ConsumerWorkgroup", n["consumer_workgroup"], "consumer", 1024**3))
        res.update(workgroup(ctx, "Workgroup", n["workgroup"], "pipeline", 10 * 1024**3))
        res.update(iam_role(ctx, "ConsumerRole", ctx.name("consumer"), None,
                            consumer_s3_table_statements(ctx, n["database"], n["table"], "data/"),
                            trust_platform=True))
        dims = [{"Name": "DeliveryStreamName", "Value": n["delivery"]}]
        res.update(alarm(ctx, "FreshnessAlarm", "delivery-freshness", "AWS/Firehose", "DeliveryToS3.DataFreshness",
                         dims, statistic="Maximum", threshold=body.freshness.max_lag_seconds,
                         comparison="GreaterThanThreshold", description="Oldest undelivered record exceeds budget"))
        res.update(alarm(ctx, "TransformFailedAlarm", "transform-failed", "pmcp", "ProcessingFailed",
                         [{"Name": "Flow", "Value": ctx.flow_id}], description="Records rejected by the transform"))
        res.update(alarm(ctx, "LambdaErrorsAlarm", "lambda-errors", "AWS/Lambda", "Errors",
                         [{"Name": "FunctionName", "Value": n["function"]}], description="Transform Lambda errors"))
        tmpl = template(f"pmcp flow {ctx.flow_id} ({self.pattern_id})", res, {
            "DataBucket": {"Ref": "DataBucket"},
            "Database": n["database"],
            "OutputTable": n["table"],
            "StreamName": stream_name,
            "StreamArn": stream_arn,
            "DeliveryStreamName": n["delivery"],
            "TransformFunction": n["function"],
            "ConsumerRoleArn": {"Fn::GetAtt": ["ConsumerRole", "Arn"]},
            "ConsumerWorkgroup": n["consumer_workgroup"],
            "Workgroup": n["workgroup"],
            "AlertsTopicArn": {"Ref": "AlertsTopic"},
        })
        r = Rendered(template=tmpl)
        r.artifacts.append(RenderedArtifact("handler.py", "python", code))
        r.artifacts.append(RenderedArtifact(zip_key, "zip", zip_bytes, upload=True))
        r.desired_summary = {
            "stream": stream_name, "delivery": n["delivery"], "output": f"s3://{ctx.data_bucket}/data/dt=YYYY-MM-DD/",
            "table": f"{n['database']}.{n['table']}", "format": p.output_format,
            "buffer": f"{p.buffer_seconds}s / {p.buffer_mb}MB",
        }
        if ctx.previous_params and ctx.previous_params.get("transform_code") != p.transform_code:
            r.reprocess_reason = ("transform changed; objects delivered earlier keep the old rule. Replay from the "
                                  f"stream is possible only within its retention ({p.retention_hours}h).")
        if ctx.previous_contract:
            old = {c.name for c in ctx.previous_contract.body.output.columns}
            r.schema_changes = [f"add column {c.name}" for c in body.output.columns if c.name not in old]
            r.schema_changes += [f"remove column {c}" for c in old - {c.name for c in body.output.columns}]
            r.breaking = any(s.startswith("remove") for s in r.schema_changes)
        return r

    def reprocess_steps(self, ctx: RenderContext, p: FirehoseParams, window: dict[str, str]) -> list[PlanStep]:
        return [PlanStep(
            step_id="reprocess", action="kinesis.replay_from_stream",
            description=f"Re-read stream records {window['start']}..{window['end']} and put them back on the stream",
            params={"window_start": window["start"], "window_end": window["end"]}, reversible=False,
            compensation="Replayed records are delivered again (at-least-once); consumers may see duplicates of "
            "records delivered before the change.")]

    def run_step(self, step: PlanStep, rt: Runtime, op_id: str) -> dict[str, Any]:
        session = rt.deployer or rt.reader
        if step.action == "firehose.replay_errors":
            return _replay_errors(session, rt, step)
        if step.action == "kinesis.replay_from_stream":
            return _replay_stream(session, rt, step)
        return super().run_step(step, rt, op_id)

    def estimate(self, body: DataContractBody, p: FirehoseParams, region: str, prices: PriceBook) -> CostEstimate:
        assumptions = []
        gb_day = (body.volume.bytes_per_day or 0) / 1024**3
        if not gb_day:
            rpd = body.volume.records_per_day or (body.volume.peak_records_per_second or 10) * 86400 // 4
            gb_day = rpd * 1024 / 1024**3
            assumptions.append("bytes/day estimated at 1 KB/record")
        gb_month = gb_day * 30
        records_month = (body.volume.records_per_day or gb_day * 1024**3 / 1024) * 30
        fh = prices.get("firehose_gb_ingested", region)
        ks_h = prices.get("kinesis_ondemand_stream_hour", region)
        ks_in = prices.get("kinesis_ondemand_gb_in", region)
        ks_out = prices.get("kinesis_ondemand_gb_out", region)
        lam_r = prices.get("lambda_request", region)
        lam_gb = prices.get("lambda_gb_second_arm", region)
        s3 = prices.get("s3_gb_month", region)
        alarm_p = prices.get("cloudwatch_alarm_month", region)
        invocations = max(records_month / 500, 30 * 86400 / max(p.buffer_seconds, 1))
        lines = []
        if p.stream == "create":
            if p.stream_mode == "ON_DEMAND":
                lines += [
                    CostLine(item="Kinesis on-demand stream-hours", quantity=720, unit=ks_h.unit,
                             unit_price_usd=ks_h.usd, monthly_usd=round(720 * ks_h.usd, 2),
                             price_source=ks_h.source, price_as_of=ks_h.as_of),  # type: ignore[arg-type]
                    CostLine(item="Kinesis data in", quantity=round(gb_month, 3), unit=ks_in.unit,
                             unit_price_usd=ks_in.usd, monthly_usd=round(gb_month * ks_in.usd, 2),
                             price_source=ks_in.source, price_as_of=ks_in.as_of),  # type: ignore[arg-type]
                    CostLine(item="Kinesis data out (Firehose)", quantity=round(gb_month, 3), unit=ks_out.unit,
                             unit_price_usd=ks_out.usd, monthly_usd=round(gb_month * ks_out.usd, 2),
                             price_source=ks_out.source, price_as_of=ks_out.as_of),  # type: ignore[arg-type]
                ]
            else:
                lines.append(CostLine(item=f"Kinesis provisioned shard-hours ({p.shards} shards)",
                                      quantity=720 * p.shards, unit="shard-hour", unit_price_usd=0.015,
                                      monthly_usd=round(720 * p.shards * 0.015, 2)))
        lines += [
            CostLine(item="Firehose ingestion (5 KB minimum per record not modelled)", quantity=round(gb_month, 3),
                     unit=fh.unit, unit_price_usd=fh.usd, monthly_usd=round(gb_month * fh.usd, 2),
                     price_source=fh.source, price_as_of=fh.as_of),  # type: ignore[arg-type]
            CostLine(item="Transform Lambda requests", quantity=round(invocations), unit=lam_r.unit,
                     unit_price_usd=lam_r.usd, monthly_usd=round(invocations * lam_r.usd, 4),
                     price_source=lam_r.source, price_as_of=lam_r.as_of),  # type: ignore[arg-type]
            CostLine(item="Transform Lambda compute (256 MB, ~200 ms)", quantity=round(invocations * 0.25 * 0.2),
                     unit=lam_gb.unit, unit_price_usd=lam_gb.usd,
                     monthly_usd=round(invocations * 0.25 * 0.2 * lam_gb.usd, 4),
                     price_source=lam_gb.source, price_as_of=lam_gb.as_of),  # type: ignore[arg-type]
            CostLine(item="S3 storage (gzip ~25% of raw), 30 days", quantity=round(gb_month * 0.25, 3),
                     unit=s3.unit, unit_price_usd=s3.usd, monthly_usd=round(gb_month * 0.25 * s3.usd, 4),
                     price_source=s3.source, price_as_of=s3.as_of),  # type: ignore[arg-type]
            CostLine(item="CloudWatch alarms (3)", quantity=3, unit=alarm_p.unit, unit_price_usd=alarm_p.usd,
                     monthly_usd=round(3 * alarm_p.usd, 2), price_source=alarm_p.source,  # type: ignore[arg-type]
                     price_as_of=alarm_p.as_of),
        ]
        if p.output_format == "parquet":
            conv = prices.get("firehose_gb_format_conversion", region)
            lines.append(CostLine(item="Firehose Parquet conversion", quantity=round(gb_month, 3), unit=conv.unit,
                                  unit_price_usd=conv.usd, monthly_usd=round(gb_month * conv.usd, 2),
                                  price_source=conv.source, price_as_of=conv.as_of))  # type: ignore[arg-type]
        assumptions.append(f"{gb_day:.3f} GB/day")
        return CostEstimate(monthly_usd=round(sum(line.monthly_usd for line in lines), 2), lines=lines,
                            assumptions=assumptions,
                            confidence="medium" if body.volume.bytes_per_day or body.volume.records_per_day else "low")

    def verify(self, rt: Runtime, window: dict[str, str] | None) -> tuple[dict[str, str], list[Check]]:
        assert rt.contract is not None
        body = rt.contract.body
        n = _n(flow_short(rt))
        win = window or default_window(60, lag_minutes=max(2, rt.flow.pattern_params.get("buffer_seconds", 60) // 60 + 2))
        start = datetime.fromisoformat(win["start"].replace("Z", "+00:00"))
        end = datetime.fromisoformat(win["end"].replace("Z", "+00:00"))
        checks: list[Check] = []
        sc, actual = schema_check(rt, n["database"], n["table"])
        s3 = rt.reader.client("s3", config=BOTO_CONFIG)
        bucket = rt.flow.outputs.get("DataBucket", "")
        listing = s3.list_objects_v2(Bucket=bucket, Prefix="data/", MaxKeys=5)
        checks.append(Check(check_id="exists", category="existence",
                            status="passed" if listing.get("KeyCount", 0) > 0 else "failed",
                            description="Delivered objects exist under data/", expected="> 0 objects",
                            observed=listing.get("KeyCount", 0)))
        checks.append(sc)
        stream_name = rt.flow.outputs.get("StreamName", "")
        incoming = metric_sum(rt, "AWS/Kinesis", "IncomingRecords", [{"Name": "StreamName", "Value": stream_name}],
                              start, end)
        flow_dim = [{"Name": "Flow", "Value": rt.flow.flow_id}]
        ok = metric_sum(rt, "pmcp", "Ok", flow_dim, start, end) or 0
        dropped = metric_sum(rt, "pmcp", "Dropped", flow_dim, start, end) or 0
        failed = metric_sum(rt, "pmcp", "ProcessingFailed", flow_dim, start, end) or 0
        if incoming is None:
            checks.append(unverified("row_count", "row_count", "Records in vs records out",
                                     "no IncomingRecords datapoints for the window"))
        else:
            accounted = ok + dropped + failed
            # Metrics are bucketed by processing time, so allow 2% skew at window edges.
            status = "passed" if failed == 0 and abs(accounted - incoming) <= max(2, incoming * 0.02) else "failed"
            checks.append(Check(check_id="row_count", category="row_count", status=status, window=win,  # type: ignore[arg-type]
                                description="Kinesis IncomingRecords = transform Ok + Dropped + Failed, Failed = 0",
                                expected=incoming, observed={"ok": ok, "dropped": dropped, "failed": failed},
                                evidence=["cloudwatch:AWS/Kinesis/IncomingRecords", "cloudwatch:pmcp/Ok"]))
        errs = s3.list_objects_v2(Bucket=bucket, Prefix="errors/", MaxKeys=50)
        err_keys = [o["Key"] for o in errs.get("Contents", []) if start <= o["LastModified"] <= end + timedelta(minutes=15)]
        checks.append(Check(check_id="error_objects", category="quality", status="failed" if err_keys else "passed",
                            description="No records delivered to errors/ in the window", expected=0,
                            observed=len(err_keys), evidence=[f"s3://{bucket}/{k}" for k in err_keys[:5]]))
        dates = sorted({(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((end - start).days + 1)}
                       | {end.strftime("%Y-%m-%d")})
        dt_list = ", ".join(f"'{d}'" for d in dates)
        m = body.mutation
        if m.key and actual:
            keys = ", ".join(f'"{k}"' for k in m.key)
            r = athena_query(rt.reader, f"SELECT count(*) AS dup FROM (SELECT {keys} FROM {n['database']}.{n['table']} "
                             f"WHERE dt IN ({dt_list}) GROUP BY {keys} HAVING count(*) > 1)",
                             workgroup=n["workgroup"], database=n["database"])
            dup = int(r["rows"][0]["dup"]) if r["state"] == "SUCCEEDED" else None
            allowed = m.duplicates == "allowed"
            checks.append(Check(check_id="duplicates", category="keys",
                                status="passed" if dup == 0 or (allowed and dup is not None) else
                                ("failed" if dup else "error"),
                                description=f"Duplicate keys {m.key} in delivered partitions {dates}",
                                expected=0 if not allowed else "allowed", observed=dup,
                                evidence=[f"athena:{r['query_id']}"], reason=r.get("error")))
        checks.append(unverified("mutations", "mutations", "Updates/deletes", "pattern is append-only by design"))
        fresh = metric_sum(rt, "AWS/Firehose", "DeliveryToS3.DataFreshness",
                           [{"Name": "DeliveryStreamName", "Value": n["delivery"]}],
                           datetime.now(UTC) - timedelta(minutes=15), datetime.now(UTC), stat="Maximum")
        checks.append(Check(check_id="freshness", category="freshness",
                            status="unverified" if fresh is None else
                            ("passed" if fresh <= body.freshness.max_lag_seconds else "failed"),
                            description="Firehose DeliveryToS3.DataFreshness (max, last 15 min)",
                            expected=f"<= {body.freshness.max_lag_seconds}s", observed=fresh,
                            reason="no datapoints (no traffic in last 15 min)" if fresh is None else None))
        checks.append(consumer_athena_check(rt, f"SELECT * FROM {n['database']}.{n['table']} WHERE dt IN ({dt_list}) "
                                                "LIMIT 5", n["consumer_workgroup"], n["database"]))
        return win, checks

    def observe(self, rt: Runtime) -> dict[str, Any]:
        n = _n(flow_short(rt))
        now = datetime.now(UTC)
        fh = rt.client("firehose").describe_delivery_stream(DeliveryStreamName=n["delivery"])["DeliveryStreamDescription"]
        flow_dim = [{"Name": "Flow", "Value": rt.flow.flow_id}]
        hour = now - timedelta(hours=1)
        alarms = rt.client("cloudwatch").describe_alarms(AlarmNamePrefix=f"pmcp-{flow_short(rt)}-")["MetricAlarms"]
        return {
            "delivery_stream_status": fh["DeliveryStreamStatus"],
            "last_hour": {
                "incoming_records": metric_sum(rt, "AWS/Kinesis", "IncomingRecords",
                                               [{"Name": "StreamName", "Value": rt.flow.outputs.get("StreamName", "")}],
                                               hour, now),
                "transform_ok": metric_sum(rt, "pmcp", "Ok", flow_dim, hour, now),
                "transform_failed": metric_sum(rt, "pmcp", "ProcessingFailed", flow_dim, hour, now),
                "delivery_freshness_max_s": metric_sum(rt, "AWS/Firehose", "DeliveryToS3.DataFreshness",
                                                       [{"Name": "DeliveryStreamName", "Value": n["delivery"]}],
                                                       hour, now, stat="Maximum"),
            },
            "alarms": [{"name": a["AlarmName"], "state": a["StateValue"]} for a in alarms],
        }

    def diagnose(self, rt: Runtime, window: dict[str, str]) -> DiagnosisParts:
        n = _n(flow_short(rt))
        start = datetime.fromisoformat(window["start"].replace("Z", "+00:00"))
        end = datetime.fromisoformat(window["end"].replace("Z", "+00:00"))
        facts: list[dict[str, Any]] = []
        flow_dim = [{"Name": "Flow", "Value": rt.flow.flow_id}]
        failed = metric_sum(rt, "pmcp", "ProcessingFailed", flow_dim, start, end) or 0
        incoming = metric_sum(rt, "AWS/Kinesis", "IncomingRecords",
                              [{"Name": "StreamName", "Value": rt.flow.outputs.get("StreamName", "")}], start, end)
        facts.append({"fact_id": "F1", "kind": "metrics", "incoming_records": incoming, "transform_failed": failed})
        logs = rt.client("logs")
        errors: list[str] = []
        try:
            ev = logs.filter_log_events(logGroupName=f"/aws/lambda/{n['function']}", filterPattern='"ERROR"',
                                        startTime=int(start.timestamp() * 1000), endTime=int(end.timestamp() * 1000),
                                        limit=20)["events"]
            for e in ev:
                try:
                    j = json.loads(e["message"])
                    errors.append(f"{j.get('error')}: {j.get('message', '')[:200]}")
                except ValueError:
                    errors.append(e["message"][:200])
        except Exception as exc:
            errors.append(f"log read failed: {exc}"[:200])
        if errors:
            facts.append({"fact_id": "F2", "kind": "transform_errors", "samples": errors[:10]})
        s3 = rt.reader.client("s3", config=BOTO_CONFIG)
        bucket = rt.flow.outputs.get("DataBucket", "")
        err_objs = [o for o in s3.list_objects_v2(Bucket=bucket, Prefix="errors/").get("Contents", [])
                    if start <= o["LastModified"] <= end + timedelta(minutes=15)]
        if err_objs:
            facts.append({"fact_id": "F3", "kind": "error_objects", "count": len(err_objs),
                          "types": sorted({o["Key"].split("/")[1] for o in err_objs})})
        alarms = rt.client("cloudwatch").describe_alarms(AlarmNamePrefix=f"pmcp-{flow_short(rt)}-",
                                                         StateValue="ALARM")["MetricAlarms"]
        for a in alarms:
            facts.append({"fact_id": f"F{len(facts) + 1}", "kind": "alarm", "name": a["AlarmName"]})
        hyps: list[Hypothesis] = []
        further: list[str] = []
        if failed or err_objs:
            hyps.append(Hypothesis(
                hypothesis_id="H1", likelihood="high",
                statement="The transform rejects some records (exception in transform or unexpected input shape)",
                supporting_evidence=[f["fact_id"] for f in facts if f["kind"] in ("transform_errors", "error_objects",
                                                                                 "metrics")],
                suggested_recovery=["fix_transform_then_replay", "replay_errors"]))
            further.append("Read one errors/processing-failed object to see the rejected payload shape")
        if incoming == 0 or incoming is None:
            hyps.append(Hypothesis(hypothesis_id=f"H{len(hyps) + 1}", likelihood="medium",
                                   statement="The producer stopped sending (no IncomingRecords)",
                                   supporting_evidence=["F1"], suggested_recovery=["none_in_pipeline"]))
            further.append("Check the producer application and its credentials")
        if any("freshness" in f.get("name", "") for f in facts):
            hyps.append(Hypothesis(hypothesis_id=f"H{len(hyps) + 1}", likelihood="medium",
                                   statement="Delivery to S3 is delayed (throttling, Lambda slowness or S3 permission)",
                                   supporting_evidence=[f["fact_id"] for f in facts if f["kind"] == "alarm"],
                                   suggested_recovery=["restore_template"]))
            further.append("Check /aws/kinesisfirehose log group for delivery errors")
        return DiagnosisParts(facts=facts, hypotheses=hyps, further_checks=further,
                              affected={"error_object_keys": [o["Key"] for o in err_objs][:200]})

    def recovery_options(self, rt: Runtime, incident: Any) -> list[RecoveryOption]:
        keys = incident.affected.get("error_object_keys", [])
        verify = PlanStep(step_id="verify", action="verify", description="Verify after replay",
                          params={"window": None})
        opts = [RecoveryOption("restore_template", "Re-deploy the last applied template", [verify],
                               needs_template_restore=True)]
        if keys:
            opts.insert(0, RecoveryOption(
                "replay_errors",
                f"Put {len(keys)} error object(s)' raw records back on the stream, then archive the objects",
                [PlanStep(step_id="replay", action="firehose.replay_errors",
                          description="Replay processing-failed records into the source stream",
                          params={"keys": keys}, reversible=False,
                          compensation="Replayed records are delivered once more if they pass the transform; "
                          "records still failing return to errors/."), verify],
                notes=["Run after fixing the transform (a change plan) or the records fail again."]))
        return opts


def _replay_errors(session: Any, rt: Runtime, step: PlanStep) -> dict[str, Any]:
    s3 = session.client("s3", config=BOTO_CONFIG)
    kinesis = session.client("kinesis", config=BOTO_CONFIG)
    bucket = rt.flow.outputs["DataBucket"]
    stream = rt.flow.outputs["StreamName"]
    replayed, archived = 0, []
    import gzip

    for key in step.params["keys"]:
        try:
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        except Exception as exc:
            if "NoSuchKey" in str(exc):  # archived by an earlier attempt
                continue
            raise
        if body[:2] == b"\x1f\x8b":
            body = gzip.decompress(body)
        batch = []
        for line in body.decode().splitlines():
            if not line.strip():
                continue
            raw = base64.b64decode(json.loads(line)["rawData"])
            batch.append({"Data": raw, "PartitionKey": sha256_hex(raw)[:32]})
            if len(batch) == 500:
                kinesis.put_records(StreamName=stream, Records=batch)
                replayed += len(batch)
                batch = []
        if batch:
            kinesis.put_records(StreamName=stream, Records=batch)
            replayed += len(batch)
        new_key = "errors-replayed/" + key.split("/", 1)[1]
        s3.copy_object(Bucket=bucket, Key=new_key, CopySource={"Bucket": bucket, "Key": key})
        s3.delete_object(Bucket=bucket, Key=key)
        archived.append(new_key)
    return {"replayed_records": replayed, "archived": archived}


def _replay_stream(session: Any, rt: Runtime, step: PlanStep) -> dict[str, Any]:
    kinesis = session.client("kinesis", config=BOTO_CONFIG)
    stream = rt.flow.outputs["StreamName"]
    start = datetime.fromisoformat(step.params["window_start"].replace("Z", "+00:00"))
    end = datetime.fromisoformat(step.params["window_end"].replace("Z", "+00:00"))
    shards = kinesis.list_shards(StreamName=stream)["Shards"]
    total = 0
    for sh in shards:
        it = kinesis.get_shard_iterator(StreamName=stream, ShardId=sh["ShardId"], ShardIteratorType="AT_TIMESTAMP",
                                        Timestamp=start)["ShardIterator"]
        while it:
            resp = kinesis.get_records(ShardIterator=it, Limit=1000)
            recs = [r for r in resp["Records"] if r["ApproximateArrivalTimestamp"] < end]
            if recs:
                kinesis.put_records(StreamName=stream, Records=[{"Data": r["Data"], "PartitionKey": r["PartitionKey"]}
                                                                for r in recs])
                total += len(recs)
            if len(recs) < len(resp["Records"]) or resp.get("MillisBehindLatest", 0) == 0:
                break
            it = resp.get("NextShardIterator")
    return {"replayed_records": total, "window": [iso(start), iso(end)]}
