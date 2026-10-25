"""Pattern kinesis_lambda_dynamodb: Kinesis Data Streams → Lambda → DynamoDB.

Writes are conditional on the contract's ordering column, so retries, replays and out-of-order
batches never overwrite a newer version. Batch failures go to an on-failure SQS destination
(metadata: shard + sequence range), which a recovery plan replays from the stream.
"""

from __future__ import annotations

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
    iam_role,
    template,
    unverified,
)
from pmcp.adapters.codegen import (
    render_dynamodb_handler,
    validate_transform_code,
    zip_single,
)
from pmcp.adapters.common import default_window, flow_short, metric_sum
from pmcp.aws.client import BOTO_CONFIG
from pmcp.catalog.pricing import PriceBook
from pmcp.domain.base import Model
from pmcp.domain.contract import DataContractBody
from pmcp.domain.records import Check, CostEstimate, CostLine, Hypothesis, PlanStep
from pmcp.errors import ErrorCode, PmcpError
from pmcp.ids import sha256_hex

DDB_TYPES = {"string": "S", "date": "S", "timestamp": "S", "int": "N", "bigint": "N", "double": "N",
             "decimal(18,2)": "N", "decimal(38,9)": "N", "boolean": "S"}


class LambdaDynamoParams(Model):
    stream: str = Field("create", description="'create' or the name of an existing Kinesis stream.")
    stream_mode: Literal["ON_DEMAND", "PROVISIONED"] = "ON_DEMAND"
    shards: int = Field(1, ge=1, le=50)
    retention_hours: int = Field(24, ge=24, le=8760)
    transform_code: str | None = Field(
        None, description="Python `transform(record) -> dict | None`; output must contain the contract key and "
        "ordering column; set pmcp_deleted=True to delete the key. Pure stdlib imports only."
    )
    batch_size: int = Field(100, ge=1, le=10000)
    parallelization_factor: int = Field(1, ge=1, le=10)
    starting_position: Literal["LATEST", "TRIM_HORIZON"] = "TRIM_HORIZON"
    alert_email: str | None = None


def _n(s: str) -> dict[str, str]:
    return {"stream": f"pmcp-{s}-in", "function": f"pmcp-{s}-writer", "table": f"pmcp-{s}-items",
            "dlq": f"pmcp-{s}-failed"}


class KinesisLambdaDynamoAdapter(Adapter):
    pattern_id = "kinesis_lambda_dynamodb"
    Params = LambdaDynamoParams

    def check_supported(self, p: LambdaDynamoParams, contract: DataContractBody) -> None:
        problems = []
        m = contract.mutation
        if not m.key or len(m.key) > 2:
            problems.append("DynamoDB needs a 1- or 2-column key (partition [+ sort])")
        if not m.ordering_column:
            problems.append("mutation.ordering_column is required for idempotent conditional writes")
        if p.transform_code:
            problems += validate_transform_code(p.transform_code)
        if p.stream != "create" and not any(s.uri == f"kinesis:{p.stream}" for s in contract.sources):
            problems.append(f"existing stream '{p.stream}' must be a contract source (kinesis:{p.stream})")
        if problems:
            raise PmcpError(ErrorCode.UNSUPPORTED_CONFIGURATION, "pattern_params cannot be deployed as given",
                            details={"problems": problems})

    def render(self, ctx: RenderContext, p: LambdaDynamoParams) -> Rendered:
        body = ctx.contract.body
        m = body.mutation
        n = _n(ctx.short)
        acct, region = ctx.account_id, ctx.region
        types = {c.name: c.type for c in body.output.columns}
        res: dict[str, Any] = {}
        res.update(alerts_topic(ctx, p.alert_email))
        if p.stream == "create":
            props: dict[str, Any] = {"Name": n["stream"], "RetentionPeriodHours": p.retention_hours,
                                     "StreamModeDetails": {"StreamMode": p.stream_mode},
                                     "StreamEncryption": {"EncryptionType": "KMS", "KeyId": "alias/aws/kinesis"},
                                     "Tags": [{"Key": k, "Value": v} for k, v in ctx.tags.items()]}
            if p.stream_mode == "PROVISIONED":
                props["ShardCount"] = p.shards
            res["Stream"] = {"Type": "AWS::Kinesis::Stream", "Properties": props}
            stream_arn: Any = {"Fn::GetAtt": ["Stream", "Arn"]}
            stream_name = n["stream"]
        else:
            stream_name = p.stream
            stream_arn = f"arn:aws:kinesis:{region}:{acct}:stream/{p.stream}"
        key_schema = [{"AttributeName": m.key[0], "KeyType": "HASH"}]
        attrs = [{"AttributeName": m.key[0], "AttributeType": DDB_TYPES[types[m.key[0]]]}]
        if len(m.key) == 2:
            key_schema.append({"AttributeName": m.key[1], "KeyType": "RANGE"})
            attrs.append({"AttributeName": m.key[1], "AttributeType": DDB_TYPES[types[m.key[1]]]})
        res["Table"] = {
            "Type": "AWS::DynamoDB::Table",
            "DeletionPolicy": "Retain",
            "UpdateReplacePolicy": "Retain",
            "Properties": {
                "TableName": n["table"],
                "BillingMode": "PAY_PER_REQUEST",
                "KeySchema": key_schema,
                "AttributeDefinitions": attrs,
                "PointInTimeRecoverySpecification": {"PointInTimeRecoveryEnabled": True},
                "SSESpecification": {"SSEEnabled": True},
                "Tags": [{"Key": k, "Value": v} for k, v in ctx.tags.items()],
            },
        }
        res["FailedBatches"] = {"Type": "AWS::SQS::Queue",
                                "Properties": {"QueueName": n["dlq"], "MessageRetentionPeriod": 1209600,
                                               "SqsManagedSseEnabled": True,
                                               "Tags": [{"Key": k, "Value": v} for k, v in ctx.tags.items()]}}
        code = render_dynamodb_handler(p.transform_code, ctx.flow_id, m.key, m.ordering_column or m.key[0])
        zip_bytes = zip_single("handler.py", code)
        zip_key = f"pmcp/artifacts/{sha256_hex(zip_bytes)}.zip"
        res.update(iam_role(ctx, "WriterRole", ctx.name("writer"), "lambda.amazonaws.com", [
            {"Effect": "Allow", "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
             "Resource": f"arn:aws:logs:{region}:{acct}:log-group:/aws/lambda/{n['function']}*"},
            {"Effect": "Allow", "Action": ["dynamodb:PutItem", "dynamodb:DeleteItem", "dynamodb:GetItem"],
             "Resource": {"Fn::GetAtt": ["Table", "Arn"]}},
            {"Effect": "Allow", "Action": ["kinesis:DescribeStream", "kinesis:DescribeStreamSummary",
                                           "kinesis:GetShardIterator", "kinesis:GetRecords", "kinesis:ListShards",
                                           "kinesis:ListStreams", "kinesis:SubscribeToShard"],
             "Resource": stream_arn},
            {"Effect": "Allow", "Action": ["kms:Decrypt"], "Resource": "*",
             "Condition": {"StringEquals": {"kms:ViaService": f"kinesis.{region}.amazonaws.com"}}},
            {"Effect": "Allow", "Action": ["sqs:SendMessage"], "Resource": {"Fn::GetAtt": ["FailedBatches", "Arn"]}},
        ]))
        res["WriterLogs"] = {"Type": "AWS::Logs::LogGroup",
                             "Properties": {"LogGroupName": f"/aws/lambda/{n['function']}", "RetentionInDays": 14}}
        res["Writer"] = {
            "Type": "AWS::Lambda::Function",
            "DependsOn": ["WriterLogs"],
            "Properties": {
                "FunctionName": n["function"], "Runtime": "python3.13", "Architectures": ["arm64"],
                "Handler": "handler.handler", "Timeout": 60, "MemorySize": 256,
                "Role": {"Fn::GetAtt": ["WriterRole", "Arn"]},
                "Environment": {"Variables": {"TABLE_NAME": n["table"]}},
                "Code": {"S3Bucket": f"pmcp-artifacts-{acct}-{region}", "S3Key": zip_key},
                "Tags": [{"Key": k, "Value": v} for k, v in ctx.tags.items()],
            },
        }
        res["Mapping"] = {
            "Type": "AWS::Lambda::EventSourceMapping",
            "Properties": {
                "FunctionName": {"Ref": "Writer"},
                "EventSourceArn": stream_arn,
                "StartingPosition": p.starting_position,
                "BatchSize": p.batch_size,
                "ParallelizationFactor": p.parallelization_factor,
                "BisectBatchOnFunctionError": True,
                "MaximumRetryAttempts": 5,
                "FunctionResponseTypes": ["ReportBatchItemFailures"],
                "DestinationConfig": {"OnFailure": {"Destination": {"Fn::GetAtt": ["FailedBatches", "Arn"]}}},
            },
        }
        res.update(iam_role(ctx, "ConsumerRole", ctx.name("consumer"), None, [
            {"Effect": "Allow", "Action": ["dynamodb:GetItem", "dynamodb:Query", "dynamodb:DescribeTable"],
             "Resource": {"Fn::GetAtt": ["Table", "Arn"]}},
        ], trust_platform=True))
        res.update(alarm(ctx, "WriterErrorsAlarm", "writer-errors", "AWS/Lambda", "Errors",
                         [{"Name": "FunctionName", "Value": n["function"]}]))
        res.update(alarm(ctx, "IteratorAgeAlarm", "iterator-age", "AWS/Lambda", "IteratorAge",
                         [{"Name": "FunctionName", "Value": n["function"]}], statistic="Maximum",
                         threshold=body.freshness.max_lag_seconds * 1000, comparison="GreaterThanThreshold",
                         description="Records wait longer than the freshness budget"))
        res.update(alarm(ctx, "FailedBatchesAlarm", "failed-batches", "AWS/SQS", "ApproximateNumberOfMessagesVisible",
                         [{"Name": "QueueName", "Value": n["dlq"]}], statistic="Maximum",
                         description="Batches exhausted retries"))
        tmpl = template(f"pmcp flow {ctx.flow_id} ({self.pattern_id})", res, {
            "TableName": n["table"], "StreamName": stream_name, "StreamArn": stream_arn,
            "WriterFunction": n["function"], "FailedBatchesQueueUrl": {"Ref": "FailedBatches"},
            "ConsumerRoleArn": {"Fn::GetAtt": ["ConsumerRole", "Arn"]}, "AlertsTopicArn": {"Ref": "AlertsTopic"},
        })
        r = Rendered(template=tmpl)
        r.artifacts.append(RenderedArtifact("handler.py", "python", code))
        r.artifacts.append(RenderedArtifact(zip_key, "zip", zip_bytes, upload=True))
        r.desired_summary = {"stream": stream_name, "table": n["table"], "key": m.key,
                             "ordering": m.ordering_column, "consumer_role": ctx.name("consumer")}
        if ctx.previous_params and ctx.previous_params.get("transform_code") != p.transform_code:
            r.reprocess_reason = (f"transform changed; items written earlier keep the old rule. Replay is possible "
                                  f"within stream retention ({p.retention_hours}h); older items are not reprocessed.")
        if ctx.previous_contract and ctx.previous_contract.body.mutation.key != m.key:
            r.breaking = True
            r.schema_changes.append("table key changed: the table is replaced and old items are not migrated")
        return r

    def reprocess_steps(self, ctx: RenderContext, p: LambdaDynamoParams, window: dict[str, str]) -> list[PlanStep]:
        return [PlanStep(step_id="reprocess", action="kinesis.replay_from_stream",
                         description=f"Replay stream records {window['start']}..{window['end']}",
                         params={"window_start": window["start"], "window_end": window["end"]}, reversible=False,
                         compensation="Conditional writes make replays idempotent; items keep the newest version.")]

    def run_step(self, step: PlanStep, rt: Runtime, op_id: str) -> dict[str, Any]:
        from pmcp.adapters.kinesis_firehose import _replay_stream

        session = rt.deployer or rt.reader
        if step.action == "kinesis.replay_from_stream":
            return _replay_stream(session, rt, step)
        if step.action == "lambda.replay_failed_batches":
            return _replay_failed(session, rt)
        return super().run_step(step, rt, op_id)

    def estimate(self, body: DataContractBody, p: LambdaDynamoParams, region: str, prices: PriceBook) -> CostEstimate:
        rpd = body.volume.records_per_day or (body.volume.peak_records_per_second or 10) * 86400 // 4
        assumptions = [] if body.volume.records_per_day else ["records/day derived from peak rate / 4"]
        rec_month = rpd * 30
        gb_month = rec_month * 1024 / 1024**3
        wru = prices.get("dynamodb_write_request_unit", region)
        st = prices.get("dynamodb_gb_month", region)
        ks_h = prices.get("kinesis_ondemand_stream_hour", region)
        ks_in = prices.get("kinesis_ondemand_gb_in", region)
        lam_r = prices.get("lambda_request", region)
        lam_gb = prices.get("lambda_gb_second_arm", region)
        invocations = rec_month / max(p.batch_size / 4, 1)
        lines = [
            CostLine(item="DynamoDB writes (1 KB items, 1 WRU each)", quantity=rec_month, unit=wru.unit,
                     unit_price_usd=wru.usd, monthly_usd=round(rec_month * wru.usd, 2), price_source=wru.source,  # type: ignore[arg-type]
                     price_as_of=wru.as_of),
            CostLine(item="DynamoDB storage (keys x 1 KB)", quantity=round(gb_month / 30, 3), unit=st.unit,
                     unit_price_usd=st.usd, monthly_usd=round(gb_month / 30 * st.usd, 3), price_source=st.source,  # type: ignore[arg-type]
                     price_as_of=st.as_of),
            CostLine(item="Lambda requests", quantity=round(invocations), unit=lam_r.unit, unit_price_usd=lam_r.usd,
                     monthly_usd=round(invocations * lam_r.usd, 4), price_source=lam_r.source,  # type: ignore[arg-type]
                     price_as_of=lam_r.as_of),
            CostLine(item="Lambda compute (256 MB, ~300 ms)", quantity=round(invocations * 0.25 * 0.3),
                     unit=lam_gb.unit, unit_price_usd=lam_gb.usd,
                     monthly_usd=round(invocations * 0.25 * 0.3 * lam_gb.usd, 4), price_source=lam_gb.source,  # type: ignore[arg-type]
                     price_as_of=lam_gb.as_of),
            CostLine(item="CloudWatch alarms (3)", quantity=3, unit="alarm-month", unit_price_usd=0.10,
                     monthly_usd=0.3),
        ]
        if p.stream == "create" and p.stream_mode == "ON_DEMAND":
            lines += [
                CostLine(item="Kinesis on-demand stream-hours", quantity=720, unit=ks_h.unit, unit_price_usd=ks_h.usd,
                         monthly_usd=round(720 * ks_h.usd, 2), price_source=ks_h.source, price_as_of=ks_h.as_of),  # type: ignore[arg-type]
                CostLine(item="Kinesis data in", quantity=round(gb_month, 3), unit=ks_in.unit,
                         unit_price_usd=ks_in.usd, monthly_usd=round(gb_month * ks_in.usd, 2),
                         price_source=ks_in.source, price_as_of=ks_in.as_of),  # type: ignore[arg-type]
            ]
        elif p.stream == "create":
            lines.append(CostLine(item=f"Kinesis shard-hours ({p.shards})", quantity=720 * p.shards,
                                  unit="shard-hour", unit_price_usd=0.015, monthly_usd=round(720 * p.shards * .015, 2)))
        return CostEstimate(monthly_usd=round(sum(line.monthly_usd for line in lines), 2), lines=lines,
                            assumptions=assumptions + ["1 KB items; overwrites count as writes"],
                            confidence="medium" if body.volume.records_per_day else "low")

    def verify(self, rt: Runtime, window: dict[str, str] | None) -> tuple[dict[str, str], list[Check]]:
        assert rt.contract is not None
        body = rt.contract.body
        n = _n(flow_short(rt))
        win = window or default_window(60, lag_minutes=5)
        start = datetime.fromisoformat(win["start"].replace("Z", "+00:00"))
        end = datetime.fromisoformat(win["end"].replace("Z", "+00:00"))
        ddb = rt.reader.client("dynamodb", config=BOTO_CONFIG)
        checks: list[Check] = []
        try:
            desc = ddb.describe_table(TableName=n["table"])["Table"]
        except Exception as exc:
            return win, [Check(check_id="exists", category="existence", status="failed",
                               description="DynamoDB table exists", observed=str(exc)[:200])]
        sample = ddb.scan(TableName=n["table"], Limit=20)
        items = sample.get("Items", [])
        checks.append(Check(check_id="exists", category="existence", status="passed" if items else "failed",
                            description="Table exists and holds items", expected="> 0 items",
                            observed=f"{len(items)} in first scan page; ~{desc.get('ItemCount')} (6-hourly estimate)"))
        key_names = [k["AttributeName"] for k in desc["KeySchema"]]
        checks.append(Check(check_id="keys", category="keys",
                            status="passed" if key_names == body.mutation.key else "failed",
                            description="Table primary key equals contract key (uniqueness enforced by DynamoDB)",
                            expected=body.mutation.key, observed=key_names))
        missing: dict[str, int] = {}
        for it in items:
            for c in body.output.columns:
                if not c.nullable and c.name not in it:
                    missing[c.name] = missing.get(c.name, 0) + 1
        checks.append(Check(check_id="schema", category="schema", status="failed" if missing else "passed",
                            description="Sampled items carry all non-nullable contract attributes",
                            expected=[c.name for c in body.output.columns if not c.nullable], observed=missing or "ok",
                            reason="DynamoDB is schemaless; checked on a sample of up to 20 items"))
        stream = rt.flow.outputs.get("StreamName", "")
        incoming = metric_sum(rt, "AWS/Kinesis", "IncomingRecords", [{"Name": "StreamName", "Value": stream}],
                              start, end)
        dim = [{"Name": "Flow", "Value": rt.flow.flow_id}]
        counts = {k: metric_sum(rt, "pmcp", k, dim, start, end) or 0
                  for k in ("Written", "Deleted", "StaleSkipped", "Dropped", "Failed")}
        if incoming is None:
            checks.append(unverified("row_count", "row_count", "Records in vs handled",
                                     "no IncomingRecords datapoints in the window"))
        else:
            handled = sum(v for k, v in counts.items() if k != "Failed")
            ok = counts["Failed"] == 0 and abs(handled - incoming) <= max(2, incoming * 0.02)
            checks.append(Check(check_id="row_count", category="row_count", status="passed" if ok else "failed",
                                window=win, description="Every incoming record was written, deleted, skipped as "
                                "stale or dropped by the transform; none failed", expected=incoming, observed=counts))
        checks.append(Check(check_id="mutations", category="mutations",
                            status="passed" if counts["Failed"] == 0 else "failed", window=win,
                            description="Conditional writes applied (newer version wins); deletes applied",
                            observed={"deleted": counts["Deleted"], "stale_skipped": counts["StaleSkipped"]},
                            reason="per-key latest-version equality is enforced by the write condition, not re-read"))
        age = metric_sum(rt, "AWS/Lambda", "IteratorAge", [{"Name": "FunctionName", "Value": n["function"]}],
                         datetime.now(UTC) - timedelta(minutes=15), datetime.now(UTC), stat="Maximum")
        checks.append(Check(check_id="freshness", category="freshness",
                            status="unverified" if age is None else
                            ("passed" if age / 1000 <= body.freshness.max_lag_seconds else "failed"),
                            description="Lambda IteratorAge max over the last 15 minutes",
                            expected=f"<= {body.freshness.max_lag_seconds}s",
                            observed=None if age is None else round(age / 1000, 1),
                            reason="no datapoints" if age is None else None))
        role = rt.flow.outputs.get("ConsumerRoleArn")
        if items and role:
            key = {k: items[0][k] for k in key_names}
            try:
                got = rt.consumer(role).client("dynamodb", config=BOTO_CONFIG).get_item(TableName=n["table"], Key=key)
                ok = "Item" in got
                checks.append(Check(check_id="consumer_read", category="consumer_read",
                                    status="passed" if ok else "failed",
                                    description=f"GetItem on a sampled key as consumer role {role}",
                                    observed="item returned" if ok else "no item"))
            except Exception as exc:
                checks.append(Check(check_id="consumer_read", category="consumer_read", status="failed",
                                    description=f"GetItem as {role}", reason=str(exc)[:200]))
        else:
            checks.append(unverified("consumer_read", "consumer_read", "GetItem as consumer", "no items to read"))
        return win, checks

    def observe(self, rt: Runtime) -> dict[str, Any]:
        n = _n(flow_short(rt))
        now = datetime.now(UTC)
        hour = now - timedelta(hours=1)
        dim = [{"Name": "Flow", "Value": rt.flow.flow_id}]
        sqs = rt.client("sqs")
        url = rt.flow.outputs.get("FailedBatchesQueueUrl")
        depth = None
        if url:
            depth = int(sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages"])[
                "Attributes"]["ApproximateNumberOfMessages"])
        alarms = rt.client("cloudwatch").describe_alarms(AlarmNamePrefix=f"pmcp-{flow_short(rt)}-")["MetricAlarms"]
        return {
            "last_hour": {k: metric_sum(rt, "pmcp", k, dim, hour, now) for k in
                          ("Written", "Deleted", "StaleSkipped", "Dropped", "Failed")},
            "iterator_age_max_ms": metric_sum(rt, "AWS/Lambda", "IteratorAge",
                                              [{"Name": "FunctionName", "Value": n["function"]}], hour, now,
                                              stat="Maximum"),
            "failed_batches_waiting": depth,
            "alarms": [{"name": a["AlarmName"], "state": a["StateValue"]} for a in alarms],
        }

    def diagnose(self, rt: Runtime, window: dict[str, str]) -> DiagnosisParts:
        n = _n(flow_short(rt))
        start = datetime.fromisoformat(window["start"].replace("Z", "+00:00"))
        end = datetime.fromisoformat(window["end"].replace("Z", "+00:00"))
        obs = self.observe(rt)
        facts: list[dict[str, Any]] = [{"fact_id": "F1", "kind": "metrics", **obs["last_hour"],
                                        "failed_batches_waiting": obs["failed_batches_waiting"]}]
        errors = []
        try:
            for e in rt.client("logs").filter_log_events(
                    logGroupName=f"/aws/lambda/{n['function']}", filterPattern='"ERROR"',
                    startTime=int(start.timestamp() * 1000), endTime=int(end.timestamp() * 1000), limit=20)["events"]:
                errors.append(e["message"][:250])
        except Exception as exc:
            errors.append(f"log read failed: {exc}"[:200])
        if errors:
            facts.append({"fact_id": "F2", "kind": "writer_errors", "samples": errors[:10]})
        text = " ".join(errors).lower()
        hyps: list[Hypothesis] = []
        if "accessdenied" in text or "not authorized" in text:
            hyps.append(Hypothesis(hypothesis_id="H1", likelihood="high",
                                   statement="The writer lost DynamoDB/Kinesis permission",
                                   supporting_evidence=["F2"], suggested_recovery=["restore_template",
                                                                                   "replay_failed_batches"]))
        elif "keyerror" in text or "validationexception" in text:
            hyps.append(Hypothesis(hypothesis_id="H1", likelihood="high",
                                   statement="Records lack the key/ordering attribute or have the wrong type",
                                   supporting_evidence=["F2"], suggested_recovery=["change_plan",
                                                                                   "replay_failed_batches"]))
        elif errors:
            hyps.append(Hypothesis(hypothesis_id="H1", likelihood="medium", statement="Transform raised errors",
                                   supporting_evidence=["F2"], suggested_recovery=["replay_failed_batches"]))
        if (obs["failed_batches_waiting"] or 0) > 0:
            facts.append({"fact_id": f"F{len(facts) + 1}", "kind": "failed_batches",
                          "count": obs["failed_batches_waiting"]})
        return DiagnosisParts(facts=facts, hypotheses=hyps,
                              further_checks=["Inspect one failed-batch message for its shard/sequence range"]
                              if obs["failed_batches_waiting"] else [])

    def recovery_options(self, rt: Runtime, incident: Any) -> list[RecoveryOption]:
        verify = PlanStep(step_id="verify", action="verify", description="Verify", params={"window": None})
        return [
            RecoveryOption("replay_failed_batches", "Replay batches recorded in the failure queue from the stream",
                           [PlanStep(step_id="replay", action="lambda.replay_failed_batches",
                                     description="Re-put failed sequence ranges onto the stream",
                                     reversible=False, compensation="Conditional writes make replay idempotent"),
                            verify],
                           notes=["Only records still within stream retention can be replayed."]),
            RecoveryOption("restore_template", "Re-deploy last applied template", [verify],
                           needs_template_restore=True),
        ]


def _replay_failed(session: Any, rt: Runtime) -> dict[str, Any]:
    sqs = session.client("sqs", config=BOTO_CONFIG)
    kinesis = session.client("kinesis", config=BOTO_CONFIG)
    url = rt.flow.outputs["FailedBatchesQueueUrl"]
    stream = rt.flow.outputs["StreamName"]
    replayed, messages = 0, 0
    while True:
        msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1).get("Messages", [])
        if not msgs:
            break
        for msg in msgs:
            info = json.loads(msg["Body"]).get("KinesisBatchInfo", {})
            it = kinesis.get_shard_iterator(StreamName=stream, ShardId=info["shardId"],
                                            ShardIteratorType="AT_SEQUENCE_NUMBER",
                                            StartingSequenceNumber=info["startSequenceNumber"])["ShardIterator"]
            last = int(info["endSequenceNumber"])
            while it:
                resp = kinesis.get_records(ShardIterator=it, Limit=1000)
                recs = [r for r in resp["Records"] if int(r["SequenceNumber"]) <= last]
                if recs:
                    kinesis.put_records(StreamName=stream, Records=[{"Data": r["Data"],
                                                                     "PartitionKey": r["PartitionKey"]} for r in recs])
                    replayed += len(recs)
                if len(recs) < len(resp["Records"]) or not resp["Records"]:
                    break
                it = resp.get("NextShardIterator")
            sqs.delete_message(QueueUrl=url, ReceiptHandle=msg["ReceiptHandle"])
            messages += 1
    return {"failed_batches": messages, "replayed_records": replayed}
