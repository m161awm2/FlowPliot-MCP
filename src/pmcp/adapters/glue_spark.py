"""Pattern s3_glue_spark_batch: scheduled Glue (Spark) job that MERGEs a window of S3 data into Iceberg.

The agent supplies `transform(spark, source_df, window_start, window_end) -> DataFrame`; pmcp wraps it
with the source read and the Iceberg MERGE, so idempotency/upsert semantics stay the same as the Athena
pattern. Expected-state comparison (row_count/mutations) needs a SQL equivalent of the PySpark logic,
so those checks report `unverified` unless a contract-level row_count rule exists.
"""

from __future__ import annotations

import json
from datetime import UTC
from typing import Any, Literal

from pydantic import Field

from pmcp.adapters.athena_iceberg import ATHENA_TYPES, _merge_windows, _parse
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
from pmcp.adapters.codegen import validate_transform_code
from pmcp.adapters.common import (
    consumer_athena_check,
    consumer_s3_table_statements,
    flow_short,
    schema_check,
    workgroup,
)
from pmcp.catalog.pricing import PriceBook
from pmcp.domain.base import Model
from pmcp.domain.contract import DataContractBody
from pmcp.domain.records import Check, CostEstimate, CostLine, Hypothesis, PlanStep
from pmcp.errors import ErrorCode, PmcpError
from pmcp.ids import iso, sha256_hex


class GlueSparkParams(Model):
    source_uri: str
    source_format: Literal["json", "csv", "parquet"]
    transform_code: str = Field(
        description="PySpark: `def transform(spark, source_df, window_start, window_end):` returning a DataFrame "
        "with the contract's output columns (+ boolean pmcp_deleted for upsert_with_deletes). window_* are "
        "ISO-8601 strings. Imports: pyspark and pure stdlib only."
    )
    schedule_expression: str = "rate(1 day)"
    window_minutes: int = Field(1440, ge=10)
    workers: int = Field(2, ge=2, le=20)
    worker_type: Literal["G.1X", "G.2X"] = "G.1X"
    timeout_minutes: int = Field(60, ge=5, le=480)
    alert_email: str | None = None


SCRIPT = '''
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["window_start", "window_end"])
if args["window_start"].startswith("auto-"):  # scheduled run: start = end - N minutes
    from datetime import datetime, timedelta
    _end = datetime.fromisoformat(args["window_end"].replace("Z", "+00:00"))
    _mins = int(args["window_start"][5:].rstrip("m"))
    args["window_start"] = (_end - timedelta(minutes=_mins)).isoformat().replace("+00:00", "Z")
spark = SparkSession.builder.getOrCreate()
CFG = {cfg}

{user_code}

reader = spark.read.option("header", "true") if CFG["format"] == "csv" else spark.read
source_df = reader.format(CFG["format"]).load(CFG["source_uri"])
out = transform(spark, source_df, args["window_start"], args["window_end"])
cols = CFG["columns"]
for c, t in CFG["types"].items():
    out = out.withColumn(c, F.col(c).cast(t))
if CFG["key"]:
    w = Window.partitionBy(*CFG["key"]).orderBy(F.col(CFG["ordering"]).desc())
    out = out.withColumn("pmcp_rn", F.row_number().over(w)).filter("pmcp_rn = 1").drop("pmcp_rn")
out.createOrReplaceTempView("pmcp_s")
target = "glue_catalog." + CFG["database"] + "." + CFG["table"]
if not CFG["key"]:
    tc = CFG["time_col"]
    spark.sql(f"DELETE FROM {{target}} WHERE {{tc}} >= timestamp'{{args['window_start']}}' "
              f"AND {{tc}} < timestamp'{{args['window_end']}}'")
    spark.sql(f"INSERT INTO {{target}} SELECT {{', '.join(cols)}} FROM pmcp_s")
else:
    on = " AND ".join(f"t.{{k}} = s.{{k}}" for k in CFG["key"])
    o = CFG["ordering"]
    upd = ", ".join(f"t.{{c}} = s.{{c}}" for c in cols if c not in CFG["key"])
    clauses = []
    if CFG["mode"] == "upsert_with_deletes":
        clauses.append(f"WHEN MATCHED AND s.pmcp_deleted AND s.{{o}} >= t.{{o}} THEN DELETE")
    if CFG["mode"] != "append" and upd:
        clauses.append(f"WHEN MATCHED AND s.{{o}} >= t.{{o}} THEN UPDATE SET {{upd}}")
    nd = " AND coalesce(s.pmcp_deleted, false) = false" if CFG["mode"] == "upsert_with_deletes" else ""
    clauses.append(f"WHEN NOT MATCHED{{nd}} THEN INSERT ({{', '.join(cols)}}) "
                   f"VALUES ({{', '.join('s.' + c for c in cols)}})")
    spark.sql(f"MERGE INTO {{target}} t USING pmcp_s s ON {{on}} " + " ".join(clauses))
'''

SPARK_TYPES = {"string": "string", "int": "int", "bigint": "bigint", "double": "double",
               "decimal(18,2)": "decimal(18,2)", "decimal(38,9)": "decimal(38,9)", "boolean": "boolean",
               "date": "date", "timestamp": "timestamp"}


def _n(s: str) -> dict[str, str]:
    return {"database": f"pmcp_{s}", "job": f"pmcp-{s}-job", "state_machine": f"pmcp-{s}-run",
            "schedule": f"pmcp-{s}-schedule", "workgroup": f"pmcp-{s}", "consumer_workgroup": f"pmcp-{s}-consumer"}


class GlueSparkAdapter(Adapter):
    pattern_id = "s3_glue_spark_batch"
    Params = GlueSparkParams

    def check_supported(self, p: GlueSparkParams, contract: DataContractBody) -> None:
        problems = validate_transform_code(p.transform_code, extra_imports=frozenset({"pyspark"}), arity=4)
        if not p.source_uri.startswith("s3://"):
            problems.append("source_uri must be s3://")
        if not contract.mutation.key and not contract.freshness.measured_on:
            problems.append("append without key needs freshness.measured_on for idempotent window replace")
        if problems:
            raise PmcpError(ErrorCode.UNSUPPORTED_CONFIGURATION, "pattern_params cannot be deployed as given",
                            details={"problems": problems})

    def render(self, ctx: RenderContext, p: GlueSparkParams) -> Rendered:
        body = ctx.contract.body
        n = _n(ctx.short)
        acct, region = ctx.account_id, ctx.region
        src_bucket, src_prefix = s3_parts(p.source_uri)
        data_arn = f"arn:aws:s3:::{ctx.data_bucket}"
        artifacts_bucket = f"pmcp-artifacts-{acct}-{region}"
        cfg = {
            "format": p.source_format, "source_uri": p.source_uri, "database": n["database"],
            "table": body.output.name, "columns": [c.name for c in body.output.columns],
            "types": {c.name: SPARK_TYPES[c.type] for c in body.output.columns},
            "key": body.mutation.key, "ordering": body.mutation.ordering_column or (body.mutation.key or [""])[0],
            "mode": body.mutation.mode, "time_col": body.freshness.measured_on,
        }
        script = SCRIPT.format(cfg=json.dumps(cfg), user_code=p.transform_code.strip()).lstrip()
        script_key = f"pmcp/artifacts/{sha256_hex(script)}.py"
        res: dict[str, Any] = {}
        res.update(data_bucket(ctx))
        res.update(alerts_topic(ctx, p.alert_email))
        res["Database"] = {"Type": "AWS::Glue::Database",
                           "Properties": {"CatalogId": acct, "DatabaseInput": {"Name": n["database"]}}}
        res.update(workgroup(ctx, "Workgroup", n["workgroup"], "pipeline", 10 * 1024**3))
        res.update(workgroup(ctx, "ConsumerWorkgroup", n["consumer_workgroup"], "consumer", 1024**3))
        res.update(iam_role(ctx, "JobRole", ctx.name("job"), "glue.amazonaws.com", [
            {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": [f"arn:aws:s3:::{src_bucket}/{src_prefix}*",
                                                                         f"arn:aws:s3:::{artifacts_bucket}/pmcp/*"]},
            {"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": f"arn:aws:s3:::{src_bucket}"},
            {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
             "Resource": [data_arn, f"{data_arn}/*"]},
            {"Effect": "Allow", "Action": ["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:UpdateTable",
                                           "glue:CreateTable", "glue:GetPartitions"],
             "Resource": [f"arn:aws:glue:{region}:{acct}:catalog",
                          f"arn:aws:glue:{region}:{acct}:database/{n['database']}",
                          f"arn:aws:glue:{region}:{acct}:table/{n['database']}/*"]},
            {"Effect": "Allow", "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
             "Resource": f"arn:aws:logs:{region}:{acct}:log-group:/aws-glue/*"},
            {"Effect": "Allow", "Action": ["cloudwatch:PutMetricData"], "Resource": "*",
             "Condition": {"StringEquals": {"cloudwatch:namespace": "Glue"}}},
        ]))
        res["Job"] = {
            "Type": "AWS::Glue::Job",
            "Properties": {
                "Name": n["job"],
                "Role": {"Fn::GetAtt": ["JobRole", "Arn"]},
                "GlueVersion": "5.0",
                "WorkerType": p.worker_type,
                "NumberOfWorkers": p.workers,
                "Timeout": p.timeout_minutes,
                "MaxRetries": 0,
                "ExecutionProperty": {"MaxConcurrentRuns": 1},
                "Command": {"Name": "glueetl", "PythonVersion": "3",
                            "ScriptLocation": f"s3://{artifacts_bucket}/{script_key}"},
                "DefaultArguments": {
                    "--datalake-formats": "iceberg",
                    "--enable-metrics": "true",
                    "--conf": (
                        "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
                        "--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog "
                        f"--conf spark.sql.catalog.glue_catalog.warehouse=s3://{ctx.data_bucket}/tables/ "
                        "--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog "
                        "--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
                    ),
                },
                "Tags": dict(ctx.tags),
            },
        }
        definition = {
            "StartAt": "Run",
            "States": {
                "Run": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {
                        "JobName": n["job"],
                        "Arguments": {"--window_start.$": "$.window_start", "--window_end.$": "$.window_end"},
                    },
                    "Retry": [{"ErrorEquals": ["Glue.ConcurrentRunsExceededException"], "IntervalSeconds": 60,
                               "MaxAttempts": 5, "BackoffRate": 1.5}],
                    "End": True,
                }
            },
        }
        res.update(iam_role(ctx, "RunRole", ctx.name("run"), "states.amazonaws.com", [
            {"Effect": "Allow", "Action": ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns",
                                           "glue:BatchStopJobRun"],
             "Resource": f"arn:aws:glue:{region}:{acct}:job/{n['job']}"},
        ]))
        res["StateMachine"] = {"Type": "AWS::StepFunctions::StateMachine", "Properties": {
            "StateMachineName": n["state_machine"], "RoleArn": {"Fn::GetAtt": ["RunRole", "Arn"]},
            "DefinitionString": json.dumps(definition),
            "Tags": [{"Key": k, "Value": v} for k, v in ctx.tags.items()]}}
        res.update(iam_role(ctx, "SchedulerRole", ctx.name("sched"), "scheduler.amazonaws.com",
                            [{"Effect": "Allow", "Action": "states:StartExecution",
                              "Resource": {"Ref": "StateMachine"}}]))
        # Scheduler cannot compute window_start, so scheduled runs pass "auto-<N>m" and the job derives it.
        res["Schedule"] = {"Type": "AWS::Scheduler::Schedule", "Properties": {
            "Name": n["schedule"], "ScheduleExpression": p.schedule_expression, "ScheduleExpressionTimezone": "UTC",
            "FlexibleTimeWindow": {"Mode": "OFF"},
            "Target": {"Arn": {"Ref": "StateMachine"}, "RoleArn": {"Fn::GetAtt": ["SchedulerRole", "Arn"]},
                       "Input": json.dumps({"window_start": f"auto-{p.window_minutes}m",
                                            "window_end": "<aws.scheduler.scheduled-time>"})}}}
        res.update(iam_role(ctx, "ConsumerRole", ctx.name("consumer"), None,
                            consumer_s3_table_statements(ctx, n["database"], body.output.name,
                                                         f"tables/{body.output.name}/"), trust_platform=True))
        sm_dim = [{"Name": "StateMachineArn", "Value": {"Ref": "StateMachine"}}]
        res.update(alarm(ctx, "FailedRunsAlarm", "failed-runs", "AWS/States", "ExecutionsFailed", sm_dim))
        tmpl = template(f"pmcp flow {ctx.flow_id} ({self.pattern_id})", res, {
            "DataBucket": {"Ref": "DataBucket"}, "Database": n["database"], "OutputTable": body.output.name,
            "JobName": n["job"], "StateMachineArn": {"Ref": "StateMachine"},
            "ConsumerRoleArn": {"Fn::GetAtt": ["ConsumerRole", "Arn"]}, "Workgroup": n["workgroup"],
            "ConsumerWorkgroup": n["consumer_workgroup"], "AlertsTopicArn": {"Ref": "AlertsTopic"},
        })
        r = Rendered(template=tmpl)
        r.artifacts.append(RenderedArtifact(script_key, "python", script, upload=True))
        cols = ", ".join(f"`{c.name}` {ATHENA_TYPES[c.type]}" for c in body.output.columns)
        create = (f"CREATE TABLE IF NOT EXISTS `{n['database']}`.`{body.output.name}` ({cols}) "
                  f"LOCATION 's3://{ctx.data_bucket}/tables/{body.output.name}/' "
                  "TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet')")
        r.post_deploy_steps.append(PlanStep(step_id="create_tables", action="athena.ddl",
                                            description="Create Iceberg output table", params={"statements": [create]}))
        r.desired_summary = {"job": n["job"], "workers": f"{p.workers} x {p.worker_type}",
                             "schedule": p.schedule_expression, "output": f"{n['database']}.{body.output.name}"}
        if ctx.previous_params and ctx.previous_params.get("transform_code") != p.transform_code:
            r.reprocess_reason = "transform_code changed"
        r.warnings.append("The scheduled window_start is derived inside the job ('auto-<N>m'); explicit windows "
                          "are used for backfills.")
        return r

    def reprocess_steps(self, ctx: RenderContext, p: GlueSparkParams, window: dict[str, str]) -> list[PlanStep]:
        return [PlanStep(step_id="reprocess", action="sfn.run_window", description="Re-run the job over the window",
                         params={"window_start": window["start"], "window_end": window["end"], "chunk_hours": 24},
                         reversible=False)]

    def run_step(self, step: PlanStep, rt: Runtime, op_id: str) -> dict[str, Any]:
        from pmcp.adapters.athena_iceberg import AthenaIcebergAdapter

        n = _n(flow_short(rt))
        if step.action == "athena.ddl":
            session = rt.deployer or rt.reader
            ids = []
            for sql in step.params["statements"]:
                r = athena_query(session, sql, workgroup=n["workgroup"], database=n["database"], timeout_s=300)
                if r["state"] != "SUCCEEDED":
                    raise PmcpError(ErrorCode.AWS_ERROR, f"DDL failed: {r['error']}")
                ids.append(r["query_id"])
            return {"query_ids": ids}
        if step.action == "sfn.run_window":
            return AthenaIcebergAdapter()._run_windows(rt, rt.deployer or rt.reader, step, op_id)
        return super().run_step(step, rt, op_id)

    def estimate(self, body: DataContractBody, p: GlueSparkParams, region: str, prices: PriceBook) -> CostEstimate:
        from pmcp.adapters.athena_iceberg import _schedule_minutes

        runs = 30 * 1440 / (_schedule_minutes(p.schedule_expression) or 1440)
        dpu = prices.get("glue_dpu_hour", region)
        dpus = p.workers * (1 if p.worker_type == "G.1X" else 2)
        minutes = 5
        lines = [CostLine(item=f"Glue job ({dpus} DPU x ~{minutes} min/run, {runs:.0f} runs)",
                          quantity=round(runs * dpus * minutes / 60, 2), unit=dpu.unit, unit_price_usd=dpu.usd,
                          monthly_usd=round(runs * dpus * minutes / 60 * dpu.usd, 2), price_source=dpu.source,  # type: ignore[arg-type]
                          price_as_of=dpu.as_of),
                 CostLine(item="CloudWatch alarm", quantity=1, unit="alarm-month", unit_price_usd=0.1,
                          monthly_usd=0.1)]
        return CostEstimate(monthly_usd=round(sum(line.monthly_usd for line in lines), 2), lines=lines,
                            assumptions=[f"~{minutes} min per run including startup; 1-minute minimum billing"],
                            confidence="low")

    def verify(self, rt: Runtime, window: dict[str, str] | None) -> tuple[dict[str, str], list[Check]]:
        assert rt.contract is not None
        body = rt.contract.body
        n = _n(flow_short(rt))
        win = window or {"start": "", "end": iso()}
        checks: list[Check] = []
        sc, actual = schema_check(rt, n["database"], body.output.name)
        checks.append(sc)
        target = f'{n["database"]}."{body.output.name}"'
        r = athena_query(rt.reader, f"SELECT count(*) AS n FROM {target}", workgroup=n["workgroup"],
                         database=n["database"])
        total = int(r["rows"][0]["n"]) if r["state"] == "SUCCEEDED" else None
        checks.append(Check(check_id="exists", category="existence", status="passed" if total else "failed",
                            description="Output table holds rows", observed=total, evidence=[f"athena:{r['query_id']}"]))
        if body.mutation.key and actual:
            keys = ", ".join(f'"{k}"' for k in body.mutation.key)
            r = athena_query(rt.reader, f"SELECT count(*) AS dup FROM (SELECT {keys} FROM {target} GROUP BY {keys} "
                             "HAVING count(*) > 1)", workgroup=n["workgroup"], database=n["database"])
            dup = int(r["rows"][0]["dup"]) if r["state"] == "SUCCEEDED" else None
            checks.append(Check(check_id="keys_unique", category="keys", status="passed" if dup == 0 else "failed",
                                description="No duplicate keys", expected=0, observed=dup))
        checks.append(unverified("row_count", "row_count", "Expected rows for the window",
                                 "transform is PySpark; no SQL expectation available"))
        checks.append(unverified("mutations", "mutations", "Latest version / deletes",
                                 "transform is PySpark; no SQL expectation available"))
        checks.append(consumer_athena_check(rt, f"SELECT * FROM {target} LIMIT 5", n["consumer_workgroup"],
                                            n["database"]))
        return win, checks

    def observe(self, rt: Runtime) -> dict[str, Any]:
        n = _n(flow_short(rt))
        runs = rt.client("glue").get_job_runs(JobName=n["job"], MaxResults=10)["JobRuns"]
        return {"recent_runs": [{"id": r["Id"], "state": r["JobRunState"], "started": str(r.get("StartedOn")),
                                 "seconds": r.get("ExecutionTime")} for r in runs]}

    def diagnose(self, rt: Runtime, window: dict[str, str]) -> DiagnosisParts:
        n = _n(flow_short(rt))
        start, end = _parse(window["start"]), _parse(window["end"])
        facts = []
        for r in rt.client("glue").get_job_runs(JobName=n["job"], MaxResults=50)["JobRuns"]:
            started = r.get("StartedOn")
            if r["JobRunState"] in ("FAILED", "TIMEOUT", "ERROR") and started and \
                    start <= started.astimezone(UTC) <= end:
                facts.append({"fact_id": f"F{len(facts) + 1}", "kind": "failed_run", "run_id": r["Id"],
                              "state": r["JobRunState"], "error_excerpt": (r.get("ErrorMessage") or "")[:400],
                              "window": {"start": r.get("Arguments", {}).get("--window_start"),
                                         "end": r.get("Arguments", {}).get("--window_end")}})
        text = " ".join(f.get("error_excerpt", "") for f in facts).lower()
        hyps = []
        if "accessdenied" in text or "access denied" in text:
            hyps.append(Hypothesis(hypothesis_id="H1", likelihood="high", statement="Missing permission",
                                   supporting_evidence=[f["fact_id"] for f in facts],
                                   suggested_recovery=["restore_template", "rerun_window"]))
        elif "analysisexception" in text or "cannot resolve" in text:
            hyps.append(Hypothesis(hypothesis_id="H1", likelihood="high",
                                   statement="Source schema no longer matches the transform",
                                   supporting_evidence=[f["fact_id"] for f in facts], suggested_recovery=["change_plan"]))
        elif facts:
            hyps.append(Hypothesis(hypothesis_id="H1", likelihood="low", statement="Job failure; see excerpts",
                                   supporting_evidence=[f["fact_id"] for f in facts], suggested_recovery=["rerun_window"]))
        windows = [f["window"] for f in facts if f["window"]["start"] and not f["window"]["start"].startswith("auto")]
        return DiagnosisParts(facts=facts, hypotheses=hyps, further_checks=["Read the job's driver log in "
                                                                           "/aws-glue/jobs/error"] if facts else [],
                              affected={"windows": _merge_windows(windows)})

    def recovery_options(self, rt: Runtime, incident: Any) -> list[RecoveryOption]:
        windows = incident.affected.get("windows") or [incident.window]
        steps = [PlanStep(step_id=f"rerun_{i}", action="sfn.run_window", description=f"Re-run {w['start']}..{w['end']}",
                          params={"window_start": w["start"], "window_end": w["end"], "chunk_hours": 24},
                          reversible=False) for i, w in enumerate(windows)]
        verify = PlanStep(step_id="verify", action="verify", description="Verify", params={"window": None})
        return [RecoveryOption("rerun_window", "Re-run failed windows", steps + [verify]),
                RecoveryOption("restore_template", "Re-deploy last template then re-run", steps + [verify],
                               needs_template_restore=True)]

