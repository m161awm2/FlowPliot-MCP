"""Pieces shared by adapters whose output is an S3 table read through Athena."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from pmcp.adapters.base import RenderContext, Runtime, athena_query, unverified
from pmcp.aws.client import BOTO_CONFIG
from pmcp.domain.contract import DataContractBody
from pmcp.domain.records import Check
from pmcp.errors import PmcpError
from pmcp.ids import short

ATHENA_ACTIONS = [
    "athena:StartQueryExecution",
    "athena:GetQueryExecution",
    "athena:GetQueryResults",
    "athena:StopQueryExecution",
    "athena:GetWorkGroup",
]


def workgroup(ctx: RenderContext, logical: str, name: str, results_prefix: str, cutoff_bytes: int) -> dict[str, Any]:
    return {
        logical: {
            "Type": "AWS::Athena::WorkGroup",
            "Properties": {
                "Name": name,
                "RecursiveDeleteOption": True,
                "WorkGroupConfiguration": {
                    "EnforceWorkGroupConfiguration": True,
                    "PublishCloudWatchMetricsEnabled": True,
                    "BytesScannedCutoffPerQuery": cutoff_bytes,
                    "EngineVersion": {"SelectedEngineVersion": "Athena engine version 3"},
                    "ResultConfiguration": {"OutputLocation": f"s3://{ctx.data_bucket}/athena-results/{results_prefix}/"},
                },
                "Tags": [{"Key": k, "Value": v} for k, v in ctx.tags.items()],
            },
            "DependsOn": ["DataBucket"],
        }
    }


def consumer_s3_table_statements(ctx: RenderContext, database: str, table: str, data_prefix: str) -> list[dict]:
    acct, region = ctx.account_id, ctx.region
    data_arn = f"arn:aws:s3:::{ctx.data_bucket}"
    return [
        {"Effect": "Allow", "Action": ATHENA_ACTIONS,
         "Resource": f"arn:aws:athena:{region}:{acct}:workgroup/{ctx.name('consumer')}"},
        {"Effect": "Allow", "Action": ["glue:GetDatabase", "glue:GetTable", "glue:GetPartitions"],
         "Resource": [f"arn:aws:glue:{region}:{acct}:catalog", f"arn:aws:glue:{region}:{acct}:database/{database}",
                      f"arn:aws:glue:{region}:{acct}:table/{database}/{table}"]},
        {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": f"{data_arn}/{data_prefix}*"},
        {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject"],
         "Resource": f"{data_arn}/athena-results/consumer/*"},
        {"Effect": "Allow", "Action": ["s3:ListBucket", "s3:GetBucketLocation"], "Resource": data_arn},
    ]


def glue_columns(body: DataContractBody) -> list[dict[str, str]]:
    from pmcp.adapters.athena_iceberg import ATHENA_TYPES

    return [{"Name": c.name, "Type": ATHENA_TYPES[c.type]} for c in body.output.columns]


def schema_check(rt: Runtime, database: str, table: str) -> tuple[Check, dict[str, str] | None]:
    from pmcp.adapters.athena_iceberg import ATHENA_TYPES, _type_match

    assert rt.contract is not None
    glue = rt.reader.client("glue", config=BOTO_CONFIG)
    try:
        t = glue.get_table(DatabaseName=database, Name=table)["Table"]
    except Exception as exc:
        return Check(check_id="schema", category="schema", status="failed",
                     description="Output table exists with contract schema", observed=str(exc)[:200]), None
    actual = {c["Name"]: c["Type"] for c in t["StorageDescriptor"]["Columns"]}
    expected = {c.name: ATHENA_TYPES[c.type] for c in rt.contract.body.output.columns}
    mism = {k: v for k, v in expected.items() if not _type_match(v, actual.get(k))}
    return Check(check_id="schema", category="schema", status="failed" if mism else "passed",
                 description="Output columns and types match the contract", expected=expected, observed=actual,
                 reason=f"mismatched: {mism}" if mism else None), actual


def consumer_athena_check(rt: Runtime, sql: str, workgroup_name: str, database: str) -> Check:
    role_arn = rt.flow.outputs.get("ConsumerRoleArn")
    if not role_arn:
        return unverified("consumer_read", "consumer_read", "Read as consumer", "no consumer role output")
    try:
        r = athena_query(rt.consumer(role_arn), sql, workgroup=workgroup_name, database=database, timeout_s=120)
    except PmcpError as exc:
        return Check(check_id="consumer_read", category="consumer_read", status="failed",
                     description=f"Query as {role_arn}", reason=exc.message)
    ok = r["state"] == "SUCCEEDED" and len(r["rows"]) > 0
    return Check(check_id="consumer_read", category="consumer_read", status="passed" if ok else "failed",
                 description=f"Query as consumer role {role_arn}", expected=">= 1 row", observed=len(r["rows"]),
                 evidence=[f"athena:{r['query_id']}"], reason=r.get("error"))


def metric_sum(rt: Runtime, namespace: str, name: str, dims: list[dict[str, str]], start: datetime,
               end: datetime, stat: str = "Sum") -> float | None:
    cw = rt.reader.client("cloudwatch", config=BOTO_CONFIG)
    span = max(60, int((end - start).total_seconds()))
    period = min(86400, -(-span // 60) * 60)
    resp = cw.get_metric_statistics(Namespace=namespace, MetricName=name, Dimensions=dims, StartTime=start,
                                    EndTime=end, Period=period, Statistics=[stat])
    pts = resp.get("Datapoints", [])
    if not pts:
        return None
    if stat == "Sum":
        return float(sum(p["Sum"] for p in pts))
    return float(max(p[stat] for p in pts))


def default_window(minutes: int = 60, lag_minutes: int = 10) -> dict[str, str]:
    from pmcp.ids import iso

    end = datetime.now(UTC) - timedelta(minutes=lag_minutes)
    return {"start": iso(end - timedelta(minutes=minutes)), "end": iso(end)}


def flow_short(rt: Runtime) -> str:
    return short(rt.flow.flow_id)
