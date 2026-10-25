"""Adapter interface.

The MCP layer and the planner only see this interface: pattern params are validated by the
adapter's own Pydantic model, and service specifics (Glue job args, Firehose buffering, Athena
workgroups) never leak into the common plan/flow/verification records.
"""

from __future__ import annotations

import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar

import boto3
from pydantic import BaseModel, ValidationError

from pmcp.aws.client import BOTO_CONFIG
from pmcp.aws.ownership import flow_tags
from pmcp.catalog.pricing import PriceBook
from pmcp.domain.contract import DataContract, DataContractBody
from pmcp.domain.records import Check, CostEstimate, Flow, Hypothesis, PlanStep
from pmcp.errors import ErrorCode, PmcpError
from pmcp.ids import short

if TYPE_CHECKING:
    from pmcp.domain.records import AccountConnection


@dataclass
class RenderContext:
    owner: str
    flow_id: str
    plan_id: str
    account_id: str
    region: str
    stack_name: str
    contract: DataContract
    connection: AccountConnection
    trusted_principals: list[str]
    boundary_policy_arn: str | None
    previous_params: dict[str, Any] | None = None
    previous_contract: DataContract | None = None

    @property
    def short(self) -> str:
        return short(self.flow_id)

    def name(self, suffix: str = "") -> str:
        return f"pmcp-{self.short}" + (f"-{suffix}" if suffix else "")

    def glue_name(self, suffix: str = "") -> str:
        return f"pmcp_{self.short}" + (f"_{suffix}" if suffix else "")

    @property
    def data_bucket(self) -> str:
        return f"pmcp-{self.short}-{self.account_id}-{self.region}"

    @property
    def tags(self) -> dict[str, str]:
        return flow_tags(self.owner, self.flow_id)


@dataclass
class RenderedArtifact:
    name: str
    kind: str
    content: str | bytes
    # Key in the tenant artifacts bucket; content-addressed so a key never changes meaning.
    upload: bool = False


@dataclass
class Rendered:
    template: dict[str, Any]
    artifacts: list[RenderedArtifact] = field(default_factory=list)
    post_deploy_steps: list[PlanStep] = field(default_factory=list)
    desired_summary: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    schema_changes: list[str] = field(default_factory=list)
    breaking: bool = False
    reprocess_reason: str | None = None


@dataclass
class Runtime:
    """What a worker step or a read-only tool gets to talk to AWS for one flow."""

    reader: boto3.Session
    deployer: boto3.Session | None
    consumer: Any  # callable(role_arn) -> boto3.Session
    flow: Flow
    contract: DataContract | None

    def client(self, service: str, *, write: bool = False):
        session = self.deployer if write else self.reader
        if session is None:
            raise PmcpError(ErrorCode.WRITE_ACCESS_NOT_ENABLED, "this step needs the deployer role")
        return session.client(service, config=BOTO_CONFIG)


@dataclass
class DiagnosisParts:
    facts: list[dict[str, Any]]
    hypotheses: list[Hypothesis]
    further_checks: list[str]
    affected: dict[str, Any] = field(default_factory=dict)


@dataclass
class RecoveryOption:
    strategy: str
    description: str
    steps: list[PlanStep]
    needs_template_restore: bool = False
    data_rollback: str = "not_applicable"
    notes: list[str] = field(default_factory=list)
    # Pattern-param changes rendered into a new template (e.g. quarantined_paths), applied via a change set.
    param_updates: dict[str, Any] | None = None


class Adapter(ABC):
    pattern_id: ClassVar[str]
    Params: ClassVar[type[BaseModel]]

    def parse_params(self, raw: dict[str, Any], contract: DataContractBody) -> BaseModel:
        try:
            params = self.Params.model_validate(raw)
        except ValidationError as exc:
            raise PmcpError(
                ErrorCode.INVALID_INPUT,
                f"invalid pattern_params for {self.pattern_id}",
                details={"errors": json.loads(exc.json(include_url=False))},
                next_actions=[f"Call list_capabilities(pattern_id='{self.pattern_id}') for the params schema."],
            ) from exc
        self.check_supported(params, contract)
        return params

    def check_supported(self, params: Any, contract: DataContractBody) -> None:
        """Raise UNSUPPORTED_CONFIGURATION for combinations this adapter cannot deploy correctly."""

    @abstractmethod
    def render(self, ctx: RenderContext, params: Any) -> Rendered: ...

    @abstractmethod
    def estimate(self, contract: DataContractBody, params: Any, region: str, prices: PriceBook) -> CostEstimate: ...

    def reprocess_steps(self, ctx: RenderContext, params: Any, window: dict[str, str]) -> list[PlanStep]:
        return []

    def run_step(self, step: PlanStep, rt: Runtime, op_id: str) -> dict[str, Any]:
        raise PmcpError(ErrorCode.INTERNAL, f"{self.pattern_id} has no step action {step.action}")

    @abstractmethod
    def verify(self, rt: Runtime, window: dict[str, str] | None) -> tuple[dict[str, str], list[Check]]: ...

    @abstractmethod
    def observe(self, rt: Runtime) -> dict[str, Any]: ...

    @abstractmethod
    def diagnose(self, rt: Runtime, window: dict[str, str]) -> DiagnosisParts: ...

    def probe(self, rt: Runtime, incident_window: dict[str, str]) -> dict[str, Any]:
        return {"probes": [], "note": "no deep probes for this pattern"}

    def recovery_options(self, rt: Runtime, incident: Any) -> list[RecoveryOption]:
        return []


# --- shared helpers --------------------------------------------------------------------------------


def iam_role(
    ctx: RenderContext,
    logical: str,
    name: str,
    service_principal: str | None,
    statements: list[dict[str, Any]],
    *,
    trust_platform: bool = False,
) -> dict[str, Any]:
    principals: dict[str, Any] = {}
    if service_principal:
        principals["Service"] = service_principal
    trust: list[dict[str, Any]] = []
    if service_principal:
        trust.append({"Effect": "Allow", "Principal": {"Service": service_principal}, "Action": "sts:AssumeRole"})
    if trust_platform:
        aws_principals = ctx.trusted_principals or [f"arn:aws:iam::{ctx.account_id}:root"]
        trust.append(
            {
                "Effect": "Allow",
                "Principal": {"AWS": aws_principals},
                "Action": "sts:AssumeRole",
                "Condition": {"StringEquals": {"sts:ExternalId": ctx.connection.external_id}},
            }
        )
    props: dict[str, Any] = {
        "RoleName": name,
        "AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": trust},
        "Policies": [
            {"PolicyName": "pmcp", "PolicyDocument": {"Version": "2012-10-17", "Statement": statements}}
        ],
        "Tags": [{"Key": k, "Value": v} for k, v in ctx.tags.items()],
    }
    if ctx.boundary_policy_arn:
        props["PermissionsBoundary"] = ctx.boundary_policy_arn
    return {logical: {"Type": "AWS::IAM::Role", "Properties": props}}


def tags_list(ctx: RenderContext) -> list[dict[str, str]]:
    return [{"Key": k, "Value": v} for k, v in ctx.tags.items()]


def data_bucket(ctx: RenderContext, logical: str = "DataBucket") -> dict[str, Any]:
    return {
        logical: {
            "Type": "AWS::S3::Bucket",
            "DeletionPolicy": "Retain",
            "UpdateReplacePolicy": "Retain",
            "Properties": {
                "BucketName": ctx.data_bucket,
                "BucketEncryption": {
                    "ServerSideEncryptionConfiguration": [{"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]
                },
                "PublicAccessBlockConfiguration": {
                    "BlockPublicAcls": True,
                    "BlockPublicPolicy": True,
                    "IgnorePublicAcls": True,
                    "RestrictPublicBuckets": True,
                },
                "OwnershipControls": {"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]},
                "LifecycleConfiguration": {
                    "Rules": [
                        {"Id": "athena-results", "Status": "Enabled", "Prefix": "athena-results/",
                         "ExpirationInDays": 7},
                        {"Id": "errors", "Status": "Enabled", "Prefix": "errors/", "ExpirationInDays": 30},
                    ]
                },
                "Tags": tags_list(ctx),
            },
        }
    }


def alerts_topic(ctx: RenderContext, email: str | None) -> dict[str, Any]:
    res: dict[str, Any] = {
        "AlertsTopic": {"Type": "AWS::SNS::Topic", "Properties": {"TopicName": ctx.name("alerts"),
                                                                   "Tags": tags_list(ctx)}}
    }
    if email:
        res["AlertsEmail"] = {
            "Type": "AWS::SNS::Subscription",
            "Properties": {"TopicArn": {"Ref": "AlertsTopic"}, "Protocol": "email", "Endpoint": email},
        }
    return res


def alarm(
    ctx: RenderContext,
    logical: str,
    suffix: str,
    namespace: str,
    metric: str,
    dimensions: list[dict[str, Any]],
    *,
    statistic: str = "Sum",
    period: int = 300,
    threshold: float = 1,
    comparison: str = "GreaterThanOrEqualToThreshold",
    missing: str = "notBreaching",
    description: str = "",
) -> dict[str, Any]:
    return {
        logical: {
            "Type": "AWS::CloudWatch::Alarm",
            "Properties": {
                "AlarmName": ctx.name(suffix),
                "AlarmDescription": description or f"pmcp flow {ctx.flow_id}: {suffix}",
                "Namespace": namespace,
                "MetricName": metric,
                "Dimensions": dimensions,
                "Statistic": statistic,
                "Period": period,
                "EvaluationPeriods": 1,
                "Threshold": threshold,
                "ComparisonOperator": comparison,
                "TreatMissingData": missing,
                "AlarmActions": [{"Ref": "AlertsTopic"}],
                "OKActions": [{"Ref": "AlertsTopic"}],
            },
        }
    }


def template(description: str, resources: dict[str, Any], outputs: dict[str, Any]) -> dict[str, Any]:
    return {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": description,
        "Resources": resources,
        "Outputs": {k: {"Value": v} for k, v in outputs.items()},
    }


def s3_parts(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise PmcpError(ErrorCode.INVALID_INPUT, f"expected s3:// uri, got {uri}")
    bucket, _, key = uri[5:].partition("/")
    return bucket, key


def athena_query(
    session: boto3.Session,
    sql: str,
    *,
    workgroup: str,
    database: str | None = None,
    timeout_s: int = 120,
    max_rows: int = 1000,
) -> dict[str, Any]:
    """Run one Athena statement to completion. Returns {query_id, state, rows, columns, error, scanned_bytes}."""
    athena = session.client("athena", config=BOTO_CONFIG)
    kwargs: dict[str, Any] = {"QueryString": sql, "WorkGroup": workgroup}
    if database:
        kwargs["QueryExecutionContext"] = {"Database": database}
    qid = athena.start_query_execution(**kwargs)["QueryExecutionId"]
    deadline = time.time() + timeout_s
    delay = 0.5
    while True:
        qe = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = qe["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        if time.time() > deadline:
            athena.stop_query_execution(QueryExecutionId=qid)
            return {"query_id": qid, "state": "TIMEOUT", "rows": [], "columns": [], "error": "timeout"}
        time.sleep(delay)
        delay = min(delay * 1.5, 3)
    out: dict[str, Any] = {
        "query_id": qid,
        "state": state,
        "rows": [],
        "columns": [],
        "error": qe["Status"].get("StateChangeReason"),
        "scanned_bytes": qe.get("Statistics", {}).get("DataScannedInBytes"),
    }
    if state == "SUCCEEDED" and qe.get("StatementType") in ("DML", None):
        res = athena.get_query_results(QueryExecutionId=qid, MaxResults=min(max_rows + 1, 1000))
        cols = [c["Name"] for c in res["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
        rows = [[d.get("VarCharValue") for d in r["Data"]] for r in res["ResultSet"]["Rows"]]
        if rows and rows[0] == cols:
            rows = rows[1:]
        out["columns"] = cols
        out["rows"] = [dict(zip(cols, r, strict=False)) for r in rows]
    return out


def unverified(check_id: str, category: str, description: str, reason: str) -> Check:
    return Check(check_id=check_id, category=category, status="unverified", description=description,  # type: ignore[arg-type]
                 reason=reason)
