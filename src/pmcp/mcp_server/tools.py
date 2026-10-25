"""The MCP tool surface (20 tools). Each tool is a thin shell: resolve the caller, rate-limit, call a
service function off the event loop, shape the result, map errors to a JSON error payload."""

from __future__ import annotations

import functools
import threading
import time
from collections import defaultdict, deque
from collections.abc import Callable
from typing import Annotated, Any, Literal

from botocore.exceptions import BotoCoreError, ClientError
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp_types import ToolAnnotations
from pydantic import Field

from pmcp.app import App
from pmcp.aws.client import run_sync
from pmcp.catalog.patterns import Requirements
from pmcp.domain.base import Model
from pmcp.domain.contract import DataContractBody
from pmcp.domain.records import (
    AccountConnection,
    Flow,
    Incident,
    Operation,
    Plan,
    Verification,
)
from pmcp.errors import ErrorCode, PmcpError, from_boto
from pmcp.mcp_server import views as v
from pmcp.services import contracts, design, investigate, ops, planning, verification
from pmcp.services.operations import lease_alive

AccountId = Annotated[str, Field(pattern=r"^\d{12}$", description="Connected AWS account id (see list_workspace).")]
Region = Annotated[str, Field(pattern=r"^[a-z]{2}(-gov)?-[a-z]+-\d$", description="AWS region, e.g. ap-southeast-2.")]
IdemKey = Annotated[str, Field(min_length=8, max_length=128, description="Caller-chosen key; re-sending the same key "
                                                                          "returns (or resumes) the same operation.")]


class Window(Model):
    start: str = Field(description="ISO-8601 UTC, inclusive, e.g. 2026-10-25T00:00:00Z")
    end: str = Field(description="ISO-8601 UTC, exclusive")


class Candidate(Model):
    pattern_id: str
    pattern_params: dict[str, Any] | None = Field(None, description="Omit to judge fitness only.")


class RateLimiter:
    def __init__(self, per_minute: int) -> None:
        self.per_minute = per_minute
        self.hits: dict[str, deque[float]] = defaultdict(deque)
        self.lock = threading.Lock()

    def check(self, who: str) -> None:
        now = time.time()
        with self.lock:
            q = self.hits[who]
            while q and q[0] < now - 60:
                q.popleft()
            if len(q) >= self.per_minute:
                raise PmcpError(ErrorCode.RATE_LIMITED, f"more than {self.per_minute} calls/minute",
                                details={"retry_after_seconds": int(60 - (now - q[0])) + 1})
            q.append(now)


def register(server: MCPServer, app: App) -> None:  # noqa: C901 - one place that lists every tool
    # One limiter per process (shared by all server instances built from this app).
    limiter = app.__dict__.setdefault("rate_limiter", RateLimiter(app.settings.rate_limit_per_minute))

    def owner() -> str:
        if app.settings.mode == "local":
            return app.settings.local_user_id
        from mcp.server.auth.middleware.auth_context import get_access_token

        tok = get_access_token()
        if tok is None or not tok.subject:
            raise PmcpError(ErrorCode.FORBIDDEN, "no authenticated user")
        return tok.subject

    def tool(name: str, title: str, *, read_only: bool, destructive: bool = False, idempotent: bool = False,
             open_world: bool = True) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def deco(fn: Callable[..., Any]) -> Callable[..., Any]:
            @functools.wraps(fn)
            async def wrapper(*args: Any, **kwargs: Any) -> Any:
                try:
                    limiter.check(owner())
                    return await fn(*args, **kwargs)
                except PmcpError as exc:
                    raise ToolError(exc.to_json()) from exc
                except (ClientError, BotoCoreError) as exc:
                    raise ToolError(from_boto(exc, action=name).to_json()) from exc

            server.tool(name=name, title=title, annotations=ToolAnnotations(
                title=title, readOnlyHint=read_only, destructiveHint=destructive, idempotentHint=idempotent,
                openWorldHint=open_world))(wrapper)
            return wrapper

        return deco

    # ------------------------------------------------------------------ workspace
    @tool("list_workspace", "List connected accounts, flows, plans, operations", read_only=True, open_world=False)
    async def list_workspace() -> v.Workspace:
        """Start here in a new session. Lists the caller's connected AWS accounts (with regions, write access and
        cost caps), deployed flows, plans waiting for approval, recent operations and open incidents, so you can
        find ids for the other tools. Reads pmcp state only; it does not call AWS (use inspect_aws_environment
        for that)."""
        o = owner()

        def run() -> v.Workspace:
            s = app.store
            plans = [p for p in s.list(Plan, o, limit=30) if p.status in ("draft", "previewed", "approved")]
            return v.Workspace(
                accounts=[{"account_id": c.account_id, "status": c.status, "write_enabled": c.write_enabled,
                           "allowed_regions": c.allowed_regions, "monthly_cost_cap_usd": c.monthly_cost_cap_usd,
                           "mode": c.mode} for c in s.list(AccountConnection, o)],
                flows=[{"flow_id": f.flow_id, "name": f.name, "status": f.status, "pattern_id": f.pattern_id,
                        "account_id": f.account_id, "region": f.region,
                        "contract": [f.contract_id, f.contract_version]} for f in s.list(Flow, o, limit=100)],
                pending_plans=[{"plan_id": p.plan_id, "kind": p.kind, "status": p.status, "summary": p.summary,
                                "expires_at": p.expires_at} for p in plans],
                recent_operations=[{"operation_id": x.operation_id, "kind": x.kind, "status": x.status,
                                    "flow_id": x.flow_id, "updated_at": x.updated_at}
                                   for x in s.list(Operation, o, limit=10)],
                open_incidents=[{"incident_id": i.incident_id, "flow_id": i.flow_id, "status": i.status}
                                for i in s.list(Incident, o, limit=20) if i.status != "resolved"],
                connect_url=None if app.settings.mode == "local" else f"{app.settings.public_base_url}/connect",
            )

        return await run_sync(run)

    # ------------------------------------------------------------------ investigation
    @tool("inspect_repository", "Scan producer code", read_only=True, idempotent=True)
    async def inspect_repository(
        source: Annotated[str, Field(description="https:// git URL, or s3://bucket/key.(zip|tar.gz) in a connected "
                                                 "account; a local directory only in local mode.")],
        ref: Annotated[str | None, Field(description="Branch or tag; default branch if omitted.")] = None,
        account_id: Annotated[str | None, Field(description="Needed for s3:// archives or token_secret_arn.")] = None,
        region: Annotated[str | None, Field(description="Region of the s3:// archive.")] = None,
        token_secret_arn: Annotated[str | None, Field(
            description="Secrets Manager ARN (name pmcp/*) in the connected account holding a git token, for "
                        "private repos.")] = None,
    ) -> v.RepositoryReport:
        """Find how the producer emits data: SDK calls that write to Kinesis/Firehose/S3/Kafka/SQS/DynamoDB/SQL
        (file:line), resource names, likely event fields, schema and sample files, IaC resources. Use before
        profile_source when the data's origin is code. Pattern matching only; confirm findings in the code."""
        r = await run_sync(investigate.inspect_repository, app, owner(), source, ref, account_id, region,
                           token_secret_arn)
        return v.RepositoryReport(**r, next_actions=[
            "profile_source on the destinations the producer writes to",
            "define_data_contract once outputs/rules are agreed with the user"])

    @tool("inspect_aws_environment", "Inventory an AWS account/region", read_only=True, idempotent=True)
    async def inspect_aws_environment(
        account_id: AccountId, region: Region,
        categories: Annotated[list[Literal["storage", "catalog", "streaming", "databases", "warehouses", "compute",
                                           "orchestration", "governance", "pmcp"]] | None,
                              Field(description="Subset to inspect; all if omitted.")] = None,
    ) -> v.EnvironmentReport:
        """Inventory reusable resources in a connected account: S3 buckets, Glue tables, Kinesis/Firehose/MSK,
        DynamoDB/RDS/OpenSearch, Redshift, Lambda/Glue jobs, Step Functions/schedules, Lake Formation mode
        (decides lake_formation_grants) and existing pmcp stacks. Names and metadata only, no data. For data
        shape and volume use profile_source."""
        r = await run_sync(investigate.inspect_environment, app, owner(), account_id, region, categories)
        errors = r.pop("errors")
        return v.EnvironmentReport(account_id=r.pop("account_id"), region=r.pop("region"), inventory=r,
                                   errors=errors, next_actions=["profile_source on candidate sources",
                                                                "inspect_consumer_contract on existing targets"])

    @tool("profile_source", "Profile source data", read_only=True, idempotent=True)
    async def profile_source(
        account_id: AccountId, region: Region,
        source_uri: Annotated[str, Field(description="s3://bucket/prefix/, glue:<db>.<table>, kinesis:<stream> or "
                                                     "dynamodb:<table>")],
        sample_records: Annotated[int, Field(ge=10, le=2000)] = 200,
        include_raw_samples: Annotated[bool, Field(description="Return 5 unmasked records. Only when the user "
                                                               "explicitly asks; default returns masked values.")] = False,
    ) -> v.SourceProfile:
        """Measure a source: object counts/bytes per day (growth), formats, hive partitions, inferred columns and
        types, null rates, key/event-time/delete-marker candidates, duplicates and newest event time, from a
        bounded sample. Values are masked unless include_raw_samples=true. Output columns plug into
        define_data_contract (sources[].columns) and pattern_params.source_columns."""
        r = await run_sync(investigate.profile_source, app, owner(), account_id, region, source_uri, sample_records,
                           include_raw_samples)
        cols = r.get("columns") or r.get("storage", {}).get("columns") or []
        suggested = [{"name": c["name"], "type": c["type"] if c["type"] != "date" else "date",
                      "nullable": (c.get("null_rate") or 0) > 0} for c in cols]
        return v.SourceProfile(source_uri=source_uri, profile=r, suggested_source_columns=suggested,
                               next_actions=["confirm keys, event time and delete semantics with the user",
                                             "define_data_contract"])

    @tool("inspect_consumer_contract", "Inspect what a consumer reads", read_only=True, idempotent=True)
    async def inspect_consumer_contract(
        account_id: AccountId, region: Region,
        target: Annotated[str, Field(description="Existing destination: glue:<db>.<table> or dynamodb:<table>")],
        principal_arn: Annotated[str | None, Field(description="IAM role/user the consumer reads as; its access is "
                                                               "simulated.")] = None,
        contract_id: Annotated[str | None, Field(description="Compare the target schema with this contract.")] = None,
    ) -> v.ConsumerReport:
        """Look at the consumer side of an existing destination: current schema/partitions/format, newest data
        time, whether a consumer principal can read it (IAM simulation), and differences from a contract. Use to
        avoid breaking existing readers. Real reads with the flow's own consumer role happen in
        verify_data_flow."""
        r = await run_sync(investigate.inspect_consumer, app, owner(), account_id, region, target, principal_arn,
                           contract_id)
        return v.ConsumerReport(target=target, report=r, next_actions=["define_data_contract (consumers, output "
                                                                       "schema)"])

    # ------------------------------------------------------------------ contract + design
    @tool("define_data_contract", "Save the agreed data contract", read_only=False, idempotent=True,
          open_world=False)
    async def define_data_contract(
        contract: DataContractBody,
        contract_id: Annotated[str | None, Field(description="Omit to create; give to add a new version.")] = None,
        expected_version: Annotated[int | None, Field(description="Latest version you read; guards concurrent "
                                                                  "edits.")] = None,
    ) -> v.ContractResult:
        """Store the requirements agreed with the user as a versioned contract: sources, output dataset schema,
        key and upsert/delete semantics, freshness, transform complexity, volume, consumers, quality rules,
        reprocessing window and cost cap. Every design/change/verify tool reads the contract by id instead of
        free-form JSON. Re-sending an identical body returns the same version. Writes pmcp state only (no AWS)."""
        rec, created = await run_sync(contracts.define, app, owner(), contract, contract_id, expected_version)
        return v.ContractResult(contract_id=rec.contract_id, contract_version=rec.contract_version,
                                created_new_version=created,
                                derived_requirements=Requirements.from_contract(rec.body).model_dump(),
                                next_actions=["list_capabilities / validate_architecture to compare patterns"])

    @tool("list_capabilities", "List patterns, adapters, statuses", read_only=True, idempotent=True,
          open_world=False)
    async def list_capabilities(
        category: Annotated[Literal["ingest", "transform", "store", "orchestrate", "monitor"] | None,
                            Field(description="Filter by component category.")] = None,
        pattern_id: Annotated[str | None, Field(description="Return one pattern with its params_schema.")] = None,
    ) -> v.Capabilities:
        """List the architecture patterns pmcp knows, their components, adapter_status (implemented vs
        catalog_only) and integration status (aws_verified / mock_tested / not_verified, with evidence). With
        pattern_id, also returns the JSON schema for that pattern's pattern_params. Catalog knowledge only: for
        fitness against a contract use validate_architecture."""
        return v.Capabilities(**design.list_capabilities(category, pattern_id))

    @tool("get_service_constraints", "Service limits and availability", read_only=True, idempotent=True)
    async def get_service_constraints(
        services: Annotated[list[str], Field(min_length=1, description="service_ids from list_capabilities, e.g. "
                                                                        "athena, firehose, dynamodb")],
        region: Region,
        account_id: Annotated[str | None, Field(description="Adds live Service Quotas values for this account.")]
        = None,
    ) -> v.ServiceConstraints:
        """Facts about specific services: latency floor, delivery semantics, documented limits, strengths and
        weaknesses, region availability, and (with account_id) live quota values. Use when a requirement is near
        a limit (throughput, record size, run time). Does not judge a whole architecture (validate_architecture
        does)."""
        return v.ServiceConstraints(**await run_sync(design.service_constraints, app, owner(), services, region,
                                                     account_id))

    @tool("estimate_cost", "Estimate monthly cost", read_only=True, idempotent=True)
    async def estimate_cost(
        contract_id: str, pattern_id: str, pattern_params: dict[str, Any], region: Region,
        contract_version: int | None = None,
    ) -> v.CostResult:
        """Monthly cost estimate for one implemented pattern with concrete params, from the contract's volume:
        line items with unit prices and where each price came from (Price List API or dated static table),
        assumptions and confidence. prepare_changes includes the same estimate; use this to compare options
        before preparing a plan."""
        return v.CostResult(**await run_sync(design.estimate_cost, app, owner(), contract_id, contract_version,
                                             pattern_id, pattern_params, region))

    @tool("validate_architecture", "Compare candidate architectures", read_only=True, idempotent=True)
    async def validate_architecture(
        contract_id: str,
        candidates: Annotated[list[Candidate], Field(min_length=1, max_length=8)],
        account_id: Annotated[str | None, Field(description="Check deployability in this account.")] = None,
        region: Annotated[str | None, Field(description="Region for availability and cost.")] = None,
        contract_version: int | None = None,
    ) -> v.ArchitectureValidation:
        """Judge your candidate patterns against the contract. For each: requirement fitness (strong/acceptable/
        weak/unsuitable with reasons, blockers, unknowns), adapter status, integration status, params validity,
        cost, and deployable_now with blockers. Reports best_fit and best_deployable_now separately and lists
        better-fitting patterns you did not propose. Never treats 'implemented' as 'best'."""
        r = await run_sync(design.validate_architecture, app, owner(), contract_id, contract_version,
                           [c.model_dump() for c in candidates], account_id, region)
        nxt = ["prepare_changes with best_deployable_now"] if r["best_deployable_now"] else []
        if r["best_fit"] and r["best_fit"] != r["best_deployable_now"]:
            nxt.append("Tell the user the best fit is not deployable yet and what implementing it requires")
        return v.ArchitectureValidation(**r, next_actions=nxt)

    # ------------------------------------------------------------------ change
    @tool("prepare_changes", "Create a change plan", read_only=False, idempotent=False)
    async def prepare_changes(
        account_id: AccountId, region: Region, contract_id: str, pattern_id: str,
        pattern_params: Annotated[dict[str, Any], Field(description="Per list_capabilities(pattern_id).params_schema")],
        flow_id: Annotated[str | None, Field(description="Existing flow to modify; omit for a new flow.")] = None,
        flow_name: str | None = None,
        change_reason: str | None = None,
        contract_version: int | None = None,
        reprocess: Annotated[planning.ReprocessRequest | None, Field(
            description="Reprocessing of existing data for modifications; default auto.")] = None,
    ) -> v.PlanView:
        """Build a reviewable plan for a new or existing flow, without touching AWS resources: target
        account/region/stack, current vs desired state, CloudFormation resource diff, code/SQL diffs, data and
        consumer impact, reprocessing need and window, cost, ordered steps with preconditions and failure
        handling. Rejects unsupported parameter combinations. Next: preview_changes."""
        req = planning.PrepareRequest(account_id=account_id, region=region, contract_id=contract_id,
                                      contract_version=contract_version, pattern_id=pattern_id,
                                      pattern_params=pattern_params, flow_id=flow_id, flow_name=flow_name,
                                      change_reason=change_reason,
                                      reprocess=reprocess or planning.ReprocessRequest())
        plan = await run_sync(planning.prepare, app, owner(), req)
        return v.PlanView.of(plan, None, ["preview_changes to create the CloudFormation change set and get the "
                                          "digest for approval"])

    @tool("preview_changes", "Create change set and get approval link", read_only=False, idempotent=True)
    async def preview_changes(plan_id: str) -> v.PlanView:
        """Turn a draft plan into an exact, approvable artifact: uploads code artifacts to the account's pmcp
        artifacts bucket, creates a CloudFormation change set (no resources change yet), records the real
        resource changes/replacements, and returns plan_digest plus the approval link. Show the user the
        summary and link; a person must approve outside MCP. Fails with STALE_STATE if the target changed."""
        plan = await run_sync(planning.preview, app, owner(), plan_id)
        return v.PlanView.of(plan, planning.approval_hint(app, plan), [
            "Ask the user to review and approve at `approval`",
            "Then apply_changes (or execute_recovery) with plan_id, plan_digest and a new idempotency_key"])

    async def _start(plan_id: str, plan_digest: str, key: str, kind: Literal["change", "recovery"]) -> v.OperationView:
        op = await run_sync(planning.start_execution, app, owner(), plan_id, plan_digest, key, kind)
        return v.OperationView.of(op, False, None, ["get_operation until status is succeeded or failed"])

    @tool("apply_changes", "Execute an approved change plan", read_only=False, destructive=True, idempotent=True)
    async def apply_changes(plan_id: str, plan_digest: str, idempotency_key: IdemKey) -> v.OperationView:
        """Execute exactly the approved plan as a background operation and return operation_id. Refuses unless a
        person approved this digest, the plan has not expired, the target has not changed since review, the
        change set is still available, and limits/cost cap allow it. Same idempotency_key returns or resumes the
        same operation. Success means deployed, not verified: run verify_data_flow afterwards."""
        return await _start(plan_id, plan_digest, idempotency_key, "change")

    # ------------------------------------------------------------------ verification
    @tool("verify_data_flow", "Verify data against the contract", read_only=True, idempotent=True)
    async def verify_data_flow(
        flow_id: str,
        window: Annotated[Window | None, Field(description="Data window to check; default: the last completed run "
                                                           "window (batch) or the last hour (streaming).")] = None,
        idempotency_key: Annotated[str | None, Field(description="Re-send to get the same verification "
                                                                 "operation.")] = None,
    ) -> v.OperationView:
        """Check the destination data itself, not job status: existence, schema/types, key uniqueness, expected
        vs actual rows for the window, updates/deletes applied, freshness and progress vs source, contract
        quality rules, and a real read as the flow's consumer role. Runs as an operation; get_operation returns
        each check as passed/failed/unverified with window, expected, observed and evidence."""
        op = await run_sync(verification.start_verification, app, owner(), flow_id,
                            window.model_dump() if window else None, idempotency_key)
        return v.OperationView.of(op, False, None, ["get_operation for the check results"])

    # ------------------------------------------------------------------ operations
    @tool("observe_flow", "Current state of a flow", read_only=True, idempotent=True)
    async def observe_flow(
        flow_id: str,
        include_cost: Annotated[bool, Field(description="Query Cost Explorer (USD 0.01 per call, billed to the "
                                                        "account).")] = False,
    ) -> v.FlowObservation:
        """Snapshot of a deployed flow: stack status, recent runs/throughput, alarm states, schedule state,
        time since last success, last verification verdict, active operations and optionally recent cost. Fast
        and read-only; for explanations of a failure use diagnose_failure."""
        obs = await run_sync(ops.observe, app, owner(), flow_id, include_cost)
        bad = [a for a in obs.get("runtime", {}).get("alarms", []) if a.get("state") == "ALARM"]
        return v.FlowObservation(observation=obs, next_actions=["diagnose_failure"] if bad else [])

    @tool("get_run_failure", "Read full execution and Athena errors", read_only=True, idempotent=True)
    async def get_run_failure(flow_id: str, execution: str, next_token: str | None = None) -> dict[str, Any]:
        """Read existing Step Functions failure details and Athena query errors for this flow. execution is
        an execution ARN or name from observe_flow/diagnose_failure/get_operation. Returns full error causes,
        query ids, StateChangeReason and AthenaError; follow next_token for older history. Does not create
        incidents, start queries or run recovery. Use this when an error excerpt is truncated."""
        from pmcp.services.run_failure import get_run_failure as read_failure

        return await run_sync(read_failure, app, owner(), flow_id, execution, next_token)

    @tool("diagnose_failure", "Collect evidence and hypotheses", read_only=True, idempotent=False)
    async def diagnose_failure(
        flow_id: str,
        window: Annotated[Window | None, Field(description="Default: last 24 hours.")] = None,
        deep_probe: Annotated[bool, Field(description="Also run probe queries that locate offending input files "
                                                      "(runs Athena in the account; small cost).")] = False,
        idempotency_key: str | None = None,
    ) -> v.IncidentView:
        """Open an incident for a flow: observed facts (failed runs with error excerpts, alarms, stack state,
        metrics), ranked hypotheses each citing supporting fact ids, further checks to run, and affected
        windows/files. Facts and hypotheses are kept separate. deep_probe starts a background operation that adds
        offending-file evidence. Next: plan_recovery."""
        inc, op = await run_sync(ops.diagnose, app, owner(), flow_id, window.model_dump() if window else None,
                                 deep_probe, idempotency_key)
        nxt = ["plan_recovery with this incident_id"]
        if op:
            nxt.insert(0, f"get_operation {op.operation_id} until the probe finishes, then plan_recovery")
        return v.IncidentView(incident_id=inc.incident_id, flow_id=inc.flow_id, window=inc.window,
                              observed_facts=inc.observed_facts, hypotheses=[h.model_dump() for h in inc.hypotheses],
                              further_checks=inc.further_checks, affected=inc.affected,
                              probe_operation_id=op.operation_id if op else None, next_actions=nxt)

    @tool("plan_recovery", "Create a recovery plan", read_only=False, idempotent=False)
    async def plan_recovery(
        incident_id: str,
        strategy: Annotated[str | None, Field(description="One of available_strategies; default follows the top "
                                                          "hypothesis.")] = None,
    ) -> v.RecoveryPlanView:
        """Build a recovery plan for an incident with explicit targets and impact: e.g. quarantine bad files and
        re-run affected windows, replay failed records, re-create tables, re-apply the last template, enable the
        schedule. Infrastructure restore and data recovery are listed separately; every recovery ends with a
        verification step. Same preview/approval flow as changes: preview_changes, then execute_recovery."""
        plan, options = await run_sync(ops.plan_recovery, app, owner(), incident_id, strategy)
        return v.RecoveryPlanView(plan=v.PlanView.of(plan, None, ["preview_changes", "execute_recovery after "
                                                                 "approval"]),
                                  available_strategies=options)

    @tool("execute_recovery", "Execute an approved recovery plan", read_only=False, destructive=True,
          idempotent=True)
    async def execute_recovery(plan_id: str, plan_digest: str, idempotency_key: IdemKey) -> v.OperationView:
        """Execute an approved recovery plan (same guards as apply_changes) and re-verify the affected data; the
        operation fails if verification fails after recovery. Returns operation_id; same idempotency_key
        returns or resumes the same operation."""
        return await _start(plan_id, plan_digest, idempotency_key, "recovery")

    @tool("get_operation", "Status of a long-running operation", read_only=True, idempotent=True,
          open_world=False)
    async def get_operation(operation_id: str) -> v.OperationView:
        """Status of an apply/recovery/verification/probe operation: per-step status and errors, result, and
        for verifications the full check list. stalled=true means the worker died; re-call the starting tool with
        the same idempotency_key to resume from the first unfinished step."""
        o = owner()

        def run() -> v.OperationView:
            op = app.store.require(Operation, o, operation_id)
            stalled = op.status in ("queued", "running") and not lease_alive(app, o, operation_id) and \
                time.time() - _ts(op.updated_at) > 120
            ver = None
            vid = op.result.get("verification_id") or next(
                (s.detail.get("verification_id") for s in op.steps if s.detail.get("verification_id")), None)
            if vid:
                ver = app.store.get(Verification, o, vid)
            nxt = []
            if op.status in ("queued", "running"):
                nxt.append("poll get_operation again in ~10-30 s")
            elif op.status == "failed":
                nxt.append("diagnose_failure" if op.kind != "verify_data_flow" else "diagnose_failure for evidence")
            elif op.kind == "apply_changes":
                nxt.append("verify_data_flow")
            return v.OperationView.of(op, stalled, ver, nxt + list(op.result.get("next_actions", [])))

        return await run_sync(run)


def _ts(iso_ts: str) -> float:
    from datetime import datetime

    return datetime.fromisoformat(iso_ts.replace("Z", "+00:00")).timestamp()
