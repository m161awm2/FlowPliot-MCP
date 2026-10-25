"""Change plans: prepare → preview → (human approval, outside MCP) → apply.

The digest covers everything apply will do (template + artifact hashes, steps, target fingerprint,
change set id). apply re-computes it, compares it with the approved digest, and re-reads the target
state; any difference refuses the call. Approval is a separate record written only by the web
approval page or the local terminal CLI, never by an MCP tool.
"""

from __future__ import annotations

import base64
import difflib
import json
import time
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from botocore.exceptions import ClientError
from pydantic import Field

from pmcp.adapters import get_adapter
from pmcp.adapters.base import RenderContext
from pmcp.app import App
from pmcp.aws.client import BOTO_CONFIG, error_code
from pmcp.aws.ownership import as_cfn_tags, flow_tags, is_managed_by
from pmcp.domain.base import Model
from pmcp.domain.contract import DataContract
from pmcp.domain.records import (
    AccountConnection,
    Approval,
    Artifact,
    DataImpact,
    Flow,
    Operation,
    Plan,
    PlanStep,
    ResourceChange,
)
from pmcp.errors import ErrorCode, PmcpError, from_boto
from pmcp.ids import canonical_json, digest, iso, new_id, sha256_hex, short
from pmcp.services.contracts import load_contract
from pmcp.services.operations import new_steps

TEMPLATE_BODY_LIMIT = 51_200
IN_PROGRESS = ("_IN_PROGRESS",)


class ReprocessRequest(Model):
    mode: Literal["auto", "none", "window"] = Field(
        "auto", description="auto: reprocess when the adapter says the change requires it, over the contract's "
        "reprocessing_window_days (default 7) ending now. none: never. window: use start/end."
    )
    start: str | None = None
    end: str | None = None


class PrepareRequest(Model):
    account_id: str = Field(pattern=r"^\d{12}$")
    region: str
    contract_id: str
    contract_version: int | None = Field(None, description="Defaults to the latest version.")
    pattern_id: str
    pattern_params: dict[str, Any]
    flow_id: str | None = Field(None, description="Existing flow to modify; omit to create a new flow.")
    flow_name: str | None = Field(None, max_length=64)
    change_reason: str | None = Field(None, max_length=500)
    reprocess: ReprocessRequest = Field(default_factory=ReprocessRequest)


# --- target state ----------------------------------------------------------------------------------


def stack_state(session: Any, stack_name: str, owner: str) -> dict[str, Any]:
    cfn = session.client("cloudformation", config=BOTO_CONFIG)
    try:
        st = cfn.describe_stacks(StackName=stack_name)["Stacks"][0]
    except ClientError as exc:
        if "does not exist" in str(exc):
            return {"stack_exists": False}
        raise from_boto(exc, action="describe stack") from exc
    if st["StackStatus"] == "REVIEW_IN_PROGRESS":
        # Created by a CREATE change set and never executed. AWS does not expose stack tags in this state, so
        # ownership is read from the pending change sets instead.
        pending = [c["ChangeSetName"] for c in cfn.list_change_sets(StackName=stack_name).get("Summaries", [])]
        owned = [n for n in pending if is_managed_by(
            cfn.describe_change_set(ChangeSetName=n, StackName=stack_name).get("Tags"), owner)]
        if not pending or (owned != pending and not is_managed_by(st.get("Tags"), owner)):
            raise PmcpError(ErrorCode.FORBIDDEN, f"stack {stack_name} is pending review by someone else")
        return {"stack_exists": True, "stack_id": st["StackId"], "stack_status": "REVIEW_IN_PROGRESS",
                "last_updated": str(st.get("CreationTime")), "template_sha256": None, "template": None,
                "outputs": {}, "pending_change_sets": sorted(pending)}
    if not is_managed_by(st.get("Tags"), owner):
        raise PmcpError(ErrorCode.FORBIDDEN, f"stack {stack_name} exists but is not managed by pmcp for this user")
    body = cfn.get_template(StackName=stack_name, TemplateStage="Original")["TemplateBody"]
    tmpl = body if isinstance(body, dict) else json.loads(body)
    return {
        "stack_exists": True,
        "stack_id": st["StackId"],
        "stack_status": st["StackStatus"],
        "last_updated": str(st.get("LastUpdatedTime") or st.get("CreationTime")),
        "template_sha256": sha256_hex(canonical_json(tmpl)),
        "template": tmpl,
        "outputs": {o["OutputKey"]: o["OutputValue"] for o in st.get("Outputs", [])},
    }


def fingerprint(state: dict[str, Any], flow: Flow | None) -> dict[str, Any]:
    fp = {k: state.get(k) for k in ("stack_exists", "stack_status", "last_updated", "template_sha256")}
    # Only the flow's deployed configuration counts; verification/observation bookkeeping must not stale a plan.
    fp["flow_config"] = digest([flow.pattern_params, flow.contract_id, flow.contract_version, flow.template_sha256,
                                flow.last_plan_id, flow.status == "deleted"]) if flow else None
    return fp


def _resource_diff(old: dict[str, Any] | None, new: dict[str, Any]) -> list[ResourceChange]:
    old_r = (old or {}).get("Resources", {})
    new_r = new.get("Resources", {})
    out = []
    for lid, res in new_r.items():
        if lid not in old_r:
            out.append(ResourceChange(logical_id=lid, resource_type=res["Type"], action="add", replacement="false"))
        elif canonical_json(old_r[lid]) != canonical_json(res):
            changed = sorted(
                k for k in set(res.get("Properties", {})) | set(old_r[lid].get("Properties", {}))
                if canonical_json(res.get("Properties", {}).get(k)) != canonical_json(old_r[lid].get("Properties",
                                                                                                    {}).get(k))
            )
            out.append(ResourceChange(logical_id=lid, resource_type=res["Type"], action="modify",
                                      details=[f"property {k} changes" for k in changed]))
    for lid, res in old_r.items():
        if lid not in new_r:
            out.append(ResourceChange(logical_id=lid, resource_type=res["Type"], action="remove"))
    return out


def lint_template(tmpl: dict[str, Any], region: str) -> tuple[list[str], list[str]]:
    """cfn-lint errors and warnings. Errors block the plan."""
    try:
        from cfnlint.api import ManualArgs, lint
    except ImportError:  # pragma: no cover
        return [], ["cfn-lint unavailable; template not linted"]
    matches = lint(json.dumps(tmpl), config=ManualArgs(regions=[region]))
    errors, warnings = [], []
    for m in matches:
        msg = f"{m.rule.id} {m.message} ({'/'.join(str(p) for p in getattr(m, 'path', []) or [])})"
        if m.rule.id.startswith("E"):
            errors.append(msg)
        elif m.rule.id.startswith("W"):
            warnings.append(msg)
    return errors, warnings


def _artifact(name: str, kind: str, content: str | bytes) -> Artifact:
    raw = content.encode() if isinstance(content, str) else content
    text = content if isinstance(content, str) else base64.b64encode(content).decode()
    return Artifact(name=name, kind=kind, sha256=sha256_hex(raw), size=len(raw), content=text)  # type: ignore[arg-type]


def artifact_bytes(a: Artifact) -> bytes:
    assert a.content is not None
    return base64.b64decode(a.content) if a.kind == "zip" else a.content.encode()


# --- prepare ----------------------------------------------------------------------------------------


def prepare(app: App, owner: str, req: PrepareRequest) -> Plan:
    conn = app.sessions.connection(owner, req.account_id)
    app.sessions.check_region(conn, req.region)
    contract = load_contract(app, owner, req.contract_id, req.contract_version)
    adapter = get_adapter(req.pattern_id)
    params = adapter.parse_params(req.pattern_params, contract.body)

    flow: Flow | None = None
    previous_contract: DataContract | None = None
    previous_artifacts: dict[str, Artifact] = {}
    if req.flow_id:
        flow = app.store.require(Flow, owner, req.flow_id)
        if (flow.account_id, flow.region) != (req.account_id, req.region):
            raise PmcpError(ErrorCode.INVALID_INPUT, "flow lives in a different account/region",
                            details={"account_id": flow.account_id, "region": flow.region})
        if flow.pattern_id != req.pattern_id:
            raise PmcpError(
                ErrorCode.UNSUPPORTED_CONFIGURATION,
                f"changing a flow's pattern ({flow.pattern_id} -> {req.pattern_id}) in place is not supported",
                next_actions=["prepare_changes without flow_id to build the new flow side by side, verify it, "
                              "then retire the old flow"],
            )
        if flow.contract_id and flow.contract_version:
            previous_contract = load_contract(app, owner, flow.contract_id, flow.contract_version)
        if flow.last_plan_id:
            prev_plan = app.store.get(Plan, owner, flow.last_plan_id)
            if prev_plan:
                previous_artifacts = {a.name: a for a in prev_plan.artifacts}
        flow_id, stack_name = flow.flow_id, flow.stack_name
    else:
        flow_id = new_id("fl")
        stack_name = f"pmcp-{short(flow_id)}"

    reader = app.sessions.session(owner, req.account_id, req.region, "reader")
    state = stack_state(reader, stack_name, owner)
    if any(state.get("stack_status", "").endswith(s) for s in IN_PROGRESS):
        raise PmcpError(ErrorCode.PRECONDITION_FAILED, f"stack is busy ({state['stack_status']})",
                        next_actions=["Wait and retry; check get_operation for a running operation."])

    plan_id = new_id("pl")
    ctx = RenderContext(
        owner=owner, flow_id=flow_id, plan_id=plan_id, account_id=req.account_id, region=req.region,
        stack_name=stack_name, contract=contract, connection=conn,
        trusted_principals=_trusted_principals(app, conn), boundary_policy_arn=conn.boundary_policy_arn,
        previous_params=flow.pattern_params if flow else None, previous_contract=previous_contract,
    )
    rendered = adapter.render(ctx, params)
    errors, lint_warnings = lint_template(rendered.template, req.region)
    if errors:
        raise PmcpError(ErrorCode.UNSUPPORTED_CONFIGURATION, "rendered template failed cfn-lint",
                        details={"errors": errors[:20]})

    template_text = canonical_json(rendered.template)
    artifacts = [_artifact("template.json", "template", template_text)]
    artifacts += [_artifact(a.name, a.kind, a.content) for a in rendered.artifacts]
    code_diffs = []
    for a in artifacts:
        if a.kind in ("sql", "python", "template") and a.name in previous_artifacts:
            old = previous_artifacts[a.name]
            if old.sha256 != a.sha256 and old.content and a.content:
                old_text = old.content if a.kind != "template" else json.dumps(json.loads(old.content), indent=1)
                new_text = a.content if a.kind != "template" else json.dumps(json.loads(a.content), indent=1)
                diff = "".join(difflib.unified_diff(old_text.splitlines(True), new_text.splitlines(True),
                                                    f"a/{a.name}", f"b/{a.name}", n=2))
                code_diffs.append({"artifact": a.name, "diff": diff[:20000]})

    # Reprocessing decision.
    reprocess_required = bool(flow and rendered.reprocess_reason)
    window = None
    warnings = list(rendered.warnings) + lint_warnings[:10]
    if req.reprocess.mode == "window":
        if not (req.reprocess.start and req.reprocess.end):
            raise PmcpError(ErrorCode.INVALID_INPUT, "reprocess.mode=window needs start and end")
        window = {"start": req.reprocess.start, "end": req.reprocess.end}
    elif req.reprocess.mode == "auto" and reprocess_required:
        days = contract.body.reprocessing_window_days or 7
        end = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        window = {"start": iso(end - timedelta(days=days)), "end": iso(end)}
    elif req.reprocess.mode == "none" and reprocess_required:
        warnings.append(f"reprocessing skipped by request although: {rendered.reprocess_reason}")

    impact = DataImpact(
        datasets=[contract.body.output.name],
        schema_changes=rendered.schema_changes,
        breaking_for_consumers=rendered.breaking,
        consumer_notes=[f"{c.name} ({c.access}) reads {contract.body.output.name}" for c in contract.body.consumers]
        if rendered.breaking or rendered.schema_changes else [],
        reprocessing_required=reprocess_required,
        reprocessing_reason=rendered.reprocess_reason,
        reprocessing_window=window,
        data_rollback="rebuild_from_source" if (window or rendered.breaking) else "not_applicable",
        infra_rollback="cloudformation_automatic" if state["stack_exists"] else
        "cloudformation_automatic (failed create deletes created resources; Retain resources stay)",
    )

    cost = adapter.estimate(contract.body, params, req.region, app.prices)
    if window and hasattr(adapter, "reprocess_cost"):
        cost.one_off_usd = adapter.reprocess_cost(contract.body, params, window, req.region, app.prices)
        cost.assumptions.append(f"one-off reprocessing of {window['start']}..{window['end']}")
    cap = min(conn.monthly_cost_cap_usd, contract.body.monthly_cost_cap_usd or conn.monthly_cost_cap_usd)
    if cost.monthly_usd > cap:
        warnings.append(f"estimated {cost.monthly_usd} USD/month exceeds the cost cap {cap}; apply will refuse")

    steps: list[PlanStep] = [
        PlanStep(step_id="precheck", action="precheck",
                 description="Re-check approval, digest, target state and change set before touching AWS"),
    ]
    if state.get("stack_status") == "ROLLBACK_COMPLETE":
        steps.append(PlanStep(step_id="delete_failed_stack", action="cfn.delete_failed_stack",
                              description="Delete the stack left by a failed create (no live resources)",
                              reversible=False))
    if any(a.kind == "zip" or a.name.endswith(".py") and a.name.startswith("pmcp/") for a in artifacts) or \
            len(template_text) > TEMPLATE_BODY_LIMIT:
        steps.append(PlanStep(step_id="verify_artifacts", action="verify_artifacts",
                              description="Confirm uploaded artifacts still match their SHA-256"))
    steps += [
        PlanStep(step_id="execute_change_set", action="execute_change_set",
                 description="Execute the reviewed CloudFormation change set (exactly the previewed changes)",
                 preconditions=["change set status AVAILABLE"], on_failure="abort",
                 compensation="CloudFormation rolls back automatically on failure"),
        PlanStep(step_id="wait_stack", action="wait_stack", description="Wait for the stack to finish",
                 on_failure="abort", compensation="Stack rollback restores the previous configuration"),
        *rendered.post_deploy_steps,
    ]
    if window:
        steps += adapter.reprocess_steps(ctx, params, window)
    steps.append(PlanStep(step_id="record_flow", action="record_flow", description="Record the new flow state"))

    now = datetime.now(UTC)
    plan = Plan(
        id=plan_id, owner=owner, plan_id=plan_id, kind="change", account_id=req.account_id, region=req.region,
        flow_id=flow_id, stack_name=stack_name, pattern_id=req.pattern_id,
        pattern_params=params.model_dump(mode="json"), contract_id=contract.contract_id,
        contract_version=contract.contract_version,
        summary=f"{'Update' if flow else 'Create'} flow {flow_id} ({req.pattern_id}) in {req.account_id}/{req.region}",
        change_reason=req.change_reason,
        base_fingerprint=fingerprint(state, flow),
        current_state={k: v for k, v in state.items() if k != "template"},
        desired_state=rendered.desired_summary,
        resource_changes=_resource_diff(state.get("template"), rendered.template),
        code_diffs=code_diffs, artifacts=artifacts, data_impact=impact, cost=cost, steps=steps,
        warnings=warnings, expires_at=iso(now + timedelta(seconds=app.settings.plan_ttl_seconds)),
    )
    if flow is None:
        plan.desired_state["flow_name"] = req.flow_name or contract.body.title
    plan.digest = digest(plan.digest_material())
    return app.store.create(plan)


def _trusted_principals(app: App, conn: AccountConnection) -> list[str]:
    if conn.mode == "direct":
        return [f"arn:aws:iam::{conn.account_id}:root"]
    return app.settings.platform_principal_arns


# --- preview ----------------------------------------------------------------------------------------


def _artifacts_bucket(account_id: str, region: str) -> str:
    return f"pmcp-artifacts-{account_id}-{region}"


def ensure_artifacts_bucket(session: Any, account_id: str, region: str, owner: str) -> str:
    s3 = session.client("s3", config=BOTO_CONFIG)
    bucket = _artifacts_bucket(account_id, region)
    try:
        s3.head_bucket(Bucket=bucket, ExpectedBucketOwner=account_id)
        return bucket
    except ClientError as exc:
        if error_code(exc) not in ("404", "NoSuchBucket", "NotFound"):
            raise from_boto(exc, action=f"head bucket {bucket}") from exc
    kwargs: dict[str, Any] = {"Bucket": bucket}
    if region != "us-east-1":
        kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}
    s3.create_bucket(**kwargs)
    s3.put_public_access_block(Bucket=bucket, PublicAccessBlockConfiguration={
        "BlockPublicAcls": True, "IgnorePublicAcls": True, "BlockPublicPolicy": True, "RestrictPublicBuckets": True})
    s3.put_bucket_tagging(Bucket=bucket, Tagging={"TagSet": as_cfn_tags({"managed-by": "pipeline-mcp",
                                                                          "pmcp:owner": owner})})
    return bucket


def upload_artifacts(session: Any, plan: Plan) -> None:
    s3 = session.client("s3", config=BOTO_CONFIG)
    bucket = ensure_artifacts_bucket(session, plan.account_id, plan.region, plan.owner)
    for a in plan.artifacts:
        if not (a.kind == "zip" or a.name.startswith("pmcp/")):
            continue
        data = artifact_bytes(a)
        try:
            head = s3.head_object(Bucket=bucket, Key=a.name, ChecksumMode="ENABLED")
            if head.get("Metadata", {}).get("sha256") == a.sha256:
                a.s3_uri = f"s3://{bucket}/{a.name}"
                continue
        except ClientError:
            pass
        s3.put_object(Bucket=bucket, Key=a.name, Body=data, Metadata={"sha256": a.sha256},
                      ChecksumAlgorithm="SHA256")
        a.s3_uri = f"s3://{bucket}/{a.name}"


def preview(app: App, owner: str, plan_id: str, wait_seconds: int = 20) -> Plan:
    plan = app.store.require(Plan, owner, plan_id)
    _check_not_expired(app, plan)
    if plan.status not in ("draft", "previewed"):
        raise PmcpError(ErrorCode.PLAN_NOT_EXECUTABLE, f"plan is {plan.status}; preview needs a draft plan")
    conn = app.sessions.connection(owner, plan.account_id)
    reader = app.sessions.session(owner, plan.account_id, plan.region, "reader")
    flow = app.store.get(Flow, owner, plan.flow_id)
    state = stack_state(reader, plan.stack_name, owner)
    cs_name = f"pmcp-{short(plan.plan_id, 16)}"
    own_partial_preview = (not plan.base_fingerprint.get("stack_exists") and
                           state.get("stack_status") == "REVIEW_IN_PROGRESS" and
                           set(state.get("pending_change_sets", [])) <= {cs_name})
    if fingerprint(state, flow) != plan.base_fingerprint and not own_partial_preview:
        plan.status = "superseded"
        app.store.save(plan)
        raise PmcpError(ErrorCode.STALE_STATE, "the target changed since the plan was prepared",
                        details={"planned": plan.base_fingerprint, "current": fingerprint(state, flow)},
                        next_actions=["prepare_changes again"])
    if plan.change_set and plan.change_set.get("status") in ("CREATE_COMPLETE", "NO_CHANGES"):
        return plan  # already previewed; idempotent
    template = next((a for a in plan.artifacts if a.name == "template.json"), None)
    if not conn.write_enabled:
        plan.warnings.append("account is connected read-only: no change set was created; apply is not possible")
        plan.status = "previewed"
        plan.digest = digest(plan.digest_material())
        return app.store.save(plan)
    if template is None:  # recovery plan without infrastructure change
        plan.status = "previewed"
        plan.digest = digest(plan.digest_material())
        return app.store.save(plan)

    deployer = app.sessions.session(owner, plan.account_id, plan.region, "deployer")
    upload_artifacts(deployer, plan)
    cfn = deployer.client("cloudformation", config=BOTO_CONFIG)
    cs_type = "CREATE" if (not state["stack_exists"] or own_partial_preview or
                           state.get("stack_status") == "ROLLBACK_COMPLETE") else "UPDATE"
    if state.get("stack_status") == "ROLLBACK_COMPLETE":
        # The change set can only be created after the failed stack is deleted; apply does that first.
        plan.change_set = {"name": cs_name, "type": "CREATE", "status": "DEFERRED_UNTIL_DELETE",
                           "template_sha256": template.sha256}
        plan.status = "previewed"
        plan.digest = digest(plan.digest_material())
        return app.store.save(plan)
    cs_id = create_change_set(app, deployer, plan, template, cs_name, cs_type)
    deadline = time.time() + wait_seconds
    while True:
        d = cfn.describe_change_set(ChangeSetName=cs_id)
        if d["Status"] not in ("CREATE_PENDING", "CREATE_IN_PROGRESS") or time.time() > deadline:
            break
        time.sleep(2)
    status = d["Status"]
    if status == "FAILED" and ("didn't contain changes" in d.get("StatusReason", "") or
                               "No updates" in d.get("StatusReason", "")):
        status = "NO_CHANGES"
    elif status == "FAILED":
        raise PmcpError(ErrorCode.UNSUPPORTED_CONFIGURATION, f"change set failed: {d.get('StatusReason')}",
                        details={"change_set": cs_id})
    changes = [
        ResourceChange(
            logical_id=c["ResourceChange"]["LogicalResourceId"],
            resource_type=c["ResourceChange"]["ResourceType"],
            action={"Add": "add", "Modify": "modify", "Remove": "remove"}.get(c["ResourceChange"]["Action"],
                                                                              "unknown"),  # type: ignore[arg-type]
            replacement=str(c["ResourceChange"].get("Replacement", "unknown")).lower(),  # type: ignore[arg-type]
            details=[f"{d_.get('Target', {}).get('Attribute')}:{d_.get('Target', {}).get('Name')}"
                     for d_ in c["ResourceChange"].get("Details", [])][:10],
        )
        for c in d.get("Changes", [])
    ]
    plan.change_set = {
        "id": cs_id, "name": cs_name, "type": cs_type, "status": status,
        "stack_id": d.get("StackId"), "template_sha256": template.sha256,
        "changes_digest": digest([c.model_dump() for c in changes]),
    }
    if status in ("CREATE_PENDING", "CREATE_IN_PROGRESS"):
        app.store.save(plan)
        raise PmcpError(ErrorCode.TIMEOUT, "change set is still being created", details={"change_set": cs_id},
                        next_actions=["Call preview_changes again in a few seconds"])
    plan.resource_changes = changes or plan.resource_changes
    # Creating a CREATE change set itself creates the stack (REVIEW_IN_PROGRESS). The state the person approves
    # is the post-preview state, so re-capture it; it is part of the digest.
    plan.base_fingerprint = fingerprint(stack_state(reader, plan.stack_name, owner), flow)
    plan.status = "previewed"
    plan.digest = digest(plan.digest_material())
    return app.store.save(plan)


def create_change_set(app: App, deployer: Any, plan: Plan, template: Artifact, cs_name: str, cs_type: str) -> str:
    cfn = deployer.client("cloudformation", config=BOTO_CONFIG)
    conn = app.sessions.connection(plan.owner, plan.account_id)
    kwargs: dict[str, Any] = {
        "StackName": plan.stack_name,
        "ChangeSetName": cs_name,
        "ChangeSetType": cs_type,
        "Capabilities": ["CAPABILITY_NAMED_IAM"],
        # No per-plan tag: stack tags propagate to every resource, so a changing tag would mark them all modified.
        "Tags": as_cfn_tags(flow_tags(plan.owner, plan.flow_id)),
        "Description": f"pmcp plan {plan.plan_id}",
    }
    if conn.cfn_exec_role_arn:
        kwargs["RoleARN"] = conn.cfn_exec_role_arn
    body = artifact_bytes(template).decode()
    if len(body) > TEMPLATE_BODY_LIMIT:
        bucket = ensure_artifacts_bucket(deployer, plan.account_id, plan.region, plan.owner)
        key = f"pmcp/templates/{template.sha256}.json"
        deployer.client("s3", config=BOTO_CONFIG).put_object(Bucket=bucket, Key=key, Body=body.encode())
        kwargs["TemplateURL"] = f"https://{bucket}.s3.{plan.region}.amazonaws.com/{key}"
    else:
        kwargs["TemplateBody"] = body
    try:
        return cfn.create_change_set(**kwargs)["Id"]
    except ClientError as exc:
        if error_code(exc) == "AlreadyExistsException":
            return cfn.describe_change_set(ChangeSetName=cs_name, StackName=plan.stack_name)["ChangeSetId"]
        raise from_boto(exc, action="create change set") from exc


def _check_not_expired(app: App, plan: Plan) -> None:
    if plan.status in ("applied", "failed", "applying"):
        return
    if datetime.fromisoformat(plan.expires_at.replace("Z", "+00:00")) < datetime.now(UTC):
        if plan.status != "expired":
            plan.status = "expired"
            app.store.save(plan)
        raise PmcpError(ErrorCode.PLAN_EXPIRED, "plan expired", next_actions=["prepare_changes again"])


# --- approval (web page / local terminal only) -------------------------------------------------------


def record_approval(app: App, owner: str, plan_id: str, plan_digest: str, *, auth_time: int,
                    channel: Literal["web", "local_cli"], email: str | None = None) -> Approval:
    plan = app.store.require(Plan, owner, plan_id)
    _check_not_expired(app, plan)
    if plan.status not in ("previewed", "approved"):
        raise PmcpError(ErrorCode.PLAN_NOT_EXECUTABLE, f"plan is {plan.status}; only previewed plans can be approved")
    if plan.digest != plan_digest:
        raise PmcpError(ErrorCode.DIGEST_MISMATCH, "the plan changed after you opened it; reload and review again")
    existing = app.store.get(Approval, owner, plan_id)
    if existing:
        if existing.digest == plan_digest:
            return existing
        raise PmcpError(ErrorCode.CONFLICT, "plan already approved with a different digest")
    appr = app.store.create(Approval(id=plan_id, owner=owner, plan_id=plan_id, digest=plan_digest,
                                     approved_by=owner, approved_by_email=email, auth_time=auth_time, channel=channel))
    plan.status = "approved"
    app.store.save(plan)
    return appr


def approval_hint(app: App, plan: Plan) -> str:
    if app.settings.mode == "local":
        return f"Run in your own terminal (not via the agent): pmcp approve {plan.plan_id} --digest {plan.digest}"
    return f"{app.settings.public_base_url}/approve/{plan.plan_id}"


# --- apply -----------------------------------------------------------------------------------------


def start_execution(app: App, owner: str, plan_id: str, plan_digest: str, idempotency_key: str,
                    expected_kind: Literal["change", "recovery"]) -> Operation:
    idem_name = f"idem:{idempotency_key}"
    prior = app.store.peek_claim(owner, idem_name)
    if prior:
        if prior.get("plan_id") != plan_id:
            raise PmcpError(ErrorCode.CONFLICT, "idempotency_key was already used for a different plan")
        return _existing_or_resume(app, owner, prior["operation_id"])

    plan = app.store.require(Plan, owner, plan_id)
    if plan.kind != expected_kind:
        tool = "apply_changes" if plan.kind == "change" else "execute_recovery"
        raise PmcpError(ErrorCode.INVALID_INPUT, f"plan is a {plan.kind} plan; use {tool}")
    if plan.operation_id:
        return _existing_or_resume(app, owner, plan.operation_id)
    _check_not_expired(app, plan)
    if plan_digest != plan.digest:
        raise PmcpError(ErrorCode.DIGEST_MISMATCH, "plan_digest does not match the reviewed plan",
                        next_actions=["Use the digest returned by preview_changes"])
    if digest(plan.digest_material()) != plan.digest:
        raise PmcpError(ErrorCode.DIGEST_MISMATCH, "stored plan content no longer matches its digest")
    if plan.status == "previewed":
        raise PmcpError(ErrorCode.APPROVAL_REQUIRED, "a person must approve this plan first",
                        details={"approve": approval_hint(app, plan)},
                        next_actions=["Ask the user to approve at the link, then call this tool again"])
    if plan.status != "approved":
        raise PmcpError(ErrorCode.PLAN_NOT_EXECUTABLE, f"plan is {plan.status}")
    appr = app.store.get(Approval, owner, plan_id)
    if not appr or appr.digest != plan.digest or appr.approved_by != owner:
        raise PmcpError(ErrorCode.APPROVAL_REQUIRED, "no approval recorded for this digest",
                        details={"approve": approval_hint(app, plan)})
    conn = app.sessions.connection(owner, plan.account_id)
    if not conn.write_enabled:
        raise PmcpError(ErrorCode.WRITE_ACCESS_NOT_ENABLED, "account is connected read-only")

    reader = app.sessions.session(owner, plan.account_id, plan.region, "reader")
    flow = app.store.get(Flow, owner, plan.flow_id)
    state = stack_state(reader, plan.stack_name, owner)
    if fingerprint(state, flow) != plan.base_fingerprint:
        plan.status = "superseded"
        app.store.save(plan)
        raise PmcpError(ErrorCode.STALE_STATE, "the target changed after review; the plan was superseded",
                        details={"planned": plan.base_fingerprint, "current": fingerprint(state, flow)},
                        next_actions=["prepare_changes again and get it re-approved"])
    if plan.change_set and plan.change_set.get("id"):
        cfn = reader.client("cloudformation", config=BOTO_CONFIG)
        d = cfn.describe_change_set(ChangeSetName=plan.change_set["id"])
        if d["ExecutionStatus"] != "AVAILABLE":
            raise PmcpError(ErrorCode.STALE_STATE, f"change set is {d['ExecutionStatus']}, not AVAILABLE")

    active = [o for o in app.store.list(Operation, owner, limit=50) if o.status in ("queued", "running")]
    if len(active) >= app.settings.max_concurrent_operations_per_user:
        raise PmcpError(ErrorCode.QUOTA_EXCEEDED,
                        f"{len(active)} operations already running (limit "
                        f"{app.settings.max_concurrent_operations_per_user})",
                        details={"running": [o.operation_id for o in active]})
    if plan.cost and plan.kind == "change":
        others = sum(_flow_monthly(app, owner, f) for f in app.store.list(Flow, owner, limit=200)
                     if f.account_id == plan.account_id and f.flow_id != plan.flow_id and f.status != "deleted")
        if plan.cost.monthly_usd + others > conn.monthly_cost_cap_usd:
            raise PmcpError(ErrorCode.COST_CAP_EXCEEDED,
                            f"estimated {plan.cost.monthly_usd} + existing flows {others:.2f} USD/month exceeds the "
                            f"account cap {conn.monthly_cost_cap_usd}",
                            next_actions=["Lower the schedule/volume, or raise the cap on the connect page"])

    op_id = new_id("op")
    plan.status = "applying"
    plan.operation_id = op_id
    try:
        plan = app.store.save(plan)  # CAS: a concurrent apply with another key loses here
    except PmcpError:
        fresh = app.store.require(Plan, owner, plan_id)
        if fresh.operation_id:
            return _existing_or_resume(app, owner, fresh.operation_id)
        raise
    won = app.store.claim(owner, idem_name, {"operation_id": op_id, "plan_id": plan_id},
                          ttl_seconds=7 * 86400)
    if won is not None:  # lost a race on the same key
        return _existing_or_resume(app, owner, won["operation_id"])
    lock = app.store.claim(owner, f"lock:flow:{plan.flow_id}", {"operation_id": op_id},
                           ttl_seconds=app.settings.operation_max_seconds + 600)
    if lock is not None:
        holder = app.store.get(Operation, owner, lock.get("operation_id", ""))
        if holder is None or holder.status in ("succeeded", "failed", "cancelled"):
            app.store.release(owner, f"lock:flow:{plan.flow_id}", only_if=lock)
            lock = app.store.claim(owner, f"lock:flow:{plan.flow_id}", {"operation_id": op_id},
                                   ttl_seconds=app.settings.operation_max_seconds + 600)
        if lock is not None:
            plan.status = "approved"
            plan.operation_id = None
            app.store.save(plan)
            app.store.release(owner, idem_name)
            raise PmcpError(ErrorCode.CONFLICT, f"flow {plan.flow_id} has another operation in progress",
                            details={"operation_id": lock.get("operation_id")})
    if flow is None and plan.kind == "change":
        flow = app.store.create(Flow(
            id=plan.flow_id, owner=owner, flow_id=plan.flow_id,
            name=str(plan.desired_state.get("flow_name") or plan.flow_id), account_id=plan.account_id,
            region=plan.region, stack_name=plan.stack_name, pattern_id=plan.pattern_id, status="deploying",
        ))
    kind = "apply_changes" if plan.kind == "change" else "execute_recovery"
    op = app.store.create(Operation(
        id=op_id, owner=owner, operation_id=op_id, kind=kind, account_id=plan.account_id, region=plan.region,
        flow_id=plan.flow_id, plan_id=plan.plan_id, idempotency_key=idempotency_key,
        steps=new_steps(plan.steps), deadline_at=time.time() + app.settings.operation_max_seconds,
    ))
    op.runner_ref = app.runner.launch(op)
    try:
        op = app.store.save(op)
    except PmcpError:  # the worker already advanced the record; the runner ref is cosmetic
        op = app.store.require(Operation, owner, op_id)
    return op


def _flow_monthly(app: App, owner: str, flow: Flow) -> float:
    if not flow.last_plan_id:
        return 0.0
    p = app.store.get(Plan, owner, flow.last_plan_id)
    return p.cost.monthly_usd if p and p.cost else 0.0


def _existing_or_resume(app: App, owner: str, op_id: str) -> Operation:
    from pmcp.services.operations import lease_alive

    op = app.store.get(Operation, owner, op_id)
    if op is None:
        raise PmcpError(ErrorCode.CONFLICT, "the operation for this request is still being created; retry shortly",
                        details={"operation_id": op_id})
    if op.status in ("queued", "running") and not lease_alive(app, owner, op_id):
        # Worker died (or never started). Relaunch; it resumes at the first unfinished step.
        op.runner_ref = app.runner.launch(op)
        try:
            op = app.store.save(op)
        except PmcpError:
            op = app.store.require(Operation, owner, op_id)
    return op
