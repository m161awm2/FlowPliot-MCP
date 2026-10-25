"""Worker step implementations (generic ones; pattern-specific actions live in the adapters)."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pmcp.adapters import get_adapter
from pmcp.adapters.base import Adapter, Runtime
from pmcp.aws.client import BOTO_CONFIG
from pmcp.domain.contract import DataContract
from pmcp.domain.records import Flow, Incident, Operation, Plan, PlanStep
from pmcp.errors import ErrorCode, PmcpError
from pmcp.ids import iso

if TYPE_CHECKING:
    from pmcp.app import App


@dataclass
class StepContext:
    app: App
    op: Operation
    plan: Plan | None
    flow: Flow
    adapter: Adapter

    @classmethod
    def load(cls, app: App, op: Operation) -> StepContext:
        plan = app.store.get(Plan, op.owner, op.plan_id) if op.plan_id else None
        flow = app.store.require(Flow, op.owner, op.flow_id)
        return cls(app=app, op=op, plan=plan, flow=flow, adapter=get_adapter(flow.pattern_id))

    def runtime(self, *, write: bool) -> Runtime:
        return build_runtime(self.app, self.op.owner, self.flow, write=write)


def build_runtime(app: App, owner: str, flow: Flow, *, write: bool) -> Runtime:
    from pmcp.services.contracts import load_contract

    reader = app.sessions.session(owner, flow.account_id, flow.region, "reader")
    deployer = app.sessions.session(owner, flow.account_id, flow.region, "deployer") if write else None
    contract: DataContract | None = None
    if flow.contract_id and flow.contract_version:
        contract = load_contract(app, owner, flow.contract_id, flow.contract_version)
    return Runtime(
        reader=reader,
        deployer=deployer,
        consumer=lambda arn: app.sessions.role_session(owner, flow.account_id, flow.region, arn),
        flow=flow,
        contract=contract,
    )


def _plan_step(ctx: StepContext, index: int) -> PlanStep:
    assert ctx.plan is not None
    return ctx.plan.steps[index]


def run_step(ctx: StepContext, op: Operation, index: int) -> dict[str, Any]:
    ctx.op = op
    ctx.flow = ctx.app.store.require(Flow, op.owner, op.flow_id)
    if ctx.plan is None:  # verify / probe operations carry their single step in params
        step = PlanStep.model_validate(op.params["steps"][index])
    else:
        step = _plan_step(ctx, index)
    handler = GENERIC.get(step.action)
    if handler:
        return handler(ctx, step)
    write = op.kind in ("apply_changes", "execute_recovery")
    rt = ctx.runtime(write=write)
    if ctx.plan and ctx.plan.contract_id:  # the plan's contract version, not the flow's previous one
        from pmcp.services.contracts import load_contract

        rt.contract = load_contract(ctx.app, op.owner, ctx.plan.contract_id, ctx.plan.contract_version)
    return ctx.adapter.run_step(step, rt, op.operation_id)


# --- generic steps ------------------------------------------------------------------------------------


def _precheck(ctx: StepContext, step: PlanStep) -> dict[str, Any]:
    from pmcp.domain.records import Approval
    from pmcp.ids import digest
    from pmcp.services.planning import fingerprint, stack_state

    plan = ctx.plan
    assert plan is not None
    if digest(plan.digest_material()) != plan.digest:
        raise PmcpError(ErrorCode.DIGEST_MISMATCH, "plan content changed after approval")
    appr = ctx.app.store.get(Approval, plan.owner, plan.plan_id)
    if not appr or appr.digest != plan.digest:
        raise PmcpError(ErrorCode.APPROVAL_REQUIRED, "approval missing at execution time")
    reader = ctx.app.sessions.session(plan.owner, plan.account_id, plan.region, "reader")
    state = stack_state(reader, plan.stack_name, plan.owner)
    # A flow record created by this very apply (first deploy) is not part of the planned state.
    flow = None if (ctx.flow.last_plan_id is None and plan.base_fingerprint.get("flow_config") is None) else ctx.flow
    if fingerprint(state, flow) != plan.base_fingerprint:
        raise PmcpError(ErrorCode.STALE_STATE, "target changed between approval and execution",
                        details={"planned": plan.base_fingerprint, "current": fingerprint(state, flow)})
    return {"checked_at": iso()}


def request_token(*parts: str) -> str:
    """CloudFormation ClientRequestToken: [a-zA-Z0-9][-a-zA-Z0-9]*, max 128."""
    import re as _re

    return _re.sub(r"[^a-zA-Z0-9-]", "-", "-".join(parts))[:128]


def _deployer(ctx: StepContext) -> Any:
    assert ctx.plan is not None
    return ctx.app.sessions.session(ctx.plan.owner, ctx.plan.account_id, ctx.plan.region, "deployer")


def _delete_failed_stack(ctx: StepContext, step: PlanStep) -> dict[str, Any]:
    assert ctx.plan is not None
    cfn = _deployer(ctx).client("cloudformation", config=BOTO_CONFIG)
    try:
        st = cfn.describe_stacks(StackName=ctx.plan.stack_name)["Stacks"][0]
    except Exception:
        return {"skipped": "stack already gone"}
    if st["StackStatus"] != "ROLLBACK_COMPLETE":
        if st["StackStatus"] == "DELETE_COMPLETE":
            return {"skipped": "already deleted"}
        raise PmcpError(ErrorCode.STALE_STATE, f"stack is {st['StackStatus']}, expected ROLLBACK_COMPLETE")
    conn = ctx.app.sessions.connection(ctx.plan.owner, ctx.plan.account_id)
    kwargs = {"RoleARN": conn.cfn_exec_role_arn} if conn.cfn_exec_role_arn else {}
    cfn.delete_stack(StackName=ctx.plan.stack_name, ClientRequestToken=request_token(ctx.op.operation_id, "del"), **kwargs)
    _wait(cfn, ctx.plan.stack_name, deleting=True)
    # Create the change set now that the name is free; it is built from the approved template artifact.
    from pmcp.services.planning import create_change_set

    tmpl = next(a for a in ctx.plan.artifacts if a.name == "template.json")
    cs = ctx.plan.change_set or {}
    cs_id = create_change_set(ctx.app, _deployer(ctx), ctx.plan, tmpl, cs.get("name", "pmcp-recreate"), "CREATE")
    _wait_change_set(cfn, cs_id)
    ctx.op.params["created_change_set"] = cs_id
    ctx.app.store.save(ctx.op)
    return {"deleted": ctx.plan.stack_name, "change_set": cs_id}


def _wait_change_set(cfn: Any, cs_id: str) -> None:
    for _ in range(60):
        d = cfn.describe_change_set(ChangeSetName=cs_id)
        if d["Status"] not in ("CREATE_PENDING", "CREATE_IN_PROGRESS"):
            if d["Status"] != "CREATE_COMPLETE":
                raise PmcpError(ErrorCode.AWS_ERROR, f"change set {d['Status']}: {d.get('StatusReason')}")
            return
        time.sleep(2)
    raise PmcpError(ErrorCode.TIMEOUT, "change set creation timed out")


def _verify_artifacts(ctx: StepContext, step: PlanStep) -> dict[str, Any]:
    assert ctx.plan is not None
    s3 = _deployer(ctx).client("s3", config=BOTO_CONFIG)
    checked = []
    for a in ctx.plan.artifacts:
        if not a.s3_uri:
            continue
        bucket, key = a.s3_uri[5:].split("/", 1)
        head = s3.head_object(Bucket=bucket, Key=key)
        if head.get("Metadata", {}).get("sha256") != a.sha256:
            raise PmcpError(ErrorCode.STALE_STATE, f"artifact {a.s3_uri} changed after review")
        checked.append(a.s3_uri)
    return {"verified": checked}


def _execute_change_set(ctx: StepContext, step: PlanStep) -> dict[str, Any]:
    assert ctx.plan is not None
    cs = ctx.plan.change_set or {}
    cs_id = ctx.op.params.get("created_change_set") or cs.get("id")
    if cs.get("status") == "NO_CHANGES":
        return {"skipped": "change set had no infrastructure changes"}
    if not cs_id:
        raise PmcpError(ErrorCode.PRECONDITION_FAILED, "plan has no change set; run preview_changes")
    cfn = _deployer(ctx).client("cloudformation", config=BOTO_CONFIG)
    d = cfn.describe_change_set(ChangeSetName=cs_id)
    if d["ExecutionStatus"] in ("EXECUTE_IN_PROGRESS", "EXECUTE_COMPLETE"):
        return {"change_set": cs_id, "note": f"already {d['ExecutionStatus']} (resumed)"}
    if d["ExecutionStatus"] != "AVAILABLE":
        raise PmcpError(ErrorCode.STALE_STATE, f"change set is {d['ExecutionStatus']}")
    # ClientRequestToken makes a retried call a no-op instead of a second execution.
    cfn.execute_change_set(ChangeSetName=cs_id, ClientRequestToken=request_token(ctx.op.operation_id))
    return {"change_set": cs_id, "executed_at": iso()}


def _wait(cfn: Any, stack: str, *, deleting: bool = False) -> dict[str, Any]:
    deadline = time.time() + 1500
    while time.time() < deadline:
        try:
            st = cfn.describe_stacks(StackName=stack)["Stacks"][0]
        except Exception as exc:
            if deleting and "does not exist" in str(exc):
                return {"status": "DELETE_COMPLETE"}
            raise
        status = st["StackStatus"]
        if not status.endswith("_IN_PROGRESS"):
            return st
        time.sleep(5)
    raise PmcpError(ErrorCode.TIMEOUT, f"stack {stack} did not settle in time")


def _wait_stack(ctx: StepContext, step: PlanStep) -> dict[str, Any]:
    assert ctx.plan is not None
    if (ctx.plan.change_set or {}).get("status") == "NO_CHANGES":
        st = ctx.app.sessions.session(ctx.plan.owner, ctx.plan.account_id, ctx.plan.region, "reader").client(
            "cloudformation", config=BOTO_CONFIG).describe_stacks(StackName=ctx.plan.stack_name)["Stacks"][0]
    else:
        cfn = _deployer(ctx).client("cloudformation", config=BOTO_CONFIG)
        st = _wait(cfn, ctx.plan.stack_name)
    status = st["StackStatus"]
    if status not in ("CREATE_COMPLETE", "UPDATE_COMPLETE", "IMPORT_COMPLETE"):
        reasons = []
        try:
            cfn = _deployer(ctx).client("cloudformation", config=BOTO_CONFIG)
            for ev in cfn.describe_stack_events(StackName=ctx.plan.stack_name)["StackEvents"][:40]:
                if ev.get("ResourceStatus", "").endswith("_FAILED"):
                    reasons.append(f"{ev['LogicalResourceId']}: {ev.get('ResourceStatusReason', '')}"[:300])
        except Exception:
            pass
        restored = status in ("UPDATE_ROLLBACK_COMPLETE",)
        raise PmcpError(
            ErrorCode.AWS_ERROR,
            f"stack ended {status}" + ("; previous configuration restored by rollback" if restored else ""),
            details={"failed_resources": reasons[:10], "infra_restored": restored,
                     "note": "Data steps were not run." if restored else
                     "Create failed; CloudFormation removed created resources except Retain ones (data bucket)."},
        )
    outputs = {o["OutputKey"]: o["OutputValue"] for o in st.get("Outputs", [])}
    flow = ctx.app.store.require(Flow, ctx.op.owner, ctx.op.flow_id)
    flow.outputs = outputs
    ctx.app.store.save(flow)
    return {"stack_status": status, "outputs": outputs}


def _record_flow(ctx: StepContext, step: PlanStep) -> dict[str, Any]:
    plan = ctx.plan
    assert plan is not None
    flow = ctx.app.store.require(Flow, ctx.op.owner, ctx.op.flow_id)
    tmpl = next((a for a in plan.artifacts if a.name == "template.json"), None)
    flow.pattern_params = plan.pattern_params
    flow.contract_id = plan.contract_id
    flow.contract_version = plan.contract_version
    if tmpl:
        flow.template_sha256 = tmpl.sha256
    flow.status = "deployed"
    flow.last_plan_id = plan.plan_id
    flow.last_operation_id = ctx.op.operation_id
    flow.history.append({"at": iso(), "plan_id": plan.plan_id, "kind": plan.kind, "summary": plan.summary,
                         "operation_id": ctx.op.operation_id})
    flow.history = flow.history[-50:]
    ctx.app.store.save(flow)
    return {"flow_status": flow.status}


def _verify(ctx: StepContext, step: PlanStep) -> dict[str, Any]:
    from pmcp.services.verification import run_verification

    ver = run_verification(ctx.app, ctx.op.owner, ctx.flow, step.params.get("window"), ctx.op.operation_id)
    ctx.op.result["verification_id"] = ver.verification_id
    if ctx.op.kind == "execute_recovery" and ver.verdict == "failed":
        raise PmcpError(ErrorCode.PRECONDITION_FAILED, "recovery ran but verification failed",
                        details={"verification_id": ver.verification_id,
                                 "failed_checks": [c.check_id for c in ver.checks if c.status == "failed"]})
    return {"verification_id": ver.verification_id, "verdict": ver.verdict}


def _probe(ctx: StepContext, step: PlanStep) -> dict[str, Any]:
    rt = ctx.runtime(write=False)
    result = ctx.adapter.probe(rt, step.params["window"])
    inc = ctx.app.store.require(Incident, ctx.op.owner, step.params["incident_id"])
    inc.affected["bad_files"] = result.get("bad_files", [])
    inc.observed_facts.append({"fact_id": f"F{len(inc.observed_facts) + 1}", "kind": "probe",
                               "operation_id": ctx.op.operation_id,
                               "bad_files": result.get("bad_files", [])[:50],
                               "summary": [{k: v for k, v in p.items() if k != "rows"} | {"rows": p.get("rows", [])[:20]}
                                           for p in result.get("probes", [])]})
    for h in inc.hypotheses:
        if result.get("bad_files") and "parsed" in h.statement:
            h.supporting_evidence.append(inc.observed_facts[-1]["fact_id"])
    ctx.app.store.save(inc)
    return {"bad_files": result.get("bad_files", [])}


GENERIC = {
    "precheck": _precheck,
    "cfn.delete_failed_stack": _delete_failed_stack,
    "verify_artifacts": _verify_artifacts,
    "execute_change_set": _execute_change_set,
    "wait_stack": _wait_stack,
    "record_flow": _record_flow,
    "verify": _verify,
    "probe": _probe,
}


def finish(ctx: StepContext, op: Operation, *, ok: bool) -> dict[str, Any]:
    app, owner = ctx.app, op.owner
    result = dict(op.result)
    for st in op.steps:
        for key in ("verification_id", "verdict", "bad_files"):
            if key in st.detail:
                result[key] = st.detail[key]
    flow = app.store.require(Flow, owner, op.flow_id)
    if ctx.plan is not None:
        plan = app.store.require(Plan, owner, ctx.plan.plan_id)
        plan.status = "applied" if ok else "failed"
        app.store.save(plan)
        app.store.release(owner, f"lock:flow:{plan.flow_id}", only_if={"operation_id": op.operation_id})
        if not ok and flow.status == "deploying":
            flow.status = "failed"
            app.store.save(flow)
            result["cleanup"] = _drop_unexecuted_stack(ctx, plan)
        if plan.kind == "recovery" and plan.incident_id:
            inc = app.store.get(Incident, owner, plan.incident_id)
            if inc:
                inc.status = "resolved" if ok else "open"
                app.store.save(inc)
    result.update({"flow_id": flow.flow_id, "flow_status": flow.status})
    if ok and op.kind == "apply_changes":
        result["next_actions"] = ["verify_data_flow: confirm data reached the destination and consumers can read it",
                                  "observe_flow: check runs and alarms"]
    elif not ok:
        result["next_actions"] = ["diagnose_failure for evidence", "get_operation shows the failed step"]
    return result


def _drop_unexecuted_stack(ctx: StepContext, plan: Plan) -> str:
    """A first deploy that failed before its change set executed leaves an empty REVIEW_IN_PROGRESS stack;
    delete it so the stack name is free and nothing dangles. Stacks with resources are never touched here."""
    try:
        cfn = _deployer(ctx).client("cloudformation", config=BOTO_CONFIG)
        st = cfn.describe_stacks(StackName=plan.stack_name)["Stacks"][0]
        if st["StackStatus"] != "REVIEW_IN_PROGRESS":
            return f"stack left as {st['StackStatus']}"
        conn = ctx.app.sessions.connection(plan.owner, plan.account_id)
        kwargs = {"RoleARN": conn.cfn_exec_role_arn} if conn.cfn_exec_role_arn else {}
        cfn.delete_stack(StackName=plan.stack_name, **kwargs)
        return "deleted empty REVIEW_IN_PROGRESS stack"
    except Exception as exc:  # cleanup is best effort; the failure itself is already recorded
        return f"cleanup skipped: {str(exc)[:200]}"
