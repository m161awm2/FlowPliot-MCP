from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from pmcp.domain.records import Flow, Operation, PlanStep, StepState, Verification
from pmcp.errors import ErrorCode, PmcpError
from pmcp.ids import new_id

if TYPE_CHECKING:
    from pmcp.app import App


def run_verification(app: App, owner: str, flow: Flow, window: dict[str, str] | None,
                     operation_id: str | None) -> Verification:
    from pmcp.adapters import get_adapter
    from pmcp.services.steps import build_runtime

    if flow.contract_id is None:
        raise PmcpError(ErrorCode.PRECONDITION_FAILED, "flow has no contract to verify against")
    rt = build_runtime(app, owner, flow, write=False)
    win, checks = get_adapter(flow.pattern_id).verify(rt, window)
    statuses = {c.status for c in checks}
    if "failed" in statuses or "error" in statuses:
        verdict = "failed"
    elif "unverified" in statuses:
        verdict = "partially_verified"
    else:
        verdict = "verified"
    vid = new_id("vr")
    ver = app.store.create(Verification(
        id=vid, owner=owner, verification_id=vid, flow_id=flow.flow_id, contract_id=flow.contract_id,
        contract_version=flow.contract_version, window=win, verdict=verdict, checks=checks,  # type: ignore[arg-type]
        operation_id=operation_id,
    ))
    fresh = app.store.require(Flow, owner, flow.flow_id)
    fresh.last_verification_id = vid
    if fresh.status in ("deployed", "verified", "degraded"):
        fresh.status = {"verified": "verified", "partially_verified": "deployed", "failed": "degraded"}[verdict]
    app.store.save(fresh)
    return ver


def start_single_step_op(app: App, owner: str, flow: Flow, kind: str, step: PlanStep,
                         idempotency_key: str | None) -> Operation:
    from pmcp.services.planning import _existing_or_resume

    key = idempotency_key or new_id("auto")
    name = f"idem:{key}"
    prior = app.store.peek_claim(owner, name)
    if prior:
        if prior.get("flow_id") != flow.flow_id or prior.get("kind") != kind:
            raise PmcpError(ErrorCode.CONFLICT, "idempotency_key was used for a different request")
        return _existing_or_resume(app, owner, prior["operation_id"])
    active = [o for o in app.store.list(Operation, owner, limit=50) if o.status in ("queued", "running")]
    if len(active) >= app.settings.max_concurrent_operations_per_user:
        raise PmcpError(ErrorCode.QUOTA_EXCEEDED, "too many running operations",
                        details={"running": [o.operation_id for o in active]})
    op_id = new_id("op")
    won = app.store.claim(owner, name, {"operation_id": op_id, "flow_id": flow.flow_id, "kind": kind},
                          ttl_seconds=86400)
    if won is not None:
        return _existing_or_resume(app, owner, won["operation_id"])
    op = app.store.create(Operation(
        id=op_id, owner=owner, operation_id=op_id, kind=kind, account_id=flow.account_id,  # type: ignore[arg-type]
        region=flow.region, flow_id=flow.flow_id, idempotency_key=key, steps=[StepState(step_id=step.step_id)],
        deadline_at=time.time() + app.settings.operation_max_seconds,
        params={"steps": [step.model_dump(mode="json")]},
    ))
    op.runner_ref = app.runner.launch(op)
    try:
        return app.store.save(op)
    except PmcpError:
        return app.store.require(Operation, owner, op_id)


def start_verification(app: App, owner: str, flow_id: str, window: dict[str, str] | None,
                       idempotency_key: str | None) -> Operation:
    flow = app.store.require(Flow, owner, flow_id)
    if flow.status in ("planned", "deploying", "deleted") or flow.last_plan_id is None:
        raise PmcpError(ErrorCode.PRECONDITION_FAILED,
                        f"flow is {flow.status} and has never been deployed successfully; nothing to verify",
                        next_actions=["get_operation on the failed apply for the cause, then prepare_changes again"])
    step = PlanStep(step_id="verify", action="verify", description="Verify data against the contract",
                    params={"window": window})
    return start_single_step_op(app, owner, flow, "verify_data_flow", step, idempotency_key)


def summarize(ver: Verification) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for c in ver.checks:
        counts[c.status] = counts.get(c.status, 0) + 1
    return {"verification_id": ver.verification_id, "verdict": ver.verdict, "window": ver.window, "counts": counts}
