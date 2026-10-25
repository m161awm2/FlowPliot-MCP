"""Observe, diagnose, plan recovery."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from pmcp.adapters import get_adapter
from pmcp.aws.client import BOTO_CONFIG
from pmcp.domain.records import Flow, Incident, Operation, Plan, PlanStep, Verification
from pmcp.errors import ErrorCode, PmcpError
from pmcp.ids import digest, iso, new_id
from pmcp.services.steps import build_runtime

if TYPE_CHECKING:
    from pmcp.app import App


def observe(app: App, owner: str, flow_id: str, include_cost: bool = False) -> dict[str, Any]:
    flow = app.store.require(Flow, owner, flow_id)
    rt = build_runtime(app, owner, flow, write=False)
    out: dict[str, Any] = {
        "flow": {"flow_id": flow.flow_id, "name": flow.name, "status": flow.status, "pattern_id": flow.pattern_id,
                 "account_id": flow.account_id, "region": flow.region,
                 "contract": [flow.contract_id, flow.contract_version], "outputs": flow.outputs},
        "observed_at": iso(),
    }
    try:
        st = rt.reader.client("cloudformation", config=BOTO_CONFIG).describe_stacks(StackName=flow.stack_name)[
            "Stacks"][0]
        out["stack"] = {"status": st["StackStatus"], "last_updated": str(st.get("LastUpdatedTime") or
                                                                         st.get("CreationTime"))}
    except Exception as exc:
        out["stack"] = {"error": str(exc)[:200]}
    if flow.status not in ("planned", "deleted"):
        try:
            out["runtime"] = get_adapter(flow.pattern_id).observe(rt)
        except PmcpError as exc:
            out["runtime"] = {"error": exc.to_dict()}
        except Exception as exc:
            out["runtime"] = {"error": f"{type(exc).__name__}: {str(exc)[:200]}"}
    if flow.last_verification_id:
        v = app.store.get(Verification, owner, flow.last_verification_id)
        if v:
            from pmcp.services.verification import summarize

            out["last_verification"] = summarize(v) | {"at": v.created_at}
    active = [o for o in app.store.list(Operation, owner, limit=50)
              if o.flow_id == flow_id and o.status in ("queued", "running")]
    out["active_operations"] = [{"operation_id": o.operation_id, "kind": o.kind, "status": o.status} for o in active]
    if include_cost:
        try:
            ce = rt.reader.client("ce", region_name="us-east-1", config=BOTO_CONFIG)
            end = datetime.now(UTC).date()
            resp = ce.get_cost_and_usage(
                TimePeriod={"Start": str(end - timedelta(days=7)), "End": str(end)}, Granularity="DAILY",
                Metrics=["UnblendedCost"], Filter={"Tags": {"Key": "pmcp:flow", "Values": [flow_id]}})
            out["cost_last_7_days_usd"] = round(sum(float(r["Total"]["UnblendedCost"]["Amount"])
                                                    for r in resp["ResultsByTime"]), 4)
            out["cost_note"] = ("Cost Explorer: tag must be activated as a cost allocation tag; data lags ~24h; "
                                "each call costs the account USD 0.01")
        except Exception as exc:
            out["cost_last_7_days_usd"] = None
            out["cost_note"] = f"unavailable: {str(exc)[:200]}"
    return out


def diagnose(app: App, owner: str, flow_id: str, window: dict[str, str] | None,
             deep_probe: bool, idempotency_key: str | None) -> tuple[Incident, Operation | None]:
    flow = app.store.require(Flow, owner, flow_id)
    if flow.status in ("planned", "deleted"):
        raise PmcpError(ErrorCode.PRECONDITION_FAILED, f"flow is {flow.status}")
    win = window or {"start": iso(datetime.now(UTC) - timedelta(hours=24)), "end": iso()}
    rt = build_runtime(app, owner, flow, write=False)
    parts = get_adapter(flow.pattern_id).diagnose(rt, win)
    # The sliding observation window is not a new failure. Stable evidence gets one incident,
    # including concurrent diagnoses (Store.create is conditional).
    evidence = sorted(digest({k: v for k, v in fact.items() if k != "fact_id"}) for fact in parts.facts)
    inc_id = "in_" + digest({"flow_id": flow_id, "evidence": evidence}).split(":")[-1][:32]
    inc = app.store.get(Incident, owner, inc_id)
    while inc is not None and inc.status == "resolved":
        inc_id = "in_" + digest({"previous": inc_id, "resolved_at": inc.updated_at}).split(":")[-1][:32]
        inc = app.store.get(Incident, owner, inc_id)
    if inc is None:
        try:
            inc = app.store.create(Incident(
                id=inc_id, owner=owner, incident_id=inc_id, flow_id=flow_id, window=win, observed_facts=parts.facts,
                hypotheses=parts.hypotheses, further_checks=parts.further_checks, affected=parts.affected,
            ))
        except PmcpError as exc:
            if exc.code != ErrorCode.CONFLICT:
                raise
            inc = app.store.require(Incident, owner, inc_id)
    op = None
    if deep_probe:
        from pmcp.services.verification import start_single_step_op

        step = PlanStep(step_id="probe", action="probe", description="Locate offending inputs",
                        params={"window": win, "incident_id": inc_id})
        op = start_single_step_op(app, owner, flow, "diagnose_probe", step, idempotency_key)
    return inc, op


def plan_recovery(app: App, owner: str, incident_id: str, strategy: str | None) -> tuple[Plan, list[dict[str, Any]]]:
    from pmcp.services.planning import fingerprint, stack_state

    inc = app.store.require(Incident, owner, incident_id)
    flow = app.store.require(Flow, owner, inc.flow_id)
    rt = build_runtime(app, owner, flow, write=False)
    options = get_adapter(flow.pattern_id).recovery_options(rt, inc)
    listing = [{"strategy": o.strategy, "description": o.description, "notes": o.notes,
                "restores_infrastructure": o.needs_template_restore} for o in options]
    if not options:
        raise PmcpError(ErrorCode.UNSUPPORTED_CONFIGURATION, "no automated recovery for this pattern")
    if strategy is None:
        suggested = [s for h in inc.hypotheses for s in h.suggested_recovery]
        strategy = next((s for s in suggested if any(o.strategy == s for o in options)), options[0].strategy)
    opt = next((o for o in options if o.strategy == strategy), None)
    if opt is None:
        raise PmcpError(ErrorCode.INVALID_INPUT, f"strategy '{strategy}' is not available for this incident",
                        details={"available": [o.strategy for o in options]})
    if "quarantine" in strategy and not inc.affected.get("bad_files"):
        raise PmcpError(ErrorCode.PRECONDITION_FAILED, "no offending files identified yet",
                        next_actions=["diagnose_failure(deep_probe=true) and wait for the probe operation"])
    state = stack_state(rt.reader, flow.stack_name, owner)
    artifacts = []
    steps = [PlanStep(step_id="precheck", action="precheck", description="Re-check approval/state")]
    plan_id = new_id("pl")
    pattern_params = flow.pattern_params
    code_diffs: list[dict[str, str]] = []
    if opt.param_updates:
        artifacts, pattern_params, code_diffs = _render_param_update(app, owner, flow, plan_id, opt.param_updates)
        steps += [PlanStep(step_id="execute_change_set", action="execute_change_set",
                           description=f"Apply the configuration change ({', '.join(opt.param_updates)})"),
                  PlanStep(step_id="wait_stack", action="wait_stack", description="Wait for stack"),
                  PlanStep(step_id="record_flow", action="record_flow",
                           description="Record the new flow configuration before re-running")]
    elif opt.needs_template_restore:
        last = app.store.get(Plan, owner, flow.last_plan_id) if flow.last_plan_id else None
        tmpl = next((a for a in (last.artifacts if last else []) if a.name == "template.json"), None)
        if tmpl is None:
            raise PmcpError(ErrorCode.PRECONDITION_FAILED, "no applied template recorded for this flow")
        artifacts = list(last.artifacts) if last else []
        steps += [PlanStep(step_id="execute_change_set", action="execute_change_set",
                           description="Re-apply the last approved template (repairs configuration drift)"),
                  PlanStep(step_id="wait_stack", action="wait_stack", description="Wait for stack")]
    steps += opt.steps
    now = datetime.now(UTC)
    from pmcp.domain.records import DataImpact

    plan = Plan(
        id=plan_id, owner=owner, plan_id=plan_id, kind="recovery", account_id=flow.account_id, region=flow.region,
        flow_id=flow.flow_id, stack_name=flow.stack_name, pattern_id=flow.pattern_id,
        pattern_params=pattern_params, contract_id=flow.contract_id, contract_version=flow.contract_version,
        incident_id=incident_id, summary=f"Recover flow {flow.flow_id}: {opt.description}",
        base_fingerprint=fingerprint(state, flow), current_state={k: v for k, v in state.items() if k != "template"},
        desired_state={"strategy": strategy, "affected": inc.affected},
        artifacts=artifacts, code_diffs=code_diffs,
        data_impact=DataImpact(datasets=[flow.outputs.get("OutputTable", flow.flow_id)],
                               data_rollback=opt.data_rollback,
                               infra_rollback="re-apply last template" if opt.needs_template_restore else
                               ("configuration change via change set; revert with a change plan"
                                if opt.param_updates else "none"),
                               consumer_notes=opt.notes),
        steps=steps, warnings=opt.notes,
        expires_at=iso(now + timedelta(seconds=app.settings.plan_ttl_seconds)),
    )
    plan.digest = digest(plan.digest_material())
    app.store.create(plan)
    inc.status = "recovering"
    app.store.save(inc)
    return plan, listing


def _render_param_update(app: App, owner: str, flow: Flow, plan_id: str,
                         updates: dict[str, Any]) -> tuple[list[Any], dict[str, Any], list[dict[str, str]]]:
    """Render the flow's template with updated pattern params (same contract) for a recovery change set."""
    import difflib

    from pmcp.adapters.base import RenderContext
    from pmcp.ids import canonical_json
    from pmcp.services.contracts import load_contract
    from pmcp.services.planning import _artifact, _trusted_principals, lint_template

    if not (flow.contract_id and flow.contract_version):
        raise PmcpError(ErrorCode.PRECONDITION_FAILED, "flow has no deployed contract")
    adapter = get_adapter(flow.pattern_id)
    contract = load_contract(app, owner, flow.contract_id, flow.contract_version)
    params = adapter.parse_params({**flow.pattern_params, **updates}, contract.body)
    conn = app.sessions.connection(owner, flow.account_id)
    ctx = RenderContext(owner=owner, flow_id=flow.flow_id, plan_id=plan_id, account_id=flow.account_id,
                        region=flow.region, stack_name=flow.stack_name, contract=contract, connection=conn,
                        trusted_principals=_trusted_principals(app, conn),
                        boundary_policy_arn=conn.boundary_policy_arn, previous_params=flow.pattern_params,
                        previous_contract=contract)
    rendered = adapter.render(ctx, params)
    errors, _ = lint_template(rendered.template, flow.region)
    if errors:
        raise PmcpError(ErrorCode.UNSUPPORTED_CONFIGURATION, "rendered template failed cfn-lint",
                        details={"errors": errors[:20]})
    artifacts = [_artifact("template.json", "template", canonical_json(rendered.template))]
    artifacts += [_artifact(a.name, a.kind, a.content) for a in rendered.artifacts]
    last = app.store.get(Plan, owner, flow.last_plan_id) if flow.last_plan_id else None
    previous = {a.name: a for a in (last.artifacts if last else [])}
    diffs = []
    for a in artifacts:
        old = previous.get(a.name)
        if a.kind == "sql" and old and old.content and old.sha256 != a.sha256:
            diffs.append({"artifact": a.name, "diff": "".join(difflib.unified_diff(
                old.content.splitlines(True), (a.content or "").splitlines(True), f"a/{a.name}", f"b/{a.name}"))})
    return artifacts, params.model_dump(mode="json"), diffs

