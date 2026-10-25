"""Plan → preview → approval → apply → worker, against moto. Covers scenario 7 (bad plan, stale state,
duplicate execution) and the approval/idempotency guarantees."""

import time

import boto3
import pytest

from pmcp.domain.records import Flow, Operation, Plan
from pmcp.errors import ErrorCode, PmcpError
from pmcp.services import contracts, planning
from pmcp.services.operations import run_operation

from .conftest import ACCOUNT, OWNER, REGION, BucketAdapter, contract_body


def _contract(app):
    c, _ = contracts.define(app, OWNER, contract_body(), None, None)
    return c


def _prepare(app, c, **kw):
    return planning.prepare(app, OWNER, planning.PrepareRequest(
        account_id=ACCOUNT, region=REGION, contract_id=c.contract_id, pattern_id="test_bucket",
        pattern_params=kw.pop("params", {"note": "v1"}), **kw))


def _wait(app, op_id, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        op = app.store.require(Operation, OWNER, op_id)
        if op.status in ("succeeded", "failed"):
            return op
        time.sleep(0.05)
    raise AssertionError("operation did not finish")


def _approve(app, plan):
    return planning.record_approval(app, OWNER, plan.plan_id, plan.digest, auth_time=int(time.time()),
                                    channel="local_cli")


def _deploy(app, c, key="k-000001", **kw):
    plan = planning.preview(app, OWNER, _prepare(app, c, **kw).plan_id)
    _approve(app, plan)
    op = planning.start_execution(app, OWNER, plan.plan_id, plan.digest, key, "change")
    return plan, _wait(app, op.operation_id)


def test_full_create_flow(app):
    c = _contract(app)
    plan = _prepare(app, c)
    assert plan.status == "draft" and plan.digest.startswith("sha256:")
    assert [s.action for s in plan.steps] == ["precheck", "execute_change_set", "wait_stack", "test.mark",
                                              "record_flow"]
    assert plan.resource_changes[0].action == "add"
    plan = planning.preview(app, OWNER, plan.plan_id)
    assert plan.status == "previewed" and plan.change_set["status"] == "CREATE_COMPLETE"
    with pytest.raises(PmcpError) as e:
        planning.start_execution(app, OWNER, plan.plan_id, plan.digest, "k-000001", "change")
    assert e.value.code == ErrorCode.APPROVAL_REQUIRED
    _approve(app, plan)
    op = planning.start_execution(app, OWNER, plan.plan_id, plan.digest, "k-000001", "change")
    op = _wait(app, op.operation_id)
    assert op.status == "succeeded", op.error
    assert all(s.status == "succeeded" for s in op.steps)
    flow = app.store.require(Flow, OWNER, plan.flow_id)
    assert flow.status == "deployed" and flow.last_plan_id == plan.plan_id
    st = boto3.client("cloudformation", region_name=REGION).describe_stacks(StackName=plan.stack_name)["Stacks"][0]
    assert st["StackStatus"] == "CREATE_COMPLETE"
    assert {"Key": "pmcp:owner", "Value": OWNER} in st["Tags"]
    assert app.store.require(Plan, OWNER, plan.plan_id).status == "applied"


def test_digest_mismatch_and_wrong_kind(app):
    c = _contract(app)
    plan = planning.preview(app, OWNER, _prepare(app, c).plan_id)
    _approve(app, plan)
    with pytest.raises(PmcpError) as e:
        planning.start_execution(app, OWNER, plan.plan_id, "sha256:" + "0" * 64, "k-000002", "change")
    assert e.value.code == ErrorCode.DIGEST_MISMATCH
    with pytest.raises(PmcpError) as e:
        planning.start_execution(app, OWNER, plan.plan_id, plan.digest, "k-000002", "recovery")
    assert e.value.code == ErrorCode.INVALID_INPUT


def test_tampered_plan_is_refused(app):
    c = _contract(app)
    plan = planning.preview(app, OWNER, _prepare(app, c).plan_id)
    _approve(app, plan)
    stored = app.store.require(Plan, OWNER, plan.plan_id)
    stored.steps[1].params["sneaky"] = True  # content edited after approval, digest field left as is
    app.store.save(stored)
    with pytest.raises(PmcpError) as e:
        planning.start_execution(app, OWNER, plan.plan_id, plan.digest, "k-000003", "change")
    assert e.value.code == ErrorCode.DIGEST_MISMATCH


def test_approval_bound_to_digest(app):
    c = _contract(app)
    plan = planning.preview(app, OWNER, _prepare(app, c).plan_id)
    with pytest.raises(PmcpError) as e:
        planning.record_approval(app, OWNER, plan.plan_id, "sha256:other", auth_time=0, channel="web")
    assert e.value.code == ErrorCode.DIGEST_MISMATCH
    with pytest.raises(PmcpError):  # draft plans cannot be approved
        planning.record_approval(app, OWNER, _prepare(app, c).plan_id, "x", auth_time=0, channel="web")


def test_stale_state_supersedes_plan(app):
    c = _contract(app)
    _, op = _deploy(app, c)
    flow_id = op.flow_id
    p1 = planning.preview(app, OWNER, _prepare(app, c, flow_id=flow_id, params={"note": "v2"}).plan_id)
    _approve(app, p1)
    # Someone else changes the same flow first.
    p2 = planning.preview(app, OWNER, _prepare(app, c, flow_id=flow_id, params={"note": "v3"}).plan_id)
    _approve(app, p2)
    assert _wait(app, planning.start_execution(app, OWNER, p2.plan_id, p2.digest, "k-other-1", "change")
                 .operation_id).status == "succeeded"
    with pytest.raises(PmcpError) as e:
        planning.start_execution(app, OWNER, p1.plan_id, p1.digest, "k-other-2", "change")
    assert e.value.code == ErrorCode.STALE_STATE
    assert app.store.require(Plan, OWNER, p1.plan_id).status == "superseded"


def test_expired_plan(app):
    c = _contract(app)
    plan = planning.preview(app, OWNER, _prepare(app, c).plan_id)
    _approve(app, plan)
    stored = app.store.require(Plan, OWNER, plan.plan_id)
    stored.expires_at = "2000-01-01T00:00:00Z"  # expiry is part of the digest, so recompute like a real old plan
    from pmcp.ids import digest

    stored.digest = digest(stored.digest_material())
    app.store.save(stored)
    with pytest.raises(PmcpError) as e:
        planning.start_execution(app, OWNER, plan.plan_id, stored.digest, "k-000004", "change")
    assert e.value.code == ErrorCode.PLAN_EXPIRED


def test_duplicate_apply_returns_same_operation(app):
    c = _contract(app)
    plan = planning.preview(app, OWNER, _prepare(app, c).plan_id)
    _approve(app, plan)
    op1 = planning.start_execution(app, OWNER, plan.plan_id, plan.digest, "k-dup-0001", "change")
    op2 = planning.start_execution(app, OWNER, plan.plan_id, plan.digest, "k-dup-0001", "change")
    op3 = planning.start_execution(app, OWNER, plan.plan_id, plan.digest, "k-dup-0002", "change")
    assert op1.operation_id == op2.operation_id == op3.operation_id
    _wait(app, op1.operation_id)
    marks = [x for x in BucketAdapter.calls if x.endswith(":mark")]
    assert len(marks) == 1, BucketAdapter.calls
    # Same key reused for another plan is refused.
    other = planning.preview(app, OWNER, _prepare(app, c, flow_id=op1.flow_id, params={"note": "v9"}).plan_id)
    _approve(app, other)
    with pytest.raises(PmcpError) as e:
        planning.start_execution(app, OWNER, other.plan_id, other.digest, "k-dup-0001", "change")
    assert e.value.code == ErrorCode.CONFLICT


def test_second_worker_for_same_operation_exits(app):
    c = _contract(app)
    plan = planning.preview(app, OWNER, _prepare(app, c).plan_id)
    _approve(app, plan)
    op = planning.start_execution(app, OWNER, plan.plan_id, plan.digest, "k-race-001", "change")
    run_operation(app, OWNER, op.operation_id)  # in parallel with the runner thread
    op = _wait(app, op.operation_id)
    assert op.status == "succeeded"
    assert len([x for x in BucketAdapter.calls if x.endswith(":mark")]) == 1


def test_crashed_worker_resumes_from_checkpoint(app, monkeypatch):
    c = _contract(app)
    plan = planning.preview(app, OWNER, _prepare(app, c).plan_id)
    _approve(app, plan)

    launched = []
    monkeypatch.setattr(app.runner, "launch", lambda op: launched.append(op.operation_id) or "noop")
    op = planning.start_execution(app, OWNER, plan.plan_id, plan.digest, "k-crash-01", "change")
    # Simulate a worker that finished the first three steps then died (no lease left behind).
    op = app.store.require(Operation, OWNER, op.operation_id)
    op.status = "running"
    for s in op.steps[:3]:
        s.status = "succeeded"
    app.store.save(op)
    from pmcp.services import steps as st

    ctx = st.StepContext.load(app, op)
    for i in range(3):
        st.run_step(ctx, app.store.require(Operation, OWNER, op.operation_id), i)  # the "dead" worker's work
    monkeypatch.undo()
    resumed = planning.start_execution(app, OWNER, plan.plan_id, plan.digest, "k-crash-01", "change")
    assert resumed.operation_id == op.operation_id
    op = _wait(app, op.operation_id)
    assert op.status == "succeeded", op.error
    assert [x.split(":")[1] for x in BucketAdapter.calls] == ["mark"]  # post-deploy step ran exactly once


def test_failure_marks_flow_and_releases_lock(app, monkeypatch):
    c = _contract(app)
    monkeypatch.setenv("PMCP_TEST_FAIL_STEP", "mark")
    plan, op = _deploy(app, c, key="k-fail-001")
    assert op.status == "failed"
    failed = [s for s in op.steps if s.status == "failed"]
    assert failed[0].step_id == "mark" and "injected" in failed[0].error
    assert app.store.require(Plan, OWNER, plan.plan_id).status == "failed"
    assert app.store.peek_claim(OWNER, f"lock:flow:{plan.flow_id}") is None
    monkeypatch.delenv("PMCP_TEST_FAIL_STEP")


def test_modification_plan_has_diff_and_reprocessing(app):
    c = _contract(app)
    _, op = _deploy(app, c)
    plan = _prepare(app, c, flow_id=op.flow_id, params={"note": "v2"}, change_reason="new rule")
    assert plan.data_impact.reprocessing_required
    assert plan.data_impact.reprocessing_window and plan.data_impact.data_rollback == "rebuild_from_source"
    assert any(s.step_id == "reprocess" for s in plan.steps)
    assert any(rc.action == "modify" and rc.logical_id == "DataBucket" for rc in plan.resource_changes)
    none = _prepare(app, c, flow_id=op.flow_id, params={"note": "v2"},
                    reprocess=planning.ReprocessRequest(mode="none"))
    assert not any(s.step_id == "reprocess" for s in none.steps)
    assert any("reprocessing skipped" in w for w in none.warnings)


def test_cost_cap_and_concurrency_limits(app, monkeypatch):
    c = _contract(app)
    from pmcp.domain.records import AccountConnection

    conn = app.store.require(AccountConnection, OWNER, ACCOUNT)
    conn.monthly_cost_cap_usd = 0.5
    app.store.save(conn)
    plan = planning.preview(app, OWNER, _prepare(app, c).plan_id)
    assert any("exceeds the cost cap" in w for w in plan.warnings)
    _approve(app, plan)
    with pytest.raises(PmcpError) as e:
        planning.start_execution(app, OWNER, plan.plan_id, plan.digest, "k-cap-0001", "change")
    assert e.value.code == ErrorCode.COST_CAP_EXCEEDED


def test_read_only_connection_cannot_apply(app):
    from pmcp.domain.records import AccountConnection

    c = _contract(app)
    conn = app.store.require(AccountConnection, OWNER, ACCOUNT)
    conn.mode = "assume_role"
    conn.reader_role_arn = f"arn:aws:iam::{ACCOUNT}:role/pmcp-reader"
    app.store.save(conn)
    iam = boto3.client("iam")
    iam.create_role(RoleName="pmcp-reader", AssumeRolePolicyDocument="{}")
    plan = planning.preview(app, OWNER, _prepare(app, c).plan_id)
    assert plan.change_set is None and any("read-only" in w for w in plan.warnings)
    _approve(app, plan)
    with pytest.raises(PmcpError) as e:
        planning.start_execution(app, OWNER, plan.plan_id, plan.digest, "k-ro-00001", "change")
    assert e.value.code == ErrorCode.WRITE_ACCESS_NOT_ENABLED


def test_other_users_cannot_see_plans(app):
    c = _contract(app)
    plan = _prepare(app, c)
    with pytest.raises(PmcpError) as e:
        planning.preview(app, "someone-else", plan.plan_id)
    assert e.value.code in (ErrorCode.NOT_FOUND, ErrorCode.NOT_CONNECTED)


def test_cloudformation_request_tokens_are_valid():
    import re

    from pmcp.services.steps import request_token

    for tok in (request_token("op_1m5rqackyqcqyh83xxg"), request_token("op_1m5rqackyqcqyh83xxg", "del")):
        assert re.fullmatch(r"[a-zA-Z0-9][-a-zA-Z0-9]*", tok) and len(tok) <= 128, tok


def test_verify_refuses_never_deployed_flow(app, monkeypatch):
    from pmcp.services.verification import start_verification

    c = _contract(app)
    monkeypatch.setenv("PMCP_TEST_FAIL_STEP", "mark")
    plan, op = _deploy(app, c, key="k-nodeploy")
    assert op.status == "failed"
    with pytest.raises(PmcpError) as e:
        start_verification(app, OWNER, plan.flow_id, None, None)
    assert e.value.code == ErrorCode.PRECONDITION_FAILED
