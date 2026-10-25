"""Verification verdicts, diagnosis hypotheses and recovery orchestration (scenario 4, mocked).

Athena cannot be executed by moto, so `athena_query` is replaced by a fake that answers by SQL pattern;
Step Functions / CloudWatch / Scheduler reads go to small stub clients. These tests check pmcp's decision
logic, not AWS behaviour."""

import json
import re
import time
from datetime import UTC, datetime, timedelta

import boto3
import pytest

import pmcp.adapters.athena_iceberg as ai
from pmcp.adapters.base import Runtime
from pmcp.domain.contract import DataContract
from pmcp.domain.records import Flow, Incident, Operation
from pmcp.errors import ErrorCode, PmcpError
from pmcp.services import contracts, ops, planning
from pmcp.services.verification import run_verification

from .conftest import ACCOUNT, OWNER, REGION, athena_params, contract_body

NOW = datetime.now(UTC)


class Stub:
    def __init__(self, **methods):
        self.__dict__.update(methods)


class FakeSession:
    def __init__(self, clients):
        self.clients = clients

    def client(self, name, **_):
        return self.clients[name]


def sfn_stub(succeeded=True, failed_cause=None):
    window_end = (NOW - timedelta(minutes=30)).isoformat().replace("+00:00", "Z")
    ok = [{"executionArn": "arn:ex:ok", "status": "SUCCEEDED", "startDate": NOW - timedelta(minutes=31),
           "stopDate": NOW - timedelta(minutes=30), "name": "ok"}] if succeeded else []
    bad = [{"executionArn": "arn:ex:bad", "status": "FAILED", "startDate": NOW - timedelta(minutes=10),
            "stopDate": NOW - timedelta(minutes=9), "name": "bad"}] if failed_cause else []

    def list_executions(stateMachineArn, statusFilter=None, maxResults=10):
        rows = ok + bad
        return {"executions": [e for e in rows if statusFilter in (None, e["status"])]}

    return Stub(
        list_executions=list_executions,
        describe_execution=lambda executionArn: {"input": json.dumps({"window_end": window_end}), "status": "x",
                                                 "error": "States.TaskFailed"},
        get_execution_history=lambda **kw: {"events": [{"taskFailedEventDetails": {
            "error": "Athena.AthenaException", "cause": failed_cause or ""}}]},
    )


def runtime(flow, contract, sfn=None, sched_state="ENABLED", consumer_ok=True, alarm_state="OK"):
    clients = {
        "glue": Stub(get_table=lambda DatabaseName, Name: {"Table": {"StorageDescriptor": {"Columns": [
            {"Name": "order_id", "Type": "string"}, {"Name": "status", "Type": "string"},
            {"Name": "amount", "Type": "decimal(18,2)"}, {"Name": "updated_at", "Type": "timestamp(6)"}]}}}),
        "stepfunctions": sfn or sfn_stub(),
        "scheduler": Stub(get_schedule=lambda Name: {"State": sched_state, "ScheduleExpression": "rate(1 hour)"}),
        "cloudwatch": Stub(describe_alarms=lambda **kw: {"MetricAlarms": [
            {"AlarmName": "pmcp-x-failed-runs", "StateValue": alarm_state}]}),
        "cloudformation": Stub(describe_stacks=lambda StackName: {"Stacks": [{"StackStatus": "UPDATE_COMPLETE"}]}),
    }
    sess = FakeSession(clients)

    def consumer(arn):
        if not consumer_ok:
            raise PmcpError(ErrorCode.AWS_ACCESS_DENIED, "consumer cannot assume role")
        return sess

    return Runtime(reader=sess, deployer=sess, consumer=consumer, flow=flow, contract=contract)


def fake_athena(answers):
    def run(session, sql, **kw):
        for pattern, rows in answers:
            if re.search(pattern, sql, re.S):
                if isinstance(rows, Exception):
                    raise rows
                return {"query_id": "q-" + pattern[:8], "state": "SUCCEEDED", "rows": rows, "columns": [],
                        "error": None}
        return {"query_id": "q-none", "state": "SUCCEEDED", "rows": [{"n": "1", "violations": "0", "dup": "0"}],
                "columns": [], "error": None}

    return run


GOOD = [
    (r"^SELECT count\(\*\) AS n FROM", [{"n": "1200"}]),
    (r"AS dup FROM", [{"dup": "0"}]),
    (r"AS expected_rows", [{"expected_rows": "100", "present": "100", "stale_versions": "0",
                            "deletes_not_applied": "0"}]),
    (r"AS out_max", [{"out_max": (NOW - timedelta(minutes=40)).strftime("%Y-%m-%d %H:%M:%S.000")}]),
    (r"AS src_max", [{"src_max": (NOW - timedelta(minutes=35)).strftime("%Y-%m-%d %H:%M:%S.000")}]),
    (r"LIMIT 5", [{"order_id": "1"}]),
]


@pytest.fixture
def flow_and_contract(app):
    c, _ = contracts.define(app, OWNER, contract_body(), None, None)
    flow = Flow(id="fl_0000000000abcdefghij", owner=OWNER, flow_id="fl_0000000000abcdefghij", name="orders",
                account_id=ACCOUNT, region=REGION, stack_name="pmcp-abcdefghij",
                pattern_id="s3_athena_iceberg_batch", pattern_params=athena_params(), contract_id=c.contract_id,
                contract_version=1, status="deployed",
                outputs={"StateMachineArn": "arn:aws:states:ap-southeast-2:123456789012:stateMachine:pmcp-x-run",
                         "ConsumerRoleArn": "arn:aws:iam::123456789012:role/pmcp-abcdefghij-consumer"})
    return flow, c


def test_all_checks_pass(monkeypatch, flow_and_contract):
    flow, c = flow_and_contract
    monkeypatch.setattr(ai, "athena_query", fake_athena(GOOD))
    win, checks = ai.AthenaIcebergAdapter().verify(runtime(flow, c), None)
    by = {ch.check_id: ch for ch in checks}
    assert {ch.status for ch in checks} == {"passed"}, [(ch.check_id, ch.status, ch.reason) for ch in checks]
    assert by["row_count"].expected == 100 and by["row_count"].window == win
    assert by["consumer_read"].category == "consumer_read"
    assert {"existence", "schema", "keys", "row_count", "mutations", "freshness", "progress", "quality",
            "consumer_read"} <= {ch.category for ch in checks}


def test_missing_rows_and_unapplied_deletes_fail(monkeypatch, flow_and_contract):
    flow, c = flow_and_contract
    bad = [(r"AS expected_rows", [{"expected_rows": "100", "present": "97", "stale_versions": "2",
                                   "deletes_not_applied": "1"}])] + GOOD
    monkeypatch.setattr(ai, "athena_query", fake_athena(bad))
    _, checks = ai.AthenaIcebergAdapter().verify(runtime(flow, c), None)
    by = {ch.check_id: ch for ch in checks}
    assert by["row_count"].status == "failed" and by["row_count"].observed == 97
    assert by["mutations"].status == "failed"


def test_consumer_without_access_fails(monkeypatch, flow_and_contract):
    flow, c = flow_and_contract
    monkeypatch.setattr(ai, "athena_query", fake_athena(GOOD))
    _, checks = ai.AthenaIcebergAdapter().verify(runtime(flow, c, consumer_ok=False), None)
    assert {ch.check_id: ch for ch in checks}["consumer_read"].status == "failed"


def test_no_runs_yet_is_unverified_not_passed(app, monkeypatch, flow_and_contract):
    flow, c = flow_and_contract
    monkeypatch.setattr(ai, "athena_query", fake_athena(GOOD))
    rt = runtime(flow, c, sfn=sfn_stub(succeeded=False))
    _, checks = ai.AthenaIcebergAdapter().verify(rt, None)
    assert [ch.status for ch in checks] == ["unverified"]
    app.store.create(flow)
    monkeypatch.setattr("pmcp.services.steps.build_runtime", lambda *a, **k: rt)
    ver = run_verification(app, OWNER, flow, None, None)
    assert ver.verdict == "partially_verified"
    assert app.store.require(Flow, OWNER, flow.flow_id).status == "deployed"  # not promoted to verified


def test_failed_verification_degrades_flow(app, monkeypatch, flow_and_contract):
    flow, c = flow_and_contract
    app.store.create(flow)
    bad = [(r"AS expected_rows", [{"expected_rows": "100", "present": "50", "stale_versions": "0",
                                   "deletes_not_applied": "0"}])] + GOOD
    monkeypatch.setattr(ai, "athena_query", fake_athena(bad))
    monkeypatch.setattr("pmcp.services.steps.build_runtime", lambda *a, **k: runtime(flow, c))
    ver = run_verification(app, OWNER, flow, None, None)
    assert ver.verdict == "failed"
    assert app.store.require(Flow, OWNER, flow.flow_id).status == "degraded"


@pytest.mark.parametrize(("cause", "expect", "recovery"), [
    ("HIVE_BAD_DATA: Error parsing field value '12x' for field 2", "cannot be parsed", "quarantine_and_rerun"),
    ("Access Denied (Service: Amazon S3; Status Code: 403)", "permission", "restore_template"),
    ("TABLE_NOT_FOUND: line 1:12: Table 'pmcp_x.orders' does not exist", "deleted or renamed", "recreate_tables"),
    ("COLUMN_NOT_FOUND: line 1:8: Column 'amount' cannot be resolved", "source schema changed", "change_plan"),
])
def test_diagnosis_hypotheses_cite_facts(flow_and_contract, cause, expect, recovery):
    flow, c = flow_and_contract
    rt = runtime(flow, c, sfn=sfn_stub(failed_cause=cause), alarm_state="ALARM")
    window = {"start": (NOW - timedelta(hours=1)).isoformat(), "end": (NOW + timedelta(minutes=1)).isoformat()}
    parts = ai.AthenaIcebergAdapter().diagnose(rt, window)
    kinds = {f["kind"] for f in parts.facts}
    assert {"failed_run", "alarm", "stack_status"} <= kinds
    top = parts.hypotheses[0]
    assert expect in top.statement
    assert recovery in top.suggested_recovery
    failed_ids = {f["fact_id"] for f in parts.facts if f["kind"] == "failed_run"}
    assert set(top.supporting_evidence) <= failed_ids and top.supporting_evidence
    assert parts.affected["windows"], "affected window derived from the failed run's input"


def test_disabled_schedule_without_failures(flow_and_contract):
    flow, c = flow_and_contract
    rt = runtime(flow, c, sfn=sfn_stub(succeeded=False), sched_state="DISABLED")
    parts = ai.AthenaIcebergAdapter().diagnose(rt, {"start": (NOW - timedelta(hours=1)).isoformat(),
                                                    "end": NOW.isoformat()})
    assert parts.hypotheses[0].suggested_recovery == ["enable_schedule"]


def test_recovery_flow_quarantine_rerun_verify(app, monkeypatch, flow_and_contract):
    """incident -> plan_recovery -> preview -> approve -> execute_recovery -> verification attached."""
    flow, c = flow_and_contract
    boto3.client("cloudformation", region_name=REGION).create_stack(
        StackName=flow.stack_name, TemplateBody=json.dumps({"Resources": {"T": {"Type": "AWS::SNS::Topic"}}}),
        Tags=[{"Key": "managed-by", "Value": "pipeline-mcp"}, {"Key": "pmcp:owner", "Value": OWNER}])
    app.store.create(flow)
    inc = app.store.create(Incident(
        id="in_1", owner=OWNER, incident_id="in_1", flow_id=flow.flow_id,
        window={"start": "2026-10-25T00:00:00Z", "end": "2026-10-25T02:00:00Z"}, observed_facts=[],
        hypotheses=[], further_checks=[],
        affected={"windows": [{"start": "2026-10-25T00:00:00Z", "end": "2026-10-25T02:00:00Z"}],
                  "bad_files": ["s3://src-bucket/orders/bad.jsonl"]}))
    calls = []
    monkeypatch.setattr(ai, "athena_query", lambda s, sql, **k: calls.append(sql) or
                        {"query_id": "q", "state": "SUCCEEDED", "rows": [], "error": None})
    monkeypatch.setattr(ai.AthenaIcebergAdapter, "_run_windows",
                        lambda self, rt, s, step, op_id: calls.append(("run", step.params["window_start"])) or {})
    monkeypatch.setattr(ai.AthenaIcebergAdapter, "verify", lambda self, rt, w: (
        w or {}, [ai.Check(check_id="x", category="row_count", status="passed", description="ok")]))
    monkeypatch.setattr(ai.AthenaIcebergAdapter, "recovery_options",
                        ai.AthenaIcebergAdapter.recovery_options)
    plan, options = ops.plan_recovery(app, OWNER, inc.incident_id, "quarantine_and_rerun")
    assert options[0]["strategy"] == "quarantine_and_rerun"
    assert [s.action for s in plan.steps] == ["precheck", "execute_change_set", "wait_stack", "record_flow",
                                              "sfn.run_window", "verify"]
    assert plan.pattern_params["quarantined_paths"] == ["s3://src-bucket/orders/bad.jsonl"]
    assert "configuration change" in plan.data_impact.infra_rollback
    assert any(d["artifact"] == "run.sql" for d in plan.code_diffs) or not plan.code_diffs  # no prior plan here
    plan = planning.preview(app, OWNER, plan.plan_id)
    planning.record_approval(app, OWNER, plan.plan_id, plan.digest, auth_time=int(time.time()), channel="web")
    with pytest.raises(PmcpError):
        planning.start_execution(app, OWNER, plan.plan_id, plan.digest, "rec-key-001", "change")
    op = planning.start_execution(app, OWNER, plan.plan_id, plan.digest, "rec-key-001", "recovery")
    for _ in range(200):
        op = app.store.require(Operation, OWNER, op.operation_id)
        if op.status in ("succeeded", "failed"):
            break
        time.sleep(0.05)
    assert op.status == "succeeded", op.error
    assert app.store.require(Flow, OWNER, flow.flow_id).pattern_params["quarantined_paths"] == [
        "s3://src-bucket/orders/bad.jsonl"]
    assert ("run", "2026-10-25T00:00:00Z") in calls
    assert op.steps[-1].detail["verdict"] == "verified"
    assert app.store.require(Incident, OWNER, "in_1").status == "resolved"


def test_quarantine_requires_identified_files(app, flow_and_contract):
    flow, c = flow_and_contract
    app.store.create(flow)
    app.store.create(Incident(id="in_2", owner=OWNER, incident_id="in_2", flow_id=flow.flow_id,
                              window={"start": "a", "end": "b"}, observed_facts=[], hypotheses=[],
                              further_checks=[], affected={}))
    with pytest.raises(PmcpError) as e:
        ops.plan_recovery(app, OWNER, "in_2", "quarantine_and_rerun")
    assert e.value.code in (ErrorCode.INVALID_INPUT, ErrorCode.PRECONDITION_FAILED)


_ = DataContract


def test_full_failure_read_is_scoped_paginated_and_does_not_write(app, monkeypatch, flow_and_contract):
    from pmcp.services import run_failure

    flow, contract = flow_and_contract
    flow.outputs['Workgroup'] = 'pmcp-test'
    app.store.create(flow)
    machine = flow.outputs['StateMachineArn']
    qid = '12345678-1234-1234-1234-123456789abc'
    cause = json.dumps({'Query': 'MERGE ' + 'x' * 2000, 'QueryExecutionId': qid,
                       'Status': {'StateChangeReason': 'ICEBERG_COMMIT_ERROR at the end'}})
    calls = []
    def history(**kwargs):
        calls.append(kwargs)
        return {'events': [{'id': 9, 'type': 'TaskFailed', 'taskFailedEventDetails':
                            {'error': 'AthenaException', 'cause': cause}}], 'nextToken': 'page2'}
    rt = runtime(flow, contract)
    rt.reader.clients['stepfunctions'] = Stub(
        describe_execution=lambda **kw: {'stateMachineArn': machine, 'status': 'FAILED', 'cause': cause},
        get_execution_history=history)
    rt.reader.clients['athena'] = Stub(get_query_execution=lambda **kw: {'QueryExecution': {
        'WorkGroup': 'pmcp-test', 'Status': {'State': 'FAILED',
        'StateChangeReason': 'ICEBERG_COMMIT_ERROR at the end', 'AthenaError': {'ErrorType': 999}}}})
    monkeypatch.setattr(run_failure, 'build_runtime', lambda *a, **kw: rt)
    result = run_failure.get_run_failure(app, OWNER, flow.flow_id, 'failed-run', 'page1')
    assert result['cause'] == cause
    assert result['failures'][0]['cause'].endswith('at the end"}}')
    assert result['athena_queries'][0]['query_execution_id'] == qid
    assert result['athena_queries'][0]['state_change_reason'] == 'ICEBERG_COMMIT_ERROR at the end'
    assert result['next_token'] == 'page2' and not result['history_complete']
    assert calls[0]['nextToken'] == 'page1'
    assert not app.store.list(Incident, OWNER)
    assert not app.store.list(Operation, OWNER)
    with pytest.raises(PmcpError, match='does not belong'):
        run_failure.get_run_failure(app, OWNER, flow.flow_id, machine.replace(':stateMachine:', ':execution:') + '-other:run')
    with pytest.raises(PmcpError):
        run_failure.get_run_failure(app, 'other-owner', flow.flow_id, 'failed-run')
    flow.outputs['Workgroup'] = 'different-workgroup'
    app.store.save(flow)
    with pytest.raises(PmcpError, match='workgroup'):
        run_failure.get_run_failure(app, OWNER, flow.flow_id, 'failed-run')


def test_repeated_diagnosis_reuses_incident_but_new_failure_does_not(app, monkeypatch, flow_and_contract):
    flow, contract = flow_and_contract
    app.store.create(flow)
    rt = runtime(flow, contract, sfn=sfn_stub(failed_cause='HIVE_BAD_DATA: invalid json'))
    monkeypatch.setattr(ops, 'build_runtime', lambda *a, **kw: rt)
    first, _ = ops.diagnose(app, OWNER, flow.flow_id, None, False, None)
    second, _ = ops.diagnose(app, OWNER, flow.flow_id, None, False, None)
    assert first.incident_id == second.incident_id
    assert len(app.store.list(Incident, OWNER)) == 1
    rt.reader.clients['stepfunctions'] = sfn_stub(failed_cause='Access Denied')
    third, _ = ops.diagnose(app, OWNER, flow.flow_id, None, False, None)
    assert third.incident_id != first.incident_id
    third.status = 'resolved'
    app.store.save(third)
    recurrence, _ = ops.diagnose(app, OWNER, flow.flow_id, None, False, None)
    repeat, _ = ops.diagnose(app, OWNER, flow.flow_id, None, False, None)
    assert recurrence.incident_id != third.incident_id
    assert recurrence.incident_id == repeat.incident_id


def test_progress_check_reads_source_through_quarantine(monkeypatch, flow_and_contract):
    """Found on real AWS: the progress query read the raw table directly and hit the quarantined bad file."""
    flow, c = flow_and_contract
    flow.pattern_params = {**flow.pattern_params, "quarantined_paths": ["s3://src-bucket/orders/bad.jsonl"]}
    seen = []

    def fake(session, sql, **kw):
        seen.append(sql)
        return fake_athena(GOOD)(session, sql, **kw)

    monkeypatch.setattr(ai, "athena_query", fake)
    _, checks = ai.AthenaIcebergAdapter().verify(runtime(flow, c), None)
    src = [q for q in seen if "AS src_max" in q]
    assert src and "\"$path\" <> 's3://src-bucket/orders/bad.jsonl'" in src[0]
    assert {ch.check_id: ch for ch in checks}["progress"].status == "passed"
