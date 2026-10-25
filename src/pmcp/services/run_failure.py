"""Read existing execution evidence without running queries or creating incidents."""

from __future__ import annotations

import json
import re
from typing import Any

from pmcp.domain.records import Flow
from pmcp.errors import ErrorCode, PmcpError
from pmcp.services.steps import build_runtime


def _query_ids(value: Any) -> set[str]:
    found: set[str] = set()
    if isinstance(value, dict):
        for key, item in value.items():
            if key.lower() == "queryexecutionid" and isinstance(item, str):
                found.add(item)
            else:
                found.update(_query_ids(item))
    elif isinstance(value, list):
        for item in value:
            found.update(_query_ids(item))
    elif isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (ValueError, TypeError):
            parsed = None
        if isinstance(parsed, (dict, list)):
            found.update(_query_ids(parsed))
        # Some AWS failures render a Java-style QueryExecutionId=uuid string.
        found.update(re.findall(r"QueryExecutionId[\s:=\"']+([0-9a-fA-F-]{36})", value))
    return {q for q in found if re.fullmatch(r"[0-9a-fA-F-]{36}", q)}


def get_run_failure(app: Any, owner: str, flow_id: str, execution: str,
                    next_token: str | None = None) -> dict[str, Any]:
    flow = app.store.require(Flow, owner, flow_id)
    machine = flow.outputs.get("StateMachineArn")
    if not machine or ":stateMachine:" not in machine:
        raise PmcpError(ErrorCode.PRECONDITION_FAILED, "flow has no Step Functions state machine")
    prefix = machine.replace(":stateMachine:", ":execution:") + ":"
    arn = execution if execution.startswith("arn:") else prefix + execution
    if not arn.startswith(prefix) or not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", arn[len(prefix):]):
        raise PmcpError(ErrorCode.FORBIDDEN, "execution does not belong to this flow")
    rt = build_runtime(app, owner, flow, write=False)
    sfn = rt.client("stepfunctions")
    detail = sfn.describe_execution(executionArn=arn)
    if detail.get("stateMachineArn") != machine:
        raise PmcpError(ErrorCode.FORBIDDEN, "execution state machine mismatch")
    args: dict[str, Any] = dict(executionArn=arn, reverseOrder=True, maxResults=100,
                                includeExecutionData=True)
    if next_token:
        args["nextToken"] = next_token
    history = sfn.get_execution_history(**args)
    failures = []
    ids = _query_ids(detail.get("cause", ""))
    for event in history["events"]:
        for key, value in event.items():
            if not key.endswith("EventDetails") or not isinstance(value, dict):
                continue
            # Task results only; never treat user-supplied execution input as a query reference.
            if key in ("taskFailedEventDetails", "executionFailedEventDetails",
                       "taskSucceededEventDetails"):
                ids.update(_query_ids(value.get("cause", "")))
                ids.update(_query_ids(value.get("output", "")))
            if "Failed" in key or "TimedOut" in key or "Aborted" in key:
                failures.append({"event_id": event.get("id"), "type": event.get("type"),
                                 "error": value.get("error"), "cause": value.get("cause")})
    queries = []
    for qid in sorted(ids):
        query = rt.client("athena").get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        if not flow.outputs.get("Workgroup") or query.get("WorkGroup") != flow.outputs["Workgroup"]:
            raise PmcpError(ErrorCode.FORBIDDEN, "query workgroup does not belong to this flow")
        status = query.get("Status", {})
        queries.append({"query_execution_id": qid, "state": status.get("State"),
                        "state_change_reason": status.get("StateChangeReason"),
                        "athena_error": status.get("AthenaError"), "workgroup": query.get("WorkGroup")})
    return {"flow_id": flow_id, "execution_arn": arn, "status": detail["status"],
            "error": detail.get("error"), "cause": detail.get("cause"),
            "failures": failures, "athena_queries": queries,
            "next_token": history.get("nextToken"), "history_complete": not bool(history.get("nextToken")),
            "query_lookup_note": None if queries else "No Athena query id found in this history page; follow next_token if present.",
            "truncated": False}
