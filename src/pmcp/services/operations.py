"""Long-running operations.

An operation is a list of plan steps executed by a worker that is independent of the MCP request: a
thread in local mode, a one-off ECS Fargate task (RunTask) in cloud mode. Progress is checkpointed in
the store after every step, so a crashed or restarted worker resumes at the first unfinished step.
Every AWS-mutating step is idempotent on its own (client request tokens, deterministic execution
names, IF NOT EXISTS DDL), which is what makes resuming safe.

Exactly-one-worker is enforced with a lease item renewed by a heartbeat thread; a second worker for
the same operation exits immediately.
"""

from __future__ import annotations

import logging
import secrets
import threading
import time
import traceback
from typing import TYPE_CHECKING, Any, Protocol

from pmcp.domain.records import Operation, StepState
from pmcp.errors import ErrorCode, PmcpError
from pmcp.ids import iso

if TYPE_CHECKING:
    from pmcp.app import App

log = logging.getLogger(__name__)

LEASE_SECONDS = 90
TERMINAL = {"succeeded", "failed", "cancelled"}


class Runner(Protocol):
    def launch(self, op: Operation) -> str: ...


class ThreadRunner:
    def __init__(self, app: App) -> None:
        self.app = app
        self.threads: dict[str, threading.Thread] = {}

    def launch(self, op: Operation) -> str:
        t = threading.Thread(target=run_operation, args=(self.app, op.owner, op.operation_id), daemon=True,
                             name=f"op-{op.operation_id}")
        self.threads[op.operation_id] = t
        t.start()
        return f"thread:{t.name}"

    def join(self, op_id: str, timeout: float = 60) -> None:
        t = self.threads.get(op_id)
        if t:
            t.join(timeout)


class EcsRunner:
    def __init__(self, app: App) -> None:
        self.app = app

    def launch(self, op: Operation) -> str:
        s = self.app.settings
        ecs = self.app.sessions.platform_session().client("ecs")
        resp = ecs.run_task(
            cluster=s.ecs_cluster,
            taskDefinition=s.worker_task_definition,
            launchType="FARGATE",
            count=1,
            startedBy=op.operation_id[:36],
            networkConfiguration={"awsvpcConfiguration": {"subnets": s.worker_subnets,
                                                          "securityGroups": s.worker_security_groups,
                                                          "assignPublicIp": "ENABLED"}},
            overrides={"containerOverrides": [{
                "name": s.worker_container_name,
                "command": ["pmcp", "worker", "--owner", op.owner, "--operation-id", op.operation_id],
            }]},
            tags=[{"key": "pmcp:operation", "value": op.operation_id}],
            propagateTags="TASK_DEFINITION",
        )
        if resp.get("failures"):
            raise PmcpError(ErrorCode.AWS_ERROR, f"could not start worker task: {resp['failures']}")
        return resp["tasks"][0]["taskArn"]


def build_runner(app: App) -> Runner:
    return EcsRunner(app) if app.settings.runner == "ecs" else ThreadRunner(app)


# --- leases -----------------------------------------------------------------------------------------


def _lease_name(op_id: str) -> str:
    return f"lease:op:{op_id}"


def lease_alive(app: App, owner: str, op_id: str) -> bool:
    return app.store.peek_claim(owner, _lease_name(op_id)) is not None


class Lease:
    def __init__(self, app: App, owner: str, op_id: str) -> None:
        self.app, self.owner, self.op_id = app, owner, op_id
        self.holder = {"worker": secrets.token_hex(8)}
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def acquire(self) -> bool:
        won = self.app.store.claim(self.owner, _lease_name(self.op_id), self.holder, ttl_seconds=LEASE_SECONDS)
        if won is not None:
            return False
        self._thread = threading.Thread(target=self._beat, daemon=True)
        self._thread.start()
        return True

    def _beat(self) -> None:
        while not self._stop.wait(LEASE_SECONDS / 3):
            try:
                if not self.app.store.renew_claim(self.owner, _lease_name(self.op_id), self.holder,
                                                  ttl_seconds=LEASE_SECONDS):
                    log.warning("lost lease on %s", self.op_id)
            except Exception:  # a missed beat only shortens the lease
                log.exception("lease heartbeat failed")

    def release(self) -> None:
        self._stop.set()
        self.app.store.release(self.owner, _lease_name(self.op_id), only_if=self.holder)


# --- worker -----------------------------------------------------------------------------------------


def _save(app: App, op: Operation) -> Operation:
    return app.store.save(op)


def run_operation(app: App, owner: str, op_id: str) -> None:
    from pmcp.services import steps

    op = app.store.get(Operation, owner, op_id)
    if op is None or op.status in TERMINAL:
        return
    lease = Lease(app, owner, op_id)
    if not lease.acquire():
        log.info("operation %s already has a live worker; exiting", op_id)
        return
    try:
        op = app.store.require(Operation, owner, op_id)
        op.status = "running"
        op.attempts += 1
        op.lease_owner = lease.holder["worker"]
        op = _save(app, op)
        ctx = steps.StepContext.load(app, op)
        for i, st in enumerate(op.steps):
            if st.status in ("succeeded", "skipped"):
                continue
            if op.deadline_at and time.time() > op.deadline_at:
                raise PmcpError(ErrorCode.TIMEOUT, "operation exceeded its time budget")
            op.steps[i].status = "running"
            op.steps[i].started_at = iso()
            op = _save(app, op)
            detail = steps.run_step(ctx, op, i)
            op = app.store.require(Operation, owner, op_id)  # steps may have saved related records
            op.steps[i].status = "skipped" if detail.get("skipped") else "succeeded"
            op.steps[i].finished_at = iso()
            op.steps[i].detail = _trim(detail)
            op = _save(app, op)
        op.status = "succeeded"
        op.result = steps.finish(ctx, op, ok=True)
        _save(app, op)
    except Exception as exc:
        err = exc if isinstance(exc, PmcpError) else PmcpError(ErrorCode.INTERNAL, f"{type(exc).__name__}: {exc}")
        if not isinstance(exc, PmcpError):
            log.error("operation %s crashed: %s", op_id, traceback.format_exc())
        op = app.store.require(Operation, owner, op_id)
        for st in op.steps:
            if st.status == "running":
                st.status = "failed"
                st.finished_at = iso()
                st.error = err.message[:1000]
        op.status = "failed"
        op.error = err.to_dict()
        try:
            ctx = steps.StepContext.load(app, op)
            op.result = steps.finish(ctx, op, ok=False)
        except Exception:
            log.exception("finish after failure failed")
        _save(app, op)
    finally:
        lease.release()


def _trim(detail: dict[str, Any], limit: int = 8000) -> dict[str, Any]:
    import json

    text = json.dumps(detail, default=str)
    if len(text) <= limit:
        return detail
    return {"truncated": True, "excerpt": text[:limit]}


def new_steps(plan_steps: list[Any]) -> list[StepState]:
    return [StepState(step_id=s.step_id) for s in plan_steps]
