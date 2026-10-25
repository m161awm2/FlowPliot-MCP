"""Mock-AWS test harness. Everything here runs against moto or stubs; nothing reaches real AWS."""

from __future__ import annotations

import os
import secrets
from typing import Any

import pytest

os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_SESSION_TOKEN", "testing")
os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"
os.environ.pop("AWS_PROFILE", None)

from moto import mock_aws  # noqa: E402

from pmcp.adapters import ADAPTERS  # noqa: E402
from pmcp.adapters.base import Adapter, Rendered, Runtime, template  # noqa: E402
from pmcp.app import build_app  # noqa: E402
from pmcp.config import Settings  # noqa: E402
from pmcp.domain.base import Model  # noqa: E402
from pmcp.domain.contract import DataContractBody  # noqa: E402
from pmcp.domain.records import AccountConnection, Check, CostEstimate, PlanStep  # noqa: E402
from pmcp.store import MemoryBackend, Store  # noqa: E402

ACCOUNT = "123456789012"
REGION = "ap-southeast-2"
OWNER = "local-user"


class BucketParams(Model):
    note: str = "v1"


class BucketAdapter(Adapter):
    """Minimal adapter whose template moto can execute; used to test the plan/operation machinery."""

    pattern_id = "test_bucket"
    Params = BucketParams
    calls: list[str] = []

    def render(self, ctx, p):  # type: ignore[override]
        res = {"DataBucket": {"Type": "AWS::S3::Bucket", "Properties": {
            "BucketName": ctx.data_bucket, "Tags": [{"Key": "note", "Value": p.note}]}}}
        r = Rendered(template=template("test", res, {"DataBucket": {"Ref": "DataBucket"}}))
        r.post_deploy_steps.append(PlanStep(step_id="mark", action="test.mark", description="count"))
        if ctx.previous_params and ctx.previous_params.get("note") != p.note:
            r.reprocess_reason = "note changed"
        return r

    def reprocess_steps(self, ctx, p, window):  # type: ignore[override]
        return [PlanStep(step_id="reprocess", action="test.mark", description="reprocess",
                         params={"window": window})]

    def estimate(self, contract, p, region, prices):  # type: ignore[override]
        return CostEstimate(monthly_usd=1.0)

    def run_step(self, step, rt: Runtime, op_id):  # type: ignore[override]
        BucketAdapter.calls.append(f"{op_id}:{step.step_id}")
        if os.environ.get("PMCP_TEST_FAIL_STEP") == step.step_id:
            raise RuntimeError("injected failure")
        return {"ok": True}

    def verify(self, rt, window):  # type: ignore[override]
        return {"start": "a", "end": "b"}, [Check(check_id="x", category="existence", status="passed",
                                                  description="stub")]

    def observe(self, rt):  # type: ignore[override]
        return {"stub": True}

    def diagnose(self, rt, window):  # type: ignore[override]
        from pmcp.adapters.base import DiagnosisParts

        return DiagnosisParts(facts=[], hypotheses=[], further_checks=[])


ADAPTERS["test_bucket"] = BucketAdapter()


@pytest.fixture
def aws():
    with mock_aws():
        yield


@pytest.fixture
def app(aws):
    settings = Settings(mode="local", platform_region=REGION, local_user_id=OWNER, operation_max_seconds=120)
    a = build_app(settings, Store(MemoryBackend()), live_prices=False)
    a.store.create(AccountConnection(id=ACCOUNT, owner=OWNER, account_id=ACCOUNT, external_id=secrets.token_hex(8),
                                     mode="direct", status="connected", allowed_regions=[REGION],
                                     monthly_cost_cap_usd=20))
    BucketAdapter.calls = []
    return a


def contract_body(**over: Any) -> DataContractBody:
    base: dict[str, Any] = {
        "title": "orders",
        "sources": [{"name": "orders_raw", "uri": "s3://src-bucket/orders/", "format": "jsonl",
                     "event_time_column": "updated_at",
                     "columns": [{"name": "order_id", "type": "string"}, {"name": "status", "type": "string"},
                                 {"name": "amount", "type": "double"}, {"name": "updated_at", "type": "string"},
                                 {"name": "deleted", "type": "boolean"}]}],
        "output": {"name": "orders", "columns": [
            {"name": "order_id", "type": "string", "nullable": False}, {"name": "status", "type": "string"},
            {"name": "amount", "type": "decimal(18,2)"}, {"name": "updated_at", "type": "timestamp"}]},
        "mutation": {"mode": "upsert_with_deletes", "key": ["order_id"], "ordering_column": "updated_at"},
        "freshness": {"max_lag_seconds": 3600, "measured_on": "updated_at"},
        "volume": {"records_per_day": 100_000, "bytes_per_day": 50_000_000},
        "consumers": [{"name": "bi", "access": "sql_analytics"}],
        "quality": [{"kind": "not_null", "column": "order_id"}],
        "reprocessing_window_days": 3,
    }
    base.update(over)
    return DataContractBody.model_validate(base)


TRANSFORM_SQL = (
    "SELECT order_id, status, CAST(amount AS decimal(18,2)) AS amount, "
    "CAST(from_iso8601_timestamp(updated_at) AS timestamp(6)) AS updated_at, "
    "coalesce(deleted, false) AS pmcp_deleted FROM {{source}} "
    "WHERE from_iso8601_timestamp(updated_at) >= {{window_start}} AND from_iso8601_timestamp(updated_at) < "
    "{{window_end}}"
)


def athena_params(**over: Any) -> dict[str, Any]:
    p = {
        "source_uri": "s3://src-bucket/orders/",
        "source_format": "jsonl",
        "source_columns": [{"name": "order_id", "type": "string"}, {"name": "status", "type": "string"},
                           {"name": "amount", "type": "double"}, {"name": "updated_at", "type": "string"},
                           {"name": "deleted", "type": "boolean"}],
        "transform_sql": TRANSFORM_SQL,
        "schedule_expression": "rate(1 hour)",
        "window_minutes": 120,
    }
    p.update(over)
    return p
