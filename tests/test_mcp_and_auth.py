"""MCP tool surface (in-memory client) and the auth boundary between MCP tokens and approvals."""

import json
import time

import anyio
import pytest
from mcp.client import Client
from starlette.testclient import TestClient

from pmcp.auth.tokens import KeyRing, McpTokenVerifier, issue_mcp_token, issue_web_session, revoke
from pmcp.config import Settings
from pmcp.domain.records import Plan
from pmcp.mcp_server.server import build_server
from pmcp.services import contracts, planning

from .conftest import ACCOUNT, OWNER, REGION, athena_params, contract_body


def err(r):
    """Tool errors are `Error executing tool <name>: <json>` (prefix added by the MCP SDK)."""
    text = r.content[0].text
    return json.loads(text[text.index("{"):])["error"]


def call(app, name, args):
    async def go():
        async with Client(build_server(app)) as c:
            return await c.call_tool(name, args)

    return anyio.run(go)


def test_tool_count_and_annotations(app):
    async def go():
        async with Client(build_server(app)) as c:
            return (await c.list_tools()).tools

    tools = {t.name: t for t in anyio.run(go)}
    assert len(tools) == 20
    assert tools["apply_changes"].annotations.destructive_hint is True
    assert tools["observe_flow"].annotations.read_only_hint is True
    assert not any("approve" in n for n in tools), "no MCP tool may approve plans"
    for t in tools.values():
        assert t.output_schema, t.name
        assert len(t.description) < 600, t.name


def test_errors_are_json_with_codes(app):
    r = call(app, "get_operation", {"operation_id": "op_nope"})
    assert r.is_error
    e = err(r)
    assert e["code"] == "NOT_FOUND" and "retryable" in e
    r = call(app, "prepare_changes", {"account_id": "999999999999", "region": REGION, "contract_id": "dc_x",
                                      "pattern_id": "s3_athena_iceberg_batch", "pattern_params": {}})
    assert err(r)["code"] == "NOT_CONNECTED"


def test_contract_and_validate_via_tools(app):
    r = call(app, "define_data_contract", {"contract": contract_body().model_dump(mode="json")})
    assert not r.is_error, r.content
    cid = r.structured_content["contract_id"]
    r = call(app, "validate_architecture", {"contract_id": cid, "account_id": ACCOUNT, "region": REGION,
                                            "candidates": [{"pattern_id": "s3_athena_iceberg_batch",
                                                            "pattern_params": athena_params()},
                                                           {"pattern_id": "redshift_sql_elt"}]})
    assert not r.is_error, r.content
    sc = r.structured_content
    assert sc["best_deployable_now"] == "s3_athena_iceberg_batch"
    r = call(app, "prepare_changes", {"account_id": ACCOUNT, "region": REGION, "contract_id": cid,
                                      "pattern_id": "redshift_sql_elt", "pattern_params": {}})
    assert err(r)["code"] == "UNSUPPORTED_CONFIGURATION"


def test_apply_without_approval_via_tool(app):
    c, _ = contracts.define(app, OWNER, contract_body(), None, None)
    r = call(app, "prepare_changes", {"account_id": ACCOUNT, "region": REGION, "contract_id": c.contract_id,
                                      "pattern_id": "test_bucket", "pattern_params": {"note": "x"}})
    pid = r.structured_content["plan_id"]
    r = call(app, "preview_changes", {"plan_id": pid})
    assert "pmcp approve" in r.structured_content["approval"]
    digest = r.structured_content["plan_digest"]
    r = call(app, "apply_changes", {"plan_id": pid, "plan_digest": digest, "idempotency_key": "agent-key-1"})
    assert err(r)["code"] == "APPROVAL_REQUIRED"
    assert app.store.require(Plan, OWNER, pid).status == "previewed"


def test_rate_limit(app):
    from pmcp.mcp_server.tools import RateLimiter

    app.rate_limiter = RateLimiter(3)
    codes = []
    for _ in range(4):
        r = call(app, "list_capabilities", {})
        codes.append(err(r)["code"] if r.is_error else "ok")
    assert codes == ["ok", "ok", "ok", "RATE_LIMITED"]


# --- auth -------------------------------------------------------------------------------------------


@pytest.fixture
def cloud(app):
    s = Settings(mode="cloud", platform_region=REGION, public_base_url="https://mcp.example.test",
                 jwt_issuer="https://mcp.example.test", cognito_domain="auth.example.test",
                 cognito_client_id="cid", cognito_user_pool_id="ap-southeast-2_x")
    from pmcp.auth.tokens import _new_pem

    s.jwt_private_key_pem = _new_pem()
    app.settings = s
    return app, KeyRing(s)


def test_token_audiences_and_revocation(cloud):
    app, keys = cloud
    v = McpTokenVerifier(keys, app.settings, app.store)
    tok, claims = issue_mcp_token(keys, app.settings, "user-1", "u@example.test", [])
    assert anyio.run(v.verify_token, tok).subject == "user-1"
    web = issue_web_session(keys, app.settings, "user-1", None, [], int(time.time()))
    assert anyio.run(v.verify_token, web) is None, "web session must not work as MCP token"
    other = KeyRing(app.settings.model_copy(update={"jwt_private_key_pem": None, "mode": "local"}))
    forged, _ = issue_mcp_token(other, app.settings, "user-1", None, [])
    assert anyio.run(v.verify_token, forged) is None
    revoke(app.store, claims["jti"], claims["exp"])
    assert anyio.run(v.verify_token, tok) is None


def _web_client(app, keys):
    from starlette.applications import Starlette

    from pmcp.auth.web import routes

    return TestClient(Starlette(routes=routes(app, keys)), base_url="https://mcp.example.test",
                      follow_redirects=False)


def _previewed_plan(app, owner):
    from pmcp.domain.records import AccountConnection

    app.store.create(AccountConnection(id=ACCOUNT, owner=owner, account_id=ACCOUNT, external_id="e", mode="direct",
                                       status="connected", allowed_regions=[REGION]))
    app.settings.allow_direct_credentials = True
    c, _ = contracts.define(app, owner, contract_body(), None, None)
    p = planning.prepare(app, owner, planning.PrepareRequest(account_id=ACCOUNT, region=REGION,
                                                             contract_id=c.contract_id, pattern_id="test_bucket",
                                                             pattern_params={}))
    return planning.preview(app, owner, p.plan_id)


def test_approval_page_rejects_mcp_bearer_token(cloud):
    app, keys = cloud
    plan = _previewed_plan(app, "user-1")
    tok, _ = issue_mcp_token(keys, app.settings, "user-1", None, [])
    client = _web_client(app, keys)
    r = client.post(f"/approve/{plan.plan_id}", data={"digest": plan.digest},
                    headers={"Authorization": f"Bearer {tok}"})
    assert r.status_code == 303 and r.headers["location"].startswith("/login")
    client.cookies.set("pmcp_session", tok)  # MCP token smuggled in as cookie: wrong audience
    r = client.post(f"/approve/{plan.plan_id}", data={"digest": plan.digest})
    assert r.status_code == 303
    assert app.store.require(Plan, "user-1", plan.plan_id).status == "previewed"


def test_approval_page_requires_fresh_login_and_csrf(cloud):
    app, keys = cloud
    plan = _previewed_plan(app, "user-1")
    client = _web_client(app, keys)
    stale = issue_web_session(keys, app.settings, "user-1", None, [], int(time.time()) - 3600)
    client.cookies.set("pmcp_session", stale)
    r = client.get(f"/approve/{plan.plan_id}")
    assert r.status_code == 303 and "fresh=1" in r.headers["location"]
    fresh = issue_web_session(keys, app.settings, "user-1", None, [], int(time.time()))
    client.cookies.set("pmcp_session", fresh)
    page = client.get(f"/approve/{plan.plan_id}")
    assert page.status_code == 200 and plan.digest in page.text
    r = client.post(f"/approve/{plan.plan_id}", data={"digest": plan.digest, "csrf": "wrong"})
    assert "Invalid form token" in r.text
    csrf = keys.decode(fresh, app.settings.jwt_audience_web)["csrf"]
    r = client.post(f"/approve/{plan.plan_id}", data={"digest": plan.digest, "csrf": csrf})
    assert "is approved" in r.text
    assert app.store.require(Plan, "user-1", plan.plan_id).status == "approved"


def test_other_user_cannot_approve(cloud):
    app, keys = cloud
    plan = _previewed_plan(app, "user-1")
    client = _web_client(app, keys)
    client.cookies.set("pmcp_session", issue_web_session(keys, app.settings, "user-2", None, [], int(time.time())))
    r = client.get(f"/approve/{plan.plan_id}")
    assert "No such plan" in r.text
