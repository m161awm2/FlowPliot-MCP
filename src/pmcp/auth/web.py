"""Browser routes: Cognito login, MCP token page, account connection, plan approval.

These routes are outside the MCP endpoint and authenticate with a web session cookie (aud pmcp-web)
minted only by the Cognito callback. Approval also demands a login within the last few minutes.
"""

from __future__ import annotations

import base64
import hashlib
import html
import json
import re
import secrets
import time
import urllib.parse
from typing import Any

import httpx
import jwt
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from starlette.routing import Route

from pmcp.app import App
from pmcp.auth.tokens import KeyRing, issue_mcp_token, issue_web_session
from pmcp.domain.records import AccountConnection, Plan
from pmcp.errors import PmcpError
from pmcp.services.planning import record_approval

SESSION_COOKIE = "pmcp_session"
OAUTH_COOKIE = "pmcp_oauth"
E = html.escape

CSS = """
:root{--bg:#f7f7f5;--fg:#1d1d1b;--muted:#6b6b66;--card:#fff;--line:#e3e3de;--accent:#2f5d50;--warn:#9a3b1b}
@media (prefers-color-scheme: dark){:root{--bg:#181816;--fg:#ecece8;--muted:#a3a39c;--card:#22221f;--line:#34342f;
--accent:#7fb8a4;--warn:#e39a7a}}
body{background:var(--bg);color:var(--fg);font:15px/1.5 system-ui,sans-serif;margin:0;padding:24px 16px}
main{max-width:860px;margin:0 auto}h1{font-size:22px}h2{font-size:17px;margin-top:28px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:16px;margin:12px 0}
code,pre{font:13px/1.45 ui-monospace,monospace}pre{white-space:pre-wrap;word-break:break-all;background:var(--bg);
padding:10px;border-radius:8px;border:1px solid var(--line);max-height:420px;overflow:auto}
.muted{color:var(--muted)}.warn{color:var(--warn)}button,.btn{background:var(--accent);color:var(--bg);border:0;
padding:9px 16px;border-radius:8px;font-weight:600;cursor:pointer;text-decoration:none;display:inline-block}
input{padding:7px;border:1px solid var(--line);border-radius:6px;background:var(--card);color:var(--fg);width:100%;
box-sizing:border-box}label{display:block;margin:10px 0 4px}table{border-collapse:collapse;width:100%}
td,th{border-bottom:1px solid var(--line);padding:6px;text-align:left;font-size:14px;vertical-align:top}
"""


def page(title: str, body: str) -> HTMLResponse:
    return HTMLResponse(f"<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' "
                        f"content='width=device-width,initial-scale=1'><title>{E(title)}</title><style>{CSS}</style>"
                        f"</head><body><main><h1>{E(title)}</h1>{body}</main></body></html>",
                        headers={"Cache-Control": "no-store", "X-Frame-Options": "DENY",
                                 "Content-Security-Policy": "default-src 'self'; style-src 'unsafe-inline'"})


class Web:
    def __init__(self, app: App, keys: KeyRing) -> None:
        self.app, self.keys, self.s = app, keys, app.settings
        self._client_secret: str | None = None
        self._cognito_jwks: jwt.PyJWKClient | None = None

    # --- session helpers ------------------------------------------------------------------------
    def session(self, request: Request) -> dict[str, Any] | None:
        raw = request.cookies.get(SESSION_COOKIE)
        if not raw:
            return None
        try:
            return self.keys.decode(raw, self.s.jwt_audience_web)
        except jwt.PyJWTError:
            return None

    def _cookie(self, resp: Response, name: str, value: str, max_age: int) -> None:
        resp.set_cookie(name, value, max_age=max_age, httponly=True, secure=self.s.mode == "cloud",
                        samesite="lax", path="/")

    def _login_redirect(self, next_path: str, fresh: bool = False) -> RedirectResponse:
        q = urllib.parse.urlencode({"next": next_path, "fresh": "1" if fresh else "0"})
        return RedirectResponse(f"/login?{q}", status_code=303)

    # --- Cognito --------------------------------------------------------------------------------
    def _secret(self) -> str | None:
        if self._client_secret is None and self.s.cognito_client_secret_id:
            sm = self.app.sessions.platform_session().client("secretsmanager")
            self._client_secret = sm.get_secret_value(SecretId=self.s.cognito_client_secret_id)["SecretString"]
        return self._client_secret

    async def login(self, request: Request) -> Response:
        next_path = request.query_params.get("next", "/")
        if not next_path.startswith("/") or next_path.startswith("//"):
            next_path = "/"
        if request.query_params.get("fresh") == "1":
            # Force a new credential prompt: end the Cognito session first. Cognito only accepts registered
            # logout URIs (exact match), so the return path travels in a short-lived cookie.
            resp = RedirectResponse(f"https://{self.s.cognito_domain}/logout?" + urllib.parse.urlencode(
                {"client_id": self.s.cognito_client_id, "logout_uri": f"{self.s.public_base_url}/relogin"}),
                status_code=303)
            self._cookie(resp, "pmcp_next", next_path, 600)
            resp.delete_cookie(SESSION_COOKIE, path="/")
            return resp
        verifier = secrets.token_urlsafe(48)
        challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
        state = secrets.token_urlsafe(16)
        url = f"https://{self.s.cognito_domain}/oauth2/authorize?" + urllib.parse.urlencode({
            "response_type": "code", "client_id": self.s.cognito_client_id,
            "redirect_uri": f"{self.s.public_base_url}/callback", "scope": "openid email",
            "state": state, "code_challenge": challenge, "code_challenge_method": "S256",
        })
        resp = RedirectResponse(url, status_code=303)
        self._cookie(resp, OAUTH_COOKIE, self.keys.sign({
            "iss": self.s.jwt_issuer, "aud": "pmcp-oauth", "sub": "oauth", "jti": state, "iat": int(time.time()),
            "exp": int(time.time()) + 600, "verifier": verifier, "next": next_path}), 600)
        return resp

    async def callback(self, request: Request) -> Response:
        raw = request.cookies.get(OAUTH_COOKIE, "")
        try:
            st = jwt.decode(raw, self.keys._load().public_key(), algorithms=["RS256"], audience="pmcp-oauth",
                            issuer=self.s.jwt_issuer)
        except jwt.PyJWTError:
            return page("Login failed", "<p>Login session expired. <a href='/login'>Try again</a>.</p>")
        if request.query_params.get("state") != st["jti"] or not request.query_params.get("code"):
            return page("Login failed", "<p>State mismatch. <a href='/login'>Try again</a>.</p>")
        data = {"grant_type": "authorization_code", "client_id": self.s.cognito_client_id,
                "code": request.query_params["code"], "redirect_uri": f"{self.s.public_base_url}/callback",
                "code_verifier": st["verifier"]}
        auth = (self.s.cognito_client_id, self._secret()) if self._secret() else None
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(f"https://{self.s.cognito_domain}/oauth2/token", data=data, auth=auth)
        if r.status_code != 200:
            return page("Login failed", f"<p>Token exchange failed ({r.status_code}).</p>")
        id_token = r.json()["id_token"]
        region = self.s.cognito_user_pool_id.split("_")[0]  # type: ignore[union-attr]
        issuer = f"https://cognito-idp.{region}.amazonaws.com/{self.s.cognito_user_pool_id}"
        if self._cognito_jwks is None:
            self._cognito_jwks = jwt.PyJWKClient(f"{issuer}/.well-known/jwks.json")
        key = self._cognito_jwks.get_signing_key_from_jwt(id_token)
        claims = jwt.decode(id_token, key.key, algorithms=["RS256"], audience=self.s.cognito_client_id, issuer=issuer)
        if claims.get("token_use") != "id":
            return page("Login failed", "<p>Unexpected token.</p>")
        session = issue_web_session(self.keys, self.s, claims["sub"], claims.get("email"),
                                    claims.get("cognito:groups", []), int(claims.get("auth_time", time.time())))
        resp = RedirectResponse(st.get("next", "/"), status_code=303)
        self._cookie(resp, SESSION_COOKIE, session, self.s.web_session_ttl_seconds)
        resp.delete_cookie(OAUTH_COOKIE, path="/")
        return resp

    async def relogin(self, request: Request) -> Response:
        next_path = request.cookies.get("pmcp_next", "/")
        if not next_path.startswith("/") or next_path.startswith("//"):
            next_path = "/"
        resp = RedirectResponse("/login?" + urllib.parse.urlencode({"next": next_path}), status_code=303)
        resp.delete_cookie("pmcp_next", path="/")
        return resp

    async def logout(self, request: Request) -> Response:
        resp = RedirectResponse(f"https://{self.s.cognito_domain}/logout?" + urllib.parse.urlencode(
            {"client_id": self.s.cognito_client_id, "logout_uri": f"{self.s.public_base_url}/"}), status_code=303)
        resp.delete_cookie(SESSION_COOKIE, path="/")
        return resp

    # --- pages ----------------------------------------------------------------------------------
    async def home(self, request: Request) -> Response:
        sess = self.session(request)
        who = f"Signed in as {E(sess.get('email') or sess['sub'])} · <a href='/logout'>Sign out</a>" if sess else \
            "<a class='btn' href='/login?next=/token'>Sign in / Sign up</a>"
        return page("Pipeline MCP", f"""
<p class='muted'>Remote MCP server that designs, deploys, verifies and operates data pipelines in <b>your own</b>
AWS account. pmcp never stores your AWS credentials; it assumes roles you create.</p>
<div class='card'>{who}</div>
<div class='card'><ol>
<li><a href='/token'>Get an MCP token</a> and add the server to your agent.</li>
<li><a href='/connect'>Connect an AWS account</a> (read-only, or with deploy rights).</li>
<li>Ask your agent for a pipeline. Plans that change AWS wait for your approval on this site.</li>
</ol></div>""")

    async def token(self, request: Request) -> Response:
        sess = self.session(request)
        if not sess:
            return self._login_redirect("/token")
        if request.method == "POST":
            form = await request.form()
            if form.get("csrf") != sess["csrf"]:
                return page("Error", "<p>Invalid form token.</p>")
            tok, claims = issue_mcp_token(self.keys, self.s, sess["sub"], sess.get("email"), sess.get("groups", []))
            url = f"{self.s.public_base_url}/mcp"
            cmd = f'claude mcp add --transport http pmcp {url} --header "Authorization: Bearer {tok}"'
            return page("Your MCP token", f"""
<p class='warn'>Shown once. Anyone with this token can use pmcp as you until
{time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime(claims['exp']))} (token id <code>{E(claims['jti'])}</code>).
It cannot approve plans.</p>
<h2>Claude Code</h2><pre>{E(cmd)}</pre>
<h2>Other clients</h2><p>URL <code>{E(url)}</code>, header <code>Authorization: Bearer &lt;token&gt;</code></p>
<pre>{E(tok)}</pre>""")
        return page("MCP token", f"""<div class='card'><form method='post'>
<input type='hidden' name='csrf' value='{E(sess["csrf"])}'>
<p>Issue a new token for <b>{E(sess.get("email") or sess["sub"])}</b>, valid
{self.s.mcp_token_ttl_seconds // 86400} days.</p><button>Issue token</button></form></div>""")

    async def connect(self, request: Request) -> Response:
        sess = self.session(request)
        if not sess:
            return self._login_redirect("/connect")
        owner = sess["sub"]
        msg = ""
        if request.method == "POST":
            form = await request.form()
            if form.get("csrf") != sess["csrf"]:
                return page("Error", "<p>Invalid form token.</p>")
            acct = str(form.get("account_id", "")).strip()
            if not re.fullmatch(r"\d{12}", acct):
                msg = "<p class='warn'>Account id must be 12 digits.</p>"
            elif form.get("action") == "start":
                regions = [r.strip() for r in str(form.get("regions", "")).split(",") if r.strip()]
                try:
                    cap = float(form.get("cap") or self.s.default_monthly_cost_cap_usd)
                except ValueError:
                    cap = self.s.default_monthly_cost_cap_usd
                conn = self.app.store.get(AccountConnection, owner, acct)
                if conn is None:
                    conn = self.app.store.create(AccountConnection(
                        id=acct, owner=owner, account_id=acct, external_id=secrets.token_urlsafe(24),
                        allowed_regions=regions or [self.s.platform_region], monthly_cost_cap_usd=cap))
                else:
                    conn.allowed_regions = regions or conn.allowed_regions
                    conn.monthly_cost_cap_usd = cap
                    conn = self.app.store.save(conn)
                return page("Create the pmcp roles", self._launch(conn, form.get("deployer") == "on", sess))
            elif form.get("action") == "verify":
                conn = self.app.store.get(AccountConnection, owner, acct)
                if conn is None:
                    msg = "<p class='warn'>Start the connection first.</p>"
                else:
                    msg = self._verify(conn)
        rows = "".join(
            f"<tr><td>{E(c.account_id)}</td><td>{E(c.status)}</td><td>{'deploy' if c.deployer_role_arn else 'read'}"
            f"</td><td>{E(', '.join(c.allowed_regions))}</td><td>{c.monthly_cost_cap_usd:g}</td>"
            f"<td class='muted'>{E(c.last_error or '')}</td></tr>"
            for c in self.app.store.list(AccountConnection, owner))
        return page("AWS accounts", f"""{msg}
<div class='card'><table><tr><th>Account</th><th>Status</th><th>Access</th><th>Regions</th><th>Cap USD/mo</th>
<th></th></tr>{rows or "<tr><td colspan=6 class='muted'>No accounts yet</td></tr>"}</table></div>
<div class='card'><form method='post'><input type='hidden' name='csrf' value='{E(sess["csrf"])}'>
<input type='hidden' name='action' value='start'>
<label>AWS account id</label><input name='account_id' required pattern='\\d{{12}}'>
<label>Allowed regions (comma separated)</label><input name='regions' value='{E(self.s.platform_region)}'>
<label>Monthly cost cap for pmcp flows (USD)</label><input name='cap' value='{self.s.default_monthly_cost_cap_usd:g}'>
<label><input type='checkbox' name='deployer' style='width:auto'> Allow pmcp to deploy (otherwise read-only)</label>
<p><button>Continue</button></p></form></div>
<div class='card'><form method='post'><input type='hidden' name='csrf' value='{E(sess["csrf"])}'>
<input type='hidden' name='action' value='verify'><label>Verify account id after the stack is created</label>
<input name='account_id' required><p><button>Verify connection</button></p></form></div>""")

    def _launch(self, conn: AccountConnection, deployer: bool, sess: dict[str, Any]) -> str:
        region = conn.allowed_regions[0]
        tmpl = f"https://pmcp-public-{self.s.platform_account_id}.s3.{self.s.platform_region}.amazonaws.com/" \
               "onboarding/tenant-onboarding.yaml"
        params = {"stackName": "pmcp-onboarding", "templateURL": tmpl, "param_ExternalId": conn.external_id,
                  "param_PlatformPrincipalArns": ",".join(self.s.platform_principal_arns),
                  "param_EnableDeployer": "true" if deployer else "false",
                  "param_AllowedRegions": ",".join(conn.allowed_regions)}
        url = f"https://{region}.console.aws.amazon.com/cloudformation/home?region={region}#/stacks/quickcreate?" + \
            urllib.parse.urlencode(params)
        extra = (", <code>pmcp-deployer</code>, <code>pmcp-cfn-exec</code> and the <code>pmcp-boundary</code> "
                 "permission boundary") if deployer else ""
        return f"""<div class='card'><p>1. Sign in to AWS account <b>{E(conn.account_id)}</b> and open:</p>
<p><a class='btn' href='{E(url)}' target='_blank' rel='noopener'>Launch onboarding stack</a></p>
<p class='muted'>Creates <code>pmcp-reader</code>{extra}. Only pmcp's platform roles can assume them, and only
with your External ID <code>{E(conn.external_id)}</code>. Template: <a href='{E(tmpl)}'>{E(tmpl)}</a></p>
<p>2. When the stack is CREATE_COMPLETE, come back to <a href='/connect'>/connect</a> and verify.</p></div>"""

    def _verify(self, conn: AccountConnection) -> str:
        acct = conn.account_id
        reader = conn.model_copy(update={
            "reader_role_arn": f"arn:aws:iam::{acct}:role/{self.s.tenant_reader_role_name}",
            "deployer_role_arn": None})
        deployer = reader.model_copy(update={
            "deployer_role_arn": f"arn:aws:iam::{acct}:role/{self.s.tenant_deployer_role_name}"})
        conn.reader_role_arn = reader.reader_role_arn
        try:
            self.app.sessions.verify_connection(reader)
        except PmcpError as exc:
            conn.status, conn.last_error = "error", exc.message[:300]
            self.app.store.save(conn)
            return f"<p class='warn'>Could not assume pmcp-reader: {E(exc.message)}</p>"
        try:
            self.app.sessions.verify_connection(deployer)
            conn.deployer_role_arn = deployer.deployer_role_arn
            conn.cfn_exec_role_arn = f"arn:aws:iam::{acct}:role/{self.s.tenant_cfn_exec_role_name}"
            conn.boundary_policy_arn = f"arn:aws:iam::{acct}:policy/{self.s.tenant_boundary_policy_name}"
        except PmcpError:
            conn.deployer_role_arn = conn.cfn_exec_role_arn = conn.boundary_policy_arn = None
        conn.status, conn.last_error, conn.last_checked_at = "connected", None, time.strftime("%Y-%m-%dT%H:%M:%SZ")
        self.app.store.save(conn)
        return (f"<p>Connected {E(acct)} with "
                f"{'deploy' if conn.deployer_role_arn else 'read-only'} access.</p>")

    async def approve(self, request: Request) -> Response:
        plan_id = request.path_params["plan_id"]
        sess = self.session(request)
        fresh = sess and time.time() - int(sess.get("auth_time", 0)) <= self.s.approval_fresh_login_seconds
        if not sess or not fresh:
            return self._login_redirect(f"/approve/{plan_id}", fresh=True)
        owner = sess["sub"]
        plan = self.app.store.get(Plan, owner, plan_id)
        if plan is None:
            return page("Not found", "<p>No such plan for your account.</p>")
        if request.method == "POST":
            form = await request.form()
            if form.get("csrf") != sess["csrf"]:
                return page("Error", "<p>Invalid form token.</p>")
            try:
                record_approval(self.app, owner, plan_id, str(form.get("digest", "")),
                                auth_time=int(sess["auth_time"]), channel="web", email=sess.get("email"))
            except PmcpError as exc:
                return page("Not approved", f"<p class='warn'>{E(exc.message)}</p>")
            return page("Approved", f"<p>Plan <code>{E(plan_id)}</code> is approved for digest "
                                    f"<code>{E(plan.digest or '')}</code>. Tell your agent to apply it.</p>")
        return page(f"Review plan {plan_id}", render_plan(plan, sess["csrf"]))


def render_plan(plan: Plan, csrf: str) -> str:
    rc = "".join(f"<tr><td>{E(c.action)}</td><td>{E(c.logical_id)}</td><td>{E(c.resource_type)}</td>"
                 f"<td>{E(c.replacement)}</td><td class='muted'>{E('; '.join(c.details[:4]))}</td></tr>"
                 for c in plan.resource_changes)
    steps = "".join(f"<li><b>{E(s.description)}</b> <span class='muted'>({E(s.action)}"
                    f"{'; not reversible' if not s.reversible else ''})</span></li>" for s in plan.steps)
    diffs = "".join(f"<h3>{E(d['artifact'])}</h3><pre>{E(d['diff'])}</pre>" for d in plan.code_diffs)
    sql = "".join(f"<h3>{E(a.name)}</h3><pre>{E((a.content or '')[:6000])}</pre>" for a in plan.artifacts
                  if a.kind in ("sql", "python") and not a.name.startswith("pmcp/"))
    di = plan.data_impact
    cost = plan.cost
    can_approve = plan.status == "previewed"
    form = (f"<form method='post'><input type='hidden' name='csrf' value='{E(csrf)}'>"
            f"<input type='hidden' name='digest' value='{E(plan.digest or '')}'>"
            f"<button>Approve exactly this plan</button></form>") if can_approve else \
        f"<p class='muted'>Status: {E(plan.status)} - nothing to approve.</p>"
    return f"""
<div class='card'><p><b>{E(plan.summary)}</b></p>
<p>Account <code>{E(plan.account_id)}</code> · region <code>{E(plan.region)}</code> · stack <code>{E(plan.stack_name)}</code>
· kind {E(plan.kind)} · expires {E(plan.expires_at)}</p>
<p class='muted'>Reason: {E(plan.change_reason or '-')}</p>
<p>Digest <code>{E(plan.digest or '')}</code></p>
{''.join(f"<p class='warn'>{E(w)}</p>" for w in plan.warnings)}</div>
<h2>Infrastructure changes</h2><div class='card'><table><tr><th>Action</th><th>Resource</th><th>Type</th>
<th>Replace</th><th>Details</th></tr>{rc or "<tr><td colspan=5 class='muted'>none</td></tr>"}</table></div>
<h2>Data impact</h2><div class='card'><pre>{E(json.dumps(di.model_dump(), indent=1))}</pre></div>
<h2>Cost</h2><div class='card'><p>{cost.monthly_usd if cost else '-'} USD/month
{f" + {cost.one_off_usd} USD one-off" if cost and cost.one_off_usd else ''} (confidence {E(cost.confidence if cost else '-')})</p>
<pre>{E(json.dumps(cost.model_dump() if cost else {}, indent=1)[:4000])}</pre></div>
<h2>Execution order</h2><div class='card'><ol>{steps}</ol></div>
{"<h2>Code/SQL diff</h2><div class='card'>" + diffs + "</div>" if diffs else ""}
{"<h2>Code/SQL</h2><div class='card'>" + sql + "</div>" if sql else ""}
<div class='card'>{form}</div>"""


def routes(app: App, keys: KeyRing) -> list[Route]:
    w = Web(app, keys)

    async def jwks(_: Request) -> Response:
        return JSONResponse(keys.jwks())

    async def health(_: Request) -> Response:
        return JSONResponse({"ok": True})

    return [
        Route("/", w.home),
        Route("/healthz", health),
        Route("/.well-known/jwks.json", jwks),
        Route("/login", w.login),
        Route("/callback", w.callback),
        Route("/relogin", w.relogin),
        Route("/logout", w.logout),
        Route("/token", w.token, methods=["GET", "POST"]),
        Route("/connect", w.connect, methods=["GET", "POST"]),
        Route("/approve/{plan_id}", w.approve, methods=["GET", "POST"]),
    ]
