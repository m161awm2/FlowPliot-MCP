from __future__ import annotations

import hmac
import logging
from typing import Any

from mcp.server.auth.settings import AuthSettings
from mcp.server.mcpserver import MCPServer
from mcp.server.transport_security import TransportSecuritySettings

from pmcp import __version__
from pmcp.app import App
from pmcp.auth.tokens import KeyRing, McpTokenVerifier
from pmcp.mcp_server.tools import register

log = logging.getLogger(__name__)

INSTRUCTIONS = """\
pmcp designs, deploys, verifies and operates data pipelines in the user's own AWS accounts.

Typical build: list_workspace -> inspect_repository / inspect_aws_environment / profile_source /
inspect_consumer_contract -> agree requirements with the user -> define_data_contract ->
validate_architecture (+ estimate_cost, get_service_constraints) -> prepare_changes -> preview_changes ->
the USER approves at the returned link -> apply_changes -> get_operation -> verify_data_flow -> get_operation.
Modify a flow: prepare_changes with flow_id (shows diff, consumer impact, reprocessing), same approval path.
Operate: observe_flow; on trouble diagnose_failure -> plan_recovery -> preview_changes -> approval ->
execute_recovery (re-verifies).
For full errors use get_run_failure with a flow_id and execution name/ARN. It reads existing execution
history and Athena error details without creating incidents or running queries.

Rules: you cannot approve plans; never say a plan is approved until apply_changes stops returning
APPROVAL_REQUIRED. 'deployed' is not 'verified'. Report unverified checks as unverified. Do not present an
implemented pattern as the best fit when validate_architecture says otherwise. Errors are JSON with a
`code`; follow `next_actions`.
"""


def build_server(app: App, keys: KeyRing | None = None) -> MCPServer:
    kwargs: dict[str, Any] = {}
    if app.settings.mode == "cloud":
        keys = keys or KeyRing(app.settings)
        kwargs["token_verifier"] = McpTokenVerifier(keys, app.settings, app.store)
        kwargs["auth"] = AuthSettings(
            issuer_url=app.settings.jwt_issuer,  # type: ignore[arg-type]
            resource_server_url=f"{app.settings.public_base_url}/mcp",  # type: ignore[arg-type]
            required_scopes=["mcp"],
            validate_token_resource=False,  # McpTokenVerifier checks the audience itself
        )
    server = MCPServer("pmcp", title="Pipeline MCP", instructions=INSTRUCTIONS, version=__version__, **kwargs)
    register(server, app)
    return server


class OriginVerify:
    """Reject requests that did not come through CloudFront (which adds a secret header)."""

    def __init__(self, inner: Any, secret: str, open_paths: tuple[str, ...] = ("/healthz",)) -> None:
        self.inner, self.secret, self.open_paths = inner, secret.encode(), open_paths

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] == "http" and scope.get("path") not in self.open_paths:
            got = dict(scope.get("headers") or []).get(b"x-pmcp-origin-verify", b"")
            if not hmac.compare_digest(got, self.secret):
                await send({"type": "http.response.start", "status": 403,
                            "headers": [(b"content-type", b"text/plain")]})
                await send({"type": "http.response.body", "body": b"forbidden"})
                return
        await self.inner(scope, receive, send)


def http_app(app: App) -> Any:
    from urllib.parse import urlparse

    from pmcp.auth.web import routes

    keys = KeyRing(app.settings)
    server = build_server(app, keys)
    host = urlparse(app.settings.public_base_url).netloc
    asgi = server.streamable_http_app(
        streamable_http_path="/mcp",
        stateless_http=True,
        json_response=True,
        host="0.0.0.0",
        transport_security=TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=[host, "localhost:*", "127.0.0.1:*"],
            allowed_origins=[app.settings.public_base_url],
        ),
    )
    asgi.router.routes.extend(routes(app, keys))
    if app.settings.origin_verify_secret_id:
        sm = app.sessions.platform_session().client("secretsmanager")
        secret = sm.get_secret_value(SecretId=app.settings.origin_verify_secret_id)["SecretString"]
        return OriginVerify(asgi, secret)
    log.warning("origin verification disabled (PMCP_ORIGIN_VERIFY_SECRET_ID not set)")
    return asgi
