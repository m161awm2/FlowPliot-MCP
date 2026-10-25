"""pmcp command line: serve, worker, local approval, token admin, local account connection."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys


def _app():
    from pmcp.app import build_app

    return build_app()


def cmd_serve(args: argparse.Namespace) -> None:
    app = _app()
    if args.transport == "stdio":
        from pmcp.mcp_server.server import build_server

        if app.settings.mode == "local" and not args.no_autoconnect:
            _autoconnect(app, args.regions)
        build_server(app).run("stdio")
        return
    import uvicorn

    from pmcp.mcp_server.server import http_app

    if app.settings.mode == "local" and not args.no_autoconnect:
        _autoconnect(app, args.regions)
    uvicorn.run(http_app(app), host=args.host, port=args.port, proxy_headers=True, forwarded_allow_ips="*",
                log_level="info")


def _autoconnect(app, regions: str) -> None:
    """Local mode: register the caller's own account as a `direct` connection (no AssumeRole)."""
    import secrets

    import boto3

    from pmcp.domain.records import AccountConnection

    try:
        acct = boto3.Session().client("sts").get_caller_identity()["Account"]
    except Exception as exc:  # no credentials: tools that need AWS will say NOT_CONNECTED
        logging.warning("local autoconnect skipped: %s", exc)
        return
    owner = app.settings.local_user_id
    if app.store.get(AccountConnection, owner, acct) is None:
        app.store.create(AccountConnection(
            id=acct, owner=owner, account_id=acct, external_id=secrets.token_urlsafe(24), mode="direct",
            status="connected", allowed_regions=[r.strip() for r in regions.split(",") if r.strip()],
            monthly_cost_cap_usd=float(os.environ.get("PMCP_LOCAL_COST_CAP", "20"))))


def cmd_worker(args: argparse.Namespace) -> None:
    from pmcp.services.operations import run_operation

    run_operation(_app(), args.owner, args.operation_id)


def cmd_approve(args: argparse.Namespace) -> None:
    """Local-mode approval. Reads confirmation from the controlling terminal so an agent shell without a
    TTY cannot approve on the user's behalf."""
    import time

    from pmcp.domain.records import Plan
    from pmcp.services.planning import record_approval

    app = _app()
    if app.settings.mode != "local":
        sys.exit("cloud mode: approve in the browser at /approve/<plan_id>")
    plan = app.store.require(Plan, app.settings.local_user_id, args.plan_id)
    try:
        tty = open("/dev/tty", "r+")  # noqa: SIM115
    except OSError:
        sys.exit("refusing: no interactive terminal (approvals must be typed by a person)")
    tty.write(f"\n{plan.summary}\naccount {plan.account_id} region {plan.region}\n")
    for c in plan.resource_changes:
        tty.write(f"  {c.action:7} {c.logical_id} ({c.resource_type}) replace={c.replacement}\n")
    for s in plan.steps:
        tty.write(f"  - {s.description}{' [not reversible]' if not s.reversible else ''}\n")
    tty.write(f"cost ~{plan.cost.monthly_usd if plan.cost else '?'} USD/month\ndigest {plan.digest}\n")
    tty.write("Type the last 6 characters of the digest to approve: ")
    tty.flush()
    if tty.readline().strip() != (plan.digest or "")[-6:] or args.digest != plan.digest:
        sys.exit("not approved")
    record_approval(app, app.settings.local_user_id, args.plan_id, plan.digest, auth_time=int(time.time()),
                    channel="local_cli")
    print("approved")


def cmd_token(args: argparse.Namespace) -> None:
    from pmcp.auth.tokens import KeyRing, issue_mcp_token, revoke

    app = _app()
    keys = KeyRing(app.settings)
    if args.action == "issue":
        tok, claims = issue_mcp_token(keys, app.settings, args.sub, args.email, ["admin"] if args.admin else [],
                                      ttl=args.ttl_days * 86400)
        print(json.dumps({"token": tok, "jti": claims["jti"], "exp": claims["exp"]}))
    else:
        revoke(app.store, args.jti, args.exp)
        print("revoked")


def cmd_admin_connect(args: argparse.Namespace) -> None:
    """Operator: register a tenant connection for a user without the web flow (tests, support). The roles
    are verified by assuming them, which only works from a platform principal (the server/worker role)."""
    from pmcp.domain.records import AccountConnection

    app = _app()
    acct = args.account_id
    conn = app.store.get(AccountConnection, args.owner, acct)
    fields = dict(
        account_id=acct, external_id=args.external_id, mode="assume_role", status="connected",
        reader_role_arn=f"arn:aws:iam::{acct}:role/pmcp-reader",
        deployer_role_arn=f"arn:aws:iam::{acct}:role/pmcp-deployer" if args.deployer else None,
        cfn_exec_role_arn=f"arn:aws:iam::{acct}:role/pmcp-cfn-exec" if args.deployer else None,
        boundary_policy_arn=f"arn:aws:iam::{acct}:policy/pmcp-boundary",
        allowed_regions=[r.strip() for r in args.regions.split(",")], monthly_cost_cap_usd=args.cap)
    if conn is None:
        app.store.create(AccountConnection(id=acct, owner=args.owner, **fields))
    else:
        for k, v in fields.items():
            setattr(conn, k, v)
        app.store.save(conn)
    print(f"connection {args.owner}/{acct} recorded (unverified until first use)")


def cmd_admin_approve(args: argparse.Namespace) -> None:
    """Operator approval on a user's behalf (recorded as channel=web with the operator note). For tests only;
    it needs the platform's signing key and state table, which agents do not have."""
    import time

    from pmcp.services.planning import record_approval

    app = _app()
    appr = record_approval(app, args.owner, args.plan_id, args.digest, auth_time=int(time.time()), channel="web",
                           email=f"operator:{args.note}")
    print(json.dumps({"plan_id": appr.plan_id, "digest": appr.digest, "approved_by": appr.approved_by,
                      "note": appr.approved_by_email}))


def cmd_tool_spec(args: argparse.Namespace) -> None:
    import anyio

    from pmcp.mcp_server.server import build_server

    async def dump():
        from mcp.client import Client

        async with Client(build_server(_app())) as c:
            tools = (await c.list_tools()).tools
            return [t.model_dump(mode="json", exclude_none=True) for t in tools]

    out = anyio.run(dump)
    with open(args.out, "w") as f:
        json.dump(out, f, indent=1, ensure_ascii=False)
    print(f"wrote {len(out)} tools to {args.out}")


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=os.environ.get("PMCP_LOG_LEVEL", "INFO"), stream=sys.stderr)
    p = argparse.ArgumentParser(prog="pmcp")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("serve")
    s.add_argument("--transport", choices=["stdio", "http"], default="stdio")
    s.add_argument("--host", default="0.0.0.0")
    s.add_argument("--port", type=int, default=8000)
    s.add_argument("--regions", default=os.environ.get("PMCP_LOCAL_REGIONS", "ap-southeast-2"))
    s.add_argument("--no-autoconnect", action="store_true")
    s.set_defaults(fn=cmd_serve)
    w = sub.add_parser("worker")
    w.add_argument("--owner", required=True)
    w.add_argument("--operation-id", required=True)
    w.set_defaults(fn=cmd_worker)
    a = sub.add_parser("approve")
    a.add_argument("plan_id")
    a.add_argument("--digest", required=True)
    a.set_defaults(fn=cmd_approve)
    t = sub.add_parser("token")
    t.add_argument("action", choices=["issue", "revoke"])
    t.add_argument("--sub")
    t.add_argument("--email")
    t.add_argument("--admin", action="store_true")
    t.add_argument("--ttl-days", type=int, default=7)
    t.add_argument("--jti")
    t.add_argument("--exp", type=int, default=0)
    t.set_defaults(fn=cmd_token)
    ac = sub.add_parser("admin-connect")
    ac.add_argument("--owner", required=True)
    ac.add_argument("--account-id", required=True)
    ac.add_argument("--external-id", required=True)
    ac.add_argument("--regions", default="ap-southeast-2")
    ac.add_argument("--cap", type=float, default=20)
    ac.add_argument("--deployer", action="store_true")
    ac.set_defaults(fn=cmd_admin_connect)
    aa = sub.add_parser("admin-approve")
    aa.add_argument("--owner", required=True)
    aa.add_argument("plan_id")
    aa.add_argument("--digest", required=True)
    aa.add_argument("--note", default="e2e")
    aa.set_defaults(fn=cmd_admin_approve)
    d = sub.add_parser("tool-spec")
    d.add_argument("--out", default="docs/tool-schemas.json")
    d.set_defaults(fn=cmd_tool_spec)
    args = p.parse_args(argv)
    args.fn(args)


if __name__ == "__main__":
    main()
