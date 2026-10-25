"""Design support: what exists, what each service can do, what it costs, and how candidates fit."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import boto3
from botocore.exceptions import ClientError

from pmcp.adapters import ADAPTERS
from pmcp.aws.client import BOTO_CONFIG
from pmcp.catalog.integration import integration_for
from pmcp.catalog.patterns import PATTERNS, Requirements, rank_key
from pmcp.catalog.services import SERVICES
from pmcp.errors import ErrorCode, PmcpError
from pmcp.services.contracts import load_contract

if TYPE_CHECKING:
    from pmcp.app import App


def list_capabilities(category: str | None, pattern_id: str | None) -> dict[str, Any]:
    pats = []
    for p in PATTERNS.values():
        if pattern_id and p.pattern_id != pattern_id:
            continue
        if category and not p.components.get(category):
            continue
        entry: dict[str, Any] = {
            "pattern_id": p.pattern_id, "title": p.title, "summary": p.summary, "components": p.components,
            "adapter_status": p.adapter_status, "integration": integration_for(p.pattern_id).model_dump(),
        }
        if p.adapter_status == "catalog_only":
            entry["implementation_work"] = p.implementation_work
        if pattern_id and p.pattern_id in ADAPTERS:
            entry["params_schema"] = ADAPTERS[p.pattern_id].Params.model_json_schema()
        pats.append(entry)
    if pattern_id and not pats:
        raise PmcpError(ErrorCode.NOT_FOUND, f"unknown pattern_id {pattern_id}")
    services = [{"service_id": s.service_id, "title": s.title, "categories": s.categories}
                for s in SERVICES.values() if not category or category in s.categories]
    return {"patterns": pats, "services": services,
            "status_legend": {
                "adapter_status": "implemented = pmcp can deploy it; catalog_only = evaluated for fit, not deployable",
                "integration.status": "aws_verified = proven on real AWS (see evidence); mock_tested = unit/moto "
                "tests only; not_verified = no tests",
            },
            "hint": None if pattern_id else "Pass pattern_id to get that pattern's params_schema."}


def service_constraints(app: App, owner: str, services: list[str], region: str,
                        account_id: str | None) -> dict[str, Any]:
    unknown = [s for s in services if s not in SERVICES]
    if unknown:
        raise PmcpError(ErrorCode.INVALID_INPUT, f"unknown service ids {unknown}",
                        details={"known": sorted(SERVICES)})
    session = app.sessions.session(owner, account_id, region, "reader") if account_id else None
    out = []
    for sid in services:
        s = SERVICES[sid]
        regions = boto3.Session().get_available_regions(s.boto_service)
        entry: dict[str, Any] = {
            "service_id": sid, "title": s.title, "latency_floor_seconds": s.latency_floor_seconds,
            "delivery": s.delivery, "strengths": s.strengths, "weaknesses": s.weaknesses, "cost_model": s.cost_model,
            "facts": [f.model_dump(exclude_none=True) for f in s.facts],
            "available_in_region": region in regions if regions else None,
            "availability_source": "botocore endpoint data (offline)",
        }
        if session and s.quota_service_code:
            live = []
            names = [f.quota_name for f in s.facts if f.quota_name]
            try:
                sq = session.client("service-quotas", config=BOTO_CONFIG)
                for page in sq.get_paginator("list_service_quotas").paginate(ServiceCode=s.quota_service_code,
                                                                             PaginationConfig={"MaxItems": 200}):
                    for q in page["Quotas"]:
                        if any(n.lower() in q["QuotaName"].lower() for n in names):
                            live.append({"name": q["QuotaName"], "value": q["Value"], "adjustable": q["Adjustable"],
                                         "code": q["QuotaCode"]})
                entry["live_quotas"] = live or "no matching quota names"
            except ClientError as exc:
                entry["live_quotas"] = f"unverified: {exc.response['Error']['Code']}"
        out.append(entry)
    return {"region": region, "services": out}


def estimate_cost(app: App, owner: str, contract_id: str, contract_version: int | None, pattern_id: str,
                  pattern_params: dict[str, Any], region: str) -> dict[str, Any]:
    contract = load_contract(app, owner, contract_id, contract_version)
    if pattern_id not in ADAPTERS:
        pat = PATTERNS.get(pattern_id)
        raise PmcpError(ErrorCode.UNSUPPORTED_CONFIGURATION,
                        f"no cost model for {pattern_id}: " + ("catalog_only pattern" if pat else "unknown pattern"),
                        details={"implementation_work": pat.implementation_work if pat else []})
    adapter = ADAPTERS[pattern_id]
    params = adapter.parse_params(pattern_params, contract.body)
    est = adapter.estimate(contract.body, params, region, app.prices)
    return {"pattern_id": pattern_id, "region": region, "contract": [contract.contract_id, contract.contract_version],
            **est.model_dump(), "note": "Estimates exclude data transfer, free tiers and taxes; price_source shows "
            "where each unit price came from."}


def validate_architecture(app: App, owner: str, contract_id: str, contract_version: int | None,
                          candidates: list[dict[str, Any]], account_id: str | None,
                          region: str | None) -> dict[str, Any]:
    contract = load_contract(app, owner, contract_id, contract_version)
    req = Requirements.from_contract(contract.body)
    conn = None
    env_notes: list[str] = []
    if account_id:
        conn = app.sessions.connection(owner, account_id)
        if region:
            app.sessions.check_region(conn, region)
    results = []
    for cand in candidates:
        pid = cand.get("pattern_id", "")
        pat = PATTERNS.get(pid)
        if pat is None:
            results.append({"pattern_id": pid, "error": "unknown pattern_id"})
            continue
        fit = pat.evaluate(req)
        entry: dict[str, Any] = {
            "pattern_id": pid,
            "title": pat.title,
            "fitness": fit.model_dump(),
            "adapter_status": pat.adapter_status,
            "integration": integration_for(pid).model_dump(),
        }
        deploy_blockers: list[str] = []
        if pat.adapter_status != "implemented":
            deploy_blockers.append("no adapter: " + "; ".join(pat.implementation_work))
        params_ok = None
        if pid in ADAPTERS and cand.get("pattern_params") is not None:
            try:
                params = ADAPTERS[pid].parse_params(cand["pattern_params"], contract.body)
                params_ok = True
                if region:
                    entry["cost"] = ADAPTERS[pid].estimate(contract.body, params, region, app.prices).model_dump(
                        include={"monthly_usd", "confidence", "assumptions"})
            except PmcpError as exc:
                params_ok = False
                deploy_blockers.append(f"params: {exc.message}: {exc.details.get('problems') or exc.details}")
        elif pid in ADAPTERS:
            deploy_blockers.append("pattern_params not given; deployability not checked")
        if region:
            for sid in {s for comps in pat.components.values() for s in comps}:
                regions = boto3.Session().get_available_regions(SERVICES[sid].boto_service)
                if regions and region not in regions:
                    deploy_blockers.append(f"{sid} not available in {region}")
        if conn is not None:
            if not conn.write_enabled:
                deploy_blockers.append("account connected read-only (deployer role not enabled)")
        elif account_id is None:
            env_notes.append("no account_id given: account permissions/LF/quotas not checked")
        if fit.level == "unsuitable":
            deploy_blockers.append("requirements not met (see fitness.blockers)")
        entry["params_valid"] = params_ok
        entry["deployable_now"] = not deploy_blockers
        entry["deploy_blockers"] = deploy_blockers
        results.append(entry)
    proposed = {c.get("pattern_id") for c in candidates}
    better_unproposed = []
    best_proposed = max((rank_key(r["fitness"]["level"]) for r in results if "fitness" in r), default=-1)
    for pat in PATTERNS.values():
        if pat.pattern_id in proposed:
            continue
        fit = pat.evaluate(req)
        if rank_key(fit.level) > best_proposed:
            better_unproposed.append({"pattern_id": pat.pattern_id, "fitness_level": fit.level,
                                      "adapter_status": pat.adapter_status, "why": fit.reasons[:4]})
    scored = [r for r in results if "fitness" in r]
    best_fit = sorted(scored, key=lambda r: (-rank_key(r["fitness"]["level"]),
                                             r.get("cost", {}).get("monthly_usd", 1e9)))
    deployable = [r for r in best_fit if r["deployable_now"]]
    return {
        "requirements": req.model_dump(),
        "candidates": results,
        "best_fit": best_fit[0]["pattern_id"] if best_fit else None,
        "best_deployable_now": deployable[0]["pattern_id"] if deployable else None,
        "better_fit_not_proposed": better_unproposed,
        "notes": sorted(set(env_notes)) + [
            "best_fit ranks by requirement fitness only; best_deployable_now additionally requires an implemented "
            "adapter, valid params and a write-enabled connection. When they differ, the gap is the implementation "
            "work listed for best_fit."],
    }
