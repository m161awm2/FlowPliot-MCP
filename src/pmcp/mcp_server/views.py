"""Tool output models (these become each tool's MCP output_schema)."""

from __future__ import annotations

from typing import Any

from pydantic import Field

from pmcp.domain.base import Model
from pmcp.domain.records import Operation, Plan, Verification


class RepositoryReport(Model):
    source: str
    ref: str | None
    files_scanned: int
    languages: dict[str, int]
    producer_calls: list[dict[str, Any]]
    iac_resources: list[dict[str, Any]]
    resource_name_hints: dict[str, list[str]]
    candidate_event_fields: list[str]
    schema_files: list[str]
    sample_data_files: list[str]
    iac_files: list[str]
    notes: list[str]
    next_actions: list[str]


class EnvironmentReport(Model):
    account_id: str
    region: str
    inventory: dict[str, Any]
    errors: dict[str, str] = Field(description="Per-category read failures (usually missing permissions).")
    next_actions: list[str]


class SourceProfile(Model):
    source_uri: str
    profile: dict[str, Any]
    suggested_source_columns: list[dict[str, Any]] = Field(
        description="Columns in contract form (name/type/nullable) inferred from the sample.")
    next_actions: list[str]


class ConsumerReport(Model):
    target: str
    report: dict[str, Any]
    next_actions: list[str]


class ContractResult(Model):
    contract_id: str
    contract_version: int
    created_new_version: bool
    derived_requirements: dict[str, Any]
    next_actions: list[str]


class Capabilities(Model):
    patterns: list[dict[str, Any]]
    services: list[dict[str, Any]]
    status_legend: dict[str, str]
    hint: str | None


class ServiceConstraints(Model):
    region: str
    services: list[dict[str, Any]]


class CostResult(Model):
    pattern_id: str
    region: str
    contract: list[Any]
    monthly_usd: float
    one_off_usd: float
    lines: list[dict[str, Any]]
    assumptions: list[str]
    confidence: str
    note: str


class ArchitectureValidation(Model):
    requirements: dict[str, Any]
    candidates: list[dict[str, Any]]
    best_fit: str | None
    best_deployable_now: str | None
    better_fit_not_proposed: list[dict[str, Any]]
    notes: list[str]
    next_actions: list[str]


class PlanView(Model):
    plan_id: str
    kind: str
    status: str
    summary: str
    account_id: str
    region: str
    flow_id: str
    stack_name: str
    pattern_id: str
    contract: list[Any]
    change_reason: str | None
    current_state: dict[str, Any]
    desired_state: dict[str, Any]
    resource_changes: list[dict[str, Any]]
    code_diffs: list[dict[str, str]]
    artifacts: list[dict[str, Any]] = Field(description="name/kind/sha256/size; content omitted.")
    data_impact: dict[str, Any]
    cost: dict[str, Any] | None
    steps: list[dict[str, Any]]
    warnings: list[str]
    change_set: dict[str, Any] | None
    plan_digest: str | None
    expires_at: str
    approval: str | None = Field(description="Where a person approves this plan (never via an MCP tool).")
    next_actions: list[str]

    @classmethod
    def of(cls, plan: Plan, approval: str | None, next_actions: list[str]) -> PlanView:
        return cls(
            plan_id=plan.plan_id, kind=plan.kind, status=plan.status, summary=plan.summary,
            account_id=plan.account_id, region=plan.region, flow_id=plan.flow_id, stack_name=plan.stack_name,
            pattern_id=plan.pattern_id, contract=[plan.contract_id, plan.contract_version],
            change_reason=plan.change_reason, current_state=plan.current_state, desired_state=plan.desired_state,
            resource_changes=[c.model_dump() for c in plan.resource_changes], code_diffs=plan.code_diffs,
            artifacts=[{"name": a.name, "kind": a.kind, "sha256": a.sha256, "size": a.size, "s3_uri": a.s3_uri}
                       for a in plan.artifacts],
            data_impact=plan.data_impact.model_dump(), cost=plan.cost.model_dump() if plan.cost else None,
            steps=[st.model_dump() for st in plan.steps], warnings=plan.warnings,
            change_set={k: v for k, v in (plan.change_set or {}).items() if k != "changes_digest"} or None,
            plan_digest=plan.digest, expires_at=plan.expires_at, approval=approval, next_actions=next_actions,
        )


class RecoveryPlanView(Model):
    plan: PlanView
    available_strategies: list[dict[str, Any]]


class OperationView(Model):
    operation_id: str
    kind: str
    status: str
    stalled: bool = Field(description="Running/queued but no live worker; re-call the start tool with the same "
                                      "idempotency_key to resume.")
    flow_id: str
    plan_id: str | None
    steps: list[dict[str, Any]]
    result: dict[str, Any]
    error: dict[str, Any] | None
    verification: dict[str, Any] | None = None
    created_at: str
    updated_at: str
    next_actions: list[str]

    @classmethod
    def of(cls, op: Operation, stalled: bool, verification: Verification | None,
           next_actions: list[str]) -> OperationView:
        return cls(operation_id=op.operation_id, kind=op.kind, status=op.status, stalled=stalled,
                   flow_id=op.flow_id, plan_id=op.plan_id, steps=[st.model_dump() for st in op.steps], result=op.result, error=op.error,
                   verification=verification.model_dump(exclude={"owner", "version", "id"}) if verification else None,
                   created_at=op.created_at, updated_at=op.updated_at, next_actions=next_actions)


class FlowObservation(Model):
    observation: dict[str, Any]
    next_actions: list[str]


class IncidentView(Model):
    incident_id: str
    flow_id: str
    window: dict[str, str]
    observed_facts: list[dict[str, Any]]
    hypotheses: list[dict[str, Any]]
    further_checks: list[str]
    affected: dict[str, Any]
    probe_operation_id: str | None
    next_actions: list[str]


class Workspace(Model):
    accounts: list[dict[str, Any]]
    flows: list[dict[str, Any]]
    pending_plans: list[dict[str, Any]]
    recent_operations: list[dict[str, Any]]
    open_incidents: list[dict[str, Any]]
    connect_url: str | None
