"""Persisted records other than contracts: connections, flows, plans, approvals, operations,
verifications and incidents."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import Field

from pmcp.domain.base import Model, Stored


class AccountConnection(Stored):
    KIND = "ACCT"

    account_id: str
    external_id: str
    # assume_role: normal tenant path. direct: local mode only, uses the caller's own credentials.
    mode: Literal["assume_role", "direct"] = "assume_role"
    status: Literal["pending", "connected", "error"] = "pending"
    reader_role_arn: str | None = None
    deployer_role_arn: str | None = None
    cfn_exec_role_arn: str | None = None
    boundary_policy_arn: str | None = None
    allowed_regions: list[str] = Field(default_factory=list)
    monthly_cost_cap_usd: float = 20.0
    last_checked_at: str | None = None
    last_error: str | None = None

    @property
    def write_enabled(self) -> bool:
        return self.mode == "direct" or bool(self.deployer_role_arn)


class Artifact(Model):
    name: str
    kind: Literal["template", "sql", "python", "zip", "json"]
    sha256: str
    size: int
    content: str | None = Field(None, description="Inline text content (kept for exact replay and diffs).")
    s3_uri: str | None = None


class ResourceChange(Model):
    logical_id: str
    resource_type: str
    action: Literal["add", "modify", "remove", "unknown"]
    replacement: Literal["true", "false", "conditional", "unknown"] = "unknown"
    details: list[str] = Field(default_factory=list)


class PlanStep(Model):
    step_id: str
    action: str
    description: str
    preconditions: list[str] = Field(default_factory=list)
    on_failure: Literal["abort", "compensate", "continue"] = "abort"
    compensation: str | None = None
    reversible: bool = True
    params: dict[str, Any] = Field(default_factory=dict)


class CostLine(Model):
    item: str
    monthly_usd: float
    unit_price_usd: float | None = None
    unit: str | None = None
    quantity: float | None = None
    price_source: Literal["pricing_api", "static_table"] = "static_table"
    price_as_of: str | None = None


class CostEstimate(Model):
    monthly_usd: float
    one_off_usd: float = 0.0
    lines: list[CostLine] = Field(default_factory=list)
    assumptions: list[str] = Field(default_factory=list)
    confidence: Literal["low", "medium", "high"] = "low"


class DataImpact(Model):
    datasets: list[str] = Field(default_factory=list)
    schema_changes: list[str] = Field(default_factory=list)
    breaking_for_consumers: bool = False
    consumer_notes: list[str] = Field(default_factory=list)
    reprocessing_required: bool = False
    reprocessing_reason: str | None = None
    reprocessing_window: dict[str, str] | None = None
    data_rollback: str = Field(
        "not_applicable",
        description="How changed data can be restored: not_applicable | rebuild_from_source | not_reversible.",
    )
    infra_rollback: str = "cloudformation_automatic"


class Plan(Stored):
    KIND = "PLAN"

    plan_id: str
    kind: Literal["change", "recovery"]
    account_id: str
    region: str
    flow_id: str
    stack_name: str
    pattern_id: str
    pattern_params: dict[str, Any]
    contract_id: str | None = None
    contract_version: int | None = None
    incident_id: str | None = None
    summary: str
    change_reason: str | None = None
    base_fingerprint: dict[str, Any] = Field(description="Target state captured when the plan was made.")
    current_state: dict[str, Any] = Field(default_factory=dict)
    desired_state: dict[str, Any] = Field(default_factory=dict)
    resource_changes: list[ResourceChange] = Field(default_factory=list)
    code_diffs: list[dict[str, str]] = Field(default_factory=list)
    artifacts: list[Artifact] = Field(default_factory=list)
    data_impact: DataImpact = Field(default_factory=DataImpact)
    cost: CostEstimate | None = None
    steps: list[PlanStep] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    change_set: dict[str, Any] | None = None
    status: Literal["draft", "previewed", "approved", "applying", "applied", "failed", "superseded", "expired"] = (
        "draft"
    )
    digest: str | None = None
    expires_at: str
    operation_id: str | None = None

    def digest_material(self) -> dict[str, Any]:
        """Everything that defines what apply will do. Status/approval/bookkeeping are excluded."""
        return {
            "plan_id": self.plan_id,
            "kind": self.kind,
            "owner": self.owner,
            "account_id": self.account_id,
            "region": self.region,
            "flow_id": self.flow_id,
            "stack_name": self.stack_name,
            "pattern_id": self.pattern_id,
            "pattern_params": self.pattern_params,
            "contract": [self.contract_id, self.contract_version],
            "incident_id": self.incident_id,
            "base_fingerprint": self.base_fingerprint,
            "artifacts": [(a.name, a.kind, a.sha256) for a in self.artifacts],
            "steps": [s.model_dump(mode="json") for s in self.steps],
            "change_set": self.change_set,
            "expires_at": self.expires_at,
        }


class Approval(Stored):
    KIND = "APPROVAL"

    plan_id: str
    digest: str
    approved_by: str
    approved_by_email: str | None = None
    auth_time: int
    channel: Literal["web", "local_cli"]


class StepState(Model):
    step_id: str
    status: Literal["pending", "running", "succeeded", "failed", "skipped"] = "pending"
    started_at: str | None = None
    finished_at: str | None = None
    detail: dict[str, Any] = Field(default_factory=dict)
    error: str | None = None


class Operation(Stored):
    KIND = "OP"

    operation_id: str
    kind: Literal["apply_changes", "execute_recovery", "verify_data_flow", "diagnose_probe"]
    account_id: str
    region: str
    flow_id: str
    plan_id: str | None = None
    idempotency_key: str
    status: Literal["queued", "running", "succeeded", "failed", "cancelled"] = "queued"
    steps: list[StepState] = Field(default_factory=list)
    lease_owner: str | None = None
    lease_expires_at: float | None = None
    attempts: int = 0
    result: dict[str, Any] = Field(default_factory=dict)
    error: dict[str, Any] | None = None
    runner_ref: str | None = None
    deadline_at: float | None = None
    params: dict[str, Any] = Field(default_factory=dict)


class Flow(Stored):
    KIND = "FLOW"

    flow_id: str
    name: str
    account_id: str
    region: str
    stack_name: str
    pattern_id: str
    pattern_params: dict[str, Any] = Field(default_factory=dict)
    contract_id: str | None = None
    contract_version: int | None = None
    status: Literal["planned", "deploying", "deployed", "verified", "degraded", "failed", "deleted"] = "planned"
    template_sha256: str | None = None
    outputs: dict[str, str] = Field(default_factory=dict)
    last_plan_id: str | None = None
    last_operation_id: str | None = None
    last_verification_id: str | None = None
    history: list[dict[str, Any]] = Field(default_factory=list)


class Check(Model):
    check_id: str
    category: Literal[
        "existence", "schema", "keys", "row_count", "mutations", "freshness", "quality", "consumer_read", "progress"
    ]
    status: Literal["passed", "failed", "unverified", "error"]
    description: str
    window: dict[str, str] | None = None
    expected: Any = None
    observed: Any = None
    evidence: list[str] = Field(default_factory=list)
    reason: str | None = None


class Verification(Stored):
    KIND = "VERIF"

    verification_id: str
    flow_id: str
    contract_id: str | None
    contract_version: int | None
    window: dict[str, str]
    verdict: Literal["verified", "failed", "partially_verified"]
    checks: list[Check]
    operation_id: str | None = None


class Hypothesis(Model):
    hypothesis_id: str
    statement: str
    likelihood: Literal["high", "medium", "low"]
    supporting_evidence: list[str] = Field(default_factory=list)
    contradicting_evidence: list[str] = Field(default_factory=list)
    suggested_recovery: list[str] = Field(default_factory=list)


class Incident(Stored):
    KIND = "INC"

    incident_id: str
    flow_id: str
    window: dict[str, str]
    observed_facts: list[dict[str, Any]]
    hypotheses: list[Hypothesis]
    further_checks: list[str]
    affected: dict[str, Any] = Field(default_factory=dict)
    status: Literal["open", "recovering", "resolved"] = "open"
