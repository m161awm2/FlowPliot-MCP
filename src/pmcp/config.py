"""Runtime settings, read from PMCP_* environment variables."""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Literal

from pydantic import BaseModel, Field


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(f"PMCP_{name}")
    return value if value not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    value = _env(name)
    return int(value) if value is not None else default


def _env_list(name: str) -> list[str]:
    value = _env(name)
    return [v.strip() for v in value.split(",") if v.strip()] if value else []


class Settings(BaseModel):
    # local: stdio transport, no JWT, the caller's own AWS credentials may be used directly.
    # cloud: Streamable HTTP behind CloudFront/ALB, JWT auth, tenants reached only via AssumeRole.
    mode: Literal["local", "cloud"] = "local"
    platform_region: str = "ap-southeast-2"
    platform_account_id: str | None = None

    # None selects the in-memory store (tests, throwaway local sessions).
    state_table: str | None = None

    public_base_url: str = "https://mcp.m161awm.kr"
    jwt_issuer: str = "https://mcp.m161awm.kr"
    jwt_audience_mcp: str = "pmcp-mcp"
    jwt_audience_web: str = "pmcp-web"
    jwt_secret_id: str | None = None
    jwt_private_key_pem: str | None = None
    mcp_token_ttl_seconds: int = 7 * 24 * 3600
    web_session_ttl_seconds: int = 3600
    approval_fresh_login_seconds: int = 600

    cognito_user_pool_id: str | None = None
    cognito_client_id: str | None = None
    cognito_client_secret_id: str | None = None
    cognito_domain: str | None = None  # e.g. pmcp-auth.auth.ap-southeast-2.amazoncognito.com

    origin_verify_secret_id: str | None = None

    runner: Literal["thread", "ecs"] = "thread"
    ecs_cluster: str | None = None
    worker_task_definition: str | None = None
    worker_container_name: str = "pmcp"
    worker_subnets: list[str] = Field(default_factory=list)
    worker_security_groups: list[str] = Field(default_factory=list)

    rate_limit_per_minute: int = 60
    max_concurrent_operations_per_user: int = 2
    operation_max_seconds: int = 1800
    plan_ttl_seconds: int = 24 * 3600
    default_monthly_cost_cap_usd: float = 20.0

    # Principals in the platform account that tenant roles trust (server + worker task roles).
    platform_principal_arns: list[str] = Field(default_factory=list)
    tenant_reader_role_name: str = "pmcp-reader"
    tenant_deployer_role_name: str = "pmcp-deployer"
    tenant_cfn_exec_role_name: str = "pmcp-cfn-exec"
    tenant_boundary_policy_name: str = "pmcp-boundary"

    local_user_id: str = "local-user"
    # local mode only: allow AccountConnection.mode == "direct" (use the caller's credentials).
    allow_direct_credentials: bool = True

    @classmethod
    def from_env(cls) -> Settings:
        mode = _env("MODE", "local")
        return cls(
            mode=mode,  # type: ignore[arg-type]
            platform_region=_env("PLATFORM_REGION", "ap-southeast-2"),  # type: ignore[arg-type]
            platform_account_id=_env("PLATFORM_ACCOUNT_ID"),
            state_table=_env("STATE_TABLE"),
            public_base_url=_env("PUBLIC_BASE_URL", "https://mcp.m161awm.kr"),  # type: ignore[arg-type]
            jwt_issuer=_env("JWT_ISSUER", _env("PUBLIC_BASE_URL", "https://mcp.m161awm.kr")),  # type: ignore[arg-type]
            jwt_secret_id=_env("JWT_SECRET_ID"),
            jwt_private_key_pem=_env("JWT_PRIVATE_KEY_PEM"),
            cognito_user_pool_id=_env("COGNITO_USER_POOL_ID"),
            cognito_client_id=_env("COGNITO_CLIENT_ID"),
            cognito_client_secret_id=_env("COGNITO_CLIENT_SECRET_ID"),
            cognito_domain=_env("COGNITO_DOMAIN"),
            origin_verify_secret_id=_env("ORIGIN_VERIFY_SECRET_ID"),
            runner=_env("RUNNER", "thread"),  # type: ignore[arg-type]
            ecs_cluster=_env("ECS_CLUSTER"),
            worker_task_definition=_env("WORKER_TASK_DEFINITION"),
            worker_container_name=_env("WORKER_CONTAINER_NAME", "pmcp"),  # type: ignore[arg-type]
            worker_subnets=_env_list("WORKER_SUBNETS"),
            worker_security_groups=_env_list("WORKER_SECURITY_GROUPS"),
            rate_limit_per_minute=_env_int("RATE_LIMIT_PER_MINUTE", 60),
            max_concurrent_operations_per_user=_env_int("MAX_CONCURRENT_OPERATIONS", 2),
            operation_max_seconds=_env_int("OPERATION_MAX_SECONDS", 1800),
            platform_principal_arns=_env_list("PLATFORM_PRINCIPAL_ARNS"),
            allow_direct_credentials=mode == "local",
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings.from_env()
