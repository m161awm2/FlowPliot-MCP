"""Small helpers around boto3."""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any, TypeVar

import anyio.to_thread
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from pmcp.errors import PmcpError, from_boto

R = TypeVar("R")

BOTO_CONFIG = Config(
    user_agent_extra="pmcp/0.1",
    retries={"max_attempts": 5, "mode": "adaptive"},
    connect_timeout=5,
    read_timeout=20,
)


async def run_sync(fn: Callable[..., R], *args: Any, **kwargs: Any) -> R:
    """Run blocking boto3 work off the event loop (pattern from awslabs redshift-mcp-server)."""
    return await anyio.to_thread.run_sync(functools.partial(fn, *args, **kwargs))


def aws_call(action: str):
    """Decorator: translate botocore errors raised inside `fn` into PmcpError."""

    def deco(fn: Callable[..., R]) -> Callable[..., R]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> R:
            try:
                return fn(*args, **kwargs)
            except PmcpError:
                raise
            except ClientError as exc:
                raise from_boto(exc, action=action) from exc
            except BotoCoreError as exc:
                raise from_boto(exc, action=action) from exc

        return wrapper

    return deco


def error_code(exc: Exception) -> str:
    return getattr(exc, "response", {}).get("Error", {}).get("Code", "")
