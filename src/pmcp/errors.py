"""Error taxonomy shared by every tool.

Tools raise PmcpError; the MCP layer turns it into an is_error result whose text is a JSON
object, so the agent can branch on `code` instead of parsing prose.
"""

from __future__ import annotations

import json
from enum import StrEnum
from typing import Any


class ErrorCode(StrEnum):
    INVALID_INPUT = "INVALID_INPUT"
    NOT_FOUND = "NOT_FOUND"
    FORBIDDEN = "FORBIDDEN"
    NOT_CONNECTED = "NOT_CONNECTED"
    WRITE_ACCESS_NOT_ENABLED = "WRITE_ACCESS_NOT_ENABLED"
    UNSUPPORTED_CONFIGURATION = "UNSUPPORTED_CONFIGURATION"
    PRECONDITION_FAILED = "PRECONDITION_FAILED"
    STALE_STATE = "STALE_STATE"
    APPROVAL_REQUIRED = "APPROVAL_REQUIRED"
    DIGEST_MISMATCH = "DIGEST_MISMATCH"
    PLAN_EXPIRED = "PLAN_EXPIRED"
    PLAN_NOT_EXECUTABLE = "PLAN_NOT_EXECUTABLE"
    CONFLICT = "CONFLICT"
    QUOTA_EXCEEDED = "QUOTA_EXCEEDED"
    COST_CAP_EXCEEDED = "COST_CAP_EXCEEDED"
    RATE_LIMITED = "RATE_LIMITED"
    AWS_ACCESS_DENIED = "AWS_ACCESS_DENIED"
    AWS_ERROR = "AWS_ERROR"
    TIMEOUT = "TIMEOUT"
    INTERNAL = "INTERNAL"


RETRYABLE = {ErrorCode.CONFLICT, ErrorCode.RATE_LIMITED, ErrorCode.TIMEOUT, ErrorCode.AWS_ERROR}


class PmcpError(Exception):
    def __init__(
        self,
        code: ErrorCode,
        message: str,
        *,
        details: dict[str, Any] | None = None,
        next_actions: list[str] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}
        self.next_actions = next_actions or []

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "code": self.code.value,
            "message": self.message,
            "retryable": self.code in RETRYABLE,
        }
        if self.details:
            out["details"] = self.details
        if self.next_actions:
            out["next_actions"] = self.next_actions
        return out

    def to_json(self) -> str:
        return json.dumps({"error": self.to_dict()}, ensure_ascii=False, default=str)


def from_boto(exc: Exception, *, action: str) -> PmcpError:
    """Map a botocore ClientError to a PmcpError without leaking request payloads."""
    code = getattr(exc, "response", {}).get("Error", {}).get("Code", type(exc).__name__)
    message = getattr(exc, "response", {}).get("Error", {}).get("Message", str(exc))
    if code in {"AccessDenied", "AccessDeniedException", "UnauthorizedOperation", "AuthorizationError"}:
        return PmcpError(
            ErrorCode.AWS_ACCESS_DENIED,
            f"{action}: access denied ({message})",
            details={"aws_error_code": code},
            next_actions=["Check that the pmcp tenant roles are deployed and include this permission."],
        )
    if code in {"ValidationError", "ValidationException", "InvalidParameterValue", "InvalidParameterException",
                "InvalidRequestException", "MalformedPolicyDocument"}:
        # The request itself is wrong; repeating it unchanged cannot succeed.
        return PmcpError(ErrorCode.INVALID_INPUT, f"{action}: {code}: {message}", details={"aws_error_code": code})
    if code in {"ThrottlingException", "Throttling", "TooManyRequestsException", "RequestLimitExceeded"}:
        return PmcpError(ErrorCode.AWS_ERROR, f"{action}: throttled by AWS", details={"aws_error_code": code})
    return PmcpError(ErrorCode.AWS_ERROR, f"{action}: {code}: {message}", details={"aws_error_code": code})
