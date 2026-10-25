"""Value masking for profiling samples, plus pmcp allowlists for API records that can echo data."""

from __future__ import annotations

import re
from typing import Any

from pmcp.vendor.awslabs.sensitive_data_filter import FieldSpec, filter_record

_EMAILISH = re.compile(r"[^@\s]+@[^@\s]+")


def mask_value(value: Any) -> Any:
    """Keep shape and type, drop content: strings become a pattern, numbers keep magnitude only."""
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int):
        return 0 if value == 0 else int("9" * len(str(abs(value)))) * (1 if value > 0 else -1)
    if isinstance(value, float):
        if value == 0:
            return 0.0
        return float("9" * len(str(int(abs(value))))) * (1 if value > 0 else -1)
    if isinstance(value, str):
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}([T ][\d:.]+)?(Z|[+-]\d{2}:?\d{2})?", value):
            return value  # timestamps are needed to judge freshness and carry no identity by themselves
        if _EMAILISH.fullmatch(value):
            return "x@x"
        return re.sub(r"[A-Za-z]", "a", re.sub(r"\d", "9", value))[:32]
    if isinstance(value, list):
        return [mask_value(v) for v in value[:5]]
    if isinstance(value, dict):
        return {k: mask_value(v) for k, v in value.items()}
    return "<masked>"


STEP_FUNCTIONS_EXECUTION_FIELDS: FieldSpec = {
    "executionArn": True,
    "name": True,
    "status": True,
    "startDate": True,
    "stopDate": True,
    "error": True,
    "redriveCount": True,
}

ATHENA_QUERY_EXECUTION_FIELDS: FieldSpec = {
    "QueryExecutionId": True,
    "StatementType": True,
    "WorkGroup": True,
    "Status": {"State": True, "SubmissionDateTime": True, "CompletionDateTime": True, "AthenaError": True},
    "Statistics": {"DataScannedInBytes": True, "TotalExecutionTimeInMillis": True},
}


def allow(record: dict[str, Any], spec: FieldSpec) -> dict[str, Any]:
    return filter_record(record, spec)[0]
