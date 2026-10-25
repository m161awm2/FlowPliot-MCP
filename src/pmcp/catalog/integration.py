"""Integration verification status per pattern.

Updated by hand from evidence files under docs/evidence/ after a real-AWS run. `mock_tested` means
the adapter's rendering and worker logic pass the moto/unit suite; it says nothing about real AWS.
"""

from __future__ import annotations

from pydantic import Field

from pmcp.catalog.patterns import IntegrationStatus
from pmcp.domain.base import Model


class IntegrationRecord(Model):
    status: IntegrationStatus
    evidence: list[str] = Field(default_factory=list)
    verified_at: str | None = None
    region: str | None = None
    notes: str | None = None


INTEGRATION: dict[str, IntegrationRecord] = {
    "s3_athena_iceberg_batch": IntegrationRecord(
        status="mock_tested", evidence=["tests/test_adapters_render.py", "tests/test_verify_diagnose.py"],
        notes="template lints; SQL/States.Format round-trip; verify/diagnose/recovery logic on stubs"),
    "kinesis_firehose_s3": IntegrationRecord(
        status="mock_tested", evidence=["tests/test_adapters_render.py", "tests/test_stream_adapters.py"],
        notes="template lints; verify logic on stubs; diagnose/replay untested"),
    "kinesis_lambda_dynamodb": IntegrationRecord(
        status="mock_tested", evidence=["tests/test_adapters_render.py", "tests/test_stream_adapters.py"],
        notes="template lints; verify logic on stubs; replay untested"),
    "s3_glue_spark_batch": IntegrationRecord(
        status="mock_tested", evidence=["tests/test_adapters_render.py"],
        notes="template renders and lints only; job script never run"),
}


def integration_for(pattern_id: str) -> IntegrationRecord:
    return INTEGRATION.get(pattern_id, IntegrationRecord(status="not_verified"))
