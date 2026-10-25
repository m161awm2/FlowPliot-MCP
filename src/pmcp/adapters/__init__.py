from pmcp.adapters.athena_iceberg import AthenaIcebergAdapter
from pmcp.adapters.base import Adapter
from pmcp.adapters.glue_spark import GlueSparkAdapter
from pmcp.adapters.kinesis_firehose import KinesisFirehoseAdapter
from pmcp.adapters.kinesis_lambda_dynamodb import KinesisLambdaDynamoAdapter
from pmcp.errors import ErrorCode, PmcpError

ADAPTERS: dict[str, Adapter] = {
    a.pattern_id: a
    for a in (AthenaIcebergAdapter(), KinesisFirehoseAdapter(), KinesisLambdaDynamoAdapter(), GlueSparkAdapter())
}


def get_adapter(pattern_id: str) -> Adapter:
    from pmcp.catalog.patterns import PATTERNS

    if pattern_id in ADAPTERS:
        return ADAPTERS[pattern_id]
    if pattern_id in PATTERNS:
        pat = PATTERNS[pattern_id]
        raise PmcpError(
            ErrorCode.UNSUPPORTED_CONFIGURATION,
            f"pattern '{pattern_id}' is in the catalog but has no deployable adapter",
            details={"adapter_status": pat.adapter_status, "implementation_work": pat.implementation_work},
        )
    raise PmcpError(ErrorCode.INVALID_INPUT, f"unknown pattern_id '{pattern_id}'",
                    next_actions=["Call list_capabilities to see pattern ids."])
