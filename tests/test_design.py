"""Scenario 5 (requirements change -> selection rationale changes) and 6 (unsupported never reported deployable)."""

from pmcp.services import contracts, design

from .conftest import ACCOUNT, OWNER, REGION, athena_params, contract_body


def _validate(app, body, candidates, **kw):
    c, _ = contracts.define(app, OWNER, body, None, None)
    return design.validate_architecture(app, OWNER, c.contract_id, None, candidates, kw.get("account"),
                                        kw.get("region"))


ALL = [{"pattern_id": p} for p in ("s3_athena_iceberg_batch", "kinesis_firehose_s3", "kinesis_lambda_dynamodb",
                                   "s3_glue_spark_batch", "redshift_sql_elt", "kinesis_flink_stateful")]


def test_hourly_batch_upserts_prefer_athena(app):
    r = _validate(app, contract_body(), ALL)
    by = {c["pattern_id"]: c for c in r["candidates"]}
    assert r["best_fit"] == "s3_athena_iceberg_batch"
    assert by["kinesis_firehose_s3"]["fitness"]["level"] == "unsuitable"  # S3 source, upserts
    assert by["s3_glue_spark_batch"]["fitness"]["level"] in ("acceptable", "strong")


def test_freshness_change_flips_choice(app):
    """Same data, freshness 1h -> 30s and source becomes a stream: batch SQL becomes unsuitable."""
    body = contract_body(
        sources=[{"name": "orders_events", "uri": "kinesis:orders", "format": "records"}],
        freshness={"max_lag_seconds": 30, "measured_on": "updated_at"},
        consumers=[{"name": "app", "access": "key_lookup", "query_latency_ms": 20}],
    )
    r = _validate(app, body, ALL)
    by = {c["pattern_id"]: c for c in r["candidates"]}
    assert by["s3_athena_iceberg_batch"]["fitness"]["level"] == "unsuitable"
    assert any("freshness" in b for b in by["s3_athena_iceberg_batch"]["fitness"]["blockers"])
    assert r["best_fit"] == "kinesis_lambda_dynamodb"
    assert by["kinesis_lambda_dynamodb"]["fitness"]["level"] == "strong"


def test_stateful_streaming_best_fit_not_deployable(app):
    body = contract_body(
        sources=[{"name": "clicks", "uri": "kinesis:clicks", "format": "records"}],
        mutation={"mode": "append", "key": [], "ordering_column": None, "duplicates": "allowed"},
        freshness={"max_lag_seconds": 10, "measured_on": "updated_at"},
        transform={"stateful_streaming": True, "expressible_in_sql": True},
        consumers=[{"name": "ops", "access": "stream"}],
    )
    r = _validate(app, body, ALL + [{"pattern_id": "kinesis_firehose_s3", "pattern_params": {}}],
                  account=ACCOUNT, region=REGION)
    by = {c["pattern_id"]: c for c in r["candidates"]}
    assert r["best_fit"] == "kinesis_flink_stateful"
    flink = by["kinesis_flink_stateful"]
    assert flink["adapter_status"] == "catalog_only"
    assert flink["deployable_now"] is False
    assert any("no adapter" in b for b in flink["deploy_blockers"])
    assert r["best_deployable_now"] != "kinesis_flink_stateful"


def test_catalog_only_patterns_are_never_deployable(app):
    r = _validate(app, contract_body(), [{"pattern_id": p} for p in ("redshift_sql_elt", "dms_cdc_s3",
                                                                       "opensearch_ingestion")],
                  account=ACCOUNT, region=REGION)
    assert all(c["deployable_now"] is False for c in r["candidates"])
    assert r["best_deployable_now"] is None


def test_valid_params_make_athena_deployable(app):
    r = _validate(app, contract_body(), [{"pattern_id": "s3_athena_iceberg_batch",
                                          "pattern_params": athena_params()}], account=ACCOUNT, region=REGION)
    c = r["candidates"][0]
    assert c["params_valid"] is True, c["deploy_blockers"]
    assert c["deployable_now"] is True
    assert c["cost"]["monthly_usd"] > 0


def test_invalid_params_block_deployability(app):
    r = _validate(app, contract_body(), [{"pattern_id": "s3_athena_iceberg_batch",
                                          "pattern_params": athena_params(schedule_expression="rate(2 hours)",
                                                                          window_minutes=60)}],
                  account=ACCOUNT, region=REGION)
    c = r["candidates"][0]
    assert c["params_valid"] is False
    assert not c["deployable_now"]
    assert any("freshness" in b or "window_minutes" in b for b in c["deploy_blockers"])


def test_better_unproposed_pattern_is_reported(app):
    r = _validate(app, contract_body(), [{"pattern_id": "kinesis_firehose_s3"}])
    assert any(x["pattern_id"] == "s3_athena_iceberg_batch" for x in r["better_fit_not_proposed"])


def test_contract_versions(app):
    c1, created = contracts.define(app, OWNER, contract_body(), None, None)
    assert created and c1.contract_version == 1
    same, created = contracts.define(app, OWNER, contract_body(), c1.contract_id, 1)
    assert not created and same.contract_version == 1
    c2, created = contracts.define(app, OWNER, contract_body(title="orders v2"), c1.contract_id, 1)
    assert created and c2.contract_version == 2
    assert contracts.load_contract(app, OWNER, c1.contract_id).contract_version == 2


def test_list_capabilities_reports_status(app):
    caps = design.list_capabilities(None, "s3_athena_iceberg_batch")
    p = caps["patterns"][0]
    assert p["adapter_status"] == "implemented"
    assert "params_schema" in p
    assert p["integration"]["status"] in ("mock_tested", "aws_verified", "not_verified")
    flink = design.list_capabilities(None, "kinesis_flink_stateful")["patterns"][0]
    assert flink["adapter_status"] == "catalog_only" and flink["implementation_work"]
