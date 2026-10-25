"""Rendering checks for every implemented adapter: cfn-lint clean, names/tags/boundaries present, SQL correct."""

import json
import re

import pytest

from pmcp.adapters import ADAPTERS
from pmcp.adapters.athena_iceberg import AthenaIcebergParams, Names, SqlBuilder, validate_transform_code_or_sql
from pmcp.adapters.base import RenderContext
from pmcp.adapters.codegen import validate_transform_code
from pmcp.domain.contract import DataContract
from pmcp.domain.records import AccountConnection
from pmcp.errors import PmcpError
from pmcp.services.planning import lint_template

from .conftest import ACCOUNT, OWNER, REGION, TRANSFORM_SQL, athena_params, contract_body


def ctx_for(body, previous=None, previous_params=None):
    contract = DataContract(id="dc_x#v0001", owner=OWNER, contract_id="dc_x", contract_version=1, body=body)
    conn = AccountConnection(id=ACCOUNT, owner=OWNER, account_id=ACCOUNT, external_id="ext123", status="connected",
                             deployer_role_arn="arn:aws:iam::123456789012:role/pmcp-deployer")
    return RenderContext(owner=OWNER, flow_id="fl_0000000000abcdefghij", plan_id="pl_1", account_id=ACCOUNT,
                         region=REGION, stack_name="pmcp-abcdefghij", contract=contract, connection=conn,
                         trusted_principals=["arn:aws:iam::999999999999:role/pmcp-platform-task"],
                         boundary_policy_arn="arn:aws:iam::123456789012:policy/pmcp-boundary",
                         previous_params=previous_params, previous_contract=previous)


STREAM_BODY = dict(
    sources=[{"name": "clicks", "uri": "kinesis:clicks", "format": "records"}],
    output={"name": "clicks", "columns": [{"name": "event_id", "type": "string", "nullable": False},
                                          {"name": "user_id", "type": "string"},
                                          {"name": "event_time", "type": "timestamp"}]},
    mutation={"mode": "append", "key": ["event_id"], "ordering_column": "event_time", "duplicates": "allowed"},
    freshness={"max_lag_seconds": 300, "measured_on": "event_time"},
    consumers=[{"name": "bi", "access": "sql_analytics"}],
    quality=[{"kind": "not_null", "column": "event_id"}],
)

CASES = [
    ("s3_athena_iceberg_batch", contract_body(), athena_params()),
    ("kinesis_firehose_s3", contract_body(**STREAM_BODY), {"stream": "create", "buffer_seconds": 60,
                                                           "transform_code": "def transform(record):\n"
                                                                             "    return record\n"}),
    ("kinesis_lambda_dynamodb", contract_body(**{**STREAM_BODY, "mutation": {
        "mode": "upsert", "key": ["event_id"], "ordering_column": "event_time"}}), {"stream": "create"}),
    ("s3_glue_spark_batch", contract_body(), {
        "source_uri": "s3://src-bucket/orders/", "source_format": "json",
        "transform_code": "def transform(spark, df, start, end):\n    return df\n"}),
]


@pytest.mark.parametrize(("pattern_id", "body", "raw"), CASES, ids=[c[0] for c in CASES])
def test_template_lints_and_is_scoped(pattern_id, body, raw):
    adapter = ADAPTERS[pattern_id]
    params = adapter.parse_params(raw, body)
    rendered = adapter.render(ctx_for(body), params)
    errors, _ = lint_template(rendered.template, REGION)
    assert errors == [], errors
    res = rendered.template["Resources"]
    for lid, r in res.items():
        props = r.get("Properties", {})
        if r["Type"] == "AWS::IAM::Role":
            assert props["RoleName"].startswith("pmcp-"), lid
            assert props["PermissionsBoundary"].endswith(":policy/pmcp-boundary"), lid
            for st in props["AssumeRolePolicyDocument"]["Statement"]:
                if "AWS" in st["Principal"]:
                    assert st["Condition"]["StringEquals"]["sts:ExternalId"] == "ext123"
        for key in ("BucketName", "Name", "FunctionName", "TableName", "StateMachineName", "DeliveryStreamName",
                    "AlarmName", "TopicName", "QueueName"):
            if isinstance(props.get(key), str):
                assert props[key].startswith("pmcp-"), (lid, props[key])
    assert "ConsumerRoleArn" in rendered.template["Outputs"]

    def strings(x):
        if isinstance(x, str):
            yield x
        elif isinstance(x, dict):
            for k, v in x.items():
                yield from strings(k)
                yield from strings(v)
        elif isinstance(x, list):
            for v in x:
                yield from strings(v)

    # CloudFormation rejects control characters anywhere in a template (found on real AWS; cfn-lint misses it).
    bad = [s for s in strings(rendered.template) if any(ord(c) < 32 and c not in "\n\t" for c in s)]
    assert not bad, bad


def _states_format(expr: str, ctx: dict) -> str:
    """Tiny emulator of States.Format('...', $.a, $.b) with AWS escaping rules."""
    m = re.fullmatch(r"States\.Format\('(.*)',\s*(.*)\)", expr, re.S)
    assert m, expr
    tmpl, args = m.group(1), [a.strip() for a in m.group(2).split(",")]
    vals = [ctx[a.split(".")[-1]] for a in args]
    out, i, k = [], 0, 0
    while i < len(tmpl):
        ch = tmpl[i]
        if ch == "\\":
            out.append(tmpl[i + 1])
            i += 2
            continue
        if tmpl.startswith("{}", i):
            out.append(vals[k])
            k += 1
            i += 2
            continue
        assert ch not in "'{}", f"unescaped {ch!r} at {i}"
        out.append(ch)
        i += 1
    assert k == len(vals)
    return "".join(out)


def test_state_machine_sql_round_trips():
    body = contract_body()
    p = AthenaIcebergParams.model_validate(athena_params())
    rendered = ADAPTERS["s3_athena_iceberg_batch"].render(ctx_for(body), p)
    sm = json.loads(rendered.template["Resources"]["StateMachine"]["Properties"]["DefinitionString"])
    stmt = sm["States"]["Statement0"]["Parameters"]["QueryString.$"]
    w = {"start_expr": "S_EXPR", "end_expr": "E_EXPR"}
    sql = _states_format(stmt, w)
    expected = SqlBuilder(body, p, Names(ctx_for(body))).run_statements("S_EXPR", "E_EXPR")[0]
    assert sql == expected
    assert sql.startswith("MERGE INTO pmcp_abcdefghij.\"orders\" t USING")
    assert "WHEN MATCHED AND s.pmcp_deleted AND s.\"updated_at\" >= t.\"updated_at\" THEN DELETE" in sql
    assert "WHEN NOT MATCHED AND coalesce(s.pmcp_deleted, false) = false THEN INSERT" in sql
    assert "FROM pmcp_abcdefghij.raw_source" in sql and "$path" not in sql  # nothing quarantined yet
    sched = sm["States"]["ScheduledWindow"]["Parameters"]["start_expr.$"]
    assert _states_format(sched, {"window_end": "2026-10-25T10:00:00Z"}) == \
        "(from_iso8601_timestamp('2026-10-25T10:00:00Z') - interval '120' minute)"


def test_quarantined_files_become_constant_path_predicates():
    """Found on real Athena: only constant "$path" predicates stop the file from being opened; a NOT IN
    (SELECT ...) filter still reads it and a malformed file fails the run."""
    body = contract_body()
    bad = ["s3://src-bucket/orders/bad'1.jsonl", "s3://src-bucket/orders/bad2.jsonl"]
    p = AthenaIcebergParams.model_validate(athena_params(quarantined_paths=bad[1:]))
    rendered = ADAPTERS["s3_athena_iceberg_batch"].render(ctx_for(body), p)
    sm = json.loads(rendered.template["Resources"]["StateMachine"]["Properties"]["DefinitionString"])
    sql = _states_format(sm["States"]["Statement0"]["Parameters"]["QueryString.$"], {"start_expr": "S", "end_expr": "E"})
    assert "(SELECT * FROM pmcp_abcdefghij.raw_source WHERE \"$path\" <> 's3://src-bucket/orders/bad2.jsonl')" in sql
    assert "SELECT path FROM" not in sql
    with pytest.raises(PmcpError):  # quoting characters / foreign prefixes are refused
        ADAPTERS["s3_athena_iceberg_batch"].parse_params(athena_params(quarantined_paths=bad[:1]), body)
    with pytest.raises(PmcpError):
        ADAPTERS["s3_athena_iceberg_batch"].parse_params(athena_params(quarantined_paths=["s3://x/y.jsonl"]), body)


def test_append_without_key_replaces_window():
    body = contract_body(mutation={"mode": "append", "key": [], "ordering_column": None})
    sql = SqlBuilder(body, AthenaIcebergParams.model_validate(athena_params(
        transform_sql=TRANSFORM_SQL.replace(", coalesce(deleted, false) AS pmcp_deleted", ""))),
        Names("abc", ACCOUNT, REGION)).run_statements("S", "E")
    assert sql[0].startswith('DELETE FROM pmcp_abc."orders" WHERE "updated_at" >= S AND "updated_at" < E')
    assert sql[1].startswith('INSERT INTO pmcp_abc."orders"')


@pytest.mark.parametrize("bad", [
    "DELETE FROM {{source}}",
    "SELECT * FROM {{source}}; DROP TABLE x",
    "SELECT * FROM orders",
    "SELECT * FROM {{source}} WHERE x = {{nope}}",
])
def test_transform_sql_rejected(bad):
    assert validate_transform_code_or_sql(bad)


def test_unsupported_combinations_rejected():
    body = contract_body()
    a = ADAPTERS["s3_athena_iceberg_batch"]
    with pytest.raises(PmcpError) as e:
        a.parse_params(athena_params(transform_sql=TRANSFORM_SQL.replace(", coalesce(deleted, false) AS "
                                                                          "pmcp_deleted", "")), body)
    assert e.value.code.value == "UNSUPPORTED_CONFIGURATION"
    with pytest.raises(PmcpError):
        a.parse_params(athena_params(source_uri="s3://other-bucket/x/"), body)
    with pytest.raises(PmcpError):  # Firehose cannot upsert
        ADAPTERS["kinesis_firehose_s3"].parse_params({"stream": "create"}, body)
    with pytest.raises(PmcpError):  # DynamoDB needs a key
        ADAPTERS["kinesis_lambda_dynamodb"].parse_params(
            {"stream": "create"}, contract_body(mutation={"mode": "append", "key": [], "ordering_column": None}))


@pytest.mark.parametrize(("code", "ok"), [
    ("def transform(record):\n    return record\n", True),
    ("import json\ndef transform(record):\n    return json.loads(json.dumps(record))\n", True),
    ("import os\ndef transform(record):\n    return os.environ\n", False),
    ("def transform(record):\n    return open('/etc/passwd').read()\n", False),
    ("def transform(record):\n    return record.__class__\n", False),
    ("x = __import__('subprocess')\ndef transform(r):\n    return r\n", False),
    ("def handler(e, c):\n    return e\n", False),
])
def test_transform_code_allowlist(code, ok):
    assert (validate_transform_code(code) == []) is ok


def test_breaking_schema_change_requires_explicit_rebuild():
    old = contract_body()
    new_body = contract_body(output={"name": "orders", "columns": [
        {"name": "order_id", "type": "string", "nullable": False}, {"name": "status", "type": "string"},
        {"name": "updated_at", "type": "timestamp"}]})
    prev = DataContract(id="dc_x#v0001", owner=OWNER, contract_id="dc_x", contract_version=1, body=old)
    a = ADAPTERS["s3_athena_iceberg_batch"]
    with pytest.raises(PmcpError) as e:
        a.render(ctx_for(new_body, prev, athena_params()), a.parse_params(athena_params(), new_body))
    assert "remove column amount" in str(e.value.details)
    r = a.render(ctx_for(new_body, prev, athena_params()),
                 a.parse_params(athena_params(allow_destructive_rebuild=True), new_body))
    assert r.breaking and r.post_deploy_steps[0].step_id == "drop_output"


def test_added_column_is_in_place_and_flags_reprocess():
    old = contract_body()
    cols = [c.model_dump() for c in old.output.columns] + [{"name": "channel", "type": "string"}]
    new_body = contract_body(output={"name": "orders", "columns": cols})
    prev = DataContract(id="dc_x#v0001", owner=OWNER, contract_id="dc_x", contract_version=1, body=old)
    a = ADAPTERS["s3_athena_iceberg_batch"]
    sql = TRANSFORM_SQL.replace("SELECT order_id,", "SELECT order_id, 'web' AS channel,")
    r = a.render(ctx_for(new_body, prev, athena_params()), a.parse_params(athena_params(transform_sql=sql), new_body))
    assert [s.action for s in r.post_deploy_steps] == ["athena.ddl", "athena.add_columns"]
    assert r.reprocess_reason and "transform_sql changed" in r.reprocess_reason
    assert not r.breaking


def test_ddl_uses_hive_quoting_and_dml_uses_trino_quoting():
    """Found on real Athena: double-quoted CREATE TABLE goes to the Trino parser and rejects LOCATION."""
    body = contract_body()
    sb = SqlBuilder(body, AthenaIcebergParams.model_validate(athena_params()), Names("abc", ACCOUNT, REGION))
    for ddl in (sb.create_target(), sb.create_quarantine()):
        assert '"' not in ddl and "`pmcp_abc`." in ddl and "LOCATION 's3://" in ddl, ddl
    assert sb.run_statements("S", "E")[0].startswith('MERGE INTO pmcp_abc."orders"')
    glue = ADAPTERS["s3_glue_spark_batch"]
    r = glue.render(ctx_for(body), glue.parse_params({"source_uri": "s3://src-bucket/orders/", "source_format": "json",
                                                     "transform_code": "def transform(s, d, a, b):\n    return d\n"},
                                                    body))
    assert '"' not in r.post_deploy_steps[0].params["statements"][0]
