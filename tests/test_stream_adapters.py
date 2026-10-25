"""Verification logic of the streaming adapters with stubbed CloudWatch/S3/DynamoDB reads (mocked)."""

from datetime import UTC, datetime, timedelta

import pytest

import pmcp.adapters.common as common
import pmcp.adapters.kinesis_firehose as kf
from pmcp.adapters.base import Runtime
from pmcp.adapters.kinesis_lambda_dynamodb import KinesisLambdaDynamoAdapter
from pmcp.domain.contract import DataContract
from pmcp.domain.records import Flow

from .conftest import ACCOUNT, OWNER, REGION, contract_body
from .test_verify_diagnose import FakeSession, Stub

NOW = datetime.now(UTC)
STREAM = dict(
    sources=[{"name": "clicks", "uri": "kinesis:clicks", "format": "records"}],
    output={"name": "clicks", "columns": [{"name": "event_id", "type": "string", "nullable": False},
                                          {"name": "event_time", "type": "timestamp"}]},
    freshness={"max_lag_seconds": 300, "measured_on": "event_time"},
    consumers=[{"name": "bi", "access": "sql_analytics"}],
    quality=[],
)


def flow(pattern):
    return Flow(id="fl_0000000000abcdefghij", owner=OWNER, flow_id="fl_0000000000abcdefghij", name="x",
                account_id=ACCOUNT, region=REGION, stack_name="pmcp-abcdefghij", pattern_id=pattern,
                pattern_params={"buffer_seconds": 60}, status="deployed",
                outputs={"DataBucket": "pmcp-b", "StreamName": "pmcp-abcdefghij-in",
                         "ConsumerRoleArn": "arn:aws:iam::123456789012:role/pmcp-abcdefghij-consumer"})


def metrics(values):
    def metric_sum(rt, ns, name, dims, start, end, stat="Sum"):
        return values.get(name)

    return metric_sum


def firehose_rt(contract, error_keys=()):
    s3 = Stub(list_objects_v2=lambda Bucket, Prefix, MaxKeys=1000: (
        {"KeyCount": 3, "Contents": [{"Key": "data/dt=x/a.gz", "LastModified": NOW}]} if Prefix == "data/" else
        {"Contents": [{"Key": k, "LastModified": NOW - timedelta(minutes=30)} for k in error_keys]}))
    glue = Stub(get_table=lambda DatabaseName, Name: {"Table": {"StorageDescriptor": {"Columns": [
        {"Name": "event_id", "Type": "string"}, {"Name": "event_time", "Type": "timestamp"}]}}})
    sess = FakeSession({"s3": s3, "glue": glue})
    return Runtime(reader=sess, deployer=None, consumer=lambda arn: sess, flow=flow("kinesis_firehose_s3"),
                   contract=contract)


@pytest.fixture
def stream_contract():
    body = contract_body(**STREAM, mutation={"mode": "append", "key": ["event_id"], "ordering_column": "event_time",
                                             "duplicates": "drop_exact"})
    return DataContract(id="dc_s#v0001", owner=OWNER, contract_id="dc_s", contract_version=1, body=body)


def _athena_ok(session, sql, **kw):
    return {"query_id": "q", "state": "SUCCEEDED", "rows": [{"dup": "0", "event_id": "1"}], "error": None}


def test_firehose_reconciles_records(monkeypatch, stream_contract):
    monkeypatch.setattr(kf, "metric_sum", metrics({"IncomingRecords": 1000, "Ok": 990, "Dropped": 10,
                                                   "ProcessingFailed": 0, "DeliveryToS3.DataFreshness": 80}))
    monkeypatch.setattr(kf, "athena_query", _athena_ok)
    monkeypatch.setattr(common, "athena_query", _athena_ok)
    _, checks = kf.KinesisFirehoseAdapter().verify(firehose_rt(stream_contract), None)
    by = {c.check_id: c for c in checks}
    assert by["row_count"].status == "passed"
    assert by["freshness"].status == "passed"
    assert by["duplicates"].status == "passed"
    assert by["mutations"].status == "unverified"
    assert by["consumer_read"].status == "passed"


def test_firehose_detects_rejected_records(monkeypatch, stream_contract):
    monkeypatch.setattr(kf, "metric_sum", metrics({"IncomingRecords": 1000, "Ok": 950, "Dropped": 0,
                                                   "ProcessingFailed": 50, "DeliveryToS3.DataFreshness": 900}))
    monkeypatch.setattr(kf, "athena_query", _athena_ok)
    monkeypatch.setattr(common, "athena_query", _athena_ok)
    _, checks = kf.KinesisFirehoseAdapter().verify(
        firehose_rt(stream_contract, ["errors/processing-failed/dt=x/f1"]), None)
    by = {c.check_id: c for c in checks}
    assert by["row_count"].status == "failed"
    assert by["error_objects"].status == "failed"
    assert by["freshness"].status == "failed"


def test_firehose_no_traffic_is_unverified(monkeypatch, stream_contract):
    monkeypatch.setattr(kf, "metric_sum", metrics({}))
    monkeypatch.setattr(kf, "athena_query", _athena_ok)
    monkeypatch.setattr(common, "athena_query", _athena_ok)
    _, checks = kf.KinesisFirehoseAdapter().verify(firehose_rt(stream_contract), None)
    by = {c.check_id: c for c in checks}
    assert by["row_count"].status == "unverified" and by["freshness"].status == "unverified"


def test_dynamodb_verify(monkeypatch):
    import pmcp.adapters.kinesis_lambda_dynamodb as kd

    body = contract_body(**STREAM, mutation={"mode": "upsert", "key": ["event_id"], "ordering_column": "event_time"})
    contract = DataContract(id="dc_d#v0001", owner=OWNER, contract_id="dc_d", contract_version=1, body=body)
    item = {"event_id": {"S": "e1"}, "event_time": {"S": "2026-10-25T00:00:00Z"}}
    ddb = Stub(describe_table=lambda TableName: {"Table": {"KeySchema": [{"AttributeName": "event_id"}],
                                                           "ItemCount": 1}},
               scan=lambda TableName, Limit: {"Items": [item]},
               get_item=lambda TableName, Key: {"Item": item})
    sess = FakeSession({"dynamodb": ddb})
    rt = Runtime(reader=sess, deployer=None, consumer=lambda arn: sess, flow=flow("kinesis_lambda_dynamodb"),
                 contract=contract)
    monkeypatch.setattr(kd, "metric_sum", metrics({"IncomingRecords": 100, "Written": 90, "Deleted": 5,
                                                   "StaleSkipped": 5, "Dropped": 0, "Failed": 0,
                                                   "IteratorAge": 2000}))
    _, checks = KinesisLambdaDynamoAdapter().verify(rt, None)
    by = {c.check_id: c for c in checks}
    assert all(by[k].status == "passed" for k in ("exists", "keys", "schema", "row_count", "mutations", "freshness",
                                                  "consumer_read")), [(c.check_id, c.status) for c in checks]
    monkeypatch.setattr(kd, "metric_sum", metrics({"IncomingRecords": 100, "Written": 80, "Failed": 20}))
    _, checks = KinesisLambdaDynamoAdapter().verify(rt, None)
    by = {c.check_id: c for c in checks}
    assert by["row_count"].status == "failed" and by["mutations"].status == "failed"
