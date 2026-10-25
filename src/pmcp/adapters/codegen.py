"""Generated Lambda code around an agent-written `transform(record) -> dict | None` function.

The user function is checked with an AST allowlist before it is ever deployed: it may only import
pure standard-library modules, must define `transform`, and must not touch the filesystem, network,
processes or AWS. It runs only in the tenant's own Lambda, never on the pmcp platform.
"""

from __future__ import annotations

import ast
import io
import zipfile

ALLOWED_IMPORTS = {
    "json", "re", "datetime", "math", "decimal", "hashlib", "uuid", "base64", "time", "zoneinfo",
    "itertools", "functools", "collections", "string", "unicodedata", "statistics", "typing",
}
FORBIDDEN_NAMES = {"open", "exec", "eval", "compile", "__import__", "globals", "locals", "vars", "input",
                   "breakpoint", "getattr", "setattr", "delattr"}


def validate_transform_code(code: str, *, extra_imports: frozenset[str] = frozenset(), arity: int = 1) -> list[str]:
    try:
        tree = ast.parse(code)
    except SyntaxError as exc:
        return [f"syntax error: {exc.msg} (line {exc.lineno})"]
    allowed = ALLOWED_IMPORTS | extra_imports
    problems = []
    has_transform = False
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                if a.name.split(".")[0] not in allowed:
                    problems.append(f"import of '{a.name}' is not allowed")
        elif isinstance(node, ast.ImportFrom):
            if (node.module or "").split(".")[0] not in allowed:
                problems.append(f"import from '{node.module}' is not allowed")
        elif isinstance(node, ast.Name) and node.id in FORBIDDEN_NAMES:
            problems.append(f"use of '{node.id}' is not allowed")
        elif isinstance(node, ast.Attribute) and node.attr.startswith("__"):
            problems.append(f"dunder attribute access '{node.attr}' is not allowed")
        elif isinstance(node, ast.FunctionDef) and node.name == "transform":
            has_transform = len(node.args.args) == arity
    if not has_transform:
        problems.append(f"code must define `def transform(...)` taking {arity} argument(s)")
    for node in tree.body:
        if not isinstance(node, ast.FunctionDef | ast.Import | ast.ImportFrom | ast.Assign | ast.Expr):
            problems.append(f"top-level {type(node).__name__} is not allowed (only defs, imports, constants)")
    return problems


IDENTITY = "def transform(record):\n    return record\n"

FIREHOSE_HANDLER = '''
import base64 as _b64
import json as _json
import time as _time

{user_code}


def _emf(counts):
    print(_json.dumps({{
        "_aws": {{"Timestamp": int(_time.time() * 1000), "CloudWatchMetrics": [{{
            "Namespace": "pmcp", "Dimensions": [["Flow"]],
            "Metrics": [{{"Name": k, "Unit": "Count"}} for k in counts]}}]}},
        "Flow": "{flow_id}", **counts}}))


def handler(event, context):
    out, counts = [], {{"Ok": 0, "Dropped": 0, "ProcessingFailed": 0}}
    for r in event["records"]:
        try:
            res = transform(_json.loads(_b64.b64decode(r["data"])))
        except Exception as exc:  # the record goes to the errors/ prefix with its raw data
            print(_json.dumps({{"level": "ERROR", "recordId": r["recordId"], "error": type(exc).__name__,
                               "message": str(exc)[:300]}}))
            out.append({{"recordId": r["recordId"], "result": "ProcessingFailed", "data": r["data"]}})
            counts["ProcessingFailed"] += 1
            continue
        if res is None:
            out.append({{"recordId": r["recordId"], "result": "Dropped", "data": r["data"]}})
            counts["Dropped"] += 1
        else:
            data = _b64.b64encode((_json.dumps(res, default=str) + "\\n").encode()).decode()
            out.append({{"recordId": r["recordId"], "result": "Ok", "data": data}})
            counts["Ok"] += 1
    _emf(counts)
    return {{"records": out}}
'''

DYNAMODB_HANDLER = '''
import base64 as _b64
import json as _json
import os as _os
import time as _time
from decimal import Decimal as _D

import boto3 as _boto3
from botocore.exceptions import ClientError as _CE

{user_code}

_TABLE = _boto3.resource("dynamodb").Table(_os.environ["TABLE_NAME"])
_KEYS = {keys!r}
_ORDER = {ordering!r}


def _emf(counts):
    print(_json.dumps({{
        "_aws": {{"Timestamp": int(_time.time() * 1000), "CloudWatchMetrics": [{{
            "Namespace": "pmcp", "Dimensions": [["Flow"]],
            "Metrics": [{{"Name": k, "Unit": "Count"}} for k in counts]}}]}},
        "Flow": "{flow_id}", **counts}}))


def _apply(item):
    item = _json.loads(_json.dumps(item, default=str), parse_float=_D)
    deleted = bool(item.pop("pmcp_deleted", False))
    key = {{k: item[k] for k in _KEYS}}
    cond = "attribute_not_exists(#o) OR #o <= :o"
    names, values = {{"#o": _ORDER}}, {{":o": item[_ORDER]}}
    try:
        if deleted:
            _TABLE.delete_item(Key=key, ConditionExpression=cond, ExpressionAttributeNames=names,
                               ExpressionAttributeValues=values)
            return "Deleted"
        _TABLE.put_item(Item=item, ConditionExpression=cond, ExpressionAttributeNames=names,
                        ExpressionAttributeValues=values)
        return "Written"
    except _CE as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return "StaleSkipped"  # a newer version is already stored; replays are harmless
        raise


def handler(event, context):
    counts = {{"Written": 0, "Deleted": 0, "StaleSkipped": 0, "Dropped": 0, "Failed": 0}}
    failures = []
    for r in event["Records"]:
        seq = r["kinesis"]["sequenceNumber"]
        try:
            res = transform(_json.loads(_b64.b64decode(r["kinesis"]["data"])))
            if res is None:
                counts["Dropped"] += 1
                continue
            counts[_apply(res)] += 1
        except Exception as exc:
            print(_json.dumps({{"level": "ERROR", "sequenceNumber": seq, "error": type(exc).__name__,
                               "message": str(exc)[:300]}}))
            counts["Failed"] += 1
            failures.append({{"itemIdentifier": seq}})
            break  # report from the first failure; Lambda retries the rest in order
    _emf(counts)
    return {{"batchItemFailures": failures}}
'''


def render_firehose_handler(user_code: str | None, flow_id: str) -> str:
    return FIREHOSE_HANDLER.format(user_code=(user_code or IDENTITY).strip(), flow_id=flow_id).lstrip()


def render_dynamodb_handler(user_code: str | None, flow_id: str, keys: list[str], ordering: str) -> str:
    return DYNAMODB_HANDLER.format(
        user_code=(user_code or IDENTITY).strip(), flow_id=flow_id, keys=keys, ordering=ordering
    ).lstrip()


def zip_single(filename: str, source: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        info = zipfile.ZipInfo(filename, date_time=(2020, 1, 1, 0, 0, 0))  # fixed time => stable hash
        info.external_attr = 0o644 << 16
        zf.writestr(info, source)
    return buf.getvalue()
