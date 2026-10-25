"""Investigation tools: producer code, AWS environment, source data, consumer side. All read-only."""

from __future__ import annotations

import csv
import gzip
import io
import json
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
import zipfile
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from botocore.exceptions import ClientError

from pmcp.aws.client import BOTO_CONFIG
from pmcp.aws.redaction import mask_value
from pmcp.errors import ErrorCode, PmcpError, from_boto

if TYPE_CHECKING:
    from pmcp.app import App

MAX_REPO_BYTES = 50 * 1024**2
MAX_FILE_BYTES = 1024**2
SKIP_DIRS = {".git", "node_modules", "vendor", ".venv", "venv", "dist", "build", "__pycache__", ".terraform",
             "target", ".next", ".idea"}
LANG = {".py": "python", ".js": "javascript", ".ts": "typescript", ".java": "java", ".kt": "kotlin", ".go": "go",
        ".rb": "ruby", ".cs": "csharp", ".scala": "scala", ".sql": "sql", ".tf": "terraform", ".rs": "rust",
        ".php": "php"}

PRODUCER_PATTERNS = [
    ("kinesis", r"\.put_records?\(|PutRecords?Command|putRecords?\(|KinesisProducer|kinesis\.PutRecord"),
    ("firehose", r"\.put_record_batch\(|PutRecordBatchCommand|firehose\.put_record\(|putRecordBatch\("),
    ("s3", r"\.put_object\(|PutObjectCommand|putObject\(|upload_file(obj)?\(|s3\.Upload|\.upload\("),
    ("kafka", r"KafkaProducer|producer\.send\(|kafka\.Writer|ProducerRecord<|confluent_kafka"),
    ("sqs", r"\.send_message(_batch)?\(|SendMessageCommand|sendMessage\("),
    ("sns", r"\.publish\(|PublishCommand"),
    ("eventbridge", r"\.put_events\(|PutEventsCommand"),
    ("dynamodb", r"\.put_item\(|PutItemCommand|batch_write_item|\.update_item\("),
    ("database", r"INSERT\s+INTO|executemany\(|\.save\(\)|session\.add\("),
]
NAME_HINTS = r"""(StreamName|DeliveryStreamName|Bucket|TopicArn|QueueUrl|TableName|topic|stream_name|bucket)\s*[=:]\s*["']([\w.\-/:]+)["']"""
SCHEMA_FILES = (".avsc", ".proto", ".schema.json", ".graphql")
SAMPLE_EXT = (".jsonl", ".ndjson", ".csv")


# --- inspect_repository ------------------------------------------------------------------------------


def _safe_extract(archive: Path, dest: Path) -> None:
    def ok(name: str) -> bool:
        target = (dest / name).resolve()
        return str(target).startswith(str(dest.resolve()) + os.sep)

    if zipfile.is_zipfile(archive):
        with zipfile.ZipFile(archive) as zf:
            total = 0
            for info in zf.infolist():
                total += info.file_size
                if total > MAX_REPO_BYTES or not ok(info.filename) or (info.external_attr >> 16) & 0o170000 == 0o120000:
                    continue
                zf.extract(info, dest)
    else:
        with tarfile.open(archive) as tf:
            members = [m for m in tf.getmembers() if (m.isfile() or m.isdir()) and ok(m.name)
                       and m.size <= MAX_FILE_BYTES * 5]
            tf.extractall(dest, members=members, filter="data")


def fetch_repository(app: App, owner: str, source: str, ref: str | None, account_id: str | None,
                     region: str | None, token_secret_arn: str | None) -> Path:
    workdir = Path(tempfile.mkdtemp(prefix="pmcp-repo-"))
    if source.startswith("https://"):
        url = source
        if token_secret_arn:
            if not account_id or f":{account_id}:" not in token_secret_arn or ":secret:pmcp/" not in token_secret_arn:
                raise PmcpError(ErrorCode.FORBIDDEN, "token secret must be a pmcp/* secret in the connected account")
            sess = app.sessions.session(owner, account_id, region or token_secret_arn.split(":")[3], "reader")
            token = sess.client("secretsmanager", config=BOTO_CONFIG).get_secret_value(
                SecretId=token_secret_arn)["SecretString"].strip()
            url = re.sub(r"^https://", f"https://x-access-token:{token}@", source)
        cmd = ["git", "clone", "--depth", "1", "--single-branch", "--no-tags", "--filter=blob:limit=1m"]
        if ref:
            cmd += ["--branch", ref]
        env = {"GIT_TERMINAL_PROMPT": "0", "PATH": os.environ.get("PATH", ""), "GIT_CONFIG_NOSYSTEM": "1",
               "HOME": str(workdir)}
        try:
            subprocess.run([*cmd, url, str(workdir / "repo")], check=True, capture_output=True, timeout=25, env=env)
        except subprocess.TimeoutExpired as exc:
            shutil.rmtree(workdir, ignore_errors=True)
            raise PmcpError(ErrorCode.TIMEOUT, "git clone took longer than 25s") from exc
        except subprocess.CalledProcessError as exc:
            shutil.rmtree(workdir, ignore_errors=True)
            msg = exc.stderr.decode(errors="replace").replace(url, source)[-400:]
            raise PmcpError(ErrorCode.INVALID_INPUT, f"git clone failed: {msg}") from exc
        return workdir / "repo"
    if source.startswith("s3://"):
        if not account_id or not region:
            raise PmcpError(ErrorCode.INVALID_INPUT, "account_id and region are required for s3:// archives")
        bucket, _, key = source[5:].partition("/")
        s3 = app.sessions.session(owner, account_id, region, "reader").client("s3", config=BOTO_CONFIG)
        head = s3.head_object(Bucket=bucket, Key=key)
        if head["ContentLength"] > MAX_REPO_BYTES:
            raise PmcpError(ErrorCode.INVALID_INPUT, "archive larger than 50 MB")
        archive = workdir / "archive"
        s3.download_file(bucket, key, str(archive))
        dest = workdir / "repo"
        dest.mkdir()
        _safe_extract(archive, dest)
        return dest
    if app.settings.mode == "local" and Path(source).is_dir():
        return Path(source)
    raise PmcpError(ErrorCode.INVALID_INPUT, "source must be an https:// git URL or an s3:// archive"
                    + (" (or a local directory in local mode)" if app.settings.mode == "local" else ""))


def scan_repository(root: Path, max_findings: int = 200) -> dict[str, Any]:
    langs: Counter[str] = Counter()
    findings: list[dict[str, Any]] = []
    names: dict[str, set[str]] = {}
    schema_files, samples, iac = [], [], []
    event_fields: Counter[str] = Counter()
    files = 0
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for fn in filenames:
            path = Path(dirpath) / fn
            rel = str(path.relative_to(root))
            try:
                if path.is_symlink() or path.stat().st_size > MAX_FILE_BYTES:
                    continue
            except OSError:
                continue
            files += 1
            ext = path.suffix.lower()
            if ext in LANG:
                langs[LANG[ext]] += 1
            if fn.endswith(SCHEMA_FILES):
                schema_files.append(rel)
            if fn.endswith(SAMPLE_EXT):
                samples.append(rel)
            if fn in ("template.yaml", "template.yml", "serverless.yml", "cdk.json") or ext == ".tf":
                iac.append(rel)
            if ext not in LANG and ext not in (".yaml", ".yml", ".json"):
                continue
            try:
                text = path.read_text(errors="ignore")
            except OSError:
                continue
            for lineno, line in enumerate(text.splitlines(), 1):
                for kind, pat in PRODUCER_PATTERNS:
                    if re.search(pat, line, re.IGNORECASE if kind == "database" else 0):
                        if len(findings) < max_findings:
                            findings.append({"kind": "producer_call", "target": kind, "file": rel, "line": lineno,
                                             "code": line.strip()[:160]})
                for m in re.finditer(NAME_HINTS, line):
                    names.setdefault(m.group(1), set()).add(m.group(2))
                for m in re.finditer(r"AWS::(Kinesis|KinesisFirehose|S3|DynamoDB|Glue|Redshift|MSK|SQS|SNS|"
                                     r"Events|Lambda|StepFunctions|RDS)::\w+", line):
                    if len(findings) < max_findings:
                        findings.append({"kind": "iac_resource", "type": m.group(0), "file": rel, "line": lineno})
            # Keys of dict literals that are serialized close to a producer call: likely event fields.
            for m in re.finditer(r"json\.dumps\(\s*\{([^{}]{0,800})\}", text):
                for k in re.findall(r"""["'](\w+)["']\s*:""", m.group(1)):
                    event_fields[k] += 1
            for m in re.finditer(r"JSON\.stringify\(\s*\{([^{}]{0,800})\}", text):
                for k in re.findall(r"(\w+)\s*:", m.group(1)):
                    event_fields[k] += 1
    return {
        "files_scanned": files,
        "languages": dict(langs.most_common()),
        "producer_calls": [f for f in findings if f["kind"] == "producer_call"],
        "iac_resources": [f for f in findings if f["kind"] == "iac_resource"],
        "resource_name_hints": {k: sorted(v)[:20] for k, v in names.items()},
        "candidate_event_fields": [k for k, _ in event_fields.most_common(40)],
        "schema_files": schema_files[:50],
        "sample_data_files": samples[:50],
        "iac_files": iac[:50],
        "notes": ["Findings are pattern matches with file:line evidence, not a semantic analysis; confirm with the "
                  "code before relying on them."],
    }


def inspect_repository(app: App, owner: str, source: str, ref: str | None, account_id: str | None,
                       region: str | None, token_secret_arn: str | None) -> dict[str, Any]:
    root = fetch_repository(app, owner, source, ref, account_id, region, token_secret_arn)
    try:
        size = sum(p.stat().st_size for p in root.rglob("*") if p.is_file() and not p.is_symlink())
        if size > MAX_REPO_BYTES * 2:
            raise PmcpError(ErrorCode.INVALID_INPUT, "repository larger than 100 MB after checkout")
        result = scan_repository(root)
        result["source"] = source
        result["ref"] = ref
        return result
    finally:
        if root.parent.name.startswith("pmcp-repo-"):
            shutil.rmtree(root.parent, ignore_errors=True)


# --- inspect_aws_environment -------------------------------------------------------------------------

CATEGORIES = ("storage", "catalog", "streaming", "databases", "warehouses", "compute", "orchestration",
              "governance", "pmcp")


def inspect_environment(app: App, owner: str, account_id: str, region: str, categories: list[str] | None,
                        limit: int = 50) -> dict[str, Any]:
    sess = app.sessions.session(owner, account_id, region, "reader")

    def c(svc: str):
        return sess.client(svc, config=BOTO_CONFIG)

    cats = set(categories or CATEGORIES)
    out: dict[str, Any] = {"account_id": account_id, "region": region, "errors": {}}

    def attempt(name: str, fn):
        try:
            out[name] = fn()
        except ClientError as exc:
            out["errors"][name] = from_boto(exc, action=name).message[:200]
        except Exception as exc:
            out["errors"][name] = f"{type(exc).__name__}: {str(exc)[:160]}"

    if "storage" in cats:
        def buckets():
            resp = c("s3").list_buckets(MaxBuckets=limit, BucketRegion=region)
            return [{"name": b["Name"], "created": str(b["CreationDate"])} for b in resp.get("Buckets", [])]
        attempt("s3_buckets_in_region", buckets)
    if "catalog" in cats:
        def glue():
            g = c("glue")
            dbs = g.get_databases(MaxResults=limit)["DatabaseList"]
            res = []
            for db in dbs[:20]:
                tables = g.get_tables(DatabaseName=db["Name"], MaxResults=50)["TableList"]
                res.append({"database": db["Name"], "tables": [
                    {"name": t["Name"], "type": t.get("TableType"),
                     "format": t.get("Parameters", {}).get("table_type") or t.get("Parameters", {}).get("classification"),
                     "location": t.get("StorageDescriptor", {}).get("Location")} for t in tables]})
            return res
        attempt("glue_catalog", glue)
        attempt("athena_workgroups", lambda: [w["Name"] for w in c("athena").list_work_groups(MaxResults=50)["WorkGroups"]])
    if "streaming" in cats:
        attempt("kinesis_streams", lambda: [
            {"name": s["StreamName"], "mode": s.get("StreamModeDetails", {}).get("StreamMode"),
             "status": s["StreamStatus"]} for s in c("kinesis").list_streams(Limit=limit).get("StreamSummaries", [])])
        attempt("firehose_streams", lambda: c("firehose").list_delivery_streams(Limit=limit)["DeliveryStreamNames"])
        attempt("msk_clusters", lambda: [x["ClusterName"] for x in c("kafka").list_clusters_v2(MaxResults=limit)
                                         .get("ClusterInfoList", [])])
    if "databases" in cats:
        attempt("dynamodb_tables", lambda: c("dynamodb").list_tables(Limit=limit)["TableNames"])
        attempt("rds", lambda: [{"id": d["DBInstanceIdentifier"], "engine": d["Engine"],
                                 "public": d.get("PubliclyAccessible")}
                                for d in c("rds").describe_db_instances(MaxRecords=min(limit, 100))["DBInstances"]])
        attempt("opensearch_domains", lambda: [d["DomainName"] for d in c("opensearch").list_domain_names()
                                               ["DomainNames"]])
    if "warehouses" in cats:
        attempt("redshift_clusters", lambda: [x["ClusterIdentifier"] for x in c("redshift").describe_clusters(
            MaxRecords=min(limit, 100))["Clusters"]])
        attempt("redshift_serverless", lambda: [w["workgroupName"] for w in c("redshift-serverless").list_workgroups(
            maxResults=min(limit, 100))["workgroups"]])
    if "compute" in cats:
        attempt("lambda_functions", lambda: [f["FunctionName"] for f in c("lambda").list_functions(
            MaxItems=limit)["Functions"]])
        attempt("glue_jobs", lambda: c("glue").list_jobs(MaxResults=limit)["JobNames"])
    if "orchestration" in cats:
        attempt("state_machines", lambda: [s["name"] for s in c("stepfunctions").list_state_machines(
            maxResults=limit)["stateMachines"]])
        attempt("schedules", lambda: [s["Name"] for s in c("scheduler").list_schedules(MaxResults=min(limit, 100))
                                      ["Schedules"]])
    if "governance" in cats:
        def lf():
            s = c("lakeformation").get_data_lake_settings()["DataLakeSettings"]
            iam_allowed = any(p.get("Principal", {}).get("DataLakePrincipalIdentifier") == "IAM_ALLOWED_PRINCIPALS"
                              for p in s.get("CreateTableDefaultPermissions", []))
            return {"admins": [a["DataLakePrincipalIdentifier"] for a in s.get("DataLakeAdmins", [])],
                    "iam_allowed_principals_default": iam_allowed,
                    "implication": "IAM permissions suffice for new tables" if iam_allowed else
                    "Lake Formation grants are required (set lake_formation_grants=true)"}
        attempt("lake_formation", lf)
    if "pmcp" in cats:
        def stacks():
            res = []
            pager = c("cloudformation").get_paginator("describe_stacks")
            for page in pager.paginate():
                for st in page["Stacks"]:
                    if st["StackName"].startswith("pmcp-"):
                        tags = {t["Key"]: t["Value"] for t in st.get("Tags", [])}
                        res.append({"stack": st["StackName"], "status": st["StackStatus"],
                                    "flow_id": tags.get("pmcp:flow"),
                                    "owned_by_you": tags.get("pmcp:owner") == owner})
            return res
        attempt("pmcp_stacks", stacks)
    return out


# --- profile_source ----------------------------------------------------------------------------------

TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?)?(Z|[+-]\d{2}:?\d{2})?$")
CHANGE_MARKERS = {"op", "_op", "__op", "operation", "is_deleted", "_deleted", "deleted", "__deleted", "is_delete",
                  "deleted_at", "change_type", "action"}


def _infer(values: list[Any]) -> str:
    kinds = Counter()
    for v in values:
        if v is None or v == "":
            continue
        if isinstance(v, bool):
            kinds["boolean"] += 1
        elif isinstance(v, int):
            kinds["bigint" if abs(v) > 2**31 else "int"] += 1
        elif isinstance(v, float):
            kinds["double"] += 1
        elif isinstance(v, str):
            if TS_RE.match(v):
                kinds["timestamp" if len(v) > 10 else "date"] += 1
            elif re.fullmatch(r"-?\d+", v):
                kinds["bigint"] += 1
            elif re.fullmatch(r"-?\d+\.\d+", v):
                kinds["double"] += 1
            elif v.lower() in ("true", "false"):
                kinds["boolean"] += 1
            else:
                kinds["string"] += 1
        elif isinstance(v, dict | list):
            kinds["string"] += 1  # nested: flatten or keep as JSON string
            kinds["_nested"] += 1
    if not kinds:
        return "string"
    if "string" in kinds and kinds["string"] - kinds["_nested"] > 0:
        return "string"
    kinds.pop("_nested", None)
    if set(kinds) <= {"int", "bigint"}:
        return "bigint" if "bigint" in kinds else "int"
    if set(kinds) <= {"int", "bigint", "double"}:
        return "double"
    return kinds.most_common(1)[0][0]


def _parse_records(data: bytes, fmt: str, limit: int) -> list[dict[str, Any]]:
    if data[:2] == b"\x1f\x8b":
        data = gzip.decompress(data)
    text = data.decode(errors="replace")
    rows: list[dict[str, Any]] = []
    if fmt == "csv":
        for row in csv.DictReader(io.StringIO(text)):
            rows.append(dict(row))
            if len(rows) >= limit:
                break
        return rows
    stripped = text.lstrip()
    if stripped.startswith("["):
        try:
            arr = json.loads(stripped)
            return [r for r in arr if isinstance(r, dict)][:limit]
        except ValueError:
            pass
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except ValueError:
            rows.append({"__unparsable__": True})
            continue
        if isinstance(obj, dict):
            rows.append(obj)
        if len(rows) >= limit:
            break
    return rows


def _fmt_from_key(key: str) -> str:
    k = key.lower().removesuffix(".gz")
    if k.endswith(".csv"):
        return "csv"
    if k.endswith(".parquet"):
        return "parquet"
    if k.endswith((".jsonl", ".ndjson", ".json")):
        return "jsonl"
    return "unknown"


def _summarize_records(rows: list[dict[str, Any]], include_raw: bool) -> dict[str, Any]:
    bad = sum(1 for r in rows if r.get("__unparsable__"))
    rows = [r for r in rows if not r.get("__unparsable__")]
    fields: dict[str, list[Any]] = {}
    for r in rows:
        for k, v in r.items():
            fields.setdefault(k, []).append(v)
    n = len(rows)
    cols = []
    for k, vals in fields.items():
        non_null = [v for v in vals if v not in (None, "")]
        hashable = [json.dumps(v, sort_keys=True, default=str) for v in non_null]
        distinct = len(set(hashable))
        cols.append({
            "name": re.sub(r"[^a-z0-9_]", "_", k.lower()), "source_name": k, "type": _infer(vals),
            "null_rate": round(1 - len(non_null) / n, 3) if n else None, "distinct_in_sample": distinct,
            "unique_in_sample": n > 0 and distinct == n and len(non_null) == n,
            "nested": any(isinstance(v, dict | list) for v in non_null),
        })
    ts_cols = [c["name"] for c in cols if c["type"] in ("timestamp", "date")]
    times = []
    for c in cols:
        if c["type"] == "timestamp":
            for v in fields[c["source_name"]]:
                if isinstance(v, str) and TS_RE.match(v):
                    times.append(v)
    exact_dups = n - len({json.dumps(r, sort_keys=True, default=str) for r in rows})
    out = {
        "records_sampled": n,
        "unparsable_records": bad,
        "columns": cols,
        "suggestions": {
            "event_time_column": ts_cols[0] if ts_cols else None,
            "key_candidates": [c["name"] for c in cols if c["unique_in_sample"]][:5],
            "change_marker_candidates": [c["name"] for c in cols if c["name"] in CHANGE_MARKERS],
            "nested_fields": [c["name"] for c in cols if c["nested"]],
        },
        "exact_duplicate_records_in_sample": exact_dups,
        "newest_event_time_in_sample": max(times) if times else None,
        "masked_samples": [mask_value(r) for r in rows[:5]],
    }
    if include_raw:
        out["raw_samples"] = rows[:5]
        out["raw_samples_note"] = "raw values returned because include_raw_samples=true"
    return out


def profile_source(app: App, owner: str, account_id: str, region: str, source_uri: str, sample_records: int,
                   include_raw_samples: bool) -> dict[str, Any]:
    sess = app.sessions.session(owner, account_id, region, "reader")
    sample_records = max(10, min(sample_records, 2000))
    if source_uri.startswith("glue:"):
        db, _, table = source_uri[5:].partition(".")
        t = sess.client("glue", config=BOTO_CONFIG).get_table(DatabaseName=db, Name=table)["Table"]
        loc = t.get("StorageDescriptor", {}).get("Location", "")
        res = {"glue_table": {"columns": t["StorageDescriptor"]["Columns"],
                              "partition_keys": t.get("PartitionKeys", []), "location": loc,
                              "parameters": {k: v for k, v in t.get("Parameters", {}).items()
                                             if not k.startswith("spark.")}}}
        if loc.startswith("s3://"):
            res["storage"] = _profile_s3(sess, loc if loc.endswith("/") else loc + "/", sample_records,
                                         include_raw_samples)
        return {"source_uri": source_uri, **res}
    if source_uri.startswith("s3://"):
        return {"source_uri": source_uri, **_profile_s3(sess, source_uri, sample_records, include_raw_samples)}
    if source_uri.startswith("kinesis:"):
        return {"source_uri": source_uri, **_profile_kinesis(sess, source_uri[8:], sample_records,
                                                             include_raw_samples)}
    if source_uri.startswith("dynamodb:"):
        ddb = sess.client("dynamodb", config=BOTO_CONFIG)
        name = source_uri[9:]
        desc = ddb.describe_table(TableName=name)["Table"]
        from boto3.dynamodb.types import TypeDeserializer

        de = TypeDeserializer()
        items = [{k: de.deserialize(v) for k, v in it.items()}
                 for it in ddb.scan(TableName=name, Limit=sample_records).get("Items", [])]
        items = [json.loads(json.dumps(i, default=float)) for i in items]
        return {"source_uri": source_uri, "key_schema": desc["KeySchema"], "item_count_estimate": desc.get("ItemCount"),
                "size_bytes_estimate": desc.get("TableSizeBytes"),
                "stream": desc.get("StreamSpecification"), **_summarize_records(items, include_raw_samples)}
    raise PmcpError(ErrorCode.INVALID_INPUT, "source_uri must be s3://, glue:<db>.<table>, kinesis:<stream> or "
                    "dynamodb:<table>", next_actions=["Relational/Kafka sources are profiled via inspect_aws_environment "
                                                      "only (no data sampling yet)"])


def _profile_s3(sess: Any, uri: str, sample_records: int, include_raw: bool) -> dict[str, Any]:
    s3 = sess.client("s3", config=BOTO_CONFIG)
    bucket, _, prefix = uri[5:].partition("/")
    objs = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix,
                                                              PaginationConfig={"MaxItems": 10000}):
        objs += [o for o in page.get("Contents", []) if not o["Key"].endswith("/")]
    if not objs:
        return {"objects": 0, "note": "no objects under prefix"}
    total = sum(o["Size"] for o in objs)
    by_day: Counter[str] = Counter()
    for o in objs:
        by_day[o["LastModified"].astimezone(UTC).strftime("%Y-%m-%d")] += o["Size"]
    last7 = [by_day.get((datetime.now(UTC) - timedelta(days=i)).strftime("%Y-%m-%d"), 0) for i in range(1, 8)]
    partitions = Counter()
    for o in objs:
        for part in o["Key"][len(prefix):].split("/")[:-1]:
            if "=" in part:
                partitions[part.split("=")[0]] += 1
    formats = Counter(_fmt_from_key(o["Key"]) for o in objs)
    newest = sorted(objs, key=lambda o: o["LastModified"], reverse=True)
    fmt = formats.most_common(1)[0][0]
    rows: list[dict[str, Any]] = []
    sampled_files = []
    if fmt in ("jsonl", "csv"):
        for o in newest[:20]:
            body = s3.get_object(Bucket=bucket, Key=o["Key"], Range="bytes=0-1048575")["Body"].read()
            got = _parse_records(body, fmt, sample_records - len(rows))
            if got and fmt == "jsonl" and o["Size"] > 1048576 and got[-1].get("__unparsable__"):
                got = got[:-1]  # the last line was cut by the 1 MB range read
            rows += got
            sampled_files.append(o["Key"])
            if len(rows) >= sample_records:
                break
    summary: dict[str, Any] = {
        "objects": len(objs), "objects_truncated_at_10000": len(objs) >= 10000, "total_bytes": total,
        "formats": dict(formats), "hive_partition_keys": list(partitions),
        "bytes_by_day_last_7": dict(zip([(datetime.now(UTC) - timedelta(days=i)).strftime("%Y-%m-%d")
                                         for i in range(1, 8)], last7, strict=True)),
        "avg_bytes_per_day_last_7": int(sum(last7) / 7),
        "newest_object": {"key": newest[0]["Key"], "last_modified": str(newest[0]["LastModified"])},
        "sampled_files": sampled_files[:20],
    }
    if fmt in ("jsonl", "csv"):
        summary.update(_summarize_records(rows, include_raw))
        summary["detected_format"] = fmt
        if summary.get("records_sampled"):
            avg = sum(o["Size"] for o in newest[:len(sampled_files)]) / max(1, len(sampled_files))
            summary["note"] = f"~{avg:.0f} bytes per sampled object"
    else:
        summary["schema"] = "unverified"
        summary["schema_note"] = f"{fmt} content is not parsed by pmcp; register a Glue table and profile glue:<db>.<t>"
    return summary


def _profile_kinesis(sess: Any, stream: str, sample_records: int, include_raw: bool) -> dict[str, Any]:
    k = sess.client("kinesis", config=BOTO_CONFIG)
    desc = k.describe_stream_summary(StreamName=stream)["StreamDescriptionSummary"]
    cw = sess.client("cloudwatch", config=BOTO_CONFIG)
    now = datetime.now(UTC)
    stats = {}
    for metric in ("IncomingRecords", "IncomingBytes"):
        pts = cw.get_metric_statistics(Namespace="AWS/Kinesis", MetricName=metric,
                                       Dimensions=[{"Name": "StreamName", "Value": stream}],
                                       StartTime=now - timedelta(hours=24), EndTime=now, Period=3600,
                                       Statistics=["Sum", "Maximum"])["Datapoints"]
        stats[metric] = {"last_24h_sum": sum(p["Sum"] for p in pts),
                         "peak_hour": max((p["Sum"] for p in pts), default=0)}
    rows: list[dict[str, Any]] = []
    for sh in k.list_shards(StreamName=stream)["Shards"][:4]:
        it = k.get_shard_iterator(StreamName=stream, ShardId=sh["ShardId"], ShardIteratorType="AT_TIMESTAMP",
                                  Timestamp=now - timedelta(hours=1))["ShardIterator"]
        for _ in range(5):
            resp = k.get_records(ShardIterator=it, Limit=min(1000, sample_records))
            for r in resp["Records"]:
                try:
                    obj = json.loads(r["Data"])
                    rows.append(obj if isinstance(obj, dict) else {"value": obj})
                except ValueError:
                    rows.append({"__unparsable__": True})
            it = resp.get("NextShardIterator")
            if len(rows) >= sample_records or not it or resp.get("MillisBehindLatest", 0) == 0:
                break
        if len(rows) >= sample_records:
            break
    return {"stream": {"mode": desc.get("StreamModeDetails", {}).get("StreamMode"),
                       "open_shards": desc.get("OpenShardCount"), "retention_hours": desc.get("RetentionPeriodHours"),
                       "status": desc.get("StreamStatus")},
            "metrics": stats,
            "peak_records_per_second_estimate": round(stats["IncomingRecords"]["peak_hour"] / 3600, 2),
            **_summarize_records(rows[:sample_records], include_raw)}


# --- inspect_consumer_contract -----------------------------------------------------------------------


def inspect_consumer(app: App, owner: str, account_id: str, region: str, target: str, principal_arn: str | None,
                     contract_id: str | None) -> dict[str, Any]:
    sess = app.sessions.session(owner, account_id, region, "reader")
    out: dict[str, Any] = {"target": target}
    actions: list[str] = []
    resources: list[str] = []
    if target.startswith("glue:"):
        db, _, table = target[5:].partition(".")
        try:
            t = sess.client("glue", config=BOTO_CONFIG).get_table(DatabaseName=db, Name=table)["Table"]
        except ClientError as exc:
            out["exists"] = False
            out["error"] = from_boto(exc, action="get table").message
            t = None
        if t:
            loc = t.get("StorageDescriptor", {}).get("Location", "")
            out.update({"exists": True, "schema": [{"name": c["Name"], "type": c["Type"]}
                                                    for c in t["StorageDescriptor"]["Columns"]],
                        "partition_keys": [p["Name"] for p in t.get("PartitionKeys", [])],
                        "table_format": t.get("Parameters", {}).get("table_type", t.get("TableType")),
                        "location": loc})
            if loc.startswith("s3://"):
                b, _, p = loc[5:].partition("/")
                objs = sess.client("s3", config=BOTO_CONFIG).list_objects_v2(Bucket=b, Prefix=p, MaxKeys=1000)
                newest = max((o["LastModified"] for o in objs.get("Contents", [])), default=None)
                out["newest_object_modified"] = str(newest) if newest else None
                resources = [f"arn:aws:glue:{region}:{account_id}:table/{db}/{table}", f"arn:aws:s3:::{b}/{p}*"]
            actions = ["athena:StartQueryExecution", "glue:GetTable", "s3:GetObject"]
    elif target.startswith("dynamodb:"):
        name = target[9:]
        try:
            d = sess.client("dynamodb", config=BOTO_CONFIG).describe_table(TableName=name)["Table"]
            out.update({"exists": True, "key_schema": d["KeySchema"], "attributes": d["AttributeDefinitions"],
                        "item_count_estimate": d.get("ItemCount")})
        except ClientError as exc:
            out["exists"] = False
            out["error"] = from_boto(exc, action="describe table").message
        actions = ["dynamodb:GetItem", "dynamodb:Query"]
        resources = [f"arn:aws:dynamodb:{region}:{account_id}:table/{name}"]
    else:
        raise PmcpError(ErrorCode.INVALID_INPUT, "target must be glue:<db>.<table> or dynamodb:<table>")
    if principal_arn:
        try:
            iam = sess.client("iam", config=BOTO_CONFIG)
            res = iam.simulate_principal_policy(PolicySourceArn=principal_arn, ActionNames=actions,
                                                ResourceArns=resources or ["*"])
            out["principal_access"] = {
                "principal": principal_arn,
                "results": [{"action": r["EvalActionName"], "resource": r.get("EvalResourceName"),
                             "decision": r["EvalDecision"]} for r in res["EvaluationResults"]],
                "note": "IAM policy simulation only; bucket policies, KMS key policies and Lake Formation grants are "
                        "not included. verify_data_flow performs a real read with the flow's consumer role.",
            }
        except ClientError as exc:
            out["principal_access"] = {"status": "unverified", "reason": from_boto(exc, action="simulate").message}
    if contract_id and out.get("schema"):
        from pmcp.adapters.athena_iceberg import ATHENA_TYPES, _type_match
        from pmcp.services.contracts import load_contract

        body = load_contract(app, owner, contract_id).body
        actual = {c["name"]: c["type"] for c in out["schema"]}
        out["contract_comparison"] = {
            "missing_in_target": [c.name for c in body.output.columns if c.name not in actual],
            "extra_in_target": [k for k in actual if k not in {c.name for c in body.output.columns}],
            "type_mismatches": [{"column": c.name, "contract": c.type, "target": actual[c.name]}
                                for c in body.output.columns
                                if c.name in actual and not _type_match(ATHENA_TYPES[c.type], actual[c.name])],
        }
    return out
