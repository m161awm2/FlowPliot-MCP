"""State store: one owner-scoped key space with optimistic concurrency.

Every record lives at (pk="U#<owner>", sk="<KIND>#<id>"). Writes carry the version the caller
read; a mismatch raises CONFLICT, which is how duplicate workers and racing requests are kept
from both succeeding. Idempotency keys and locks are ordinary items with conditional puts.
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any, Protocol, TypeVar

from pmcp.domain.base import Stored
from pmcp.errors import ErrorCode, PmcpError
from pmcp.ids import iso

T = TypeVar("T", bound=Stored)

GLOBAL_OWNER = "_global"


class Backend(Protocol):
    def get_item(self, pk: str, sk: str) -> dict[str, Any] | None: ...
    def put_item(self, item: dict[str, Any], *, expected_version: int | None, must_not_exist: bool) -> None: ...
    def query(self, pk: str, sk_prefix: str, limit: int) -> list[dict[str, Any]]: ...
    def delete_item(self, pk: str, sk: str, *, expected_version: int | None = None) -> None: ...


class ConditionFailed(Exception):
    pass


class MemoryBackend:
    def __init__(self) -> None:
        self._items: dict[tuple[str, str], dict[str, Any]] = {}
        self._lock = threading.Lock()

    def get_item(self, pk: str, sk: str) -> dict[str, Any] | None:
        with self._lock:
            item = self._items.get((pk, sk))
            if item and item.get("ttl") and item["ttl"] < time.time():
                return None
            return dict(item) if item else None

    def put_item(self, item: dict[str, Any], *, expected_version: int | None, must_not_exist: bool) -> None:
        key = (item["pk"], item["sk"])
        with self._lock:
            existing = self._items.get(key)
            if existing and existing.get("ttl") and existing["ttl"] < time.time():
                existing = None
            if must_not_exist and existing is not None:
                raise ConditionFailed()
            if expected_version is not None and (existing is None or existing.get("version") != expected_version):
                raise ConditionFailed()
            self._items[key] = dict(item)

    def query(self, pk: str, sk_prefix: str, limit: int) -> list[dict[str, Any]]:
        with self._lock:
            rows = [dict(v) for (p, s), v in self._items.items() if p == pk and s.startswith(sk_prefix)]
        rows.sort(key=lambda r: r["sk"], reverse=True)
        return rows[:limit]

    def delete_item(self, pk: str, sk: str, *, expected_version: int | None = None) -> None:
        with self._lock:
            existing = self._items.get((pk, sk))
            if expected_version is not None and (existing is None or existing.get("version") != expected_version):
                raise ConditionFailed()
            self._items.pop((pk, sk), None)


class DynamoBackend:
    def __init__(self, table_name: str, region: str) -> None:
        import boto3

        self._table = boto3.resource("dynamodb", region_name=region).Table(table_name)

    def get_item(self, pk: str, sk: str) -> dict[str, Any] | None:
        item = self._table.get_item(Key={"pk": pk, "sk": sk}, ConsistentRead=True).get("Item")
        if item and item.get("ttl") and int(item["ttl"]) < time.time():
            return None
        return _from_ddb(item) if item else None

    def put_item(self, item: dict[str, Any], *, expected_version: int | None, must_not_exist: bool) -> None:
        from botocore.exceptions import ClientError

        kwargs: dict[str, Any] = {"Item": item}
        if must_not_exist:
            # An expired-but-not-yet-deleted item (TTL) counts as absent.
            kwargs["ConditionExpression"] = "attribute_not_exists(pk) OR (attribute_exists(#ttl) AND #ttl < :now)"
            kwargs["ExpressionAttributeNames"] = {"#ttl": "ttl"}
            kwargs["ExpressionAttributeValues"] = {":now": int(time.time())}
        elif expected_version is not None:
            kwargs["ConditionExpression"] = "#v = :v"
            kwargs["ExpressionAttributeNames"] = {"#v": "version"}
            kwargs["ExpressionAttributeValues"] = {":v": expected_version}
        try:
            self._table.put_item(**kwargs)
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                raise ConditionFailed() from exc
            raise

    def query(self, pk: str, sk_prefix: str, limit: int) -> list[dict[str, Any]]:
        from boto3.dynamodb.conditions import Key

        resp = self._table.query(
            KeyConditionExpression=Key("pk").eq(pk) & Key("sk").begins_with(sk_prefix),
            ScanIndexForward=False,
            Limit=limit,
            ConsistentRead=True,
        )
        now = time.time()
        return [_from_ddb(i) for i in resp.get("Items", []) if not (i.get("ttl") and int(i["ttl"]) < now)]

    def delete_item(self, pk: str, sk: str, *, expected_version: int | None = None) -> None:
        from botocore.exceptions import ClientError

        kwargs: dict[str, Any] = {"Key": {"pk": pk, "sk": sk}}
        if expected_version is not None:
            kwargs["ConditionExpression"] = "#v = :v"
            kwargs["ExpressionAttributeNames"] = {"#v": "version"}
            kwargs["ExpressionAttributeValues"] = {":v": expected_version}
        try:
            self._table.delete_item(**kwargs)
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                raise ConditionFailed() from exc
            raise


def _from_ddb(item: dict[str, Any]) -> dict[str, Any]:
    out = dict(item)
    for k in ("version", "ttl"):
        if k in out and out[k] is not None:
            out[k] = int(out[k])
    return out


class Store:
    def __init__(self, backend: Backend) -> None:
        self.backend = backend

    # --- typed records -------------------------------------------------------------------------
    @staticmethod
    def _key(kind: str, owner: str, id_: str) -> tuple[str, str]:
        return f"U#{owner}", f"{kind}#{id_}"

    def create(self, rec: T, *, ttl: int | None = None) -> T:
        rec.version = 1
        rec.created_at = rec.updated_at = iso()
        try:
            self.backend.put_item(self._item(rec, ttl), expected_version=None, must_not_exist=True)
        except ConditionFailed as exc:
            raise PmcpError(ErrorCode.CONFLICT, f"{rec.KIND} {rec.id} already exists") from exc
        return rec

    def save(self, rec: T, *, ttl: int | None = None) -> T:
        """Compare-and-set on the version the caller read."""
        expected = rec.version
        rec.version = expected + 1
        rec.updated_at = iso()
        try:
            self.backend.put_item(self._item(rec, ttl), expected_version=expected, must_not_exist=False)
        except ConditionFailed as exc:
            rec.version = expected
            raise PmcpError(
                ErrorCode.CONFLICT,
                f"{rec.KIND} {rec.id} was changed concurrently; re-read and retry",
            ) from exc
        return rec

    def get(self, cls: type[T], owner: str, id_: str) -> T | None:
        pk, sk = self._key(cls.KIND, owner, id_)
        item = self.backend.get_item(pk, sk)
        return cls.model_validate_json(item["data"]) if item else None

    def require(self, cls: type[T], owner: str, id_: str) -> T:
        rec = self.get(cls, owner, id_)
        if rec is None:
            raise PmcpError(ErrorCode.NOT_FOUND, f"{cls.KIND.lower()} '{id_}' not found for this user")
        return rec

    def list(self, cls: type[T], owner: str, *, prefix: str = "", limit: int = 100) -> list[T]:
        pk, _ = self._key(cls.KIND, owner, "")
        rows = self.backend.query(pk, f"{cls.KIND}#{prefix}", limit)
        return [cls.model_validate_json(r["data"]) for r in rows]

    def delete(self, cls: type[T], owner: str, id_: str) -> None:
        pk, sk = self._key(cls.KIND, owner, id_)
        self.backend.delete_item(pk, sk)

    def _item(self, rec: Stored, ttl: int | None) -> dict[str, Any]:
        pk, sk = self._key(rec.KIND, rec.owner, rec.id)
        item: dict[str, Any] = {
            "pk": pk,
            "sk": sk,
            "kind": rec.KIND,
            "version": rec.version,
            "updated_at": rec.updated_at,
            "data": rec.model_dump_json(),
        }
        if ttl:
            item["ttl"] = int(ttl)
        return item

    # --- idempotency keys and locks -------------------------------------------------------------
    def claim(self, owner: str, name: str, value: dict[str, Any], *, ttl_seconds: int) -> dict[str, Any] | None:
        """Atomically claim `name` for `owner`. Returns None if we won, else the existing value."""
        pk, sk = f"U#{owner}", f"CLAIM#{name}"
        item = {
            "pk": pk,
            "sk": sk,
            "kind": "CLAIM",
            "version": 1,
            "data": json.dumps(value),
            "ttl": int(time.time()) + ttl_seconds,
        }
        try:
            self.backend.put_item(item, expected_version=None, must_not_exist=True)
            return None
        except ConditionFailed:
            existing = self.backend.get_item(pk, sk)
            if existing is None:  # expired between our put and get; try once more
                try:
                    self.backend.put_item(item, expected_version=None, must_not_exist=True)
                    return None
                except ConditionFailed:
                    existing = self.backend.get_item(pk, sk)
            return json.loads(existing["data"]) if existing else {}

    def renew_claim(self, owner: str, name: str, value: dict[str, Any], *, ttl_seconds: int) -> bool:
        """Extend a claim we hold. False if it expired or someone else holds it now."""
        pk, sk = f"U#{owner}", f"CLAIM#{name}"
        item = self.backend.get_item(pk, sk)
        if not item or json.loads(item["data"]) != value:
            return False
        new = dict(item, version=item["version"] + 1, ttl=int(time.time()) + ttl_seconds)
        try:
            self.backend.put_item(new, expected_version=item["version"], must_not_exist=False)
            return True
        except ConditionFailed:
            return False

    def peek_claim(self, owner: str, name: str) -> dict[str, Any] | None:
        item = self.backend.get_item(f"U#{owner}", f"CLAIM#{name}")
        return json.loads(item["data"]) if item else None

    def release(self, owner: str, name: str, *, only_if: dict[str, Any] | None = None) -> None:
        pk, sk = f"U#{owner}", f"CLAIM#{name}"
        if only_if is not None:
            item = self.backend.get_item(pk, sk)
            if not item or json.loads(item["data"]) != only_if:
                return
        self.backend.delete_item(pk, sk)

    # --- global, non-tenant data (revoked token ids) ---------------------------------------------
    def put_global(self, name: str, value: dict[str, Any], *, ttl: int | None = None) -> None:
        item: dict[str, Any] = {
            "pk": f"U#{GLOBAL_OWNER}",
            "sk": name,
            "kind": "GLOBAL",
            "version": 1,
            "data": json.dumps(value),
        }
        if ttl:
            item["ttl"] = ttl
        self.backend.put_item(item, expected_version=None, must_not_exist=False)

    def get_global(self, name: str) -> dict[str, Any] | None:
        item = self.backend.get_item(f"U#{GLOBAL_OWNER}", name)
        return json.loads(item["data"]) if item else None


def build_store(table: str | None, region: str) -> Store:
    return Store(DynamoBackend(table, region) if table else MemoryBackend())
