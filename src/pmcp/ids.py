"""Identifiers and content digests."""

from __future__ import annotations

import hashlib
import json
import secrets
import time
from datetime import UTC, datetime
from typing import Any

_ALPHABET = "0123456789abcdefghjkmnpqrstvwxyz"  # Crockford base32, lowercase


def _b32(n: int, length: int) -> str:
    out = []
    for _ in range(length):
        out.append(_ALPHABET[n & 31])
        n >>= 5
    return "".join(reversed(out))


def new_id(prefix: str) -> str:
    """Time-sortable id: <prefix>_<8 chars ms timestamp><10 random chars>."""
    ms = int(time.time() * 1000)
    return f"{prefix}_{_b32(ms, 9)}{_b32(secrets.randbits(50), 10)}"


def short(id_: str, n: int = 10) -> str:
    """Short form of an id for AWS resource names (lowercase, no prefix)."""
    return id_.split("_", 1)[-1][-n:]


def canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def sha256_hex(data: str | bytes) -> str:
    if isinstance(data, str):
        data = data.encode()
    return hashlib.sha256(data).hexdigest()


def digest(obj: Any) -> str:
    return "sha256:" + sha256_hex(canonical_json(obj))


def utcnow() -> datetime:
    return datetime.now(UTC)


def iso(dt: datetime | None = None) -> str:
    return (dt or utcnow()).astimezone(UTC).isoformat().replace("+00:00", "Z")
