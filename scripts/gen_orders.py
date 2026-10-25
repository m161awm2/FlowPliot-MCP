"""Synthetic order events for scenario A (batch, upserts + deletes) and the failure scenario.

Writes JSON Lines to s3://<bucket>/orders/ — one file per batch. Each batch inserts new orders, updates some
earlier ones (later updated_at) and deletes a few (deleted=true). A ground-truth summary of the expected final
state is written next to the data so verification results can be cross-checked independently.

  python scripts/gen_orders.py --bucket pmcp-e2e-src-608420805531 --batches 3 --orders 500
  python scripts/gen_orders.py --bucket ... --bad-file       # one malformed file (failure scenario)
"""

from __future__ import annotations

import argparse
import json
import random
from datetime import UTC, datetime, timedelta

import boto3

STATUSES = ["created", "paid", "shipped", "delivered", "cancelled"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", default="orders/")
    ap.add_argument("--batches", type=int, default=3)
    ap.add_argument("--orders", type=int, default=500)
    ap.add_argument("--bad-file", action="store_true")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--region", default="ap-southeast-2")
    a = ap.parse_args()
    s3 = boto3.client("s3", region_name=a.region)
    if a.bad_file:
        body = '{"order_id": "bad-1", "status": "paid", "amount": 10.0, "updated_at": "' + \
            datetime.now(UTC).isoformat().replace("+00:00", "Z") + '", "deleted": false}\n{"order_id": "bad-2", "amo\n'
        key = f"{a.prefix}batch-bad-{datetime.now(UTC):%Y%m%dT%H%M%S}.jsonl"
        s3.put_object(Bucket=a.bucket, Key=key, Body=body.encode())
        print(f"wrote malformed s3://{a.bucket}/{key}")
        return
    rnd = random.Random(a.seed)
    now = datetime.now(UTC).replace(microsecond=0)
    state: dict[str, dict] = {}
    seq = 0
    for b in range(a.batches):
        t0 = now - timedelta(minutes=10 * (a.batches - b))
        rows = []
        for _ in range(a.orders):
            seq += 1
            oid = f"o-{seq:06d}"
            row = {"order_id": oid, "status": "created", "amount": round(rnd.uniform(5, 500), 2),
                   "updated_at": (t0 + timedelta(seconds=rnd.randint(0, 500))).isoformat().replace("+00:00", "Z"),
                   "deleted": False}
            rows.append(row)
            state[oid] = row
        for oid in rnd.sample(sorted(state), k=min(len(state), a.orders // 5)):
            prev = state[oid]
            upd = dict(prev, status=rnd.choice(STATUSES[1:]),
                       updated_at=(datetime.fromisoformat(prev["updated_at"].replace("Z", "+00:00"))
                                   + timedelta(seconds=rnd.randint(1, 60))).isoformat().replace("+00:00", "Z"))
            if rnd.random() < 0.15:
                upd["deleted"] = True
            rows.append(upd)
            state[oid] = upd
        # Duplicate delivery of a few rows (same version twice) — must not create duplicates downstream.
        rows += rnd.sample(rows, k=5)
        rnd.shuffle(rows)
        key = f"{a.prefix}batch-{t0:%Y%m%dT%H%M%S}.jsonl"
        s3.put_object(Bucket=a.bucket, Key=key, Body="\n".join(json.dumps(r) for r in rows).encode() + b"\n")
        print(f"wrote {len(rows)} rows to s3://{a.bucket}/{key}")
    live = [r for r in state.values() if not r["deleted"]]
    truth = {"generated_at": now.isoformat(), "orders_total": len(state), "orders_live": len(live),
             "orders_deleted": len(state) - len(live),
             "window": {"start": (now - timedelta(minutes=10 * a.batches + 5)).isoformat().replace("+00:00", "Z"),
                        "end": (now + timedelta(minutes=5)).isoformat().replace("+00:00", "Z")}}
    s3.put_object(Bucket=a.bucket, Key="ground-truth/orders.json", Body=json.dumps(truth, indent=1).encode())
    print(json.dumps(truth))


if __name__ == "__main__":
    main()
