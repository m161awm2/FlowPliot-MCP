"""Unit prices for cost estimates.

Live prices come from the AWS Price List API (called from the platform account, which costs nothing
for the tenant). When a lookup fails or no product matches, a static table is used and the line
item says so (`price_source="static_table"`), so an estimate never hides where a number came from.
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from dataclasses import dataclass

import boto3

from pmcp.aws.client import BOTO_CONFIG

log = logging.getLogger(__name__)

STATIC_AS_OF = "2026-06 (us-east-1 list prices; other regions are typically 0-25% higher)"

# key -> (usd, unit)
STATIC: dict[str, tuple[float, str]] = {
    "athena_tb_scanned": (5.0, "TB scanned"),
    "sfn_state_transition": (0.000025, "state transition"),
    "scheduler_invocation": (0.000001, "invocation (after 14M free/month)"),
    "s3_gb_month": (0.023, "GB-month"),
    "s3_put_request": (0.000005, "PUT request"),
    "s3_get_request": (0.0000004, "GET request"),
    "glue_dpu_hour": (0.44, "DPU-hour"),
    "lambda_request": (0.0000002, "request"),
    "lambda_gb_second_arm": (0.0000133334, "GB-second (arm64)"),
    "kinesis_ondemand_stream_hour": (0.04, "stream-hour"),
    "kinesis_ondemand_gb_in": (0.08, "GB ingested"),
    "kinesis_ondemand_gb_out": (0.04, "GB retrieved"),
    "firehose_gb_ingested": (0.029, "GB ingested"),
    "firehose_gb_format_conversion": (0.018, "GB converted"),
    "dynamodb_write_request_unit": (0.000000625, "write request unit"),
    "dynamodb_read_request_unit": (0.000000125, "read request unit"),
    "dynamodb_gb_month": (0.25, "GB-month"),
    "cloudwatch_alarm_month": (0.10, "alarm-month"),
}

# key -> (ServiceCode, unit regex, description regex)
LIVE: dict[str, tuple[str, str, str]] = {
    "athena_tb_scanned": ("AmazonAthena", r"^Terabytes?$", r"(?i)data scanned|per TB"),
    "glue_dpu_hour": ("AWSGlue", r"(?i)DPU-Hour", r"(?i)ETL job"),
    "firehose_gb_ingested": ("AmazonKinesisFirehose", r"^GB$", r"(?i)ingest|direct put|bytes"),
    "dynamodb_write_request_unit": ("AmazonDynamoDB", r"(?i)WriteRequestUnits", r"(?i)write request"),
}


@dataclass
class Price:
    usd: float
    unit: str
    source: str
    as_of: str


class PriceBook:
    def __init__(self, session: boto3.Session | None = None, live: bool = True) -> None:
        self._session = session
        self._live = live
        self._cache: dict[tuple[str, str], tuple[float, Price]] = {}
        self._lock = threading.Lock()

    def get(self, key: str, region: str) -> Price:
        with self._lock:
            hit = self._cache.get((key, region))
            if hit and time.time() - hit[0] < 86400:
                return hit[1]
        price = self._lookup_live(key, region) if self._live and key in LIVE else None
        if price is None:
            usd, unit = STATIC[key]
            price = Price(usd=usd, unit=unit, source="static_table", as_of=STATIC_AS_OF)
        with self._lock:
            self._cache[(key, region)] = (time.time(), price)
        return price

    def _lookup_live(self, key: str, region: str) -> Price | None:
        service_code, unit_re, desc_re = LIVE[key]
        try:
            session = self._session or boto3.Session()
            client = session.client("pricing", region_name="us-east-1", config=BOTO_CONFIG)
            resp = client.get_products(
                ServiceCode=service_code,
                Filters=[{"Type": "TERM_MATCH", "Field": "regionCode", "Value": region}],
                MaxResults=100,
            )
            for raw in resp.get("PriceList", []):
                product = json.loads(raw) if isinstance(raw, str) else raw
                for term in product.get("terms", {}).get("OnDemand", {}).values():
                    for dim in term.get("priceDimensions", {}).values():
                        if not re.search(unit_re, dim.get("unit", "")):
                            continue
                        if not re.search(desc_re, dim.get("description", "")):
                            continue
                        if str(dim.get("beginRange", "0")) not in ("0", "0.0"):
                            continue
                        usd = float(dim.get("pricePerUnit", {}).get("USD", "0"))
                        if usd <= 0:
                            continue
                        return Price(
                            usd=usd, unit=STATIC[key][1], source="pricing_api", as_of=term.get("effectiveDate", "")
                        )
        except Exception as exc:  # pricing is advisory; fall back to the static table
            log.info("live price lookup failed for %s/%s: %s", key, region, exc)
        return None
