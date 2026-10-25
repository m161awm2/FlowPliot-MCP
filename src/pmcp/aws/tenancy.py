"""Reaching tenant accounts.

The platform never stores tenant credentials. Each call assumes a role the tenant created with the
onboarding stack; the trust policy requires the per-connection ExternalId, so knowing an account id
is not enough to act in it.
"""

from __future__ import annotations

import re
import threading
import time
from typing import Literal

import boto3
from botocore.exceptions import ClientError

from pmcp.aws.client import BOTO_CONFIG
from pmcp.config import Settings
from pmcp.domain.records import AccountConnection
from pmcp.errors import ErrorCode, PmcpError, from_boto
from pmcp.store import Store

Access = Literal["reader", "deployer"]


class TenantSessions:
    def __init__(self, settings: Settings, store: Store) -> None:
        self.settings = settings
        self.store = store
        self._cache: dict[tuple[str, str, str], tuple[float, dict[str, str]]] = {}
        self._lock = threading.Lock()

    def platform_session(self, region: str | None = None) -> boto3.Session:
        return boto3.Session(region_name=region or self.settings.platform_region)

    def connection(self, owner: str, account_id: str) -> AccountConnection:
        conn = self.store.get(AccountConnection, owner, account_id)
        if conn is None or conn.status != "connected":
            raise PmcpError(
                ErrorCode.NOT_CONNECTED,
                f"AWS account {account_id} is not connected for this user",
                next_actions=[f"Open {self.settings.public_base_url}/connect to connect the account."],
            )
        return conn

    def check_region(self, conn: AccountConnection, region: str) -> None:
        if conn.allowed_regions and region not in conn.allowed_regions:
            raise PmcpError(
                ErrorCode.FORBIDDEN,
                f"region {region} is not in the allowed regions for account {conn.account_id}",
                details={"allowed_regions": conn.allowed_regions},
            )

    def session(self, owner: str, account_id: str, region: str, access: Access = "reader") -> boto3.Session:
        conn = self.connection(owner, account_id)
        self.check_region(conn, region)
        if conn.mode == "direct":
            if not self.settings.allow_direct_credentials:
                raise PmcpError(ErrorCode.FORBIDDEN, "direct credentials are only allowed in local mode")
            return boto3.Session(region_name=region)
        if access == "deployer":
            if not conn.deployer_role_arn:
                raise PmcpError(
                    ErrorCode.WRITE_ACCESS_NOT_ENABLED,
                    f"account {account_id} is connected read-only",
                    next_actions=["Update the pmcp onboarding stack with EnableDeployer=true, then reconnect."],
                )
            role_arn = conn.deployer_role_arn
        else:
            role_arn = conn.reader_role_arn or ""
        return self._assumed(owner, role_arn, conn.external_id, region, tag=access)

    def role_session(self, owner: str, account_id: str, region: str, role_arn: str) -> boto3.Session:
        """Assume a pmcp-created role in the tenant account (e.g. a flow's consumer role)."""
        conn = self.connection(owner, account_id)
        self.check_region(conn, region)
        if f":{account_id}:" not in role_arn or ":role/pmcp-" not in role_arn:
            raise PmcpError(ErrorCode.FORBIDDEN, "only pmcp-* roles in the connected account can be assumed")
        # In local "direct" mode the caller's own credentials do the AssumeRole; the role's trust
        # policy still demands the same ExternalId.
        return self._assumed(owner, role_arn, conn.external_id, region, tag="consumer")

    def _assumed(self, owner: str, role_arn: str, external_id: str, region: str, *, tag: str) -> boto3.Session:
        key = (owner, role_arn, external_id)
        with self._lock:
            hit = self._cache.get(key)
            if hit and hit[0] - time.time() > 300:
                creds = hit[1]
            else:
                creds = None
        if creds is None:
            sts = self.platform_session().client("sts", config=BOTO_CONFIG)
            session_name = re.sub(r"[^\w+=,.@-]", "-", f"pmcp-{tag}-{owner}")[:64]
            try:
                resp = sts.assume_role(
                    RoleArn=role_arn,
                    RoleSessionName=session_name,
                    ExternalId=external_id,
                    DurationSeconds=3600,
                )
            except ClientError as exc:
                raise from_boto(exc, action=f"assume {role_arn}") from exc
            c = resp["Credentials"]
            creds = {
                "aws_access_key_id": c["AccessKeyId"],
                "aws_secret_access_key": c["SecretAccessKey"],
                "aws_session_token": c["SessionToken"],
            }
            with self._lock:
                self._cache[key] = (c["Expiration"].timestamp(), creds)
        return boto3.Session(region_name=region, **creds)

    def verify_connection(self, conn: AccountConnection) -> AccountConnection:
        """Assume each configured role once and confirm it lands in the claimed account."""
        if conn.mode == "direct":
            ident = boto3.Session().client("sts").get_caller_identity()
            if ident["Account"] != conn.account_id:
                raise PmcpError(ErrorCode.FORBIDDEN, "local credentials belong to a different account")
            return conn
        for arn in filter(None, [conn.reader_role_arn, conn.deployer_role_arn]):
            sess = self._assumed(conn.owner, arn, conn.external_id, self.settings.platform_region, tag="check")
            ident = sess.client("sts", config=BOTO_CONFIG).get_caller_identity()
            if ident["Account"] != conn.account_id:
                raise PmcpError(ErrorCode.FORBIDDEN, f"role {arn} is not in account {conn.account_id}")
        return conn
