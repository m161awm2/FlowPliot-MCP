"""JWTs issued by pmcp.

Two audiences, never interchangeable:
  pmcp-mcp  - bearer token for the MCP endpoint (scope "mcp"); pasted into the agent's client config.
  pmcp-web  - short-lived browser session cookie for /connect and /approve, minted only after a
              Cognito login. Approving a plan requires this, so an agent holding the MCP token
              cannot approve its own plans.
"""

from __future__ import annotations

import base64
import hashlib
import secrets
import threading
import time
from typing import Any

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from pmcp.config import Settings
from pmcp.store import Store

_lock = threading.Lock()


class KeyRing:
    def __init__(self, settings: Settings, session: Any | None = None) -> None:
        self.settings = settings
        self._session = session
        self._private: rsa.RSAPrivateKey | None = None

    def _load(self) -> rsa.RSAPrivateKey:
        with _lock:
            if self._private is not None:
                return self._private
            pem = self.settings.jwt_private_key_pem
            if not pem and self.settings.jwt_secret_id:
                pem = self._from_secrets_manager()
            if not pem:
                if self.settings.mode == "cloud":
                    raise RuntimeError("no JWT signing key configured (PMCP_JWT_SECRET_ID)")
                pem = _new_pem()  # local mode: ephemeral key, tokens die with the process
            self._private = serialization.load_pem_private_key(pem.encode(), password=None)  # type: ignore[assignment]
            return self._private  # type: ignore[return-value]

    def _from_secrets_manager(self) -> str:
        import boto3
        from botocore.exceptions import ClientError

        sm = (self._session or boto3.Session(region_name=self.settings.platform_region)).client("secretsmanager")
        try:
            return sm.get_secret_value(SecretId=self.settings.jwt_secret_id)["SecretString"]
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ResourceNotFoundException":
                raise
        # First start: create the key. If a concurrent task created it first, use theirs.
        try:
            sm.create_secret(Name=self.settings.jwt_secret_id, SecretString=_new_pem(),
                             Description="pmcp JWT signing key (RS256)")
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ResourceExistsException":
                raise
        return sm.get_secret_value(SecretId=self.settings.jwt_secret_id)["SecretString"]

    @property
    def kid(self) -> str:
        der = self._load().public_key().public_bytes(serialization.Encoding.DER,
                                                    serialization.PublicFormat.SubjectPublicKeyInfo)
        return hashlib.sha256(der).hexdigest()[:16]

    def jwks(self) -> dict[str, Any]:
        nums = self._load().public_key().public_numbers()

        def b64(n: int) -> str:
            return base64.urlsafe_b64encode(n.to_bytes((n.bit_length() + 7) // 8, "big")).rstrip(b"=").decode()

        return {"keys": [{"kty": "RSA", "alg": "RS256", "use": "sig", "kid": self.kid, "n": b64(nums.n),
                          "e": b64(nums.e)}]}

    def sign(self, claims: dict[str, Any]) -> str:
        return jwt.encode(claims, self._load(), algorithm="RS256", headers={"kid": self.kid})

    def decode(self, token: str, audience: str) -> dict[str, Any]:
        return jwt.decode(token, self._load().public_key(), algorithms=["RS256"], audience=audience,
                          issuer=self.settings.jwt_issuer, options={"require": ["exp", "iat", "sub", "jti"]})


def _new_pem() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                             serialization.NoEncryption()).decode()


def issue_mcp_token(keys: KeyRing, settings: Settings, sub: str, email: str | None, groups: list[str],
                    ttl: int | None = None) -> tuple[str, dict[str, Any]]:
    now = int(time.time())
    claims = {
        "iss": settings.jwt_issuer, "aud": settings.jwt_audience_mcp, "sub": sub, "email": email,
        "groups": groups, "scope": "mcp", "iat": now, "exp": now + (ttl or settings.mcp_token_ttl_seconds),
        "jti": secrets.token_urlsafe(12),
    }
    return keys.sign(claims), claims


def issue_web_session(keys: KeyRing, settings: Settings, sub: str, email: str | None, groups: list[str],
                      auth_time: int) -> str:
    now = int(time.time())
    return keys.sign({
        "iss": settings.jwt_issuer, "aud": settings.jwt_audience_web, "sub": sub, "email": email, "groups": groups,
        "auth_time": auth_time, "iat": now, "exp": now + settings.web_session_ttl_seconds,
        "jti": secrets.token_urlsafe(12), "csrf": secrets.token_urlsafe(16),
    })


def revoke(store: Store, jti: str, exp: int) -> None:
    store.put_global(f"JTI#{jti}", {"revoked_at": int(time.time())}, ttl=exp + 60)


def is_revoked(store: Store, jti: str) -> bool:
    return store.get_global(f"JTI#{jti}") is not None


class McpTokenVerifier:
    """mcp.server.auth TokenVerifier implementation for pmcp-mcp tokens."""

    def __init__(self, keys: KeyRing, settings: Settings, store: Store) -> None:
        self.keys, self.settings, self.store = keys, settings, store

    async def verify_token(self, token: str):  # -> AccessToken | None
        from mcp.server.auth.provider import AccessToken

        try:
            claims = self.keys.decode(token, self.settings.jwt_audience_mcp)
        except jwt.PyJWTError:
            return None
        if "mcp" not in str(claims.get("scope", "")).split() or is_revoked(self.store, claims["jti"]):
            return None
        return AccessToken(token=token, client_id=claims["sub"], scopes=["mcp"], expires_at=claims["exp"],
                           subject=claims["sub"], claims={k: claims.get(k) for k in ("email", "groups", "jti")})
