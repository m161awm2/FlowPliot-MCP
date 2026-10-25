from __future__ import annotations

from typing import ClassVar

from pydantic import BaseModel, ConfigDict, Field

from pmcp.ids import iso


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class Stored(Model):
    """A record persisted in the state store, scoped to one owner (the Cognito `sub`)."""

    KIND: ClassVar[str]

    id: str
    owner: str
    version: int = 0
    created_at: str = Field(default_factory=iso)
    updated_at: str = Field(default_factory=iso)


class AwsTarget(Model):
    account_id: str = Field(pattern=r"^\d{12}$", description="12-digit AWS account id of a connected account.")
    region: str = Field(pattern=r"^[a-z]{2}(-gov)?-[a-z]+-\d$", description="AWS region, e.g. ap-southeast-2.")
