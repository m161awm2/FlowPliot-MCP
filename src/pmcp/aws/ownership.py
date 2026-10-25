"""Ownership tags. Pattern adapted from awslabs dataprocessing `AwsHelper.is_resource_mcp_managed`,
re-implemented for pmcp's own tag keys and per-user ownership."""

from __future__ import annotations

from collections.abc import Iterable, Mapping

TAG_MANAGED = "managed-by"
TAG_MANAGED_VALUE = "pipeline-mcp"
TAG_OWNER = "pmcp:owner"
TAG_FLOW = "pmcp:flow"
TAG_PLAN = "pmcp:plan"

NAME_PREFIX = "pmcp-"


def flow_tags(owner: str, flow_id: str, plan_id: str | None = None) -> dict[str, str]:
    tags = {TAG_MANAGED: TAG_MANAGED_VALUE, TAG_OWNER: owner, TAG_FLOW: flow_id}
    if plan_id:
        tags[TAG_PLAN] = plan_id
    return tags


def as_cfn_tags(tags: Mapping[str, str]) -> list[dict[str, str]]:
    return [{"Key": k, "Value": v} for k, v in tags.items()]


def tag_map(tags: Iterable[Mapping[str, str]] | Mapping[str, str] | None) -> dict[str, str]:
    if not tags:
        return {}
    if isinstance(tags, Mapping):
        return dict(tags)
    return {t.get("Key", t.get("key", "")): t.get("Value", t.get("value", "")) for t in tags}


def is_managed_by(tags: Iterable[Mapping[str, str]] | Mapping[str, str] | None, owner: str) -> bool:
    m = tag_map(tags)
    return m.get(TAG_MANAGED) == TAG_MANAGED_VALUE and m.get(TAG_OWNER) == owner
