from __future__ import annotations

from typing import TYPE_CHECKING

from pmcp.domain.contract import DataContract, DataContractBody
from pmcp.errors import ErrorCode, PmcpError
from pmcp.ids import canonical_json, new_id

if TYPE_CHECKING:
    from pmcp.app import App


def _rid(contract_id: str, version: int) -> str:
    return f"{contract_id}#v{version:04d}"


def load_contract(app: App, owner: str, contract_id: str, version: int | None = None) -> DataContract:
    if version is not None:
        return app.store.require(DataContract, owner, _rid(contract_id, version))
    latest = app.store.list(DataContract, owner, prefix=f"{contract_id}#v", limit=1)
    if not latest:
        raise PmcpError(ErrorCode.NOT_FOUND, f"contract '{contract_id}' not found")
    return latest[0]


def define(app: App, owner: str, body: DataContractBody, contract_id: str | None,
           expected_version: int | None) -> tuple[DataContract, bool]:
    """Create a contract or add a version. Returns (contract, created_new_version)."""
    if contract_id is None:
        cid = new_id("dc")
        rec = DataContract(id=_rid(cid, 1), owner=owner, contract_id=cid, contract_version=1, body=body)
        return app.store.create(rec), True
    current = load_contract(app, owner, contract_id)
    if expected_version is not None and expected_version != current.contract_version:
        raise PmcpError(ErrorCode.CONFLICT, f"latest version is {current.contract_version}, not {expected_version}",
                        next_actions=["Re-read the contract and merge your change"])
    if canonical_json(current.body.model_dump(mode="json")) == canonical_json(body.model_dump(mode="json")):
        return current, False
    v = current.contract_version + 1
    rec = DataContract(id=_rid(contract_id, v), owner=owner, contract_id=contract_id, contract_version=v, body=body,
                       previous_version=current.contract_version)
    return app.store.create(rec), True
