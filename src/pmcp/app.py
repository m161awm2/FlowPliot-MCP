"""Process-wide wiring: settings, store, tenant sessions, prices, operation runner."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from pmcp.aws.tenancy import TenantSessions
from pmcp.catalog.pricing import PriceBook
from pmcp.config import Settings, get_settings
from pmcp.store import Store, build_store

if TYPE_CHECKING:
    from pmcp.services.operations import Runner


@dataclass
class App:
    settings: Settings
    store: Store
    sessions: TenantSessions
    prices: PriceBook
    runner: Runner = field(init=False)

    def __post_init__(self) -> None:
        from pmcp.services.operations import build_runner

        self.runner = build_runner(self)


def build_app(settings: Settings | None = None, store: Store | None = None, *, live_prices: bool = True) -> App:
    settings = settings or get_settings()
    store = store or build_store(settings.state_table, settings.platform_region)
    sessions = TenantSessions(settings, store)
    return App(settings=settings, store=store, sessions=sessions, prices=PriceBook(live=live_prices))
