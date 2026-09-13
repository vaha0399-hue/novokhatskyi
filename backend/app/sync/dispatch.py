"""Shared Q03 dispatch registry used by producers and repeatable workers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from app.sync.policies import AuthorizedSyncWork
from app.sync.repository import LeasedWorkItem

if TYPE_CHECKING:
    from app.sync.provenance import ProviderProvenance
    from app.sync.worker import AtomicWorkTransaction, WorkResult


@runtime_checkable
class Q03Dispatch(Protocol):
    def fetch(self, item: LeasedWorkItem, authorization: AuthorizedSyncWork) -> "WorkResult": ...
    def apply_result(self, writer: "AtomicWorkTransaction", item: LeasedWorkItem, result: "WorkResult") -> None: ...


class Q03DispatchRegistry:
    def __init__(self, handlers: Mapping[str, object]) -> None:
        self._handlers = {
            work_type: handler for work_type, handler in handlers.items()
            if isinstance(handler, Q03Dispatch) and callable(handler.fetch) and callable(handler.apply_result)
        }

    def available_work_types(self) -> frozenset[str]:
        return frozenset(self._handlers)

    def fetch(self, item: LeasedWorkItem, authorization: AuthorizedSyncWork) -> "WorkResult":
        return self._handler(item).fetch(item, authorization)

    def __call__(self, item: LeasedWorkItem, authorization: AuthorizedSyncWork) -> "WorkResult":
        return self.fetch(item, authorization)

    def replay(
        self, item: LeasedWorkItem, authorization: AuthorizedSyncWork, provenance: "ProviderProvenance",
    ) -> "WorkResult | None":
        replay = getattr(self._handler(item), "replay", None)
        return replay(item, authorization, provenance) if callable(replay) else None

    def apply_result(self, writer: "AtomicWorkTransaction", item: LeasedWorkItem, result: "WorkResult") -> None:
        self._handler(item).apply_result(writer, item, result)

    def _handler(self, item: LeasedWorkItem) -> Q03Dispatch:
        try:
            return self._handlers[item.job_type]
        except KeyError as exc:
            raise ValueError(f"no Q03 dispatch registered for {item.job_type}") from exc
