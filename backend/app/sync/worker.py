"""Opt-in Q03 runner for fenced repeatable work; legacy workers do not use it."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from psycopg import Connection
from psycopg.types.json import Jsonb

from app.sync.policies import AuthorizedSyncWork, SyncPolicyDenied, SyncPolicyGate, SyncWorkRequest
from app.sync.repository import LeasedWorkItem, PostgresSyncRepository


class LeaseLost(RuntimeError):
    """The result must not be applied because its fence is no longer current."""


class AtomicWorkTransaction:
    """Restricted writer capability: no commit and no connection factory."""
    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection

    def execute(self, query: str, params: Any = None) -> Any:
        return self._connection.execute(query, params)

    def transaction(self) -> Any:
        """Permit canonical writers' nested savepoints, never a top-level commit."""
        return self._connection.transaction()


@dataclass(frozen=True)
class WorkResult:
    checkpoint: Mapping[str, Any]
    dependent_work: tuple[Callable[[AtomicWorkTransaction], None], ...] = ()


class FetchExecutor(Protocol):
    def __call__(self, item: LeasedWorkItem, authorization: AuthorizedSyncWork) -> WorkResult: ...


class RepeatableSyncWorker:
    """Runs HTTP/fetch work outside a transaction, then fences the write txn.

    ``apply_result`` gets a restricted capability, not a connection. It cannot
    commit or open another connection; this is the integration boundary for
    canonical writers until those writers are adapted for Q03.
    """
    def __init__(self, connection: Connection[Any], policy_gate: SyncPolicyGate, owner: str) -> None:
        self._connection, self._gate, self._owner = connection, policy_gate, owner
        self.repository = PostgresSyncRepository(connection, policy_gate)

    def run_once(self, fetch: FetchExecutor, apply_result: Callable[[AtomicWorkTransaction, LeasedWorkItem, WorkResult], None], *, max_attempts: int = 5) -> bool:
        item = self.repository.claim_next(self._owner, max_attempts=max_attempts)
        if item is None:
            return False
        try:
            authorization = self._authorization(item)
        except SyncPolicyDenied as exc:
            self.repository.requeue(item, self._owner, {}, str(exc), contract_error=True)
            return True
        # Fetchers may wait on HTTP; no transaction is active here.
        result = fetch(item, authorization)
        with self._connection.transaction():
            guarded = self._connection.execute(
                "SELECT ops.guard_repeatable_sync_work_item_lease(%s,%s,%s)",
                (item.id, self._owner, item.lease_token),
            ).fetchone()
            if guarded is None or guarded[0] is not True:
                raise LeaseLost("repeatable work-item lease was lost before applying its result")
            writer = AtomicWorkTransaction(self._connection)
            apply_result(writer, item, result)
            for enqueue_dependent in result.dependent_work:
                enqueue_dependent(writer)
            completed = self._connection.execute(
                "SELECT ops.complete_repeatable_sync_work_item(%s,%s,%s,%s)",
                (item.id, self._owner, item.lease_token, Jsonb(dict(result.checkpoint))),
            ).fetchone()
            if completed is None or completed[0] is not True:
                raise LeaseLost("repeatable work-item lease was lost before completion")
        return True

    def _authorization(self, item: LeasedWorkItem) -> AuthorizedSyncWork:
        policy = item.scope.get("_sync_policy")
        if not isinstance(policy, Mapping):
            raise ValueError("repeatable work item has no policy metadata")
        try:
            request = SyncWorkRequest(int(policy["provider_id"]), int(policy["season_id"]), str(policy["work_type"]))
            current = self._gate.before_enqueue(request)
            authorization = AuthorizedSyncWork(request, int(policy["instance_id"]), int(policy["version"]),
                current.coverage, current.refresh_interval)
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("repeatable work item has malformed policy metadata") from exc
        return self._gate.before_execution(authorization)
