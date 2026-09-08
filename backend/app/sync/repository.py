"""Q02 repository contract for durable work in ``ops.sync_work_items``.

This is deliberately an opt-in adapter: legacy Cup and seasonal bootstrap
workers keep their reviewed run-scoped paths until their production cutover.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import json
from typing import Any, Mapping

from psycopg import Connection
from psycopg.types.json import Jsonb

from app.sync.policies import AuthorizedSyncWork, SyncPolicyGate, SyncWorkRequest


def _part(value: object, name: str) -> str:
    if not isinstance(value, (str, int)) or not str(value).strip():
        raise ValueError(f"{name} must be a nonblank string or integer")
    return str(value)


def _stable_key(kind: str, *components: str | int) -> str:
    """Encode identity components without delimiter ambiguity.

    The database treats this opaque value as the durable uniqueness key.  A
    compact JSON array retains component boundaries and scalar types, unlike
    the older colon-joined representation (where ``a:b`` and ``a``, ``b``
    could collide).
    """
    return json.dumps((kind, *components), ensure_ascii=True, separators=(",", ":"))


@dataclass(frozen=True)
class PeriodicWork:
    """Identity of an idempotent periodic observation for one closed window."""

    provider_id: int
    season_id: int
    work_type: str
    entity_key: str
    window_start: datetime
    window_end: datetime
    priority: int
    scope: Mapping[str, Any]
    execution_key: str | None = None

    def __post_init__(self) -> None:
        if self.window_start.tzinfo is None or self.window_start.utcoffset() is None:
            raise ValueError("window start must be timezone-aware")
        if self.window_end.tzinfo is None or self.window_end.utcoffset() is None:
            raise ValueError("window end must be timezone-aware")
        if self.window_start >= self.window_end:
            raise ValueError("periodic window start must precede its end")

    def stable_key(self) -> str:
        return _stable_key(
            "periodic",
            _part(self.work_type, "work type"),
            self.provider_id,
            self.season_id,
            _part(self.entity_key, "entity"),
            self.window_start.astimezone(UTC).isoformat(),
            self.window_end.astimezone(UTC).isoformat(),
        )


@dataclass(frozen=True)
class RecalculationWork:
    """Identity of a recalculation from a particular accepted input version."""

    provider_id: int
    season_id: int
    work_type: str
    entity_key: str
    input_version: str | int
    priority: int
    scope: Mapping[str, Any]
    execution_key: str | None = None

    def stable_key(self) -> str:
        return _stable_key(
            "recalculation",
            _part(self.work_type, "work type"),
            self.provider_id,
            self.season_id,
            _part(self.entity_key, "entity"),
            _part(self.input_version, "input version"),
        )


@dataclass(frozen=True)
class EnqueueResult:
    work_item_id: int
    enqueued: bool
    authorization: AuthorizedSyncWork


@dataclass(frozen=True)
class LeasedWorkItem:
    """A fenced Q03 lease; its token is required for every mutation."""
    id: int
    run_id: int
    scope_key: str
    scope: Mapping[str, Any]
    checkpoint: Mapping[str, Any]
    attempts: int
    job_type: str
    priority: int
    stable_key: str
    entity_key: str
    execution_key: str
    lease_token: int


class PostgresSyncRepository:
    """Policy-gated enqueue facade over the existing control-plane queue."""

    def __init__(self, connection: Connection[Any], policy_gate: SyncPolicyGate) -> None:
        self._connection = connection
        self._policy_gate = policy_gate

    def enqueue_periodic(self, run_id: int, work: PeriodicWork, *, available_at: datetime) -> EnqueueResult:
        return self._enqueue(run_id, work, available_at=available_at)

    def enqueue_recalculation(self, run_id: int, work: RecalculationWork, *, available_at: datetime) -> EnqueueResult:
        return self._enqueue(run_id, work, available_at=available_at)

    def _enqueue(self, run_id: int, work: PeriodicWork | RecalculationWork, *, available_at: datetime) -> EnqueueResult:
        if run_id <= 0:
            raise ValueError("run id must be positive")
        authorization = self._policy_gate.before_enqueue(
            SyncWorkRequest(provider_id=work.provider_id, season_id=work.season_id, work_type=work.work_type)
        )
        stable_key = work.stable_key()
        entity_key = _part(work.entity_key, "entity")
        # Conservative default: any writer for an entity is mutually exclusive.
        # Callers may name a narrower reviewed conflict domain explicitly.
        execution_key = work.execution_key or f"entity:{work.provider_id}:{work.season_id}:{entity_key}"
        scope = dict(work.scope)
        # Stored authorization is intentionally internal metadata: the worker
        # rereads and version-checks it before it invokes an executor.
        scope["_sync_policy"] = {"provider_id": work.provider_id, "season_id": work.season_id,
                                 "work_type": work.work_type, "instance_id": authorization.policy_instance_id,
                                 "version": authorization.policy_version}
        row = self._connection.execute(
            "SELECT * FROM ops.enqueue_repeatable_sync_work_item(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (run_id, stable_key, Jsonb(scope), work.work_type, work.priority, available_at,
             stable_key, entity_key, execution_key),
        ).fetchone()
        if row is None:
            raise RuntimeError("repeatable queue enqueue did not return a result")
        return EnqueueResult(int(row[0]), bool(row[1]), authorization)

    def claim_next(self, owner: str, *, lease_duration: str = "5 minutes", max_attempts: int = 5) -> LeasedWorkItem | None:
        row = self._connection.execute("SELECT * FROM ops.claim_next_repeatable_sync_work_item_with_lease(%s,%s::interval,%s)",
            (owner, lease_duration, max_attempts)).fetchone()
        if row is None:
            return None
        return LeasedWorkItem(int(row[0]), int(row[1]), str(row[2]), dict(row[3]), dict(row[4]), int(row[5]),
            str(row[6]), int(row[7]), str(row[8]), str(row[9]), str(row[10]), int(row[11]))

    def heartbeat(self, item: LeasedWorkItem, owner: str, *, lease_duration: str = "5 minutes") -> bool:
        return self._mutates("heartbeat_repeatable_sync_work_item", (item.id, owner, item.lease_token, lease_duration))

    def checkpoint(self, item: LeasedWorkItem, owner: str, checkpoint: Mapping[str, Any]) -> bool:
        return self._mutates("checkpoint_repeatable_sync_work_item", (item.id, owner, item.lease_token, Jsonb(dict(checkpoint))))

    def requeue(self, item: LeasedWorkItem, owner: str, checkpoint: Mapping[str, Any], error: str, *, delay: str = "0 seconds", contract_error: bool = False) -> bool:
        return self._mutates("requeue_repeatable_sync_work_item", (item.id, owner, item.lease_token, Jsonb(dict(checkpoint)), error, delay, contract_error))

    def retry_quarantined(self, item_id: int) -> bool:
        return self._mutates("retry_quarantined_repeatable_sync_work_item", (item_id,))

    def _mutates(self, function: str, args: tuple[Any, ...]) -> bool:
        placeholders = ",".join("%s" for _ in args)
        row = self._connection.execute(f"SELECT ops.{function}({placeholders})", args).fetchone()
        return row is not None and row[0] is True
