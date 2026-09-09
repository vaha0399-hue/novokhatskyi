"""PostgreSQL adapter for Q05's atomic Q02 enqueue/checkpoint transition."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from psycopg import Connection
from psycopg.types.json import Jsonb

from app.sync.policies import SyncPolicyGate, SyncWorkRequest
from app.sync.repository import PeriodicWork
from app.sync.scheduler import PeriodicScheduleState


@dataclass(frozen=True)
class SchedulerEnqueueResult:
    work_item_id: int | None
    enqueued: bool
    checkpoint_advanced: bool


class PostgresSchedulerRepository:
    """Opt-in Q05 writer; callers must already own the outer transaction."""

    def __init__(self, connection: Connection[Any], policy_gate: SyncPolicyGate) -> None:
        self._connection = connection
        self._gate = policy_gate

    def enqueue_and_advance(
        self,
        *,
        run_id: int,
        work: PeriodicWork,
        expected_state: PeriodicScheduleState | None,
        next_state: PeriodicScheduleState,
        available_at: datetime,
    ) -> SchedulerEnqueueResult:
        if run_id <= 0:
            raise ValueError("run id must be positive")
        if next_state.key() != (work.provider_id, work.season_id, work.work_type):
            raise ValueError("scheduler checkpoint does not match work scope")
        if expected_state is not None and expected_state.key() != next_state.key():
            raise ValueError("scheduler checkpoint transition changes scope")
        authorization = self._gate.before_enqueue(SyncWorkRequest(work.provider_id, work.season_id, work.work_type))
        scope = dict(work.scope)
        scope["_sync_policy"] = {
            "provider_id": work.provider_id, "season_id": work.season_id, "work_type": work.work_type,
            "instance_id": authorization.policy_instance_id, "version": authorization.policy_version,
        }
        row = self._connection.execute(
            "SELECT * FROM ops.enqueue_repeatable_sync_work_and_checkpoint("
            "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (run_id, work.stable_key(), Jsonb(scope), work.work_type, work.priority, available_at,
             work.stable_key(), work.entity_key, work.execution_key or f"entity:{work.provider_id}:{work.season_id}:{work.entity_key}",
             work.provider_id, work.season_id,
             None if expected_state is None else expected_state.last_scheduled_window_end,
             None if expected_state is None else expected_state.next_deadline,
             next_state.last_scheduled_window_end, next_state.next_deadline),
        ).fetchone()
        if row is None:
            raise RuntimeError("scheduler enqueue/checkpoint did not return a result")
        return SchedulerEnqueueResult(None if row[0] is None else int(row[0]), bool(row[1]), bool(row[2]))
