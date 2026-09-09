"""PostgreSQL adapter for Q05's atomic Q02 enqueue/checkpoint transition."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg
from psycopg import Connection
from psycopg.types.json import Jsonb

from app.sync.policies import (
    CompetitionSyncPolicy,
    PolicyDenialReason,
    PostgresCompetitionSyncPolicyReader,
    SyncPolicyDenied,
    SyncPolicyGate,
    SyncWorkRequest,
)
from app.sync.repository import PeriodicWork
from app.sync.scheduler import FixtureScheduleSnapshot, PeriodicScheduleState


@dataclass(frozen=True)
class SchedulerEnqueueResult:
    work_item_id: int | None
    enqueued: bool
    checkpoint_advanced: bool


@dataclass(frozen=True)
class BudgetSnapshot:
    daily_limit: int | None
    daily_used: int | None
    minute_limit: int | None
    minute_used: int | None
    cooldown_until: datetime | None


@dataclass(frozen=True)
class SchedulerMaterializedSnapshot:
    policies: tuple[CompetitionSyncPolicy, ...]
    checkpoints: tuple[PeriodicScheduleState, ...]
    fixtures: tuple[FixtureScheduleSnapshot, ...]
    budget: BudgetSnapshot


class PostgresSchedulerSnapshotReader:
    """One read-only transaction for the exact preview/execute input snapshot."""

    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection

    def read(self) -> SchedulerMaterializedSnapshot:
        self._connection.execute("SET TRANSACTION READ ONLY")
        rows = self._connection.execute("SELECT provider_id,season_id FROM ops.competition_sync_policies ORDER BY provider_id,season_id").fetchall()
        policy_reader = PostgresCompetitionSyncPolicyReader(self._connection)
        policies = tuple(policy_reader.get(provider_id=int(row[0]), season_id=int(row[1])) for row in rows)
        checkpoints = tuple(PeriodicScheduleState(int(row[0]), int(row[1]), str(row[2]), row[3], row[4]) for row in self._connection.execute(
            "SELECT provider_id,season_id,work_type,last_scheduled_window_end,next_deadline FROM ops.sync_scheduler_checkpoints ORDER BY provider_id,season_id,work_type").fetchall())
        fixtures = tuple(FixtureScheduleSnapshot(int(row[0]), int(row[1]), int(row[2]), row[3], str(row[4]), row[5], row[6], row[7], row[8]) for row in self._connection.execute(
            """SELECT ref.fixture_id,ref.provider_id,fixture.season_id,fixture.kickoff_at,fixture.lifecycle_state::text,
                      fixture.terminal_status_observed_at,fixture.result_finalized_at,reconciliation.terminal_observed_at,reconciliation.eligible_at
                 FROM source.fixture_provider_refs ref JOIN football.fixtures fixture ON fixture.id=ref.fixture_id
                 LEFT JOIN ops.fixture_reconciliation_state reconciliation ON reconciliation.fixture_id=fixture.id
                 JOIN ops.competition_sync_policies policy ON policy.provider_id=ref.provider_id AND policy.season_id=fixture.season_id
                 ORDER BY ref.provider_id,fixture.season_id,ref.fixture_id""").fetchall())
        row = self._connection.execute("""SELECT config.daily_limit,state.daily_used,config.minute_limit,state.minute_used,state.cooldown_until
                                          FROM ops.api_football_budget_config config LEFT JOIN ops.api_football_budget_state state ON state.singleton=true
                                         WHERE config.singleton=true""").fetchone()
        budget = BudgetSnapshot(None, None, None, None, None) if row is None else BudgetSnapshot(*row)
        return SchedulerMaterializedSnapshot(tuple(policy for policy in policies if policy is not None), checkpoints, fixtures, budget)


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
        try:
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
        except psycopg.Error as exc:
            if exc.sqlstate == "55000":
                message = str(exc)
                if "policy instance changed" in message:
                    raise SyncPolicyDenied(PolicyDenialReason.INSTANCE_CHANGED) from exc
                if "policy version changed" in message:
                    raise SyncPolicyDenied(PolicyDenialReason.VERSION_CHANGED) from exc
                raise SyncPolicyDenied(PolicyDenialReason.MISSING) from exc
            raise
        if row is None:
            raise RuntimeError("scheduler enqueue/checkpoint did not return a result")
        return SchedulerEnqueueResult(None if row[0] is None else int(row[0]), bool(row[1]), bool(row[2]))
