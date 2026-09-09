"""PostgreSQL adapter for Q05's atomic Q02 enqueue/checkpoint transition."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
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
from app.sync.repository import PeriodicWork, RecalculationWork
from app.sync.scheduler import AnalyticsInputSnapshot, FixtureScheduleSnapshot, PeriodicScheduleState, SeasonScheduleSnapshot


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
    daily_window: date | None = None
    minute_window: datetime | None = None


@dataclass(frozen=True)
class SchedulerMaterializedSnapshot:
    policies: tuple[CompetitionSyncPolicy, ...]
    checkpoints: tuple[PeriodicScheduleState, ...]
    fixtures: tuple[FixtureScheduleSnapshot, ...]
    budget: BudgetSnapshot
    seasons: tuple[SeasonScheduleSnapshot, ...]
    analytics_inputs: tuple[AnalyticsInputSnapshot, ...]
    input_gaps: tuple[str, ...]


class PostgresSchedulerSnapshotReader:
    """One read-only transaction for the exact preview/execute input snapshot."""

    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection

    def read(self, *, now: datetime | None = None) -> SchedulerMaterializedSnapshot:
        # Read Committed takes a fresh snapshot per SELECT. The scheduler must
        # calculate from one materialized view of policy, state, fixtures and
        # budget, so set this before its first data statement.
        self._connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        current = (now or datetime.now(UTC)).astimezone(UTC)
        day_start = datetime.combine(current.date(), time.min, tzinfo=UTC)
        next_day = day_start + timedelta(days=1)
        rows = self._connection.execute("SELECT provider_id,season_id FROM ops.competition_sync_policies ORDER BY provider_id,season_id").fetchall()
        policy_reader = PostgresCompetitionSyncPolicyReader(self._connection)
        policies = tuple(policy_reader.get(provider_id=int(row[0]), season_id=int(row[1])) for row in rows)
        checkpoints = tuple(PeriodicScheduleState(int(row[0]), int(row[1]), str(row[2]), row[3], row[4]) for row in self._connection.execute(
            "SELECT provider_id,season_id,work_type,last_scheduled_window_end,next_deadline FROM ops.sync_scheduler_checkpoints ORDER BY provider_id,season_id,work_type").fetchall())
        fixture_rows = self._connection.execute(
            """SELECT ref.fixture_id,ref.provider_id,fixture.season_id,fixture.kickoff_at,fixture.lifecycle_state::text,
                      fixture.terminal_status_observed_at,fixture.result_finalized_at,reconciliation.terminal_observed_at,
                      fixture.result_available_at,statistics_coverage.next_retry_at,statistics_coverage.attempts,
                      statistics_coverage.coverage_state::text,statistics_facts.pair_ready
                 FROM source.fixture_provider_refs ref JOIN football.fixtures fixture ON fixture.id=ref.fixture_id
                 LEFT JOIN ops.fixture_reconciliation_state reconciliation ON reconciliation.fixture_id=fixture.id
                 LEFT JOIN football.fixture_statistics_coverage statistics_coverage ON statistics_coverage.fixture_id=fixture.id
                 LEFT JOIN LATERAL (
                     SELECT count(*) = 2
                         AND count(DISTINCT statistics.team_id) = 2
                         AND coalesce(bool_and(statistics.team_id IN (fixture.home_team_id,fixture.away_team_id)),false)
                         AND coalesce(bool_and(statistics.last_source_fetch_id IS NOT NULL),false) AS pair_ready
                       FROM football.fixture_team_statistics statistics
                      WHERE statistics.fixture_id=fixture.id
                 ) statistics_facts ON true
                 JOIN ops.competition_sync_policies policy ON policy.provider_id=ref.provider_id AND policy.season_id=fixture.season_id
                 ORDER BY ref.provider_id,fixture.season_id,ref.fixture_id""").fetchall()
        fixtures = tuple(
            FixtureScheduleSnapshot(
                fixture_id=int(row[0]), provider_id=int(row[1]), season_id=int(row[2]), kickoff_at=row[3], lifecycle_state=str(row[4]),
                terminal_observed_at=row[5], result_finalized_at=row[6], first_terminal_observed_at=row[7],
                statistics_eligible_at=row[8] if row[11] is None and not bool(row[12]) else None,
                statistics_attempts=int(row[10] or 0), statistics_max_attempts=5, statistics_completed=bool(row[12]),
                statistics_retry_at=row[9] if row[11] is not None and not bool(row[12]) else None,
            )
            for row in fixture_rows
        )
        analytics_inputs = tuple(AnalyticsInputSnapshot(int(row[0]), int(row[1]), f"fixture:{int(row[2])}", int(row[3]), row[4], row[5], int(row[2]), row[6]) for row in self._connection.execute(
            """SELECT ref.provider_id,fixture.season_id,fixture.id,analytics_window.latest_source_fetch_id,
                      analytics_window.observed_at,analytics_window.deadline,analytics_window.window_end
                 FROM source.fixture_provider_refs ref JOIN football.fixtures fixture ON fixture.id=ref.fixture_id
                 JOIN ops.fixture_analytics_recalculation_windows analytics_window ON analytics_window.fixture_id=fixture.id
                 JOIN ops.competition_sync_policies policy ON policy.provider_id=ref.provider_id AND policy.season_id=fixture.season_id
                WHERE analytics_window.accepted_at IS NULL
                ORDER BY ref.provider_id,fixture.season_id,fixture.id,analytics_window.window_end""").fetchall())
        seasons = tuple(SeasonScheduleSnapshot(int(row[0]), int(row[1]),
            None if row[2] is None else datetime.combine(row[2], time.min, tzinfo=UTC), bool(row[3])) for row in self._connection.execute(
                """SELECT policy.provider_id,policy.season_id,season.starts_on,
                          coalesce(bool_or(fixture.kickoff_at >= %s AND fixture.kickoff_at < %s
                              AND fixture.lifecycle_state NOT IN ('cancelled','abandoned')),false)
                     FROM ops.competition_sync_policies policy
                     JOIN football.seasons season ON season.id=policy.season_id
                     LEFT JOIN football.fixtures fixture ON fixture.season_id=season.id
                    GROUP BY policy.provider_id,policy.season_id,season.starts_on
                    ORDER BY policy.provider_id,policy.season_id""", (day_start, next_day)).fetchall())
        row = self._connection.execute("""SELECT config.daily_limit,state.daily_used,config.minute_limit,state.minute_used,state.cooldown_until,
                                                 state.daily_window,state.minute_window
                                          FROM ops.api_football_budget_config config LEFT JOIN ops.api_football_budget_state state ON state.singleton=true
                                         WHERE config.singleton=true""").fetchone()
        budget = BudgetSnapshot(None, None, None, None, None) if row is None else BudgetSnapshot(*row)
        missing_analytics_history = self._connection.execute(
            """SELECT EXISTS(
                    SELECT 1 FROM source.fixture_provider_refs ref
                    JOIN football.fixtures fixture ON fixture.id=ref.fixture_id
                    JOIN ops.competition_sync_policies policy ON policy.provider_id=ref.provider_id AND policy.season_id=fixture.season_id
                   WHERE fixture.last_source_fetch_id IS NOT NULL
                     AND NOT EXISTS (SELECT 1 FROM ops.fixture_analytics_recalculation_windows analytics_window WHERE analytics_window.fixture_id=fixture.id)
                )""").fetchone()
        gaps = ["season_expected_start_unavailable"]
        if missing_analytics_history is not None and bool(missing_analytics_history[0]):
            gaps.append("analytics_window_history_unavailable")
        return SchedulerMaterializedSnapshot(tuple(policy for policy in policies if policy is not None), checkpoints, fixtures, budget, seasons, analytics_inputs,
                                             tuple(gaps))


class PostgresSchedulerRepository:
    """Opt-in Q05 writer; callers must already own the outer transaction."""

    def __init__(self, connection: Connection[Any], policy_gate: SyncPolicyGate) -> None:
        self._connection = connection
        self._gate = policy_gate

    @staticmethod
    def _raise_policy_denial(exc: psycopg.Error) -> None:
        if exc.sqlstate != "55000":
            raise exc
        message = str(exc)
        for marker, reason in (("policy no longer exists", PolicyDenialReason.MISSING),
                               ("policy instance changed", PolicyDenialReason.INSTANCE_CHANGED),
                               ("policy version changed", PolicyDenialReason.VERSION_CHANGED),
                               ("policy changed since calculation", PolicyDenialReason.VERSION_CHANGED)):
            if marker in message:
                raise SyncPolicyDenied(reason) from exc
        raise exc

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
        # Q01 remains an early fail-closed guard. The fingerprint below is the
        # one captured by preview, however: replacing it with this fresh read
        # would let a v1 calculation survive a v2 policy change.
        self._gate.before_enqueue(SyncWorkRequest(work.provider_id, work.season_id, work.work_type))
        scope = dict(work.scope)
        fingerprint = scope.get("_sync_policy")
        if not isinstance(fingerprint, dict) or (
            fingerprint.get("provider_id"), fingerprint.get("season_id"), fingerprint.get("work_type")
        ) != (work.provider_id, work.season_id, work.work_type):
            raise ValueError("scheduler work has no matching preview policy fingerprint")
        if not isinstance(fingerprint.get("instance_id"), int) or not isinstance(fingerprint.get("version"), int):
            raise ValueError("scheduler work has malformed preview policy fingerprint")
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
            self._raise_policy_denial(exc)
        if row is None:
            raise RuntimeError("scheduler enqueue/checkpoint did not return a result")
        return SchedulerEnqueueResult(None if row[0] is None else int(row[0]), bool(row[1]), bool(row[2]))

    def enqueue_event(self, *, run_id: int, work: PeriodicWork, available_at: datetime) -> SchedulerEnqueueResult:
        """Atomically mark one fixture/event identity without season checkpoint reuse."""
        if run_id <= 0:
            raise ValueError("run id must be positive")
        self._gate.before_enqueue(SyncWorkRequest(work.provider_id, work.season_id, work.work_type))
        scope = dict(work.scope)
        fingerprint = scope.get("_sync_policy")
        if not isinstance(fingerprint, dict) or (
            fingerprint.get("provider_id"), fingerprint.get("season_id"), fingerprint.get("work_type")
        ) != (work.provider_id, work.season_id, work.work_type):
            raise ValueError("scheduler work has no matching preview policy fingerprint")
        try:
            row = self._connection.execute(
                "SELECT * FROM ops.enqueue_repeatable_sync_work_and_event_checkpoint("
                "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (run_id, work.stable_key(), Jsonb(scope), work.work_type, work.priority, available_at,
                 work.stable_key(), work.entity_key, work.execution_key or f"entity:{work.provider_id}:{work.season_id}:{work.entity_key}",
                 work.provider_id, work.season_id),
            ).fetchone()
        except psycopg.Error as exc:
            self._raise_policy_denial(exc)
        if row is None:
            raise RuntimeError("scheduler event enqueue/checkpoint did not return a result")
        return SchedulerEnqueueResult(None if row[0] is None else int(row[0]), bool(row[1]), bool(row[2]))

    def enqueue_recalculation(self, *, run_id: int, work: RecalculationWork, available_at: datetime) -> SchedulerEnqueueResult:
        self._gate.before_enqueue(SyncWorkRequest(work.provider_id, work.season_id, work.work_type))
        window_fixture_id = work.scope.get("_analytics_window_fixture_id")
        window_end = work.scope.get("_analytics_window_end")
        if window_fixture_id is not None and (not isinstance(window_fixture_id, int) or isinstance(window_fixture_id, bool) or window_fixture_id <= 0):
            raise ValueError("analytics work has malformed window fixture id")
        if window_end is not None and not isinstance(window_end, str):
            raise ValueError("analytics work has malformed window end")
        try:
            row = self._connection.execute("SELECT * FROM ops.enqueue_repeatable_analytics_work_and_window_checkpoint(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (run_id,work.stable_key(),Jsonb(dict(work.scope)),work.work_type,work.priority,available_at,work.stable_key(),work.entity_key,work.execution_key or f"entity:{work.provider_id}:{work.season_id}:{work.entity_key}",work.provider_id,work.season_id,str(work.input_version),window_fixture_id,window_end,None if window_fixture_id is None else str(work.input_version))).fetchone()
        except psycopg.Error as exc:
            self._raise_policy_denial(exc)
        if row is None: raise RuntimeError("analytics enqueue/checkpoint did not return a result")
        return SchedulerEnqueueResult(None if row[0] is None else int(row[0]),bool(row[1]),bool(row[2]))
