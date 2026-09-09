from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

from app.sync.policies import (
    AuthorizedSyncWork, CoverageObservation, CoverageState, PolicyDenialReason, RefreshInterval, SyncPolicyDenied,
)
from app.sync.repository import PeriodicWork
from app.sync.scheduler import PeriodicScheduleState
from app.sync.scheduler_repository import PostgresSchedulerRepository


class _Gate:
    def __init__(self, denied: bool = False) -> None:
        self.denied = denied

    def before_enqueue(self, request):
        if self.denied:
            raise SyncPolicyDenied(PolicyDenialReason.DISABLED)
        return AuthorizedSyncWork(request, 7, 3, CoverageObservation(CoverageState.COVERED, date(2026, 9, 1)), RefreshInterval(1, "hour"))


class _Cursor:
    def fetchone(self):
        return (41, True, True)


class _Connection:
    def __init__(self) -> None:
        self.calls = []

    def execute(self, sql, params):
        self.calls.append((sql, params))
        return _Cursor()


def _work() -> PeriodicWork:
    start = datetime(2026, 9, 8, tzinfo=UTC)
    return PeriodicWork(7, 101, "calendar_refresh", "season:101", start, start + timedelta(hours=1), 3, {})


def _state(end: datetime) -> PeriodicScheduleState:
    return PeriodicScheduleState(7, 101, "calendar_refresh", end, end + timedelta(hours=1))


def test_atomic_scheduler_adapter_uses_q02_enqueue_inside_checkpoint_transition() -> None:
    connection = _Connection()
    work = _work()
    result = PostgresSchedulerRepository(connection, _Gate()).enqueue_and_advance(
        run_id=5, work=work, expected_state=_state(work.window_start), next_state=_state(work.window_end), available_at=work.window_end,
    )
    assert result.work_item_id == 41 and result.enqueued and result.checkpoint_advanced
    sql, params = connection.calls[0]
    assert "enqueue_repeatable_sync_work_and_checkpoint" in sql
    assert params[6] == work.stable_key() and params[13] == work.window_end


def test_policy_denial_touches_neither_queue_nor_checkpoint() -> None:
    connection = _Connection()
    work = _work()
    with pytest.raises(SyncPolicyDenied):
        PostgresSchedulerRepository(connection, _Gate(denied=True)).enqueue_and_advance(
            run_id=5, work=work, expected_state=_state(work.window_start), next_state=_state(work.window_end), available_at=work.window_end,
        )
    assert connection.calls == []
