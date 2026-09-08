from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from app.sync.policies import (
    AuthorizedSyncWork,
    CoverageObservation,
    CoverageState,
    PolicyDenialReason,
    RefreshInterval,
    SyncPolicyDenied,
)
from app.sync.repository import PeriodicWork, PostgresSyncRepository, RecalculationWork


class _Gate:
    def __init__(self, denied: bool = False) -> None:
        self.denied = denied
        self.requests: list[Any] = []

    def before_enqueue(self, request):
        self.requests.append(request)
        if self.denied:
            raise SyncPolicyDenied(PolicyDenialReason.DISABLED)
        return AuthorizedSyncWork(request, 1, 2, CoverageObservation(CoverageState.COVERED, None), RefreshInterval(1, "minute"))


class _Cursor:
    def fetchone(self):
        return (71, True)


class _Connection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, sql: str, params: tuple[Any, ...]):
        self.calls.append((sql, params))
        return _Cursor()


def _periodic() -> PeriodicWork:
    now = datetime(2026, 9, 8, tzinfo=UTC)
    return PeriodicWork(7, 101, "fixtures", "fixture:42", now, now + timedelta(minutes=5), 3, {"fixture_id": 42})


def test_keys_include_provider_season_window_and_input_version() -> None:
    periodic = _periodic()
    assert periodic.stable_key() == "periodic:fixtures:7:101:fixture:42:2026-09-08T00:00:00+00:00:2026-09-08T00:05:00+00:00"
    same_entity_other_season = PeriodicWork(7, 102, "fixtures", "fixture:42", periodic.window_start, periodic.window_end, 3, {})
    assert same_entity_other_season.stable_key() != periodic.stable_key()
    first = RecalculationWork(7, 101, "rolling_metrics", "team:9", "accepted:1", 0, {})
    second = RecalculationWork(7, 101, "rolling_metrics", "team:9", "accepted:2", 0, {})
    assert first.stable_key() != second.stable_key()


def test_periodic_key_requires_ordered_aware_windows_and_canonicalizes_utc() -> None:
    now = datetime(2026, 9, 8, tzinfo=UTC)
    with pytest.raises(ValueError, match="timezone-aware"):
        PeriodicWork(7, 101, "fixtures", "fixture:42", now.replace(tzinfo=None), now + timedelta(minutes=1), 0, {})
    with pytest.raises(ValueError, match="precede"):
        PeriodicWork(7, 101, "fixtures", "fixture:42", now, now, 0, {})
    equivalent = PeriodicWork(7, 101, "fixtures", "fixture:42", now.astimezone(UTC), now.astimezone(UTC) + timedelta(minutes=5), 0, {})
    assert equivalent.stable_key() == _periodic().stable_key()


def test_enqueue_checks_policy_before_it_touches_database() -> None:
    connection = _Connection()
    repository = PostgresSyncRepository(connection, _Gate(denied=True))  # type: ignore[arg-type]
    with pytest.raises(SyncPolicyDenied, match="disabled"):
        repository.enqueue_periodic(4, _periodic(), available_at=datetime(2026, 9, 8, tzinfo=UTC))
    assert connection.calls == []


def test_enqueue_uses_existing_queue_atomic_function_and_default_entity_conflict() -> None:
    connection = _Connection()
    repository = PostgresSyncRepository(connection, _Gate())  # type: ignore[arg-type]
    result = repository.enqueue_periodic(4, _periodic(), available_at=datetime(2026, 9, 8, tzinfo=UTC))
    assert result.work_item_id == 71 and result.enqueued is True
    sql, params = connection.calls[0]
    assert "enqueue_repeatable_sync_work_item" in sql
    assert params[6] == _periodic().stable_key()
    assert params[7] == "fixture:42" and params[8] == "entity:7:101:fixture:42"
