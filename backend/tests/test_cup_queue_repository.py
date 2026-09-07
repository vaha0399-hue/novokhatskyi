from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import pytest

from app.importer.cup_bootstrap import CupCompetition
from app.importer.cup_queue import OPERATION, POLICY_VERSION, CupQueueError
from app.importer.cup_queue_repository import PostgresCupQueueRepository


@dataclass
class _Cursor:
    row: tuple[Any, ...] | None = None
    rowcount: int = 1

    def fetchone(self):
        return self.row


class _Connection:
    def __init__(self, rows: list[tuple[Any, ...] | None] | None = None) -> None:
        self.rows = list(rows or [])
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> _Cursor:
        self.calls.append((sql, params))
        return _Cursor(self.rows.pop(0) if self.rows else None)

    @contextmanager
    def transaction(self):
        yield self


def _repository(connection: _Connection) -> PostgresCupQueueRepository:
    repository = PostgresCupQueueRepository("postgresql://not-used", lease_owner="test-owner")
    repository._conn_value = connection  # type: ignore[assignment]
    repository._provider_id = 7
    return repository


def _cup(identifier: int = 2, season: int = 2026, coverage: bool = True) -> CupCompetition:
    return CupCompetition(identifier, "Cup One", season, coverage)


def test_create_run_uses_strict_cup_operation_and_exact_id_season_scopes() -> None:
    connection = _Connection(rows=[(41,)])
    repository = _repository(connection)
    assert repository.create_run([_cup(), _cup(2, 2025, False)], operation=OPERATION, policy_version=POLICY_VERSION) == 41
    run_sql, run_params = connection.calls[0]
    assert "ops.sync_runs" in run_sql and run_params[1] == OPERATION
    assert run_params[2].obj == {"policy_version": 1, "provider_type": "Cup"}
    work_items = connection.calls[1:]
    assert [params[1] for _sql, params in work_items] == ["cup-bootstrap:2:2026", "cup-bootstrap:2:2025"]
    assert [params[2].obj["fixture_statistics_coverage"] for _sql, params in work_items] == [True, False]
    assert all(params[2].obj["provider_type"] == "Cup" for _sql, params in work_items)


def test_create_run_rejects_wrong_operation_policy_and_duplicate_scope() -> None:
    repository = _repository(_Connection())
    with pytest.raises(CupQueueError, match="operation"):
        repository.create_run([_cup()], operation="catalogue_regular_league_bootstrap_v2", policy_version=1)
    with pytest.raises(CupQueueError, match="operation"):
        repository.create_run([_cup()], operation=OPERATION, policy_version=2)
    with pytest.raises(CupQueueError, match="duplicate"):
        repository.create_run([_cup(), _cup()], operation=OPERATION, policy_version=POLICY_VERSION)


def test_claim_only_accepts_versioned_cup_scope_and_preserves_coverage() -> None:
    connection = _Connection(rows=[(9, "cup-bootstrap:2:2026", {
        "policy_version": POLICY_VERSION, "provider_type": "Cup", "league_external_id": 2,
        "name": "Cup One", "season_start_year": 2026, "fixture_statistics_coverage": False,
    }, {"capture_generation": 2}, 2)])
    item = _repository(connection).claim_next(41)
    assert item is not None
    assert item.competition == _cup(coverage=False)
    assert item.checkpoint == {"capture_generation": 2}
    assert "claim_next_sync_work_item" in connection.calls[0][0]


def test_claim_rejects_non_cup_scope() -> None:
    connection = _Connection(rows=[(9, "seasonal-bootstrap:39", {
        "policy_version": POLICY_VERSION, "provider_type": "League", "league_external_id": 39,
        "name": "League", "season_start_year": 2026, "fixture_statistics_coverage": True,
    }, {}, 1)])
    with pytest.raises(CupQueueError, match="valid Cup scope"):
        _repository(connection).claim_next(41)


def test_queue_operations_use_owner_lease_and_provider_quota_function() -> None:
    connection = _Connection(rows=[(True,), (True,), (True,), None])
    repository = _repository(connection)
    item = type("Item", (), {"id": 9})()
    assert repository.reserve_request(6000) is True
    repository.renew(item)  # type: ignore[arg-type]
    repository.complete(item, {"outcome": "imported"})  # type: ignore[arg-type]
    repository.requeue(item, checkpoint={"outcome": "retry_pending"}, error="bad", delay_seconds=2)  # type: ignore[arg-type]
    sql = "\n".join(call[0] for call in connection.calls)
    assert "reserve_provider_daily_request" in sql
    assert "renew_sync_work_item" in sql
    assert "complete_sync_work_item" in sql
    assert "lease_owner=%s" in sql
