from __future__ import annotations

from contextlib import nullcontext
from datetime import UTC, datetime

import pytest

from app.sync import scheduler_main
from app.sync.prematch_freshness import PrematchFetchObservation
from app.sync.scheduler import AnalyticsInputSnapshot, ApiCostEstimate, SchedulerPreview, SeasonScheduleSnapshot
from app.sync.scheduler_process import SchedulerRunResult
from app.sync.scheduler_repository import BudgetSnapshot, SchedulerMaterializedSnapshot


class _Connection:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def transaction(self):
        return nullcontext()


@pytest.mark.parametrize("enqueue", [False, True])
def test_scheduler_cli_passes_materialized_season_and_analytics_inputs(monkeypatch, enqueue: bool) -> None:
    now = datetime(2026, 9, 9, 1, tzinfo=UTC)
    snapshot = SchedulerMaterializedSnapshot(
        policies=(), checkpoints=(), fixtures=(), budget=BudgetSnapshot(10, 1, 5, 1, None),
        seasons=(SeasonScheduleSnapshot(7, 101, now, True),),
        analytics_inputs=(AnalyticsInputSnapshot(7, 101, "fixture:9", 22, now),),
        prematch_observations=(
            PrematchFetchObservation(
                provider_id=7,
                fixture_id=9,
                fetch_id=23,
                observed_kickoff_at=now,
                observed_at=now,
                endpoint="/fixtures",
                fetch_successful=True,
                fetch_normalized=True,
            ),
        ),
        input_gaps=(),
    )
    calls: list[dict[str, object]] = []

    class _Reader:
        def __init__(self, _connection) -> None:
            pass

        def read(self, **_kwargs) -> SchedulerMaterializedSnapshot:
            return snapshot

    class _Process:
        def __init__(self, *_args) -> None:
            pass

        def preview(self, **kwargs):
            calls.append(kwargs)
            return SchedulerPreview((), (), ApiCostEstimate(0))

        def enqueue_due(self, **kwargs):
            calls.append(kwargs)
            return SchedulerRunResult(SchedulerPreview((), (), ApiCostEstimate(0)), (), ())

    argv = ["scheduler", "--database-url", "postgresql://unused", "--now", now.isoformat()]
    if enqueue:
        argv.extend(["--enqueue", "--run-id", "7"])
    monkeypatch.setattr(scheduler_main.psycopg, "connect", lambda _url: _Connection())
    monkeypatch.setattr(scheduler_main, "PostgresSchedulerSnapshotReader", _Reader)
    monkeypatch.setattr(scheduler_main, "PostgresCompetitionSyncPolicyReader", lambda _connection: object())
    monkeypatch.setattr(scheduler_main, "SyncPolicyGate", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(scheduler_main, "PostgresSchedulerRepository", lambda *_args: object())
    monkeypatch.setattr(scheduler_main, "Q05SchedulerProcess", _Process)
    monkeypatch.setattr("sys.argv", argv)

    scheduler_main.main()

    assert len(calls) == 1
    assert calls[0]["seasons"] == snapshot.seasons
    assert calls[0]["analytics_inputs"] == snapshot.analytics_inputs
    assert calls[0]["prematch_observations"] == snapshot.prematch_observations
