from __future__ import annotations

import asyncio

import pytest

from app.importer.current_season_statistics import CurrentSeasonStatisticsReport, CurrentSeasonStatisticsScope
from app.importer.incremental_statistics import (
    CompletedFixturesWorker,
    IncrementalStatisticsError,
    IncrementalStatisticsSettings,
)


def test_settings_parse_multiple_scopes_and_interval() -> None:
    settings = IncrementalStatisticsSettings.from_environment(
        {
            "INCREMENTAL_STATISTICS_SCOPES": "39:2026:3,140:2026",
            "INCREMENTAL_STATISTICS_INTERVAL_SECONDS": "120",
        }
    )
    assert [(item.league_external_id, item.season_start_year, item.max_requests) for item in settings.scopes] == [
        (39, 2026, 3),
        (140, 2026, 90),
    ]
    assert all(item.require_finalized_results and item.project_discovery for item in settings.scopes)
    assert settings.interval_seconds == 120


@pytest.mark.parametrize(
    "environment",
    [
        {"INCREMENTAL_STATISTICS_SCOPES": "39"},
        {"INCREMENTAL_STATISTICS_SCOPES": "39:2026,39:2026"},
        {"INCREMENTAL_STATISTICS_INTERVAL_SECONDS": "0"},
    ],
)
def test_settings_reject_invalid_values(environment: dict[str, str]) -> None:
    with pytest.raises((IncrementalStatisticsError, ValueError)):
        IncrementalStatisticsSettings.from_environment(environment)


def test_worker_runs_each_scope_sequentially(monkeypatch: pytest.MonkeyPatch) -> None:
    import app.importer.incremental_statistics as module

    calls: list[tuple[int, object]] = []

    async def fake_run(*, scope: CurrentSeasonStatisticsScope, client: object, sleep: object) -> CurrentSeasonStatisticsReport:
        calls.append((scope.league_external_id, client))
        return CurrentSeasonStatisticsReport(
            league_external_id=scope.league_external_id,
            season_start_year=scope.season_start_year,
            fixtures_discovered=0,
            unique_fixtures_selected=0,
            fixture_discovery_requests=1,
            batch_requests=0,
            fixtures_normalized=0,
            statistics_rows_written=0,
            teams_aggregated=0,
            api_requests=1,
            retries=0,
            skipped_fixtures=0,
            errors=(),
            stopped_reason=None,
            safe_rate_limit={},
        )

    monkeypatch.setattr(module, "run_current_season_statistics_backfill_async", fake_run)
    provider = object()
    settings = IncrementalStatisticsSettings(
        scopes=(CurrentSeasonStatisticsScope(39, 2026), CurrentSeasonStatisticsScope(140, 2026)),
        interval_seconds=60,
    )
    report = asyncio.run(CompletedFixturesWorker(settings=settings, provider=provider).run_once())
    assert [item.league_external_id for item in report.runs] == [39, 140]
    assert report.api_requests == 2
    assert calls == [(39, provider), (140, provider)]
