"""Incremental completed-fixture statistics worker.

The worker is deliberately a thin scheduler around the existing, idempotent
current-season importer.  Discovery still comes from the canonical season
catalogue and the importer selects only fixtures whose two-team statistics
pair is missing.  No provider fixture is fetched by the frontend and no
second statistics normalizer is introduced here.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import asdict, dataclass
from typing import Any

from app.api_football import APIFootballClient

from .current_season_statistics import (
    CurrentSeasonStatisticsReport,
    CurrentSeasonStatisticsScope,
    run_current_season_statistics_backfill_async,
)

DEFAULT_INTERVAL_SECONDS = 900


class IncrementalStatisticsError(RuntimeError):
    """Worker configuration or execution cannot continue safely."""


@dataclass(frozen=True)
class IncrementalStatisticsSettings:
    scopes: tuple[CurrentSeasonStatisticsScope, ...]
    interval_seconds: int = DEFAULT_INTERVAL_SECONDS

    @classmethod
    def from_environment(
        cls, environ: Mapping[str, str] | None = None
    ) -> "IncrementalStatisticsSettings":
        values = os.environ if environ is None else environ
        raw_scopes = values.get("INCREMENTAL_STATISTICS_SCOPES", "39:2026")
        scopes: list[CurrentSeasonStatisticsScope] = []
        for item in raw_scopes.split(","):
            value = item.strip()
            if not value:
                continue
            parts = value.split(":")
            if len(parts) not in {2, 3}:
                raise IncrementalStatisticsError(
                    "INCREMENTAL_STATISTICS_SCOPES must use league:season[:max_requests]"
                )
            try:
                league_id, season = int(parts[0]), int(parts[1])
                max_requests = int(parts[2]) if len(parts) == 3 else 90
            except ValueError as error:
                raise IncrementalStatisticsError(
                    "INCREMENTAL_STATISTICS_SCOPES contains a non-numeric value"
                ) from error
            scopes.append(
                CurrentSeasonStatisticsScope(
                    league_external_id=league_id,
                    season_start_year=season,
                    max_requests=max_requests,
                    require_finalized_results=True,
                    project_discovery=True,
                )
            )
        if not scopes or len({(s.league_external_id, s.season_start_year) for s in scopes}) != len(scopes):
            raise IncrementalStatisticsError("incremental statistics scopes must be non-empty and unique")
        try:
            interval = int(values.get("INCREMENTAL_STATISTICS_INTERVAL_SECONDS", str(DEFAULT_INTERVAL_SECONDS)))
        except ValueError as error:
            raise IncrementalStatisticsError("INCREMENTAL_STATISTICS_INTERVAL_SECONDS must be an integer") from error
        if interval < 1:
            raise IncrementalStatisticsError("INCREMENTAL_STATISTICS_INTERVAL_SECONDS must be positive")
        return cls(scopes=tuple(scopes), interval_seconds=interval)


@dataclass(frozen=True)
class IncrementalStatisticsReport:
    runs: tuple[CurrentSeasonStatisticsReport, ...]

    @property
    def api_requests(self) -> int:
        return sum(item.api_requests for item in self.runs)


Sleep = Callable[[float], Awaitable[None]]


class CompletedFixturesWorker:
    """Run incremental completed-fixture updates sequentially per scope."""

    def __init__(
        self,
        *,
        settings: IncrementalStatisticsSettings,
        provider: Any,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        self._settings = settings
        self._provider = provider
        self._sleep = sleep

    async def run_once(self) -> IncrementalStatisticsReport:
        reports: list[CurrentSeasonStatisticsReport] = []
        for scope in self._settings.scopes:
            reports.append(
                await run_current_season_statistics_backfill_async(
                    scope=scope, client=self._provider, sleep=self._sleep
                )
            )
        return IncrementalStatisticsReport(runs=reports)

    async def run_forever(self) -> None:
        while True:
            await self.run_once()
            await self._sleep(float(self._settings.interval_seconds))


async def run_from_environment() -> IncrementalStatisticsReport:
    settings = IncrementalStatisticsSettings.from_environment()
    async with APIFootballClient.from_environment(budget_consumer="operations") as provider:
        return await CompletedFixturesWorker(settings=settings, provider=provider).run_once()


def main() -> None:
    parser = argparse.ArgumentParser(description="Incrementally import completed fixture statistics")
    parser.add_argument("--continuous", action="store_true", help="repeat at configured interval")
    args = parser.parse_args()
    settings = IncrementalStatisticsSettings.from_environment()

    async def execute() -> None:
        async with APIFootballClient.from_environment(budget_consumer="operations") as provider:
            worker = CompletedFixturesWorker(settings=settings, provider=provider)
            if args.continuous:
                await worker.run_forever()
            else:
                print(json.dumps(asdict(await worker.run_once()), default=str, sort_keys=True))

    asyncio.run(execute())


if __name__ == "__main__":
    main()
