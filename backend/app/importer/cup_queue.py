"""Managed raw-first queue for the reviewed API-Football Cup import.

Unlike the old regular-league queues, this worker never derives scopes from a
live ``current`` catalogue.  It creates work items only from the reviewed cup
allow-list and the two immutable classification artifacts.  Canonical writes
remain behind ``CupBaseSink`` so the same shared writer is used for Cups and
Leagues without copying persistence SQL here.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol

import psycopg

from app.api_football import APIFootballBudgetError, budget_retry_delay_seconds
from app.importer.cup_bootstrap import (
    CupBaseSink,
    CupBootstrapError,
    CupCompetition,
    CupRawFirstWorker,
    Provider,
    load_classified_cup_scopes,
)
from app.importer.current_season_statistics import CurrentSeasonStatisticsError, CurrentSeasonStatisticsScope
from app.importer.raw_spool import RawSpool, RawSpoolError
from app.api_football.errors import APIFootballAPIError, APIFootballHTTPError
from app.importer.season_bootstrap import SeasonBootstrapError


OPERATION = "catalogue_cup_bootstrap_v1"
POLICY_VERSION = 1
DEFAULT_DAILY_REQUEST_CAP = 6_000
DEFAULT_RUN_REQUEST_CAP = 6_000
DEFAULT_ITEM_ATTEMPTS = 3


class CupQueueError(RuntimeError):
    """The managed Cup queue cannot safely continue."""


class CupQueueQuotaExhausted(CupQueueError):
    """A configured provider-request safety limit paused the queue."""


@dataclass(frozen=True)
class CupQueueSettings:
    spool_dir: Path
    allow_list_path: Path
    with_fixture_stats_path: Path
    without_fixture_stats_path: Path
    daily_request_cap: int = DEFAULT_DAILY_REQUEST_CAP
    run_request_cap: int = DEFAULT_RUN_REQUEST_CAP
    item_attempt_limit: int = DEFAULT_ITEM_ATTEMPTS

    def __post_init__(self) -> None:
        if self.daily_request_cap < 1 or self.run_request_cap < 1:
            raise ValueError("request caps must be positive")
        if self.item_attempt_limit < 1:
            raise ValueError("item_attempt_limit must be positive")


@dataclass(frozen=True)
class CupWorkItem:
    id: int
    competition: CupCompetition
    attempts: int
    checkpoint: Mapping[str, Any]


@dataclass(frozen=True)
class CupReportEntry:
    league_external_id: int
    name: str
    season_start_year: int
    fixture_statistics_coverage: bool
    outcome: str
    detail: str | None = None


@dataclass(frozen=True)
class CupQueueReport:
    run_id: int
    status: str
    cups: tuple[CupReportEntry, ...]
    provider_request_count: int


class Repository(Protocol):
    """Persistence boundary; production DB code stays outside this pure worker."""

    def active_run(self) -> tuple[int, Mapping[str, Any]] | None: ...
    def create_run(self, items: Sequence[CupCompetition], *, operation: str, policy_version: int) -> int: ...
    def claim_next(self, run_id: int) -> CupWorkItem | None: ...
    def unfinished_delay_seconds(self, run_id: int) -> float | None: ...
    def reserve_request(self, daily_limit: int) -> bool: ...
    def renew(self, item: CupWorkItem) -> None: ...
    def complete(self, item: CupWorkItem, checkpoint: Mapping[str, Any]) -> None: ...
    def requeue(self, item: CupWorkItem, *, checkpoint: Mapping[str, Any], error: str, delay_seconds: float) -> None: ...
    def checkpoint_run(self, run_id: int, checkpoint: Mapping[str, Any]) -> None: ...
    def finish_run(self, run_id: int, *, checkpoint: Mapping[str, Any]) -> None: ...


StatisticsBackfill = Callable[[CurrentSeasonStatisticsScope], Awaitable[Any]]


class _QuotaProvider:
    """Counts every non-cached raw request before delegating to the provider."""

    def __init__(self, *, provider: Provider, reserve_request: Callable[[int], bool], settings: CupQueueSettings, requests: Callable[[], int], increment: Callable[[], None]) -> None:
        self._provider = provider
        self._reserve_request = reserve_request
        self._settings = settings
        self._requests = requests
        self._increment = increment

    async def get(self, endpoint: str, *, params: Mapping[str, str | int] | None = None):
        if self._requests() >= self._settings.run_request_cap:
            raise CupQueueQuotaExhausted("configured run request safety cap reached")
        if not self._reserve_request(self._settings.daily_request_cap):
            raise CupQueueQuotaExhausted("configured daily request safety cap reached")
        self._increment()
        return await self._provider.get(endpoint, params=params)


class Worker:
    """Injection-friendly managed queue that captures, validates, then writes Cups."""

    def __init__(
        self,
        *,
        provider: Provider,
        repository: Repository,
        spool: RawSpool,
        settings: CupQueueSettings,
        canonical_sink: CupBaseSink,
        statistics_backfill: StatisticsBackfill | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._provider = provider
        self._repository = repository
        self._spool = spool
        self._settings = settings
        self._canonical_sink = canonical_sink
        self._statistics_backfill = statistics_backfill
        self._sleep = sleep
        self._requests = 0

    def _checkpoint(self, entries: Sequence[CupReportEntry], stopped_reason: str | None = None) -> dict[str, Any]:
        checkpoint: dict[str, Any] = {
            "operation": OPERATION,
            "policy_version": POLICY_VERSION,
            "provider_request_count": self._requests,
            "outcomes": [asdict(entry) for entry in entries],
        }
        if stopped_reason is not None:
            checkpoint["stopped_reason"] = stopped_reason
        return checkpoint

    @staticmethod
    def _entry(item: CupWorkItem, outcome: str, detail: str | None = None) -> CupReportEntry:
        competition = item.competition
        return CupReportEntry(
            competition.league_external_id,
            competition.name,
            competition.season_start_year,
            competition.fixture_statistics_coverage,
            outcome,
            detail,
        )

    async def _statistics(self, competition: CupCompetition) -> Mapping[str, Any]:
        if not competition.fixture_statistics_coverage:
            return {"outcome": "unavailable_by_catalogue", "api_requests": 0}
        if self._statistics_backfill is None:
            return {"outcome": "not_configured", "api_requests": 0}
        scope = CurrentSeasonStatisticsScope(
            competition.league_external_id,
            competition.season_start_year,
            select_all_completed=True,
        )
        report = await self._statistics_backfill(scope)
        if getattr(report, "stopped_reason", None) is not None or getattr(report, "errors", ()):
            raise CurrentSeasonStatisticsError("cup statistics backfill did not complete cleanly")
        return {
            "outcome": "imported",
            "fixtures_normalized": getattr(report, "fixtures_normalized", None),
            "statistics_rows_written": getattr(report, "statistics_rows_written", None),
            "api_requests": getattr(report, "api_requests", None),
        }

    async def _process(self, run_id: int, item: CupWorkItem) -> CupReportEntry:
        generation = item.checkpoint.get("capture_generation", 1)
        if not isinstance(generation, int) or generation < 1:
            raise CupQueueError("invalid capture generation")
        self._repository.renew(item)
        quota_provider = _QuotaProvider(
            provider=self._provider,
            reserve_request=self._repository.reserve_request,
            settings=self._settings,
            requests=lambda: self._requests,
            increment=lambda: setattr(self, "_requests", self._requests + 1),
        )
        capture = CupRawFirstWorker(provider=quota_provider, spool=self._spool, sink=self._canonical_sink)
        report = await capture.capture_and_validate(run_id=run_id, competition=item.competition, generation=generation)
        self._repository.renew(item)
        statistics = await self._statistics(item.competition)
        self._repository.complete(item, {
            "outcome": "imported",
            "capture_generation": generation,
            "capture_directory": str(report.capture_directory),
            "fixture_statistics_coverage": item.competition.fixture_statistics_coverage,
            "statistics": dict(statistics),
        })
        return self._entry(item, "imported")

    async def run_once(self) -> CupQueueReport:
        entries: list[CupReportEntry] = []
        active = self._repository.active_run()
        if active is None:
            items = load_classified_cup_scopes(
                allow_list_path=self._settings.allow_list_path,
                with_fixture_stats_path=self._settings.with_fixture_stats_path,
                without_fixture_stats_path=self._settings.without_fixture_stats_path,
            )
            if not items:
                raise CupQueueError("selected Cup scope list is empty")
            run_id = self._repository.create_run(items, operation=OPERATION, policy_version=POLICY_VERSION)
        else:
            run_id, _ = active
        while True:
            item = self._repository.claim_next(run_id)
            if item is None:
                delay = self._repository.unfinished_delay_seconds(run_id)
                if delay is None:
                    self._repository.finish_run(run_id, checkpoint=self._checkpoint(entries))
                    return CupQueueReport(run_id, "succeeded", tuple(entries), self._requests)
                self._repository.checkpoint_run(run_id, self._checkpoint(entries, "waiting_retry"))
                return CupQueueReport(run_id, "waiting_retry", tuple(entries), self._requests)
            try:
                entries.append(await self._process(run_id, item))
            except APIFootballBudgetError as error:
                self._repository.requeue(item, checkpoint={**item.checkpoint, "outcome": "budget_pending", "reason": type(error).__name__}, error=type(error).__name__, delay_seconds=budget_retry_delay_seconds(error))
                self._repository.checkpoint_run(run_id, self._checkpoint(entries, type(error).__name__))
                return CupQueueReport(run_id, "paused_budget", tuple(entries), self._requests)
            except CupQueueQuotaExhausted as error:
                self._repository.requeue(item, checkpoint={**item.checkpoint, "outcome": "retry_pending", "reason": type(error).__name__}, error=type(error).__name__, delay_seconds=0)
                self._repository.checkpoint_run(run_id, self._checkpoint(entries, type(error).__name__))
                return CupQueueReport(run_id, "paused_quota", tuple(entries), self._requests)
            except (
                CupBootstrapError,
                RawSpoolError,
                CurrentSeasonStatisticsError,
                CupQueueError,
                psycopg.Error,
                APIFootballAPIError,
                APIFootballHTTPError,
                SeasonBootstrapError,
            ) as error:
                if item.attempts >= self._settings.item_attempt_limit:
                    self._repository.complete(item, {"outcome": "failed", "reason": type(error).__name__})
                    entries.append(self._entry(item, "failed", type(error).__name__))
                else:
                    delay = float(2 ** (item.attempts - 1))
                    self._repository.requeue(item, checkpoint={**item.checkpoint, "outcome": "retry_pending", "reason": type(error).__name__, "capture_generation": int(item.checkpoint.get("capture_generation", 1)) + 1}, error=type(error).__name__, delay_seconds=delay)
                    entries.append(self._entry(item, "retry_pending", type(error).__name__))
                    await self._sleep(0)
