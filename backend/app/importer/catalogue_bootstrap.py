"""Autonomous raw-first bootstrap for supported regular API-Football leagues."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import socket
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from psycopg import Connection
from psycopg.types.json import Jsonb

from app.api_football import APIFootballClient, APIFootballResponse
from app.api_football.client import safe_rate_limit_headers
from app.api_football.errors import APIFootballAPIError, APIFootballHTTPError
from app.importer.active_season import ActiveSeasonImportError, ActiveSeasonScope, base_requests, import_active_base, validate_base_responses, verify_active_season
from app.importer.raw_spool import RawSpool, RawSpoolArtifact, RawSpoolError
from app.importer.current_season_statistics import CurrentSeasonStatisticsError, CurrentSeasonStatisticsScope, run_current_season_statistics_backfill_async
from app.importer.season_bootstrap import BaseRequest, CollectedBaseResponse


PROVIDER_CODE = "api-football"
OPERATION = "catalogue_regular_league_bootstrap_v1"
POLICY_VERSION = 1
LEASE_SECONDS = 300
DEFAULT_SPOOL_DIR = Path("/var/lib/football-analytics/catalogue-bootstrap")
DEFAULT_QUOTA_RESERVE = 25
DEFAULT_DAILY_REQUEST_CAP = 6000
DEFAULT_RUN_REQUEST_CAP = 6000
DEFAULT_PACING_SECONDS = 0.25
DEFAULT_FETCH_RETRIES = 3
DEFAULT_ITEM_ATTEMPTS = 3
DEFAULT_NOT_PUBLISHED_DELAY_SECONDS = 6 * 60 * 60

# This is the next approved expansion tranche, selected from the retained
# 2026-09-01 scanner candidate snapshot.  It deliberately excludes the 24
# scopes already imported before this worker was introduced, competition 1032
# (a Copa despite the provider's `League` label), and 254 (NWSL, deferred with
# the women's-format work).  It is a safety boundary: an unset environment
# must never turn the timer into an all-catalogue import.
DEFAULT_CATALOGUE_BOOTSTRAP_LEAGUE_IDS = frozenset(
    {
        72, 80, 82, 89, 98, 114, 119, 128, 134, 144, 145, 169, 172, 179,
        197, 207, 210, 233, 235, 236, 239, 242, 244, 250, 252, 253, 262,
        265, 271, 281, 283, 286, 292, 301, 305, 307, 323, 327, 344, 345,
        357, 363, 383, 421, 475, 479, 549, 624, 813, 1104,
    }
)
assert len(DEFAULT_CATALOGUE_BOOTSTRAP_LEAGUE_IDS) == 50


class CatalogueBootstrapError(RuntimeError):
    """The autonomous catalogue bootstrap cannot safely continue."""


class ProviderQuotaExhausted(CatalogueBootstrapError):
    """Quota guard or API-Football 429 paused the existing queue."""


@dataclass(frozen=True)
class CatalogueCompetition:
    league_external_id: int
    name: str
    provider_type: str
    season_start_year: int | None
    initial_outcome: str | None = None

    @property
    def scope_key(self) -> str:
        return f"catalogue-bootstrap:{self.league_external_id}:{self.season_start_year or 'no-current'}"

    def scope(self) -> dict[str, Any]:
        return {"policy_version": POLICY_VERSION, "league_external_id": self.league_external_id, "name": self.name, "provider_type": self.provider_type, "season_start_year": self.season_start_year, "initial_outcome": self.initial_outcome}

    @classmethod
    def from_scope(cls, value: Mapping[str, Any]) -> "CatalogueCompetition":
        league_id, name, kind, season, outcome = value.get("league_external_id"), value.get("name"), value.get("provider_type"), value.get("season_start_year"), value.get("initial_outcome")
        if not isinstance(league_id, int) or league_id <= 0 or not isinstance(name, str) or not name.strip() or not isinstance(kind, str) or not kind.strip() or (season is not None and not isinstance(season, int)) or (outcome is not None and not isinstance(outcome, str)):
            raise CatalogueBootstrapError("invalid catalogue work-item scope")
        return cls(league_id, name, kind, season, outcome)


@dataclass(frozen=True)
class Settings:
    database_url: str
    spool_dir: Path
    quota_reserve: int = DEFAULT_QUOTA_RESERVE
    daily_request_cap: int = DEFAULT_DAILY_REQUEST_CAP
    run_request_cap: int = DEFAULT_RUN_REQUEST_CAP
    pacing_seconds: float = DEFAULT_PACING_SECONDS
    fetch_retries: int = DEFAULT_FETCH_RETRIES
    item_attempt_limit: int = DEFAULT_ITEM_ATTEMPTS
    not_published_delay_seconds: int = DEFAULT_NOT_PUBLISHED_DELAY_SECONDS
    league_ids: frozenset[int] | None = None

    @classmethod
    def from_environment(cls, environ: Mapping[str, str] | None = None) -> "Settings":
        values = os.environ if environ is None else environ
        database_url = values.get("SUPABASE_DB_URL", "").strip()
        if not database_url:
            raise CatalogueBootstrapError("SUPABASE_DB_URL is required")
        def integer(name: str, default: int, minimum: int = 1) -> int:
            try:
                number = int(values.get(name, str(default)))
            except ValueError as error:
                raise CatalogueBootstrapError(f"{name} must be an integer") from error
            if number < minimum:
                raise CatalogueBootstrapError(f"{name} must be >= {minimum}")
            return number
        try:
            pacing = float(values.get("CATALOGUE_BOOTSTRAP_PACING_SECONDS", str(DEFAULT_PACING_SECONDS)))
        except ValueError as error:
            raise CatalogueBootstrapError("CATALOGUE_BOOTSTRAP_PACING_SECONDS must be numeric") from error
        if pacing < 0:
            raise CatalogueBootstrapError("CATALOGUE_BOOTSTRAP_PACING_SECONDS must be non-negative")
        raw_ids = values.get("CATALOGUE_BOOTSTRAP_LEAGUE_IDS", "").strip()
        try:
            league_ids = DEFAULT_CATALOGUE_BOOTSTRAP_LEAGUE_IDS if not raw_ids else frozenset(int(value.strip()) for value in raw_ids.split(","))
        except ValueError as error:
            raise CatalogueBootstrapError("CATALOGUE_BOOTSTRAP_LEAGUE_IDS must be comma-separated positive IDs") from error
        if league_ids is not None and (not league_ids or min(league_ids) <= 0):
            raise CatalogueBootstrapError("CATALOGUE_BOOTSTRAP_LEAGUE_IDS must contain positive IDs")
        return cls(database_url, Path(values.get("CATALOGUE_BOOTSTRAP_SPOOL_DIR", str(DEFAULT_SPOOL_DIR))), integer("CATALOGUE_BOOTSTRAP_QUOTA_RESERVE", DEFAULT_QUOTA_RESERVE, 0), integer("CATALOGUE_BOOTSTRAP_DAILY_REQUEST_CAP", DEFAULT_DAILY_REQUEST_CAP), integer("CATALOGUE_BOOTSTRAP_RUN_REQUEST_CAP", DEFAULT_RUN_REQUEST_CAP), pacing, integer("CATALOGUE_BOOTSTRAP_FETCH_RETRIES", DEFAULT_FETCH_RETRIES), integer("CATALOGUE_BOOTSTRAP_ITEM_ATTEMPTS", DEFAULT_ITEM_ATTEMPTS), integer("CATALOGUE_BOOTSTRAP_NOT_PUBLISHED_DELAY_SECONDS", DEFAULT_NOT_PUBLISHED_DELAY_SECONDS), league_ids)


@dataclass(frozen=True)
class WorkItem:
    id: int
    competition: CatalogueCompetition
    attempts: int
    checkpoint: Mapping[str, Any]


@dataclass(frozen=True)
class LeagueReport:
    league_external_id: int
    name: str
    season_start_year: int | None
    outcome: str
    detail: str | None = None


@dataclass(frozen=True)
class Report:
    run_id: int
    status: str
    leagues: tuple[LeagueReport, ...]
    provider_request_count: int


class Provider(Protocol):
    async def get(self, endpoint: str, *, params: Mapping[str, str | int] | None = None) -> APIFootballResponse: ...
    def response_contains_api_key(self, body: bytes) -> bool: ...


class Repository(Protocol):
    def active_run(self) -> tuple[int, Mapping[str, Any]] | None: ...
    def create_run(self, items: Sequence[CatalogueCompetition], *, catalogue_sha256: str, request_count: int) -> int: ...
    def claim_next(self, run_id: int) -> WorkItem | None: ...
    def unfinished_delay_seconds(self, run_id: int) -> float | None: ...
    def reserve_request(self, daily_limit: int) -> bool: ...
    def renew(self, item: WorkItem) -> None: ...
    def observe_rate_limit(self, *, endpoint: str, headers: Mapping[str, str]) -> None: ...
    def canonical_scope_is_complete(self, competition: CatalogueCompetition) -> bool: ...
    def import_and_verify(self, *, scope: ActiveSeasonScope, collected: Sequence[CollectedBaseResponse]) -> None: ...
    def complete(self, item: WorkItem, checkpoint: Mapping[str, Any]) -> None: ...
    def requeue(self, item: WorkItem, *, checkpoint: Mapping[str, Any], error: str, delay_seconds: float) -> None: ...
    def checkpoint_run(self, run_id: int, checkpoint: Mapping[str, Any]) -> None: ...
    def finish_run(self, run_id: int, *, checkpoint: Mapping[str, Any]) -> None: ...


class PostgresRepository:
    def __init__(self, database_url: str, *, lease_owner: str | None = None) -> None:
        self._database_url, self._owner = database_url, lease_owner or f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex}"
        self._conn_value: Connection[Any] | None = None
        self._provider_id: int | None = None

    def __enter__(self) -> "PostgresRepository":
        self._conn_value = Connection.connect(self._database_url, autocommit=True)
        if self._conn.execute("SELECT pg_try_advisory_lock(hashtextextended(%s,0))", (OPERATION,)).fetchone()[0] is not True:
            raise CatalogueBootstrapError("another catalogue bootstrap worker is running")
        row = self._conn.execute("SELECT id FROM source.providers WHERE code=%s", (PROVIDER_CODE,)).fetchone()
        if row is None:
            raise CatalogueBootstrapError("API-Football provider is not configured")
        self._provider_id = int(row[0])
        return self

    def __exit__(self, *_: object) -> None:
        if self._conn_value is not None:
            self._conn_value.execute("SELECT pg_advisory_unlock(hashtextextended(%s,0))", (OPERATION,))
            self._conn_value.close()
        self._conn_value = None

    @property
    def _conn(self) -> Connection[Any]:
        if self._conn_value is None:
            raise CatalogueBootstrapError("repository is not connected")
        return self._conn_value

    @property
    def _provider(self) -> int:
        if self._provider_id is None:
            raise CatalogueBootstrapError("provider is not resolved")
        return self._provider_id

    def active_run(self) -> tuple[int, Mapping[str, Any]] | None:
        row = self._conn.execute("SELECT id,checkpoint FROM ops.sync_runs WHERE provider_id=%s AND operation=%s AND status='running' ORDER BY id LIMIT 1", (self._provider, OPERATION)).fetchone()
        return None if row is None else (int(row[0]), dict(row[1]))

    def create_run(self, items: Sequence[CatalogueCompetition], *, catalogue_sha256: str, request_count: int) -> int:
        with self._conn.transaction():
            row = self._conn.execute("INSERT INTO ops.sync_runs(provider_id,operation,scope,status,started_at,checkpoint) VALUES(%s,%s,%s,'running',clock_timestamp(),%s) RETURNING id", (self._provider, OPERATION, Jsonb({"policy_version": POLICY_VERSION, "catalogue_sha256": catalogue_sha256}), Jsonb({"provider_request_count": request_count}))).fetchone()
            assert row is not None
            run_id = int(row[0])
            for item in items:
                self._conn.execute("INSERT INTO ops.sync_work_items(run_id,scope_key,scope) VALUES(%s,%s,%s)", (run_id, item.scope_key, Jsonb(item.scope())))
        return run_id

    def claim_next(self, run_id: int) -> WorkItem | None:
        row = self._conn.execute("SELECT * FROM ops.claim_next_sync_work_item(%s,%s,%s::interval)", (run_id, self._owner, f"{LEASE_SECONDS} seconds")).fetchone()
        if row is None:
            return None
        item_id, _scope_key, scope, checkpoint, attempts = row
        if not isinstance(scope, dict) or not isinstance(checkpoint, dict):
            raise CatalogueBootstrapError("work item is malformed")
        return WorkItem(int(item_id), CatalogueCompetition.from_scope(scope), int(attempts), checkpoint)

    def unfinished_delay_seconds(self, run_id: int) -> float | None:
        row = self._conn.execute("SELECT extract(epoch FROM min(available_at)-clock_timestamp()) FROM ops.sync_work_items WHERE run_id=%s AND status IN ('pending','running')", (run_id,)).fetchone()
        return None if row is None or row[0] is None else max(0.0, float(row[0]))

    def reserve_request(self, daily_limit: int) -> bool:
        row = self._conn.execute("SELECT ops.reserve_provider_daily_request(%s,%s)", (self._provider, daily_limit)).fetchone()
        return row is not None and row[0] is True

    def renew(self, item: WorkItem) -> None:
        row = self._conn.execute("SELECT ops.renew_sync_work_item(%s,%s,%s::interval)", (item.id, self._owner, f"{LEASE_SECONDS} seconds")).fetchone()
        if row is None or row[0] is not True:
            raise CatalogueBootstrapError("lost work-item lease")

    def observe_rate_limit(self, *, endpoint: str, headers: Mapping[str, str]) -> None:
        self._conn.execute("SELECT source.observe_provider_rate_limit(%s,%s,%s)", (self._provider, endpoint, Jsonb(dict(headers))))

    def canonical_scope_is_complete(self, competition: CatalogueCompetition) -> bool:
        if competition.season_start_year is None:
            return False
        team_count = int(self._conn.execute("SELECT count(*) FROM football.season_teams st JOIN source.season_provider_refs ref ON ref.season_id=st.season_id WHERE ref.provider_id=%s AND ref.league_external_id=%s AND ref.external_season=%s", (self._provider, str(competition.league_external_id), competition.season_start_year)).fetchone()[0])
        if team_count < 2:
            return False
        try:
            verify_active_season(self._conn, scope=_scope(competition, team_count))
        except ActiveSeasonImportError:
            return False
        return True

    def import_and_verify(self, *, scope: ActiveSeasonScope, collected: Sequence[CollectedBaseResponse]) -> None:
        import_active_base(self._conn, collected=collected, scope=scope)
        verify_active_season(self._conn, scope=scope)

    def _terminal(self, item: WorkItem, checkpoint: Mapping[str, Any]) -> None:
        row = self._conn.execute("SELECT ops.complete_sync_work_item(%s,%s,%s)", (item.id, self._owner, Jsonb(dict(checkpoint)))).fetchone()
        if row is None or row[0] is not True:
            raise CatalogueBootstrapError("lost work-item lease")
    complete = _terminal

    def requeue(self, item: WorkItem, *, checkpoint: Mapping[str, Any], error: str, delay_seconds: float) -> None:
        changed = self._conn.execute("UPDATE ops.sync_work_items SET status='pending',checkpoint=%s,last_error=%s,available_at=clock_timestamp()+make_interval(secs=>%s),lease_owner=NULL,lease_expires_at=NULL WHERE id=%s AND status='running' AND lease_owner=%s AND lease_expires_at>=clock_timestamp()", (Jsonb(dict(checkpoint)), error[:500], delay_seconds, item.id, self._owner)).rowcount
        if changed != 1:
            raise CatalogueBootstrapError("lost work-item lease")

    def checkpoint_run(self, run_id: int, checkpoint: Mapping[str, Any]) -> None:
        self._conn.execute("UPDATE ops.sync_runs SET checkpoint=%s WHERE id=%s AND status='running'", (Jsonb(dict(checkpoint)), run_id))

    def finish_run(self, run_id: int, *, checkpoint: Mapping[str, Any]) -> None:
        changed = self._conn.execute("UPDATE ops.sync_runs SET status='succeeded',checkpoint=%s,finished_at=clock_timestamp() WHERE id=%s AND status='running' AND NOT EXISTS(SELECT 1 FROM ops.sync_work_items WHERE run_id=%s AND status IN ('pending','running'))", (Jsonb(dict(checkpoint)), run_id, run_id)).rowcount
        if changed != 1:
            raise CatalogueBootstrapError("queue has unfinished work")


def parse_catalogue(response: APIFootballResponse) -> tuple[CatalogueCompetition, ...]:
    payload = response.data
    if payload.get("get") != "leagues" or payload.get("parameters") not in ({}, []) or payload.get("errors") not in ({}, [], None) or payload.get("paging") != {"current": 1, "total": 1}:
        raise CatalogueBootstrapError("invalid global catalogue envelope")
    records = payload.get("response")
    if not isinstance(records, list) or payload.get("results") != len(records):
        raise CatalogueBootstrapError("invalid global catalogue results")
    result: list[CatalogueCompetition] = []
    seen: set[int] = set()
    for record in records:
        if not isinstance(record, Mapping):
            raise CatalogueBootstrapError("invalid global catalogue record")
        league, seasons = record.get("league"), record.get("seasons")
        if not isinstance(league, Mapping) or not isinstance(seasons, list):
            raise CatalogueBootstrapError("incomplete global catalogue record")
        league_id, name, kind = league.get("id"), league.get("name"), league.get("type")
        if not isinstance(league_id, int) or league_id <= 0 or league_id in seen or not isinstance(name, str) or not name.strip() or not isinstance(kind, str) or not kind.strip():
            raise CatalogueBootstrapError("invalid catalogue identity")
        seen.add(league_id)
        current = [value for value in seasons if isinstance(value, Mapping) and value.get("current") is True]
        season: int | None = None
        if kind != "League": outcome = "deferred_unsupported_type"
        elif not current: outcome = "deferred_no_current_season"
        elif len(current) != 1 or not isinstance(current[0].get("year"), int): outcome = "deferred_invalid_current_season"
        else:
            season = current[0]["year"]
            coverage = current[0].get("coverage")
            outcome = None if isinstance(coverage, Mapping) and coverage.get("standings") is True else "deferred_no_standings_coverage"
        result.append(CatalogueCompetition(league_id, name, kind, season, outcome))
    return tuple(sorted(result, key=lambda value: value.league_external_id))


def _scope(competition: CatalogueCompetition, team_count: int) -> ActiveSeasonScope:
    if competition.season_start_year is None or team_count < 2:
        raise ActiveSeasonImportError("season has no usable team catalogue")
    # Future deterministic adapters attach here, without changing queue/spool mechanics.
    return ActiveSeasonScope(competition.league_external_id, competition.season_start_year, team_count * (team_count - 1))


def _team_count(response: APIFootballResponse) -> int:
    rows = response.data.get("response")
    if not isinstance(rows, list) or response.data.get("results") != len(rows) or len(rows) < 2:
        raise ActiveSeasonImportError("team catalogue has not been published")
    return len(rows)


def _incomplete_calendar(collected: Sequence[CollectedBaseResponse], scope: ActiveSeasonScope) -> bool:
    by_endpoint = {item.request.endpoint: item.response.data for item in collected}
    fixtures, standings = by_endpoint["/fixtures"], by_endpoint["/standings"]
    fixture_rows = fixtures.get("response") if isinstance(fixtures, Mapping) else None
    standing_rows = standings.get("response") if isinstance(standings, Mapping) else None
    if not isinstance(fixture_rows, list) or not isinstance(standing_rows, list) or not standing_rows:
        return True
    league = standing_rows[0].get("league") if isinstance(standing_rows[0], Mapping) else None
    groups = league.get("standings") if isinstance(league, Mapping) else None
    if not isinstance(groups, list) or not groups or not isinstance(groups[0], list):
        return True
    if len(groups) != 1:
        return False
    return len(fixture_rows) < scope.expected_fixture_count or len(groups[0]) < scope.expected_team_count


class Worker:
    def __init__(self, *, provider: Provider, repository: Repository, spool: RawSpool, settings: Settings, sleep: Callable[[float], Awaitable[None]] = asyncio.sleep, statistics_backfill: Callable[[int, int], Awaitable[Any]] | None = None) -> None:
        self._provider, self._repository, self._spool, self._settings, self._sleep = provider, repository, spool, settings, sleep
        self._requests = 0
        self._last_request_at: float | None = None
        self._statistics_backfill = statistics_backfill or self._default_statistics_backfill

    async def _default_statistics_backfill(self, league_external_id: int, season_start_year: int) -> Any:
        return await run_current_season_statistics_backfill_async(
            scope=CurrentSeasonStatisticsScope(league_external_id, season_start_year), client=self._provider
        )

    async def _backfill_statistics(self, item: WorkItem) -> dict[str, Any]:
        competition = item.competition
        assert competition.season_start_year is not None
        report = await self._statistics_backfill(competition.league_external_id, competition.season_start_year)
        stopped_reason = getattr(report, "stopped_reason", None)
        errors = getattr(report, "errors", ())
        if stopped_reason is not None or errors:
            raise CurrentSeasonStatisticsError("statistics backfill did not complete cleanly")
        return {"fixtures_normalized": getattr(report, "fixtures_normalized", None), "statistics_rows_written": getattr(report, "statistics_rows_written", None), "api_requests": getattr(report, "api_requests", None)}

    async def _fetch(self, request: BaseRequest) -> RawSpoolArtifact:
        for attempt in range(self._settings.fetch_retries):
            if self._requests >= self._settings.run_request_cap:
                raise ProviderQuotaExhausted("configured request safety cap reached")
            if self._last_request_at is not None:
                delay = self._settings.pacing_seconds - (time.monotonic() - self._last_request_at)
                if delay > 0: await self._sleep(delay)
            if not self._repository.reserve_request(self._settings.daily_request_cap - self._settings.quota_reserve):
                raise ProviderQuotaExhausted("configured daily request safety cap reached")
            started = datetime.now(UTC); self._last_request_at = time.monotonic(); self._requests += 1
            try:
                response = await self._provider.get(request.endpoint, params=request.params)
            except (APIFootballHTTPError, APIFootballAPIError) as error:
                self._repository.observe_rate_limit(endpoint=request.endpoint, headers=error.safe_headers)
                if isinstance(error, APIFootballHTTPError) and error.status_code == 429: raise ProviderQuotaExhausted("API-Football returned HTTP 429") from error
                retryable = error.status_code is None or error.status_code in {0, 408} or error.status_code >= 500
                if attempt + 1 < self._settings.fetch_retries and retryable:
                    await self._sleep(float(2**attempt)); continue
                raise
            if self._provider.response_contains_api_key(response.raw_body): raise CatalogueBootstrapError("provider response contains API key")
            headers = safe_rate_limit_headers(response.headers); self._repository.observe_rate_limit(endpoint=request.endpoint, headers=headers)
            remaining = headers.get("x-ratelimit-remaining", headers.get("x-ratelimit-requests-remaining"))
            if remaining is not None and remaining.isdigit() and int(remaining) <= self._settings.quota_reserve: raise ProviderQuotaExhausted("provider quota reserve reached")
            return RawSpoolArtifact(request, response, started, datetime.now(UTC))
        raise AssertionError("unreachable retry guard")

    async def _capture(self, directory: Path, request: BaseRequest) -> CollectedBaseResponse:
        try:
            cached = self._spool.load(directory, request)
        except RawSpoolError:
            if not self._spool.discard_partial(directory, request):
                raise
            cached = None
        artifact = cached if cached is not None else await self._fetch(request)
        if cached is None: self._spool.stage(directory, artifact)
        return CollectedBaseResponse(artifact.request, artifact.response, artifact.request_started_at, artifact.response_received_at)

    @staticmethod
    def _report(item: WorkItem, outcome: str, detail: str | None = None) -> LeagueReport:
        value = item.competition
        return LeagueReport(value.league_external_id, value.name, value.season_start_year, outcome, detail)

    def _checkpoint(self, reports: Sequence[LeagueReport], stopped_reason: str | None = None) -> dict[str, Any]:
        value: dict[str, Any] = {"provider_request_count": self._requests, "outcomes": [asdict(report) for report in reports]}
        if stopped_reason: value["stopped_reason"] = stopped_reason
        return value

    async def _process(self, run_id: int, item: WorkItem) -> LeagueReport:
        competition = item.competition
        if competition.initial_outcome:
            self._repository.complete(item, {"outcome": competition.initial_outcome}); return self._report(item, competition.initial_outcome)
        if self._repository.canonical_scope_is_complete(competition):
            generation = item.checkpoint.get("capture_generation")
            if isinstance(generation, int) and generation > 0 and competition.season_start_year is not None:
                directory = self._spool.capture_directory(run_id=run_id, league_external_id=competition.league_external_id, season_start_year=competition.season_start_year, generation=generation)
                if directory.is_dir(): self._spool.purge_generation(directory)
            statistics = await self._backfill_statistics(item)
            self._repository.complete(item, {"outcome": "already_complete", "statistics": statistics}); return self._report(item, "already_complete")
        assert competition.season_start_year is not None
        generation = item.checkpoint.get("capture_generation", 1)
        if not isinstance(generation, int) or generation < 1: raise CatalogueBootstrapError("invalid capture generation")
        directory = self._spool.capture_directory(run_id=run_id, league_external_id=competition.league_external_id, season_start_year=competition.season_start_year, generation=generation)
        first, second = BaseRequest("/leagues", {"id": competition.league_external_id, "season": competition.season_start_year}), BaseRequest("/teams", {"league": competition.league_external_id, "season": competition.season_start_year})
        self._repository.renew(item)
        collected = [await self._capture(directory, first), await self._capture(directory, second)]
        try: scope = _scope(competition, _team_count(collected[1].response))
        except ActiveSeasonImportError as error:
            self._repository.requeue(item, checkpoint={"outcome": "pending_not_published", "reason": type(error).__name__, "capture_generation": generation + 1, "next_check_at": (datetime.now(UTC) + timedelta(seconds=self._settings.not_published_delay_seconds)).isoformat()}, error=type(error).__name__, delay_seconds=self._settings.not_published_delay_seconds)
            return self._report(item, "pending_not_published", type(error).__name__)
        requests = base_requests(scope)
        if tuple(requests[:2]) != (first, second): raise CatalogueBootstrapError("active-season request contract changed")
        self._repository.renew(item)
        collected.extend([await self._capture(directory, request) for request in requests[2:]])
        if _incomplete_calendar(collected, scope):
            self._repository.requeue(item, checkpoint={"outcome": "pending_not_published", "reason": "incomplete_calendar", "capture_generation": generation + 1, "next_check_at": (datetime.now(UTC) + timedelta(seconds=self._settings.not_published_delay_seconds)).isoformat()}, error="incomplete_calendar", delay_seconds=self._settings.not_published_delay_seconds)
            return self._report(item, "pending_not_published", "incomplete_calendar")
        try:
            self._repository.renew(item)
            validate_base_responses(collected, scope=scope); self._repository.import_and_verify(scope=scope, collected=collected)
        except ActiveSeasonImportError as error:
            self._repository.complete(item, {"outcome": "deferred_unsupported_format", "reason": type(error).__name__, "capture_generation": generation})
            return self._report(item, "deferred_unsupported_format", type(error).__name__)
        # Canonical DB now owns the same raw provenance; the VPS inbox is no
        # longer needed and is removed before this item becomes terminal.
        self._spool.purge_generation(directory)
        statistics = await self._backfill_statistics(item)
        self._repository.complete(item, {"outcome": "imported", "capture_generation": generation, "statistics": statistics}); return self._report(item, "imported")

    async def run_once(self) -> Report:
        reports: list[LeagueReport] = []
        active = self._repository.active_run()
        if active is None:
            catalogue_request = BaseRequest("/leagues", {})
            cached_catalogue = self._spool.latest_catalogue()
            catalogue = cached_catalogue if cached_catalogue is not None else await self._fetch(catalogue_request)
            catalogue_dir = self._spool.root / "catalogue" / hashlib.sha256(catalogue.response.raw_body).hexdigest()
            self._spool.stage(catalogue_dir, catalogue)
            self._spool.mark_catalogue_pending(catalogue_dir)
            items = parse_catalogue(catalogue.response)
            if self._settings.league_ids is not None:
                items = tuple(item for item in items if item.league_external_id in self._settings.league_ids)
            if not items: raise CatalogueBootstrapError("provider catalogue is empty")
            run_id = self._repository.create_run(items, catalogue_sha256=hashlib.sha256(catalogue.response.raw_body).hexdigest(), request_count=self._requests)
            self._spool.consume_pending_catalogues()
        else:
            run_id, _checkpoint = active
            self._spool.consume_pending_catalogues()
        while True:
            item = self._repository.claim_next(run_id)
            if item is None:
                delay = self._repository.unfinished_delay_seconds(run_id)
                if delay is None:
                    self._repository.finish_run(run_id, checkpoint=self._checkpoint(reports)); return Report(run_id, "succeeded", tuple(reports), self._requests)
                self._repository.checkpoint_run(run_id, self._checkpoint(reports, "waiting_retry")); return Report(run_id, "waiting_retry", tuple(reports), self._requests)
            try: reports.append(await self._process(run_id, item))
            except ProviderQuotaExhausted as error:
                self._repository.requeue(item, checkpoint={**item.checkpoint, "outcome": "retry_pending", "reason": type(error).__name__}, error=type(error).__name__, delay_seconds=0)
                self._repository.checkpoint_run(run_id, self._checkpoint(reports, type(error).__name__)); return Report(run_id, "paused_quota", tuple(reports), self._requests)
            except (APIFootballHTTPError, APIFootballAPIError, CatalogueBootstrapError, CurrentSeasonStatisticsError, RawSpoolError) as error:
                if item.attempts >= self._settings.item_attempt_limit:
                    self._repository.complete(item, {"outcome": "failed_provider", "reason": type(error).__name__}); reports.append(self._report(item, "failed_provider", type(error).__name__))
                else:
                    self._repository.requeue(item, checkpoint={**item.checkpoint, "outcome": "retry_pending", "reason": type(error).__name__}, error=type(error).__name__, delay_seconds=float(2 ** (item.attempts - 1))); reports.append(self._report(item, "retry_pending", type(error).__name__))


async def run_from_environment() -> Report:
    settings = Settings.from_environment()
    with PostgresRepository(settings.database_url) as repository:
        async with APIFootballClient.from_environment() as provider:
            return await Worker(provider=provider, repository=repository, spool=RawSpool(settings.spool_dir), settings=settings).run_once()


def main() -> None:
    argparse.ArgumentParser(description="Autonomous raw-first regular league bootstrap").parse_args()
    print(json.dumps(asdict(asyncio.run(run_from_environment())), sort_keys=True))


if __name__ == "__main__": main()
