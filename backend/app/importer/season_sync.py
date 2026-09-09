"""Fail-closed discovery and first import of approved active league seasons.

This is a backend-only, one-shot job intended for a daily systemd timer.  It
never enumerates the provider's global league catalogue and never updates an
already canonical season.  Its sole mutation path for a newly ready season is
the existing atomic active-season importer.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import socket
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from psycopg import Connection
from psycopg.types.json import Jsonb

from app.api_football import APIFootballBudgetError, APIFootballClient, APIFootballResponse, budget_retry_delay_seconds
from app.api_football.client import safe_rate_limit_headers
from app.api_football.errors import APIFootballAPIError, APIFootballHTTPError
from app.importer.active_season import (
    ActiveSeasonImportError,
    ActiveSeasonScope,
    import_active_base,
    validate_base_responses,
    verify_active_season,
)
from app.importer.season_bootstrap import BaseRequest, CollectedBaseResponse


LOGGER = logging.getLogger(__name__)
PROVIDER_CODE = "api-football"
OPERATION = "seasonal_active_bootstrap"
POLICY_VERSION = 1
DEFAULT_QUOTA_RESERVE = 3
LEASE_SECONDS = 300


class SeasonalSyncError(RuntimeError):
    """The seasonal worker cannot safely continue a work item."""


class ProviderQuotaExhausted(SeasonalSyncError):
    """A 429 or the configured safe quota reserve stopped the whole run."""


@dataclass(frozen=True)
class SeasonalLeaguePolicy:
    """Reviewed invariant for one competition eligible for automatic bootstrap."""

    code: str
    league_external_id: int
    expected_team_count: int

    def __post_init__(self) -> None:
        if not self.code or self.league_external_id <= 0 or self.expected_team_count < 2:
            raise ValueError("seasonal league policy is invalid")

    @property
    def expected_fixture_count(self) -> int:
        return self.expected_team_count * (self.expected_team_count - 1)

    @property
    def scope_key(self) -> str:
        return f"seasonal-bootstrap:{self.league_external_id}"


# This is deliberately versioned code, rather than editable environment or DB
# configuration.  Adding a league means reviewing its format and provider ID.
APPROVED_LEAGUE_POLICIES: tuple[SeasonalLeaguePolicy, ...] = (
    SeasonalLeaguePolicy("premier-league", 39, 20),
    SeasonalLeaguePolicy("la-liga", 140, 20),
    SeasonalLeaguePolicy("serie-a", 135, 20),
    SeasonalLeaguePolicy("bundesliga", 78, 18),
    SeasonalLeaguePolicy("ligue-1", 61, 18),
)


@dataclass(frozen=True)
class SeasonalSyncSettings:
    database_url: str
    quota_reserve: int = DEFAULT_QUOTA_RESERVE

    @classmethod
    def from_environment(cls, environ: Mapping[str, str] | None = None) -> "SeasonalSyncSettings":
        values = os.environ if environ is None else environ
        database_url = values.get("SUPABASE_DB_URL", "").strip()
        if not database_url:
            raise SeasonalSyncError("SUPABASE_DB_URL is required")
        raw_reserve = values.get("SEASONAL_SYNC_QUOTA_RESERVE", str(DEFAULT_QUOTA_RESERVE))
        try:
            quota_reserve = int(raw_reserve)
        except ValueError as error:
            raise SeasonalSyncError("SEASONAL_SYNC_QUOTA_RESERVE must be a non-negative integer") from error
        if quota_reserve < 0:
            raise SeasonalSyncError("SEASONAL_SYNC_QUOTA_RESERVE must be a non-negative integer")
        return cls(database_url=database_url, quota_reserve=quota_reserve)


@dataclass(frozen=True)
class SeasonalWorkItem:
    id: int
    policy: SeasonalLeaguePolicy


@dataclass(frozen=True)
class SeasonalLeagueReport:
    policy: str
    league_external_id: int
    outcome: str
    season_start_year: int | None = None
    detail: str | None = None


@dataclass(frozen=True)
class SeasonalSyncReport:
    run_id: int
    status: str
    leagues: tuple[SeasonalLeagueReport, ...]
    provider_request_count: int


@dataclass(frozen=True)
class SeasonalRunAcquisition:
    """Whether this worker may claim and finish the returned run."""

    run_id: int
    acquired: bool


class ProviderClient(Protocol):
    async def get(
        self, endpoint: str, *, params: Mapping[str, str | int] | None = None
    ) -> APIFootballResponse: ...

    def response_contains_api_key(self, body: bytes) -> bool: ...


class SeasonalSyncRepository(Protocol):
    def start_run(self, policies: Sequence[SeasonalLeaguePolicy]) -> SeasonalRunAcquisition: ...

    def claim_next(self, run_id: int, policies: Mapping[int, SeasonalLeaguePolicy]) -> SeasonalWorkItem | None: ...

    def discovery_due(self, *, league_external_id: int) -> bool: ...

    def season_exists(self, *, league_external_id: int, season_start_year: int) -> bool: ...

    def observe_rate_limit(self, *, endpoint: str, headers: Mapping[str, str]) -> None: ...

    def import_and_verify(
        self, *, scope: ActiveSeasonScope, collected: Sequence[CollectedBaseResponse]
    ) -> None: ...

    def complete(self, item: SeasonalWorkItem, checkpoint: Mapping[str, Any]) -> None: ...

    def fail(self, item: SeasonalWorkItem, *, checkpoint: Mapping[str, Any], error: str) -> None: ...

    def defer(self, item: SeasonalWorkItem, *, checkpoint: Mapping[str, Any], error: str, delay_seconds: float) -> None: ...

    def pending_delay_seconds(self, run_id: int) -> float | None: ...

    def finish_run(self, run_id: int, *, status: str, checkpoint: Mapping[str, Any]) -> None: ...


class PostgresSeasonalSyncRepository:
    """Control-plane reporting plus canonical import on one backend DB connection."""

    def __init__(self, database_url: str, *, lease_owner: str | None = None) -> None:
        self._database_url = database_url
        self._lease_owner = lease_owner or f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex}"
        self._connection: Connection[Any] | None = None
        self._provider_id: int | None = None

    def __enter__(self) -> "PostgresSeasonalSyncRepository":
        self._connection = Connection.connect(self._database_url, autocommit=True)
        row = self._conn.execute(
            "SELECT id FROM source.providers WHERE code=%s", (PROVIDER_CODE,)
        ).fetchone()
        if row is None:
            raise SeasonalSyncError("API-Football provider is not configured")
        self._provider_id = int(row[0])
        return self

    def __exit__(self, *_: object) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    @property
    def _conn(self) -> Connection[Any]:
        if self._connection is None:
            raise SeasonalSyncError("seasonal sync repository is not connected")
        return self._connection

    @property
    def _provider(self) -> int:
        if self._provider_id is None:
            raise SeasonalSyncError("seasonal sync provider is not resolved")
        return self._provider_id

    def start_run(self, policies: Sequence[SeasonalLeaguePolicy]) -> SeasonalRunAcquisition:
        # A quota-deferred legacy job belongs to its original run.  Resuming
        # that row preserves its attempts/checkpoint and prevents a duplicate
        # season scope when the next timer starts after ``available_at``.
        # The transaction-scoped advisory lock serializes find/resume/create
        # for this provider operation across independent timer processes.  A
        # contender that waited for a resume/create sees the resulting running
        # row and returns it instead of creating duplicate work.
        with self._conn.transaction():
            self._conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (f"seasonal-sync-run:{self._provider}:{OPERATION}",),
            )
            prior = self._conn.execute(
                """SELECT run.id, run.status FROM ops.sync_runs run
                   WHERE run.provider_id=%s AND run.operation=%s
                     AND run.status IN ('failed', 'running')
                     AND (run.status='running' OR EXISTS(
                         SELECT 1 FROM ops.sync_work_items item
                         WHERE item.run_id=run.id AND item.status='pending' AND item.job_type='legacy'
                     ))
                   ORDER BY (run.status='running') DESC, run.created_at DESC LIMIT 1""",
                (self._provider, OPERATION),
            ).fetchone()
            if prior is not None:
                run_id, status = int(prior[0]), str(prior[1])
                if status == "failed":
                    self._conn.execute(
                        "UPDATE ops.sync_runs SET status='running', finished_at=NULL WHERE id=%s",
                        (run_id,),
                    )
                    return SeasonalRunAcquisition(run_id, acquired=True)
                return SeasonalRunAcquisition(run_id, acquired=False)
            row = self._conn.execute(
                """INSERT INTO ops.sync_runs(provider_id,operation,scope,status,started_at)
                   VALUES(%s,%s,%s,'running',clock_timestamp()) RETURNING id""",
                (self._provider, OPERATION, Jsonb({"policy_version": POLICY_VERSION})),
            ).fetchone()
            assert row is not None
            run_id = int(row[0])
            for policy in policies:
                self._conn.execute(
                    """INSERT INTO ops.sync_work_items(run_id,scope_key,scope)
                       VALUES(%s,%s,%s)""",
                    (
                        run_id,
                        policy.scope_key,
                        Jsonb(
                            {
                                "policy_version": POLICY_VERSION,
                                "policy": policy.code,
                                "league_external_id": policy.league_external_id,
                                "expected_team_count": policy.expected_team_count,
                            }
                        ),
                    ),
                )
            return SeasonalRunAcquisition(run_id, acquired=True)

    def claim_next(self, run_id: int, policies: Mapping[int, SeasonalLeaguePolicy]) -> SeasonalWorkItem | None:
        row = self._conn.execute(
            "SELECT * FROM ops.claim_next_sync_work_item(%s,%s,%s::interval)",
            (run_id, self._lease_owner, f"{LEASE_SECONDS} seconds"),
        ).fetchone()
        if row is None:
            return None
        item_id, _scope_key, scope, _checkpoint, _attempts = row
        league_external_id = scope.get("league_external_id") if isinstance(scope, dict) else None
        if not isinstance(league_external_id, int) or league_external_id not in policies:
            raise SeasonalSyncError("claimed work item has an unknown league policy")
        return SeasonalWorkItem(id=int(item_id), policy=policies[league_external_id])

    def season_exists(self, *, league_external_id: int, season_start_year: int) -> bool:
        row = self._conn.execute(
            """SELECT EXISTS(
                    SELECT 1 FROM source.season_provider_refs
                    WHERE provider_id=%s AND league_external_id=%s AND external_season=%s
                )""",
            (self._provider, str(league_external_id), season_start_year),
        ).fetchone()
        return bool(row and row[0])

    def discovery_due(self, *, league_external_id: int) -> bool:
        """Avoid provider discovery while an already canonical season is active.

        A missing or undated canonical season deliberately remains due.  That
        is safer than silently assuming its lifecycle and blocking a needed
        repair/import forever.
        """
        row = self._conn.execute(
            """SELECT EXISTS(
                    SELECT 1
                    FROM source.season_provider_refs ref
                    JOIN football.seasons season ON season.id=ref.season_id
                    WHERE ref.provider_id=%s AND ref.league_external_id=%s
                      AND season.ends_on IS NOT NULL AND season.ends_on >= CURRENT_DATE
                )""",
            (self._provider, str(league_external_id)),
        ).fetchone()
        return not bool(row and row[0])

    def observe_rate_limit(self, *, endpoint: str, headers: Mapping[str, str]) -> None:
        self._conn.execute(
            "SELECT source.observe_provider_rate_limit(%s,%s,%s)",
            (self._provider, endpoint, Jsonb(dict(headers))),
        )

    def import_and_verify(
        self, *, scope: ActiveSeasonScope, collected: Sequence[CollectedBaseResponse]
    ) -> None:
        import_active_base(self._conn, collected=collected, scope=scope)
        verify_active_season(self._conn, scope=scope)

    def complete(self, item: SeasonalWorkItem, checkpoint: Mapping[str, Any]) -> None:
        row = self._conn.execute(
            "SELECT ops.complete_sync_work_item(%s,%s,%s)",
            (item.id, self._lease_owner, Jsonb(dict(checkpoint))),
        ).fetchone()
        if row is None or row[0] is not True:
            raise SeasonalSyncError("seasonal work item lease was lost before completion")

    def fail(self, item: SeasonalWorkItem, *, checkpoint: Mapping[str, Any], error: str) -> None:
        cursor = self._conn.execute(
            """UPDATE ops.sync_work_items
               SET status='failed', checkpoint=%s, last_error=%s, finished_at=clock_timestamp(),
                   lease_owner=NULL, lease_expires_at=NULL
               WHERE id=%s AND status='running' AND lease_owner=%s
                 AND lease_expires_at >= clock_timestamp() AND job_type='legacy'""",
            (Jsonb(dict(checkpoint)), error[:500], item.id, self._lease_owner),
        )
        if cursor.rowcount != 1:
            raise SeasonalSyncError("seasonal work item lease was lost before failure recording")

    def defer(self, item: SeasonalWorkItem, *, checkpoint: Mapping[str, Any], error: str, delay_seconds: float) -> None:
        if delay_seconds < 0:
            raise SeasonalSyncError("seasonal budget delay must be non-negative")
        cursor = self._conn.execute(
            """UPDATE ops.sync_work_items SET status='pending', checkpoint=%s, last_error=%s,
                   available_at=clock_timestamp()+make_interval(secs=>%s), lease_owner=NULL, lease_expires_at=NULL
               WHERE id=%s AND status='running' AND lease_owner=%s AND lease_expires_at >= clock_timestamp()
                 AND job_type='legacy'""",
            (Jsonb(dict(checkpoint)), error[:500], delay_seconds, item.id, self._lease_owner),
        )
        if cursor.rowcount != 1:
            raise SeasonalSyncError("seasonal work item lease was lost before budget deferral")

    def pending_delay_seconds(self, run_id: int) -> float | None:
        row = self._conn.execute(
            "SELECT extract(epoch FROM min(available_at)-clock_timestamp()) FROM ops.sync_work_items WHERE run_id=%s AND status='pending' AND job_type='legacy'",
            (run_id,),
        ).fetchone()
        return None if row is None or row[0] is None else max(0.0, float(row[0]))

    def finish_run(self, run_id: int, *, status: str, checkpoint: Mapping[str, Any]) -> None:
        cursor = self._conn.execute(
            """UPDATE ops.sync_runs SET status=%s, checkpoint=%s, finished_at=clock_timestamp()
               WHERE id=%s AND status='running'""",
            (status, Jsonb(dict(checkpoint)), run_id),
        )
        if cursor.rowcount != 1:
            raise SeasonalSyncError("seasonal sync run was not active at completion")


def _now() -> datetime:
    return datetime.now(UTC)


def discover_current_season(response: APIFootballResponse, policy: SeasonalLeaguePolicy) -> int | None:
    """Return the one provider-marked current season, otherwise fail closed."""
    payload = response.data
    if payload.get("parameters") != {"id": str(policy.league_external_id)}:
        raise SeasonalSyncError("league discovery response parameters do not match policy")
    records = payload.get("response")
    if not isinstance(records, list) or len(records) != 1:
        raise SeasonalSyncError("league discovery must return exactly one league")
    record = records[0]
    if not isinstance(record, Mapping):
        raise SeasonalSyncError("league discovery record is invalid")
    league = record.get("league")
    seasons = record.get("seasons")
    if not isinstance(league, Mapping) or league.get("id") != policy.league_external_id:
        raise SeasonalSyncError("league discovery returned the wrong league")
    if league.get("type") != "League":
        raise SeasonalSyncError("approved policy no longer maps to a league competition")
    if not isinstance(seasons, list):
        raise SeasonalSyncError("league discovery seasons are invalid")
    current = [item for item in seasons if isinstance(item, Mapping) and item.get("current") is True]
    if not current:
        return None
    if len(current) != 1 or not isinstance(current[0].get("year"), int):
        raise SeasonalSyncError("league discovery has multiple or invalid current seasons")
    return int(current[0]["year"])


def _standings_member_count(response: APIFootballResponse) -> int | None:
    records = response.data.get("response")
    if not isinstance(records, list) or len(records) != 1 or not isinstance(records[0], Mapping):
        return None
    league = records[0].get("league")
    if not isinstance(league, Mapping):
        return None
    standings = league.get("standings")
    if not isinstance(standings, list) or len(standings) != 1 or not isinstance(standings[0], list):
        return None
    return len(standings[0])


def base_responses_are_ready(
    collected: Sequence[CollectedBaseResponse], *, policy: SeasonalLeaguePolicy
) -> bool:
    """Recognise normal early-publication gaps without relaxing canonical validation."""
    by_endpoint = {item.request.endpoint: item.response.data for item in collected}
    teams = by_endpoint.get("/teams", {})
    fixtures = by_endpoint.get("/fixtures", {})
    standings = next((item.response for item in collected if item.request.endpoint == "/standings"), None)
    if not isinstance(teams, Mapping) or not isinstance(fixtures, Mapping) or standings is None:
        return False
    return (
        teams.get("results") == policy.expected_team_count
        and fixtures.get("results") == policy.expected_fixture_count
        and _standings_member_count(standings) == policy.expected_team_count
    )


class SeasonalSyncWorker:
    """Sequential, bounded seasonal discovery with no retry loop."""

    def __init__(
        self,
        *,
        provider: ProviderClient,
        repository: SeasonalSyncRepository,
        policies: Sequence[SeasonalLeaguePolicy] = APPROVED_LEAGUE_POLICIES,
        quota_reserve: int = DEFAULT_QUOTA_RESERVE,
    ) -> None:
        if quota_reserve < 0:
            raise ValueError("quota reserve must be non-negative")
        if not policies or len({item.league_external_id for item in policies}) != len(policies):
            raise ValueError("seasonal policies must be non-empty and unique")
        self._provider = provider
        self._repository = repository
        self._policies = tuple(policies)
        self._policy_by_league = {item.league_external_id: item for item in policies}
        self._quota_reserve = quota_reserve
        self._requests = 0

    async def _fetch(self, request: BaseRequest) -> CollectedBaseResponse:
        started = _now()
        try:
            response = await self._provider.get(request.endpoint, params=request.params)
        except APIFootballHTTPError as error:
            self._repository.observe_rate_limit(endpoint=request.endpoint, headers=error.safe_headers)
            if error.status_code == 429:
                raise ProviderQuotaExhausted("API-Football rate limit reached") from error
            raise
        except APIFootballAPIError as error:
            self._repository.observe_rate_limit(endpoint=request.endpoint, headers=error.safe_headers)
            raise
        received = _now()
        self._requests += 1
        if self._provider.response_contains_api_key(response.raw_body):
            raise SeasonalSyncError("provider response contains API key")
        headers = safe_rate_limit_headers(response.headers)
        self._repository.observe_rate_limit(endpoint=request.endpoint, headers=headers)
        remaining = headers.get("x-ratelimit-remaining", headers.get("x-ratelimit-requests-remaining"))
        if remaining is not None and remaining.isdigit() and int(remaining) <= self._quota_reserve:
            raise ProviderQuotaExhausted("provider quota reserve reached")
        return CollectedBaseResponse(
            request=request,
            response=response,
            request_started_at=started,
            response_received_at=received,
        )

    async def _process(self, item: SeasonalWorkItem) -> SeasonalLeagueReport:
        policy = item.policy
        if not self._repository.discovery_due(league_external_id=policy.league_external_id):
            checkpoint = {"outcome": "season_in_progress"}
            self._repository.complete(item, checkpoint)
            return SeasonalLeagueReport(
                policy.code,
                policy.league_external_id,
                "season_in_progress",
                detail="canonical_season_not_ended",
            )
        discovery = await self._fetch(BaseRequest("/leagues", {"id": policy.league_external_id}))
        season_start_year = discover_current_season(discovery.response, policy)
        if season_start_year is None:
            checkpoint = {"outcome": "not_ready", "reason": "no_current_provider_season"}
            self._repository.complete(item, checkpoint)
            return SeasonalLeagueReport(policy.code, policy.league_external_id, "not_ready", detail=checkpoint["reason"])
        if self._repository.season_exists(
            league_external_id=policy.league_external_id, season_start_year=season_start_year
        ):
            checkpoint = {"outcome": "already_imported", "season_start_year": season_start_year}
            self._repository.complete(item, checkpoint)
            return SeasonalLeagueReport(policy.code, policy.league_external_id, "already_imported", season_start_year)
        scope = ActiveSeasonScope(
            league_external_id=policy.league_external_id,
            season_start_year=season_start_year,
            expected_fixture_count=policy.expected_fixture_count,
        )
        requests = (
            BaseRequest("/leagues", {"id": policy.league_external_id, "season": season_start_year}),
            BaseRequest("/teams", {"league": policy.league_external_id, "season": season_start_year}),
            BaseRequest("/standings", {"league": policy.league_external_id, "season": season_start_year}),
            BaseRequest("/fixtures", {"league": policy.league_external_id, "season": season_start_year}),
        )
        collected = tuple([await self._fetch(request) for request in requests])
        if not base_responses_are_ready(collected, policy=policy):
            checkpoint = {"outcome": "not_ready", "season_start_year": season_start_year, "reason": "base_responses_partial"}
            self._repository.complete(item, checkpoint)
            return SeasonalLeagueReport(policy.code, policy.league_external_id, "not_ready", season_start_year, checkpoint["reason"])
        # This repeats the invariant validation inside import_active_base before any DML.
        validate_base_responses(collected, scope=scope)
        self._repository.import_and_verify(scope=scope, collected=collected)
        checkpoint = {"outcome": "imported", "season_start_year": season_start_year}
        self._repository.complete(item, checkpoint)
        return SeasonalLeagueReport(policy.code, policy.league_external_id, "imported", season_start_year)

    async def run_once(self) -> SeasonalSyncReport:
        acquisition = self._repository.start_run(self._policies)
        if not acquisition.acquired:
            # A different process owns the active run.  It alone may claim its
            # leased work or transition the run to a terminal status.
            return SeasonalSyncReport(acquisition.run_id, "running", (), 0)
        run_id = acquisition.run_id
        reports: list[SeasonalLeagueReport] = []
        terminal_status = "succeeded"
        try:
            while (item := self._repository.claim_next(run_id, self._policy_by_league)) is not None:
                try:
                    reports.append(await self._process(item))
                except ProviderQuotaExhausted as error:
                    terminal_status = "failed"
                    self._repository.fail(item, checkpoint={"outcome": "quota_stopped"}, error=type(error).__name__)
                    raise
                except APIFootballBudgetError as error:
                    self._repository.defer(item, checkpoint={"outcome": "budget_pending", "reason": type(error).__name__}, error=type(error).__name__, delay_seconds=budget_retry_delay_seconds(error))
                    reports.append(SeasonalLeagueReport(item.policy.code, item.policy.league_external_id, "budget_pending", detail=type(error).__name__))
                    terminal_status = "failed"
                    break
                except (APIFootballHTTPError, APIFootballAPIError, ActiveSeasonImportError, SeasonalSyncError) as error:
                    terminal_status = "failed"
                    self._repository.fail(item, checkpoint={"outcome": "failed"}, error=type(error).__name__)
                    reports.append(SeasonalLeagueReport(item.policy.code, item.policy.league_external_id, "failed", detail=type(error).__name__))
            if self._repository.pending_delay_seconds(run_id) is not None:
                terminal_status = "failed"
        except ProviderQuotaExhausted:
            pass
        finally:
            checkpoint = {
                "provider_request_count": self._requests,
                "outcomes": [asdict(report) for report in reports],
            }
            self._repository.finish_run(run_id, status=terminal_status, checkpoint=checkpoint)
        return SeasonalSyncReport(run_id, terminal_status, tuple(reports), self._requests)


async def run_from_environment() -> SeasonalSyncReport:
    """Run once with one reusable API-Football connection pool."""
    settings = SeasonalSyncSettings.from_environment()
    with PostgresSeasonalSyncRepository(settings.database_url) as repository:
        async with APIFootballClient.from_environment(budget_consumer="operations") as provider:
            return await SeasonalSyncWorker(
                provider=provider, repository=repository, quota_reserve=settings.quota_reserve
            ).run_once()


def main() -> None:
    parser = argparse.ArgumentParser(description="Discover and import approved active league seasons")
    parser.add_argument(
        "--catalogue",
        action="store_true",
        help="run the autonomous raw-first regular-league catalogue bootstrap",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    if args.catalogue:
        # Keep this import lazy: the original reviewed EPL path and its module
        # contract remain usable without loading the new catalogue worker.
        from app.importer.catalogue_bootstrap import run_from_environment as run_catalogue_from_environment

        report = asyncio.run(run_catalogue_from_environment())
    else:
        report = asyncio.run(run_from_environment())
    print(json.dumps(asdict(report), sort_keys=True))


if __name__ == "__main__":
    main()
