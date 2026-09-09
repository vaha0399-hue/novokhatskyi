"""Batch fixture-statistics importer for one current provider season.

The transport is deliberately generic: every invocation is identified by an
API-Football league id and start year.  It uses the canonical completed fixture
catalog to select each team's latest ten matches, sends fixture ids in groups
of at most twenty, retains every successful raw response, and then bulk-upserts
the two team statistic rows for every complete provider response.

This module is intentionally a one-shot job, not a scheduler.  A later worker
can invoke the same ``run_current_season_statistics_backfill`` function after a
completed-fixture sync; reruns select only fixtures that do not already have a
complete canonical statistics pair.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Protocol

import psycopg
from psycopg import Connection
from psycopg.types.json import Jsonb

from app.analytics.models import TeamMatchRecord
from app.analytics.rolling_metrics import RollingMetricRow, calculate_rolling_metrics
from app.api_football import APIFootballClient, APIFootballResponse
from app.api_football.client import safe_rate_limit_headers
from app.api_football.errors import APIFootballAPIError, APIFootballHTTPError
from app.importer.canary import parse_datetime, request_params_sha256
from app.importer.statistics_backfill import StatisticsContractError, classify_statistics_blocks

PROVIDER_CODE = "api-football"
ENDPOINT = "/fixtures"
PURPOSE = "scheduled_refresh"
MAPPING_VERSION = "api-football-batch-v1"
RAW_RETENTION_DAYS = 30
MAX_FIXTURES_PER_REQUEST = 20
HISTORY_WINDOW = 10
DEFAULT_MAX_REQUESTS = 90
DEFAULT_DAILY_REQUEST_CAP = 5_500
MAX_ATTEMPTS_PER_BATCH = 2
RETRYABLE_HTTP_STATUSES = frozenset({0, 408, 499, 500, 502, 503, 504})

Sleep = Callable[[float], Awaitable[None]]
Clock = Callable[[], datetime]


class CurrentSeasonStatisticsError(RuntimeError):
    """The job stopped without safely completing its requested slice."""


@dataclass(frozen=True)
class CurrentSeasonStatisticsScope:
    league_external_id: int
    season_start_year: int
    max_requests: int = DEFAULT_MAX_REQUESTS
    daily_request_cap: int = DEFAULT_DAILY_REQUEST_CAP
    require_finalized_results: bool = False
    project_discovery: bool = True
    select_all_completed: bool = False

    def __post_init__(self) -> None:
        if self.league_external_id <= 0:
            raise ValueError("league_external_id must be positive")
        if self.season_start_year < 1900:
            raise ValueError("season_start_year must be a four-digit year")
        if not 1 <= self.max_requests <= DEFAULT_MAX_REQUESTS:
            raise ValueError(f"max_requests must be between 1 and {DEFAULT_MAX_REQUESTS}")
        if not 1 <= self.daily_request_cap <= 6_000:
            raise ValueError("daily_request_cap must be between 1 and 6000")
        if not isinstance(self.select_all_completed, bool):
            raise ValueError("select_all_completed must be boolean")


@dataclass(frozen=True)
class FixtureTarget:
    fixture_id: int
    external_fixture_id: int
    home_team_id: int
    away_team_id: int
    home_external_team_id: int
    away_external_team_id: int
    kickoff_at: datetime
    statistics_complete: bool = False


@dataclass(frozen=True)
class BatchFetch:
    fetch_id: int
    request_started_at: datetime
    response_received_at: datetime
    response: APIFootballResponse


@dataclass(frozen=True)
class BatchParseResult:
    returned_fixture_ids: frozenset[int]
    statistics_by_fixture: Mapping[int, list[dict[str, Any]] | None]
    statistics_unavailable_fixture_ids: frozenset[int]
    statistics_partial_fixture_ids: frozenset[int]


@dataclass(frozen=True)
class DiscoveryFixture:
    external_fixture_id: int
    status_code: str
    home_external_team_id: int
    away_external_team_id: int
    kickoff_at: datetime
    home_goals: int
    away_goals: int
    home_halftime_goals: int | None
    away_halftime_goals: int | None
    home_fulltime_goals: int | None
    away_fulltime_goals: int | None
    home_extratime_goals: int | None
    away_extratime_goals: int | None
    home_penalty_goals: int | None
    away_penalty_goals: int | None


@dataclass(frozen=True)
class CurrentSeasonStatisticsReport:
    league_external_id: int
    season_start_year: int
    fixtures_discovered: int
    unique_fixtures_selected: int
    fixture_discovery_requests: int
    batch_requests: int
    fixtures_normalized: int
    statistics_rows_written: int
    teams_aggregated: int
    api_requests: int
    retries: int
    skipped_fixtures: int
    errors: tuple[str, ...]
    stopped_reason: str | None
    safe_rate_limit: Mapping[str, str]


class _Client(Protocol):
    async def get(self, endpoint: str, *, params: Mapping[str, str | int]) -> APIFootballResponse: ...

    def response_contains_api_key(self, body: bytes) -> bool: ...


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _database_url() -> str:
    value = os.environ.get("SUPABASE_DB_URL")
    if not value:
        raise CurrentSeasonStatisticsError("SUPABASE_DB_URL is required")
    return value


def fixture_ids_parameter(external_fixture_ids: Iterable[int]) -> str:
    """Return one provider ``ids`` argument, preserving caller order."""
    ids = tuple(external_fixture_ids)
    if not 1 <= len(ids) <= MAX_FIXTURES_PER_REQUEST:
        raise ValueError("a fixture batch must contain 1..20 ids")
    if any(not isinstance(value, int) or isinstance(value, bool) or value <= 0 for value in ids):
        raise ValueError("fixture ids must be positive integers")
    if len(set(ids)) != len(ids):
        raise ValueError("fixture ids must be unique")
    return "-".join(str(value) for value in ids)


def chunk_fixture_targets(targets: Sequence[FixtureTarget]) -> tuple[tuple[FixtureTarget, ...], ...]:
    """Split unique targets into API-Football's documented 20-id batches."""
    seen: set[int] = set()
    unique: list[FixtureTarget] = []
    for target in targets:
        if target.external_fixture_id in seen:
            continue
        seen.add(target.external_fixture_id)
        unique.append(target)
    return tuple(tuple(unique[index : index + MAX_FIXTURES_PER_REQUEST]) for index in range(0, len(unique), MAX_FIXTURES_PER_REQUEST))


def select_recent_history(targets: Iterable[FixtureTarget], *, window_size: int = HISTORY_WINDOW) -> tuple[FixtureTarget, ...]:
    """Union every team's latest overall, home, and away history locally.

    The home and away counters are intentionally independent.  A scanner may
    compare a future home side with its last ten home fixtures and an away side
    with its last ten away fixtures, so selecting only ten total fixtures per
    team would leave those venue windows incomplete.
    """
    if window_size <= 0:
        raise ValueError("window_size must be positive")
    ordered = sorted(targets, key=lambda target: (target.kickoff_at, target.fixture_id), reverse=True)
    home_per_team: dict[int, int] = {}
    away_per_team: dict[int, int] = {}
    selected: list[FixtureTarget] = []
    selected_ids: set[int] = set()
    for target in ordered:
        home_count = home_per_team.get(target.home_team_id, 0)
        away_count = away_per_team.get(target.away_team_id, 0)
        needs_home_history = home_count < window_size
        needs_away_history = away_count < window_size
        if not needs_home_history and not needs_away_history:
            continue
        if target.fixture_id not in selected_ids:
            selected.append(target)
            selected_ids.add(target.fixture_id)
        if needs_home_history:
            home_per_team[target.home_team_id] = home_count + 1
        if needs_away_history:
            away_per_team[target.away_team_id] = away_count + 1
    return tuple(selected)


def _require_int(value: object, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise StatisticsContractError(f"{field} must be a positive integer")
    return value


def _batch_entries(
    payload: Mapping[str, Any], *, targets: Sequence[FixtureTarget], league_external_id: int
) -> BatchParseResult:
    """Validate a batch response and map each fixture's complete/empty stats.

    ``None`` is an intentionally incomplete statistics response.  It is kept
    as raw evidence and retried by a later job rather than inventing zeroes.
    """
    requested = fixture_ids_parameter(target.external_fixture_id for target in targets)
    if payload.get("parameters") != {"ids": requested}:
        raise StatisticsContractError("batch fixture parameters mismatch")
    if payload.get("errors") not in ({}, [], None):
        raise StatisticsContractError("provider returned errors in batch response")
    paging = payload.get("paging")
    if paging != {"current": 1, "total": 1}:
        raise StatisticsContractError("batch fixture paging must be one page")
    response = payload.get("response")
    results = payload.get("results")
    if not isinstance(response, list) or not isinstance(results, int) or isinstance(results, bool) or results != len(response):
        raise StatisticsContractError("batch fixture results/response mismatch")
    by_external = {target.external_fixture_id: target for target in targets}
    parsed: dict[int, list[dict[str, Any]] | None] = {target.fixture_id: None for target in targets}
    seen: set[int] = set()
    unavailable: set[int] = set()
    partial: set[int] = set()
    for entry in response:
        if not isinstance(entry, Mapping):
            raise StatisticsContractError("batch fixture entry must be an object")
        fixture = entry.get("fixture")
        league = entry.get("league")
        teams = entry.get("teams")
        if not isinstance(fixture, Mapping) or not isinstance(league, Mapping) or not isinstance(teams, Mapping):
            raise StatisticsContractError("batch fixture entry lacks fixture, league, or teams")
        if _require_int(league.get("id"), "league.id") != league_external_id:
            raise StatisticsContractError("batch response fixture belongs to another league")
        external_id = _require_int(fixture.get("id"), "fixture.id")
        target = by_external.get(external_id)
        if target is None or external_id in seen:
            raise StatisticsContractError("batch response contains an unexpected or duplicate fixture")
        seen.add(external_id)
        home = teams.get("home")
        away = teams.get("away")
        if not isinstance(home, Mapping) or not isinstance(away, Mapping):
            raise StatisticsContractError("batch fixture teams must have home and away objects")
        if (_require_int(home.get("id"), "teams.home.id"), _require_int(away.get("id"), "teams.away.id")) != (
            target.home_external_team_id,
            target.away_external_team_id,
        ):
            raise StatisticsContractError("batch fixture team mapping differs from canonical fixture")
        status = fixture.get("status")
        if not isinstance(status, Mapping) or status.get("short") not in {"FT", "AET", "PEN"}:
            raise StatisticsContractError("batch response fixture is not completed")
        state, mapped = classify_statistics_blocks(entry.get("statistics"))
        if state == "complete":
            expected = {target.home_external_team_id, target.away_external_team_id}
            if {row["external_team_id"] for row in mapped} != expected:
                raise StatisticsContractError("statistics teams differ from canonical fixture participants")
            parsed[target.fixture_id] = mapped
        elif state == "empty":
            unavailable.add(target.fixture_id)
        elif state == "partial":
            partial.add(target.fixture_id)
    return BatchParseResult(
        frozenset(by_external[external_id].fixture_id for external_id in seen), parsed,
        frozenset(unavailable), frozenset(partial),
    )


def _context(conn: Connection[Any], scope: CurrentSeasonStatisticsScope) -> tuple[int, int]:
    row = conn.execute(
        """SELECT provider.id, season_ref.season_id
           FROM source.providers provider
           JOIN source.season_provider_refs season_ref ON season_ref.provider_id=provider.id
           WHERE provider.code=%s AND season_ref.league_external_id=%s AND season_ref.external_season=%s""",
        (PROVIDER_CODE, str(scope.league_external_id), scope.season_start_year),
    ).fetchone()
    if row is None:
        raise CurrentSeasonStatisticsError("canonical provider/league/season mapping is required")
    return int(row[0]), int(row[1])


def load_completed_targets(
    conn: Connection[Any], *, provider_id: int, season_id: int,
    require_finalized_results: bool = False,
) -> tuple[FixtureTarget, ...]:
    """Load completed fixtures; one existing team row remains incomplete.

    A previous interrupted upsert can leave one valid participant row.  It is
    intentionally selected for repair: the later two-row upsert is conflict
    safe and restores the exact pair without deleting existing data.
    """
    rows = conn.execute(
        """SELECT fixture.id, fixture_ref.external_id, fixture.home_team_id, fixture.away_team_id,
                  home_ref.external_id, away_ref.external_id, fixture.kickoff_at,
                  count(statistics.*) AS statistics_rows,
                  bool_and(statistics.team_id IN (fixture.home_team_id, fixture.away_team_id)) AS statistics_teams_ok
           FROM football.fixtures fixture
           JOIN source.fixture_provider_refs fixture_ref
             ON fixture_ref.fixture_id=fixture.id AND fixture_ref.provider_id=%s
           JOIN source.team_provider_refs home_ref
             ON home_ref.team_id=fixture.home_team_id AND home_ref.provider_id=%s
           JOIN source.team_provider_refs away_ref
             ON away_ref.team_id=fixture.away_team_id AND away_ref.provider_id=%s
           LEFT JOIN football.fixture_team_statistics statistics ON statistics.fixture_id=fixture.id
           LEFT JOIN football.fixture_statistics_coverage coverage ON coverage.fixture_id=fixture.id
           WHERE fixture.season_id=%s
             AND fixture.lifecycle_state='completed'
             AND fixture.result_available_at IS NOT NULL
             AND (%s = FALSE OR fixture.result_finalized_at IS NOT NULL)
             AND (coverage.fixture_id IS NULL OR coverage.next_retry_at <= clock_timestamp())
           GROUP BY fixture.id, fixture_ref.external_id, home_ref.external_id, away_ref.external_id
           ORDER BY fixture.kickoff_at DESC, fixture.id DESC""",
        (provider_id, provider_id, provider_id, season_id, require_finalized_results),
    ).fetchall()
    targets: list[FixtureTarget] = []
    for row in rows:
        fixture_id, external_id, home_id, away_id, home_external, away_external, kickoff, statistics_rows, teams_ok = row
        if int(statistics_rows) > 2 or (int(statistics_rows) > 0 and teams_ok is not True):
            raise CurrentSeasonStatisticsError("canonical fixture statistics are not an exact two-team pair")
        try:
            targets.append(
                FixtureTarget(
                    fixture_id=int(fixture_id), external_fixture_id=int(external_id),
                    home_team_id=int(home_id), away_team_id=int(away_id),
                    home_external_team_id=int(home_external), away_external_team_id=int(away_external), kickoff_at=kickoff,
                    statistics_complete=int(statistics_rows) == 2 and teams_ok is True,
                )
            )
        except ValueError as error:
            raise CurrentSeasonStatisticsError("provider fixture/team mappings must be numeric") from error
    return tuple(targets)


def _daily_requests_used(conn: Connection[Any], *, provider_id: int) -> int:
    row = conn.execute(
        """SELECT count(*) FROM source.provider_fetches
           WHERE provider_id=%s AND request_started_at >= date_trunc('day', clock_timestamp())""",
        (provider_id,),
    ).fetchone()
    return int(row[0])


def _persist_season_success(
    conn: Connection[Any], *, provider_id: int, season_id: int, params: Mapping[str, str | int],
    response: APIFootballResponse, request_started_at: datetime, response_received_at: datetime,
) -> BatchFetch:
    payload = response.data
    paging = payload.get("paging")
    results = payload.get("results")
    paging_current = paging.get("current") if isinstance(paging, Mapping) else None
    paging_total = paging.get("total") if isinstance(paging, Mapping) else None
    with conn.transaction():
        fetch_id = int(conn.execute(
            """INSERT INTO source.provider_fetches(
                    provider_id,endpoint,request_params,request_params_sha256,purpose,
                    request_started_at,response_received_at,http_status,outcome,
                    provider_results,paging_current,paging_total,content_sha256,subject_season_id
                ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,'success',%s,%s,%s,%s,%s)
                RETURNING id""",
            (provider_id, ENDPOINT, Jsonb(params), request_params_sha256(params), PURPOSE,
             request_started_at, response_received_at, response.status_code,
             results if isinstance(results, int) and not isinstance(results, bool) and results >= 0 else None,
             paging_current if isinstance(paging_current, int) and not isinstance(paging_current, bool) and paging_current >= 1 else None,
             paging_total if isinstance(paging_total, int) and not isinstance(paging_total, bool) and paging_total >= 1 else None,
             hashlib.sha256(response.raw_body).digest(), season_id),
        ).fetchone()[0])
        conn.execute(
            """INSERT INTO source.provider_raw_payloads(
                    fetch_id,inline_body,content_type,byte_count,retention_class,expires_at
                ) VALUES(%s,%s,'application/json',%s,'standard',%s)""",
            (fetch_id, response.raw_body, len(response.raw_body), response_received_at + timedelta(days=RAW_RETENTION_DAYS)),
        )
    return BatchFetch(fetch_id, request_started_at, response_received_at, response)


def _load_reusable_batch_fetch(
    conn: Connection[Any], *, provider_id: int, season_id: int, params: Mapping[str, str | int]
) -> BatchFetch | None:
    """Return a hash-verified retained batch before spending another API call.

    An empty or partial provider statistics response is not a completed pair,
    but it is still durable evidence. Replaying it lets a one-off run resume
    without repeatedly charging quota for the same known absence.
    """
    row = conn.execute(
        """SELECT provider_fetch.id,provider_fetch.request_started_at,provider_fetch.response_received_at,
                  provider_fetch.http_status,provider_fetch.content_sha256,payload.inline_body
           FROM source.provider_fetches provider_fetch
           JOIN source.provider_raw_payloads payload ON payload.fetch_id=provider_fetch.id
           WHERE provider_fetch.provider_id=%s AND provider_fetch.subject_season_id=%s
             AND provider_fetch.endpoint=%s AND provider_fetch.outcome='success'
             AND provider_fetch.request_params_sha256=%s AND payload.purged_at IS NULL
             AND payload.inline_body IS NOT NULL
           ORDER BY provider_fetch.response_received_at DESC NULLS LAST,provider_fetch.id DESC LIMIT 1""",
        (provider_id, season_id, ENDPOINT, request_params_sha256(params)),
    ).fetchone()
    if row is None:
        return None
    fetch_id, started, received, status, digest, body = row
    if received is None or status is None or digest is None:
        raise CurrentSeasonStatisticsError("retained batch raw metadata is incomplete")
    raw = bytes(body)
    if hashlib.sha256(raw).digest() != bytes(digest):
        raise CurrentSeasonStatisticsError("retained batch raw payload hash mismatch")
    try:
        payload = json.loads(raw)
    except ValueError as error:
        raise CurrentSeasonStatisticsError("retained batch raw payload is invalid JSON") from error
    if not isinstance(payload, dict):
        raise CurrentSeasonStatisticsError("retained batch raw payload has invalid top-level shape")
    return BatchFetch(
        int(fetch_id), started, received, APIFootballResponse(payload, raw, int(status), {})
    )


def _bind_returned_fixture_subjects(
    conn: Connection[Any], *, fetch_id: int, returned_fixture_ids: Iterable[int], targets: Sequence[FixtureTarget]
) -> None:
    """Bind provenance only after raw retention and envelope validation.

    A batch can legally omit one requested fixture.  Such a fixture must never
    gain a fetch-to-fixture binding merely because it appeared in ``ids``.
    """
    targets_by_id = {target.fixture_id: target for target in targets}
    ids = tuple(returned_fixture_ids)
    if not set(ids).issubset(targets_by_id):
        raise StatisticsContractError("batch provenance contains an unrequested fixture")
    with conn.cursor() as cursor:
        cursor.executemany(
            "INSERT INTO source.provider_fetch_fixture_subjects(fetch_id,fixture_id) VALUES(%s,%s) ON CONFLICT DO NOTHING",
            [(fetch_id, fixture_id) for fixture_id in ids],
        )


def _optional_goal(value: object, field: str) -> int | None:
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise StatisticsContractError(f"{field} must be a non-negative integer or null")
    return value


def _required_goal(value: object, field: str) -> int:
    parsed = _optional_goal(value, field)
    if parsed is None:
        raise StatisticsContractError(f"{field} is required for a completed fixture")
    return parsed


def _score_goal(score: Mapping[str, Any], period: str, side: str) -> int | None:
    value = score.get(period)
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise StatisticsContractError(f"score.{period} must be an object or null")
    return _optional_goal(value.get(side), f"score.{period}.{side}")


def _completed_discovery_entries(
    payload: Mapping[str, Any], *, scope: CurrentSeasonStatisticsScope
) -> tuple[DiscoveryFixture, ...]:
    """Parse a season-level completed-fixture response before canonical writes."""
    expected_params = {
        "league": str(scope.league_external_id),
        "season": str(scope.season_start_year),
        "status": "FT-AET-PEN",
    }
    if payload.get("parameters") != expected_params:
        raise StatisticsContractError("completed fixture discovery parameters mismatch")
    if payload.get("errors") not in ({}, [], None) or payload.get("paging") != {"current": 1, "total": 1}:
        raise StatisticsContractError("completed fixture discovery envelope is invalid")
    response = payload.get("response")
    if not isinstance(response, list) or payload.get("results") != len(response):
        raise StatisticsContractError("completed fixture discovery results/response mismatch")
    records: list[DiscoveryFixture] = []
    seen: set[int] = set()
    for entry in response:
        if not isinstance(entry, Mapping):
            raise StatisticsContractError("completed fixture discovery entry must be an object")
        fixture = entry.get("fixture")
        league = entry.get("league")
        teams = entry.get("teams")
        goals = entry.get("goals")
        score = entry.get("score")
        if not all(isinstance(value, Mapping) for value in (fixture, league, teams, goals, score)):
            raise StatisticsContractError("completed fixture discovery entry is incomplete")
        assert isinstance(fixture, Mapping) and isinstance(league, Mapping) and isinstance(teams, Mapping)
        assert isinstance(goals, Mapping) and isinstance(score, Mapping)
        external_id = _require_int(fixture.get("id"), "fixture.id")
        if external_id in seen:
            raise StatisticsContractError("completed fixture discovery contains duplicate fixture id")
        seen.add(external_id)
        if _require_int(league.get("id"), "league.id") != scope.league_external_id or league.get("season") != scope.season_start_year:
            raise StatisticsContractError("completed fixture discovery league/season mismatch")
        status = fixture.get("status")
        home = teams.get("home")
        away = teams.get("away")
        if not isinstance(status, Mapping) or status.get("short") not in {"FT", "AET", "PEN"}:
            raise StatisticsContractError("completed fixture discovery contains a non-terminal status")
        status_code = status["short"]
        assert isinstance(status_code, str)
        if not isinstance(home, Mapping) or not isinstance(away, Mapping):
            raise StatisticsContractError("completed fixture discovery teams are invalid")
        kickoff = fixture.get("date")
        if not isinstance(kickoff, str):
            raise StatisticsContractError("completed fixture discovery fixture.date must be a string")
        records.append(
            DiscoveryFixture(
                external_fixture_id=external_id,
                status_code=status_code,
                home_external_team_id=_require_int(home.get("id"), "teams.home.id"),
                away_external_team_id=_require_int(away.get("id"), "teams.away.id"),
                kickoff_at=parse_datetime(kickoff),
                home_goals=_required_goal(goals.get("home"), "goals.home"),
                away_goals=_required_goal(goals.get("away"), "goals.away"),
                home_halftime_goals=_score_goal(score, "halftime", "home"),
                away_halftime_goals=_score_goal(score, "halftime", "away"),
                home_fulltime_goals=_score_goal(score, "fulltime", "home"),
                away_fulltime_goals=_score_goal(score, "fulltime", "away"),
                home_extratime_goals=_score_goal(score, "extratime", "home"),
                away_extratime_goals=_score_goal(score, "extratime", "away"),
                home_penalty_goals=_score_goal(score, "penalty", "home"),
                away_penalty_goals=_score_goal(score, "penalty", "away"),
            )
        )
    return tuple(records)


def _normalize_completed_discovery(
    conn: Connection[Any], *, provider_id: int, season_id: int, records: Sequence[DiscoveryFixture], fetch: BatchFetch,
) -> None:
    """Finalize only eligible canonical fixtures after terminal discovery.

    The active-season importer remains responsible for creating schedules and
    participant mappings.  An unknown provider fixture is intentionally a
    controlled stop rather than a parallel fixture-import architecture.
    """
    canonical_rows = conn.execute(
        """SELECT ref.external_id,fixture.id,fixture.home_team_id,fixture.away_team_id,fixture.kickoff_at,
                  fixture.lifecycle_state::text,fixture.home_goals,fixture.away_goals,
                  fixture.home_halftime_goals,fixture.away_halftime_goals,fixture.home_fulltime_goals,
                  fixture.away_fulltime_goals,fixture.home_extratime_goals,fixture.away_extratime_goals,
                  fixture.home_penalty_goals,fixture.away_penalty_goals,fixture.result_finalized_at,
                  home_ref.external_id,away_ref.external_id
           FROM football.fixtures fixture
           JOIN source.fixture_provider_refs ref ON ref.fixture_id=fixture.id AND ref.provider_id=%s
           JOIN source.team_provider_refs home_ref ON home_ref.team_id=fixture.home_team_id AND home_ref.provider_id=%s
           JOIN source.team_provider_refs away_ref ON away_ref.team_id=fixture.away_team_id AND away_ref.provider_id=%s
           WHERE fixture.season_id=%s FOR UPDATE OF fixture""",
        (provider_id, provider_id, provider_id, season_id),
    ).fetchall()
    canonical = {int(row[0]): row[1:] for row in canonical_rows}
    remote_ids = {record.external_fixture_id for record in records}
    canonical_completed = {int(external_id) for external_id, row in canonical.items() if row[4] == "completed"}
    if not canonical_completed.issubset(remote_ids):
        raise CurrentSeasonStatisticsError("provider completed discovery regressed a canonical completed fixture")
    bindings: list[tuple[int, int]] = []
    eligible_records: list[DiscoveryFixture] = []
    for record in records:
        existing = canonical.get(record.external_fixture_id)
        if existing is None:
            raise CurrentSeasonStatisticsError("completed provider fixture is absent from canonical schedule")
        (
            fixture_id, home_team_id, away_team_id, kickoff_at, lifecycle_state,
            home_goals, away_goals, home_half, away_half, home_full, away_full,
            home_extra, away_extra, home_penalty, away_penalty, finalized_at,
            home_external, away_external,
        ) = existing
        identity = (int(home_external), int(away_external), kickoff_at)
        if identity != (record.home_external_team_id, record.away_external_team_id, record.kickoff_at):
            raise CurrentSeasonStatisticsError("provider completed fixture identity conflicts with canonical schedule")
        bindings.append((fetch.fetch_id, int(fixture_id)))
        result = (
            record.home_goals, record.away_goals, record.home_halftime_goals, record.away_halftime_goals,
            record.home_fulltime_goals, record.away_fulltime_goals, record.home_extratime_goals,
            record.away_extratime_goals, record.home_penalty_goals, record.away_penalty_goals,
        )
        existing_result = (home_goals, away_goals, home_half, away_half, home_full, away_full, home_extra, away_extra, home_penalty, away_penalty)
        if finalized_at is not None:
            if lifecycle_state != "completed" or existing_result != result:
                raise CurrentSeasonStatisticsError("provider completed fixture conflicts with an immutable result")
            eligible_records.append(record)
            continue
        if fetch.response_received_at < kickoff_at + timedelta(hours=3):
            continue
        eligible_records.append(record)
    with conn.cursor() as cursor:
        cursor.executemany(
            "INSERT INTO source.provider_fetch_fixture_subjects(fetch_id,fixture_id) VALUES(%s,%s)",
            bindings,
        )
    for record in eligible_records:
        fixture_id = int(canonical[record.external_fixture_id][0])
        conn.execute(
            """SELECT ops.finalize_season_discovery_fixture_result(
                   %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
               )""",
            (
                fixture_id, fetch.fetch_id, record.home_goals, record.away_goals,
                record.home_halftime_goals, record.away_halftime_goals,
                record.home_fulltime_goals, record.away_fulltime_goals,
                record.home_extratime_goals, record.away_extratime_goals,
                record.home_penalty_goals, record.away_penalty_goals,
            ),
        )
    if eligible_records:
        _upsert_completed_provider_statuses(
            conn,
            provider_id=provider_id,
            season_id=season_id,
            records=eligible_records,
            fetch=fetch,
        )


def _upsert_completed_provider_statuses(
    conn: Connection[Any], *, provider_id: int, season_id: int,
    records: Sequence[DiscoveryFixture], fetch: BatchFetch,
) -> None:
    """Advance exact provider statuses with the fixture lifecycle in one transaction."""
    observations = [
        {"external_id": str(record.external_fixture_id), "status_code": record.status_code}
        for record in records
    ]
    status_codes = sorted({record.status_code for record in records})
    mappings = {
        str(code): str(state)
        for code, state in conn.execute(
            """SELECT external_code,canonical_state::text
               FROM source.fixture_status_code_mappings
               WHERE provider_id=%s AND external_code=ANY(%s)""",
            (provider_id, status_codes),
        ).fetchall()
    }
    if set(mappings) != set(status_codes) or any(state != "completed" for state in mappings.values()):
        raise CurrentSeasonStatisticsError("terminal provider status lacks a completed canonical mapping")

    rows = conn.execute(
        """WITH input AS (
                 SELECT * FROM jsonb_to_recordset(%s::jsonb) AS item(external_id text,status_code text)
               ), resolved AS (
                 SELECT ref.fixture_id,input.status_code
                 FROM input
                 JOIN source.fixture_provider_refs ref
                   ON ref.provider_id=%s AND ref.external_id=input.external_id
                 JOIN football.fixtures fixture ON fixture.id=ref.fixture_id
                 WHERE fixture.season_id=%s
               )
               INSERT INTO source.fixture_provider_status(
                 provider_id,fixture_id,status_code,observed_at,source_fetch_id
               )
               SELECT %s,fixture_id,status_code,%s,%s FROM resolved
               ON CONFLICT(provider_id,fixture_id) DO UPDATE
                 SET status_code=excluded.status_code,observed_at=excluded.observed_at,
                     source_fetch_id=excluded.source_fetch_id
                 WHERE source.fixture_provider_status.observed_at < excluded.observed_at
               RETURNING fixture_id""",
        (Jsonb(observations), provider_id, season_id, provider_id, fetch.response_received_at, fetch.fetch_id),
    ).fetchall()
    if len(rows) != len(observations):
        raise CurrentSeasonStatisticsError("completed discovery could not atomically advance every provider status")


def _record_request_failure(
    conn: Connection[Any], *, provider_id: int, season_id: int, params: Mapping[str, str | int],
    request_started_at: datetime, response_received_at: datetime | None, status_code: int | None,
    outcome: str, error_class: str,
) -> None:
    conn.execute(
        """INSERT INTO source.provider_fetches(
                provider_id,endpoint,request_params,request_params_sha256,purpose,
                request_started_at,response_received_at,http_status,outcome,
                sanitized_error_class,sanitized_error_text,subject_season_id
            ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (provider_id, ENDPOINT, Jsonb(params), request_params_sha256(params), PURPOSE,
         request_started_at, response_received_at, status_code, outcome, error_class,
         "controlled current-season statistics request failed", season_id),
    )


def _mark_contract_error(conn: Connection[Any], fetch_id: int) -> None:
    conn.execute(
        """UPDATE source.provider_fetches
           SET outcome='provider_error', sanitized_error_class='StatisticsContractError',
               sanitized_error_text='batch fixture statistics response violated the importer contract'
           WHERE id=%s""",
        (fetch_id,),
    )


_STATISTICS_COLUMNS = (
    "shots_on_goal", "shots_off_goal", "total_shots", "blocked_shots", "shots_inside_box", "shots_outside_box",
    "fouls", "corner_kicks", "offsides", "yellow_cards", "red_cards", "goalkeeper_saves", "total_passes",
    "passes_accurate", "possession_pct", "pass_accuracy_pct", "expected_goals", "goals_prevented", "extra_metrics",
)


def _json_value(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _statistics_rows(
    *, parsed: Mapping[int, list[dict[str, Any]] | None], targets: Sequence[FixtureTarget], fetch: BatchFetch,
) -> tuple[list[dict[str, object]], set[int], int]:
    targets_by_id = {target.fixture_id: target for target in targets}
    rows: list[dict[str, object]] = []
    affected_teams: set[int] = set()
    skipped = 0
    for fixture_id, blocks in parsed.items():
        if blocks is None:
            skipped += 1
            continue
        target = targets_by_id[fixture_id]
        team_by_external = {
            target.home_external_team_id: target.home_team_id,
            target.away_external_team_id: target.away_team_id,
        }
        for block in blocks:
            external_team_id = int(block["external_team_id"])
            team_id = team_by_external[external_team_id]
            record: dict[str, object] = {
                "fixture_id": fixture_id, "team_id": team_id, "mapping_version": MAPPING_VERSION,
                "observed_at": fetch.response_received_at, "available_at": fetch.response_received_at,
                "last_source_fetch_id": fetch.fetch_id,
            }
            record.update({column: _json_value(block[column]) for column in _STATISTICS_COLUMNS})
            rows.append({key: _json_value(value) for key, value in record.items()})
            affected_teams.add(team_id)
    return rows, affected_teams, skipped


def _mark_statistics_incomplete(
    conn: Connection[Any], *, fixture_ids: frozenset[int], fetch: BatchFetch, coverage_state: str, team_count: int
) -> None:
    """Persist a retryable provider coverage fact without inventing a pair."""
    if not fixture_ids:
        return
    if (coverage_state, team_count) not in {("empty", 0), ("partial", 1)}:
        raise ValueError("statistics incomplete coverage is malformed")
    conn.execute(
        """INSERT INTO football.fixture_statistics_coverage(
                fixture_id,coverage_state,team_count,last_source_fetch_id,observed_at,next_retry_at,attempts
            )
            SELECT fixture_id,%s::football.snapshot_coverage_state,%s,%s,%s,%s,1
            FROM unnest(%s::bigint[]) AS fixture_id
            ON CONFLICT (fixture_id) DO UPDATE
              SET coverage_state=excluded.coverage_state,team_count=excluded.team_count,last_source_fetch_id=excluded.last_source_fetch_id,
                  observed_at=excluded.observed_at,next_retry_at=excluded.next_retry_at,
                  attempts=football.fixture_statistics_coverage.attempts + 1""",
        (coverage_state, team_count, fetch.fetch_id, fetch.response_received_at, fetch.response_received_at + timedelta(hours=6), list(fixture_ids)),
    )


def _mark_statistics_complete(
    conn: Connection[Any], *, fixture_ids: frozenset[int], fetch: BatchFetch
) -> None:
    """A validated two-team batch supersedes a retryable empty/partial marker."""
    if not fixture_ids:
        return
    conn.execute(
        """INSERT INTO football.fixture_statistics_coverage(
                fixture_id,coverage_state,team_count,last_source_fetch_id,observed_at,next_retry_at,attempts
            )
            SELECT fixture_id,'complete'::football.snapshot_coverage_state,2,%s,%s,%s,1
            FROM unnest(%s::bigint[]) AS fixture_id
            ON CONFLICT (fixture_id) DO UPDATE
              SET coverage_state='complete',team_count=2,last_source_fetch_id=excluded.last_source_fetch_id,
                  observed_at=excluded.observed_at,next_retry_at=excluded.next_retry_at""",
        (fetch.fetch_id, fetch.response_received_at, fetch.response_received_at, list(fixture_ids)),
    )


def bulk_upsert_statistics(conn: Connection[Any], *, rows: Sequence[Mapping[str, object]]) -> int:
    """Upsert all rows from one provider batch in one SQL command."""
    if not rows:
        return 0
    payload = json.dumps(list(rows), separators=(",", ":"), sort_keys=True)
    result = conn.execute(
        """WITH incoming AS (
               SELECT * FROM jsonb_to_recordset(%s::jsonb) AS row(
                 fixture_id bigint, team_id bigint,
                 shots_on_goal integer, shots_off_goal integer, total_shots integer, blocked_shots integer,
                 shots_inside_box integer, shots_outside_box integer, fouls integer, corner_kicks integer,
                 offsides integer, yellow_cards integer, red_cards integer, goalkeeper_saves integer,
                 total_passes integer, passes_accurate integer, possession_pct numeric, pass_accuracy_pct numeric,
                 expected_goals numeric, goals_prevented numeric, extra_metrics jsonb, mapping_version text,
                 observed_at timestamptz, available_at timestamptz, last_source_fetch_id bigint
               )
             )
             INSERT INTO football.fixture_team_statistics(
               fixture_id,team_id,shots_on_goal,shots_off_goal,total_shots,blocked_shots,shots_inside_box,shots_outside_box,
               fouls,corner_kicks,offsides,yellow_cards,red_cards,goalkeeper_saves,total_passes,passes_accurate,
               possession_pct,pass_accuracy_pct,expected_goals,goals_prevented,extra_metrics,mapping_version,
               observed_at,available_at,availability_basis,last_source_fetch_id,finalized_at
             )
             SELECT fixture_id,team_id,shots_on_goal,shots_off_goal,total_shots,blocked_shots,shots_inside_box,shots_outside_box,
                    fouls,corner_kicks,offsides,yellow_cards,red_cards,goalkeeper_saves,total_passes,passes_accurate,
                    possession_pct,pass_accuracy_pct,expected_goals,goals_prevented,extra_metrics,mapping_version,
                    observed_at,available_at,'observed',last_source_fetch_id,NULL
             FROM incoming
             ON CONFLICT (fixture_id,team_id) DO UPDATE SET
               shots_on_goal=excluded.shots_on_goal,shots_off_goal=excluded.shots_off_goal,total_shots=excluded.total_shots,
               blocked_shots=excluded.blocked_shots,shots_inside_box=excluded.shots_inside_box,shots_outside_box=excluded.shots_outside_box,
               fouls=excluded.fouls,corner_kicks=excluded.corner_kicks,offsides=excluded.offsides,yellow_cards=excluded.yellow_cards,
               red_cards=excluded.red_cards,goalkeeper_saves=excluded.goalkeeper_saves,total_passes=excluded.total_passes,
               passes_accurate=excluded.passes_accurate,possession_pct=excluded.possession_pct,
               pass_accuracy_pct=excluded.pass_accuracy_pct,expected_goals=excluded.expected_goals,
               goals_prevented=excluded.goals_prevented,extra_metrics=excluded.extra_metrics,mapping_version=excluded.mapping_version,
               observed_at=excluded.observed_at,available_at=excluded.available_at,last_source_fetch_id=excluded.last_source_fetch_id
             WHERE football.fixture_team_statistics.finalized_at IS NULL
             RETURNING fixture_id,team_id""",
        (payload,),
    ).fetchall()
    return len(result)


def _team_history(conn: Connection[Any], *, team_id: int, season_id: int) -> list[TeamMatchRecord]:
    rows = conn.execute(
        """SELECT fixture.id,fixture.kickoff_at,(fixture.home_team_id=%s) AS is_home,
                  CASE WHEN fixture.home_team_id=%s THEN fixture.home_goals ELSE fixture.away_goals END,
                  CASE WHEN fixture.home_team_id=%s THEN fixture.away_goals ELSE fixture.home_goals END,
                  own.expected_goals,opponent.expected_goals,own.total_shots,own.shots_on_goal,
                  own.possession_pct,own.corner_kicks,own.yellow_cards,own.red_cards
           FROM football.fixtures fixture
           JOIN football.fixture_team_statistics own ON own.fixture_id=fixture.id AND own.team_id=%s
           JOIN football.fixture_team_statistics opponent ON opponent.fixture_id=fixture.id
             AND opponent.team_id=CASE WHEN fixture.home_team_id=%s THEN fixture.away_team_id ELSE fixture.home_team_id END
           WHERE fixture.season_id=%s AND fixture.lifecycle_state='completed'
             AND fixture.result_finalized_at IS NOT NULL
             AND %s IN (fixture.home_team_id,fixture.away_team_id)
           ORDER BY fixture.kickoff_at DESC,fixture.id DESC""",
        (team_id, team_id, team_id, team_id, team_id, season_id, team_id),
    ).fetchall()
    return [
        TeamMatchRecord(
            fixture_id=int(row[0]), kickoff_at=row[1], is_home=bool(row[2]), goals_for=int(row[3]), goals_against=int(row[4]),
            expected_goals=row[5], expected_goals_against=row[6], total_shots=row[7], shots_on_goal=row[8],
            possession_pct=row[9], corner_kicks=row[10], yellow_cards=row[11], red_cards=row[12],
        )
        for row in rows
    ]


def bulk_upsert_rolling_metrics(
    conn: Connection[Any], *, season_id: int, team_ids: Iterable[int], now: datetime,
) -> int:
    records: list[dict[str, object]] = []
    for team_id in sorted(set(team_ids)):
        for metric in calculate_rolling_metrics(_team_history(conn, team_id=team_id, season_id=season_id)):
            if metric.window_size == 0:
                continue
            record = asdict(metric)
            record.update({"team_id": team_id, "season_id": season_id, "updated_at": now})
            records.append({key: _json_value(value) for key, value in record.items()})
    if not records:
        return 0
    payload = json.dumps(records, separators=(",", ":"), sort_keys=True)
    return len(conn.execute(
        """WITH incoming AS (
              SELECT * FROM jsonb_to_recordset(%s::jsonb) AS row(
                team_id bigint,season_id bigint,scope text,window_size smallint,matches_count smallint,
                avg_xg numeric,xg_sample_count smallint,avg_xga numeric,xga_sample_count smallint,
                avg_goals_for numeric,avg_goals_against numeric,
                scored_rate numeric,conceded_rate numeric,btts_rate numeric,over_1_5_rate numeric,
                over_2_5_rate numeric,over_3_5_rate numeric,avg_shots numeric,avg_shots_on_goal numeric,
                shots_sample_count smallint,shots_on_goal_sample_count smallint,avg_corners numeric,
                corners_sample_count smallint,avg_possession numeric,possession_sample_count smallint,
                source_last_kickoff_at timestamptz,updated_at timestamptz
              )
            )
            INSERT INTO football.team_rolling_metrics(
              team_id,season_id,scope,window_size,matches_count,avg_xg,xg_sample_count,avg_xga,xga_sample_count,
              avg_goals_for,avg_goals_against,
              scored_rate,conceded_rate,btts_rate,over_1_5_rate,over_2_5_rate,over_3_5_rate,avg_shots,
              shots_sample_count,avg_shots_on_goal,shots_on_goal_sample_count,avg_corners,corners_sample_count,
              avg_possession,possession_sample_count,source_last_kickoff_at,updated_at
            ) SELECT team_id,season_id,scope,window_size,matches_count,avg_xg,xg_sample_count,avg_xga,xga_sample_count,
                     avg_goals_for,avg_goals_against,
                     scored_rate,conceded_rate,btts_rate,over_1_5_rate,over_2_5_rate,over_3_5_rate,avg_shots,
                     shots_sample_count,avg_shots_on_goal,shots_on_goal_sample_count,avg_corners,corners_sample_count,
                     avg_possession,possession_sample_count,source_last_kickoff_at,updated_at FROM incoming
            ON CONFLICT(team_id,season_id,scope,window_size) DO UPDATE SET
              matches_count=excluded.matches_count,avg_xg=excluded.avg_xg,xg_sample_count=excluded.xg_sample_count,
              avg_xga=excluded.avg_xga,xga_sample_count=excluded.xga_sample_count,
              avg_goals_for=excluded.avg_goals_for,avg_goals_against=excluded.avg_goals_against,
              scored_rate=excluded.scored_rate,conceded_rate=excluded.conceded_rate,btts_rate=excluded.btts_rate,
              over_1_5_rate=excluded.over_1_5_rate,over_2_5_rate=excluded.over_2_5_rate,
              over_3_5_rate=excluded.over_3_5_rate,avg_shots=excluded.avg_shots,
              shots_sample_count=excluded.shots_sample_count,avg_shots_on_goal=excluded.avg_shots_on_goal,
              shots_on_goal_sample_count=excluded.shots_on_goal_sample_count,avg_corners=excluded.avg_corners,
              corners_sample_count=excluded.corners_sample_count,avg_possession=excluded.avg_possession,
              possession_sample_count=excluded.possession_sample_count,
              source_last_kickoff_at=excluded.source_last_kickoff_at,
              updated_at=excluded.updated_at
            RETURNING team_id""",
        (payload,),
    ).fetchall())


async def _fetch_batch(client: _Client, targets: Sequence[FixtureTarget]) -> APIFootballResponse:
    return await client.get(ENDPOINT, params={"ids": fixture_ids_parameter(target.external_fixture_id for target in targets)})


def _discovery_params(scope: CurrentSeasonStatisticsScope) -> dict[str, int | str]:
    return {"league": scope.league_external_id, "season": scope.season_start_year, "status": "FT-AET-PEN"}


async def _fetch_completed_discovery(client: _Client, scope: CurrentSeasonStatisticsScope) -> APIFootballResponse:
    return await client.get(ENDPOINT, params=_discovery_params(scope))


def _lock_key(scope: CurrentSeasonStatisticsScope) -> str:
    return f"api-football:current-season-statistics:{scope.league_external_id}:{scope.season_start_year}:v1"


def _acquire_lock(conn: Connection[Any], scope: CurrentSeasonStatisticsScope) -> None:
    row = conn.execute("SELECT pg_try_advisory_lock(hashtextextended(%s, 0))", (_lock_key(scope),)).fetchone()
    if row is None or row[0] is not True:
        raise CurrentSeasonStatisticsError("current-season statistics run is already active for this league/season")


def _release_lock(conn: Connection[Any], scope: CurrentSeasonStatisticsScope) -> None:
    conn.execute("SELECT pg_advisory_unlock(hashtextextended(%s, 0))", (_lock_key(scope),))


def _observe_rate_limit(conn: Connection[Any], *, provider_id: int, endpoint: str, headers: Mapping[str, str]) -> None:
    conn.execute("SELECT source.observe_provider_rate_limit(%s,%s,%s)", (provider_id, endpoint, Jsonb(dict(headers))))


async def run_current_season_statistics_backfill_async(
    *, scope: CurrentSeasonStatisticsScope, client: _Client | None = None, sleep: Sleep = asyncio.sleep,
    clock: Clock = _utcnow,
) -> CurrentSeasonStatisticsReport:
    """Run one generic league/season slice on one event loop.

    The synchronous wrapper is only for the CLI and tests.  A long-lived worker
    must call this coroutine directly so its reusable HTTP connection pool is
    never moved across event loops.
    """
    api = client or APIFootballClient.from_environment(budget_consumer="operations")
    owns_client = client is None
    with psycopg.connect(_database_url(), autocommit=True) as conn:
        _acquire_lock(conn, scope)
        try:
            provider_id, season_id = _context(conn, scope)
            used = _daily_requests_used(conn, provider_id=provider_id)
            api_requests = retries = batch_requests = 0
            discovery_requests = 0
            written = normalized = skipped = 0
            discovered_targets: tuple[FixtureTarget, ...] = ()
            selected: tuple[FixtureTarget, ...] = ()
            aggregate_teams: set[int] = set()
            errors: list[str] = []
            quota: dict[str, str] = {}
            stopped: str | None = None

            async def request(
                *, params: Mapping[str, str | int], kind: str, targets: Sequence[FixtureTarget] = ()
            ) -> BatchFetch | None:
                nonlocal api_requests, retries, batch_requests, discovery_requests, quota, stopped
                for attempt in range(1, MAX_ATTEMPTS_PER_BATCH + 1):
                    # This check is deliberately inside the retry loop: a failed
                    # physical request consumes both the run and daily budgets.
                    if api_requests >= scope.max_requests:
                        stopped = "run_request_cap"; return None
                    if used + api_requests >= scope.daily_request_cap:
                        stopped = "daily_request_cap"; return None
                    if attempt > 1:
                        retries += 1
                        await sleep(float(2 ** (attempt - 1)))
                    started = clock()
                    try:
                        response = await api.get(ENDPOINT, params=params)
                        received = clock(); api_requests += 1
                        if kind == "batch":
                            batch_requests += 1
                        else:
                            discovery_requests += 1
                        quota = safe_rate_limit_headers(response.headers)
                        _observe_rate_limit(conn, provider_id=provider_id, endpoint=ENDPOINT, headers=quota)
                        if api.response_contains_api_key(response.raw_body):
                            raise CurrentSeasonStatisticsError("provider response contains API key")
                        return _persist_season_success(
                            conn, provider_id=provider_id, season_id=season_id, params=params, response=response,
                            request_started_at=started, response_received_at=received,
                        )
                    except APIFootballHTTPError as error:
                        received = clock(); api_requests += 1
                        if kind == "batch":
                            batch_requests += 1
                        else:
                            discovery_requests += 1
                        quota = dict(error.safe_headers)
                        _observe_rate_limit(conn, provider_id=provider_id, endpoint=ENDPOINT, headers=quota)
                        _record_request_failure(
                            conn, provider_id=provider_id, season_id=season_id, params=params, request_started_at=started,
                            response_received_at=None if error.status_code == 0 else received, status_code=error.status_code or None,
                            outcome="transport_error" if error.status_code == 0 else "http_error", error_class=type(error).__name__,
                        )
                        if error.status_code == 429:
                            stopped = "provider_rate_limit"; return None
                        if error.status_code not in RETRYABLE_HTTP_STATUSES or attempt == MAX_ATTEMPTS_PER_BATCH:
                            stopped = "provider_request_failure"; errors.append(type(error).__name__); return None
                    except APIFootballAPIError as error:
                        received = clock(); api_requests += 1
                        if kind == "batch":
                            batch_requests += 1
                        else:
                            discovery_requests += 1
                        quota = dict(error.safe_headers)
                        _observe_rate_limit(conn, provider_id=provider_id, endpoint=ENDPOINT, headers=quota)
                        _record_request_failure(
                            conn, provider_id=provider_id, season_id=season_id, params=params, request_started_at=started,
                            response_received_at=received, status_code=error.status_code, outcome="provider_error", error_class=type(error).__name__,
                        )
                        stopped = "provider_api_error"; errors.append(type(error).__name__); return None
                return None

            if scope.project_discovery:
                discovery = await request(params=_discovery_params(scope), kind="discovery")
                if discovery is not None:
                    try:
                        discovery_records = _completed_discovery_entries(discovery.response.data, scope=scope)
                        # Raw evidence was committed before parsing.  Canonical
                        # fixture changes and the normalized marker, however,
                        # are one all-or-nothing transition for this discovery body.
                        with conn.transaction():
                            _normalize_completed_discovery(
                                conn, provider_id=provider_id, season_id=season_id, records=discovery_records, fetch=discovery,
                            )
                            conn.execute("UPDATE source.provider_fetches SET normalized_at=clock_timestamp() WHERE id=%s", (discovery.fetch_id,))
                    except (StatisticsContractError, CurrentSeasonStatisticsError) as error:
                        _mark_contract_error(conn, discovery.fetch_id)
                        errors.append(str(error)); stopped = "completed_fixture_discovery_error"

            if stopped is None:
                discovered_targets = load_completed_targets(
                    conn, provider_id=provider_id, season_id=season_id,
                    require_finalized_results=scope.require_finalized_results,
                )
                selected_candidates = (
                    discovered_targets
                    if scope.select_all_completed
                    else select_recent_history(discovered_targets)
                )
                selected = tuple(target for target in selected_candidates if not target.statistics_complete)
                for batch in chunk_fixture_targets(selected):
                    params = {"ids": fixture_ids_parameter(target.external_fixture_id for target in batch)}
                    fetched = _load_reusable_batch_fetch(
                        conn, provider_id=provider_id, season_id=season_id, params=params
                    )
                    if fetched is None:
                        fetched = await request(params=params, kind="batch", targets=batch)
                    if fetched is None:
                        break
                    try:
                        parsed = _batch_entries(
                            fetched.response.data, targets=batch, league_external_id=scope.league_external_id
                        )
                        # Provenance, statistics, rolling materialization, and
                        # the normalized marker form one retry-safe batch
                        # boundary.  A metrics failure must not leave a
                        # statistics pair that a later run will incorrectly
                        # treat as complete.
                        with conn.transaction():
                            _bind_returned_fixture_subjects(
                                conn, fetch_id=fetched.fetch_id, returned_fixture_ids=parsed.returned_fixture_ids, targets=batch
                            )
                            rows, teams, skipped_in_batch = _statistics_rows(
                                parsed=parsed.statistics_by_fixture, targets=batch, fetch=fetched
                            )
                            _mark_statistics_incomplete(
                                conn,
                                fixture_ids=parsed.statistics_unavailable_fixture_ids,
                                fetch=fetched,
                                coverage_state="empty",
                                team_count=0,
                            )
                            written += bulk_upsert_statistics(conn, rows=rows)
                            _mark_statistics_complete(
                                conn,
                                fixture_ids=frozenset(
                                    fixture_id for fixture_id, blocks in parsed.statistics_by_fixture.items() if blocks is not None
                                ),
                                fetch=fetched,
                            )
                            _mark_statistics_incomplete(
                                conn,
                                fixture_ids=parsed.statistics_partial_fixture_ids,
                                fetch=fetched,
                                coverage_state="partial",
                                team_count=1,
                            )
                            if teams:
                                bulk_upsert_rolling_metrics(conn, season_id=season_id, team_ids=teams, now=clock())
                            conn.execute("UPDATE source.provider_fetches SET normalized_at=clock_timestamp() WHERE id=%s", (fetched.fetch_id,))
                        normalized += sum(blocks is not None for blocks in parsed.statistics_by_fixture.values())
                        skipped += skipped_in_batch
                        aggregate_teams.update(teams)
                    except StatisticsContractError as error:
                        _mark_contract_error(conn, fetched.fetch_id)
                        errors.append(str(error)); stopped = "statistics_contract_error"; break
            # Season-to-date rows are intentionally withheld: this slice fetches
            # a last-ten union, not a full season statistics history.
            return CurrentSeasonStatisticsReport(
                league_external_id=scope.league_external_id, season_start_year=scope.season_start_year,
                fixtures_discovered=len(discovered_targets), unique_fixtures_selected=len(selected),
                fixture_discovery_requests=discovery_requests, batch_requests=batch_requests,
                fixtures_normalized=normalized, statistics_rows_written=written, teams_aggregated=len(aggregate_teams),
                api_requests=api_requests, retries=retries, skipped_fixtures=skipped, errors=tuple(errors),
                stopped_reason=stopped, safe_rate_limit=quota,
            )
        finally:
            _release_lock(conn, scope)
            if owns_client:
                await api.aclose()  # type: ignore[union-attr]


def run_current_season_statistics_backfill(
    *, scope: CurrentSeasonStatisticsScope, client: _Client | None = None, sleep: Sleep = asyncio.sleep,
    clock: Clock = _utcnow,
) -> CurrentSeasonStatisticsReport:
    """Synchronous boundary for one-shot CLI and synchronous test callers."""
    return asyncio.run(run_current_season_statistics_backfill_async(scope=scope, client=client, sleep=sleep, clock=clock))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one current-season batch fixture-statistics backfill")
    parser.add_argument("--league-external-id", type=int, required=True)
    parser.add_argument("--season-start-year", type=int, required=True)
    parser.add_argument("--max-requests", type=int, default=DEFAULT_MAX_REQUESTS)
    parser.add_argument("--daily-request-cap", type=int, default=DEFAULT_DAILY_REQUEST_CAP)
    args = parser.parse_args()
    report = run_current_season_statistics_backfill(scope=CurrentSeasonStatisticsScope(**vars(args)))
    print(json.dumps(asdict(report), default=str, sort_keys=True))


if __name__ == "__main__":
    main()
