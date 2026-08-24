"""Controlled importer for factual, post-match API-Football fixture lineups.

This module is intentionally manual and quota-bounded.  It records an
unmodified provider response before normalizing it into the separate
``fixture_historical_*`` tables.  It never writes pre-match lineup tables and
never treats a retrospective lineup as pre-kickoff information.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

import psycopg
from psycopg import Connection, sql
from psycopg.types.json import Jsonb

from app.api_football import APIFootballClient, APIFootballResponse
from app.api_football.client import safe_rate_limit_headers
from app.api_football.errors import APIFootballAPIError, APIFootballHTTPError
from app.importer.canary import request_params_sha256

PROVIDER_CODE = "api-football"
# Backward-compatible defaults for the already imported EPL 2024 scope.  New
# campaigns must pass their own immutable HistoricalLineupsScope explicitly.
LEAGUE_EXTERNAL_ID = "39"
SEASON_START_YEAR = 2024
EXPECTED_FIXTURE_COUNT = 380
ENDPOINT = "/fixtures/lineups"
PURPOSE = "historical_backfill"
MAPPING_VERSION = "api-football-lineups-v1"
RAW_RETENTION_DAYS = 30
ANOMALY_RETENTION_DAYS = 90
MAX_CANARY_CALLS = 10
FIRST_BATCH_CALLS = 5
QUOTA_RESERVE = 25

Sleep = Callable[[float], Awaitable[None]]
Clock = Callable[[], datetime]
CoverageState = Literal["complete", "empty", "partial"]


class HistoricalLineupsError(RuntimeError):
    """Controlled-stop condition; callers must not continue this campaign."""


class HistoricalLineupsContractError(HistoricalLineupsError):
    """A provider body cannot be safely mapped to the canonical schema."""


@dataclass(frozen=True)
class HistoricalLineupsScope:
    """One immutable provider league/season campaign.

    ``season_start_year`` is the API-Football season identifier, while the
    corresponding canonical ``football.seasons.id`` is resolved under the
    provider mapping lock.  The scope-specific advisory lock prevents one
    campaign from blocking a different league or season.
    """

    league_external_id: str = LEAGUE_EXTERNAL_ID
    season_start_year: int = SEASON_START_YEAR
    expected_fixture_count: int = EXPECTED_FIXTURE_COUNT
    provider_code: str = PROVIDER_CODE

    def __post_init__(self) -> None:
        if not self.league_external_id.strip():
            raise ValueError("league_external_id must be non-blank")
        if self.season_start_year < 1:
            raise ValueError("season_start_year must be positive")
        if self.expected_fixture_count < 1:
            raise ValueError("expected_fixture_count must be positive")
        if not self.provider_code.strip():
            raise ValueError("provider_code must be non-blank")

    @property
    def lock_key(self) -> str:
        return (
            f"{self.provider_code}:historical-lineups:"
            f"{self.league_external_id}:{self.season_start_year}:v1"
        )


DEFAULT_HISTORICAL_LINEUPS_SCOPE = HistoricalLineupsScope()


@dataclass(frozen=True)
class FixtureTarget:
    fixture_id: int
    season_id: int
    external_id: int
    home_team_id: int
    away_team_id: int
    home_external_id: int
    away_external_id: int
    kickoff_at: datetime
    result_finalized_at: datetime


@dataclass(frozen=True)
class LineupPlayer:
    external_id: int
    display_name: str
    shirt_number: int | None
    position: str | None
    grid: str | None
    role: Literal["starter", "substitute"]


@dataclass(frozen=True)
class TeamLineup:
    external_team_id: int
    coach_external_id: int | None
    coach_name: str | None
    coach_photo_url: str | None
    formation: str | None
    players: tuple[LineupPlayer, ...]


@dataclass(frozen=True)
class ParsedLineups:
    coverage_state: CoverageState
    lineups: tuple[TeamLineup, ...]


@dataclass(frozen=True)
class StoredRaw:
    fetch_id: int
    raw_body: bytes
    request_started_at: datetime
    response_received_at: datetime
    status_code: int
    provider_results: int | None


@dataclass(frozen=True)
class EntityCounts:
    players_created: int = 0
    players_reused: int = 0
    coaches_created: int = 0
    coaches_reused: int = 0

    def plus(self, *, kind: Literal["player", "coach"], created: bool) -> "EntityCounts":
        values = asdict(self)
        prefix = "players" if kind == "player" else "coaches"
        values[f"{prefix}_{'created' if created else 'reused'}"] += 1
        return EntityCounts(**values)


@dataclass(frozen=True)
class NormalizationResult:
    coverage_state: CoverageState
    snapshot_created: bool
    team_lineups_created: int
    lineup_players_created: int
    entities: EntityCounts
    replayed: bool = False


@dataclass(frozen=True)
class HistoricalLineupsCanaryReport:
    physical_api_calls: int
    retained_raw_replays: int
    complete: int
    empty: int
    partial: int
    errors: int
    snapshots_created: int
    team_lineups_created: int
    lineup_players_created: int
    players_created: int
    players_reused: int
    coaches_created: int
    coaches_reused: int
    quota: Mapping[str, str]
    selected_fixture_external_ids: tuple[int, ...]
    replay_fixture_external_id: int | None
    verification: Mapping[str, Any]


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _database_url() -> str:
    value = os.environ.get("SUPABASE_DB_URL")
    if not value:
        raise HistoricalLineupsError("SUPABASE_DB_URL is required")
    return value


def _params(external_fixture_id: int) -> dict[str, int]:
    return {"fixture": external_fixture_id}


def _required_int(value: Any, *, field: str, minimum: int = 1) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        raise HistoricalLineupsContractError(f"{field} must be an integer >= {minimum}")
    return value


def _optional_text(value: Any, *, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise HistoricalLineupsContractError(f"{field} must be a non-blank string or null")
    return value


def _optional_shirt_number(value: Any) -> int | None:
    if value is None:
        return None
    number = _required_int(value, field="player.number", minimum=0)
    if number > 199:
        return _raise_shirt_number()
    return number


def _raise_shirt_number() -> int:
    raise HistoricalLineupsContractError("player.number must be between 0 and 199")


def _parse_player(wrapper: Any, *, role: Literal["starter", "substitute"]) -> LineupPlayer:
    if not isinstance(wrapper, Mapping):
        raise HistoricalLineupsContractError("lineup player wrapper must be an object")
    player = wrapper.get("player")
    if not isinstance(player, Mapping):
        raise HistoricalLineupsContractError("lineup player wrapper is missing player object")
    name = player.get("name")
    if not isinstance(name, str) or not name.strip():
        raise HistoricalLineupsContractError("lineup player name must be non-blank")
    return LineupPlayer(
        external_id=_required_int(player.get("id"), field="player.id"),
        display_name=name,
        shirt_number=_optional_shirt_number(player.get("number")),
        position=_optional_text(player.get("pos"), field="player.pos"),
        grid=_optional_text(player.get("grid"), field="player.grid"),
        role=role,
    )


def _parse_team_lineup(entry: Any) -> TeamLineup:
    if not isinstance(entry, Mapping):
        raise HistoricalLineupsContractError("lineup entry must be an object")
    team = entry.get("team")
    if not isinstance(team, Mapping):
        raise HistoricalLineupsContractError("lineup entry is missing team object")
    coach_value = entry.get("coach")
    if coach_value in (None, {}):
        coach_external_id = None
        coach_name = None
        coach_photo_url = None
    else:
        if not isinstance(coach_value, Mapping):
            raise HistoricalLineupsContractError("coach must be an object or null")
        # API-Football can return a coach object whose identity fields are all
        # null.  It carries no durable external identity, so retaining its
        # display/photo values would manufacture an unlinked coach entity.
        # Treat that documented nullable shape exactly like an absent coach.
        # A non-null ID remains strict because it is the canonical mapping key.
        if coach_value.get("id") is None:
            coach_external_id = None
            coach_name = None
            coach_photo_url = None
        else:
            coach_external_id = _required_int(coach_value.get("id"), field="coach.id")
            coach_name = _optional_text(coach_value.get("name"), field="coach.name")
            if coach_name is None:
                raise HistoricalLineupsContractError("coach.name is required when coach.id is present")
            coach_photo_url = _optional_text(coach_value.get("photo"), field="coach.photo")

    formation = _optional_text(entry.get("formation"), field="formation")
    players: list[LineupPlayer] = []
    for response_field, role in (("startXI", "starter"), ("substitutes", "substitute")):
        group = entry.get(response_field)
        if not isinstance(group, list):
            raise HistoricalLineupsContractError(f"{response_field} must be an array")
        players.extend(_parse_player(item, role=role) for item in group)

    return TeamLineup(
        external_team_id=_required_int(team.get("id"), field="team.id"),
        coach_external_id=coach_external_id,
        coach_name=coach_name,
        coach_photo_url=coach_photo_url,
        formation=formation,
        players=tuple(players),
    )


def classify_response(payload: Mapping[str, Any], target: FixtureTarget) -> ParsedLineups:
    """Validate the raw provider contract before any canonical writes."""
    if payload.get("get") != "fixtures/lineups":
        raise HistoricalLineupsContractError("lineup response endpoint mismatch")
    if payload.get("parameters") != {"fixture": str(target.external_id)}:
        raise HistoricalLineupsContractError("lineup response parameters mismatch")
    if payload.get("errors") not in (None, {}, []):
        raise HistoricalLineupsContractError("provider lineup response contains errors")
    response = payload.get("response")
    results = payload.get("results")
    paging = payload.get("paging")
    if (
        not isinstance(response, list)
        or not isinstance(results, int)
        or isinstance(results, bool)
        or results < 0
        or results != len(response)
        or not isinstance(paging, Mapping)
        or not isinstance(paging.get("current"), int)
        or isinstance(paging.get("current"), bool)
        or not isinstance(paging.get("total"), int)
        or isinstance(paging.get("total"), bool)
        or paging.get("current") != 1
        or paging.get("total") != 1
    ):
        raise HistoricalLineupsContractError("lineup wrapper has invalid results, response, or paging")
    if results > 2:
        raise HistoricalLineupsContractError("lineup response contains more than two teams")
    if results == 0:
        return ParsedLineups("empty", ())

    lineups = tuple(_parse_team_lineup(entry) for entry in response)
    external_teams = [lineup.external_team_id for lineup in lineups]
    if len(external_teams) != len(set(external_teams)):
        raise HistoricalLineupsContractError("lineup response contains duplicate teams")
    allowed_teams = {target.home_external_id, target.away_external_id}
    if not set(external_teams).issubset(allowed_teams):
        raise HistoricalLineupsContractError("lineup response contains a non-participant team")
    if results == 2 and set(external_teams) != allowed_teams:
        raise HistoricalLineupsContractError("complete lineup response does not contain both fixture teams")

    player_ids = [player.external_id for lineup in lineups for player in lineup.players]
    if len(player_ids) != len(set(player_ids)):
        raise HistoricalLineupsContractError("lineup response assigns a player more than once")

    return ParsedLineups("complete" if results == 2 else "partial", lineups)


def acquire_context_and_lock(
    conn: Connection[Any], *, scope: HistoricalLineupsScope = DEFAULT_HISTORICAL_LINEUPS_SCOPE,
) -> tuple[int, int]:
    locked = conn.execute(
        "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))", (scope.lock_key,)
    ).fetchone()[0]
    if not locked:
        raise HistoricalLineupsError("historical lineup importer is already running")
    row = conn.execute(
        """SELECT provider.id, season_ref.season_id
           FROM source.providers provider
           JOIN source.season_provider_refs season_ref ON season_ref.provider_id = provider.id
           WHERE provider.code = %s
             AND season_ref.league_external_id = %s
             AND season_ref.external_season = %s""",
        (scope.provider_code, scope.league_external_id, scope.season_start_year),
    ).fetchone()
    if row is None:
        raise HistoricalLineupsError(
            "provider and season mappings are required for "
            f"league={scope.league_external_id} season={scope.season_start_year}"
        )
    return int(row[0]), int(row[1])


def release_lock(
    conn: Connection[Any], *, scope: HistoricalLineupsScope = DEFAULT_HISTORICAL_LINEUPS_SCOPE,
) -> None:
    conn.execute("SELECT pg_advisory_unlock(hashtextextended(%s, 0))", (scope.lock_key,))


def _targets_without_historical_attempts(
    conn: Connection[Any], *, provider_id: int, season_id: int, limit: int,
    scope: HistoricalLineupsScope = DEFAULT_HISTORICAL_LINEUPS_SCOPE,
) -> list[FixtureTarget]:
    if limit < 1:
        raise ValueError("target limit must be positive")
    fixture_count = conn.execute(
        """SELECT count(*) FROM football.fixtures
           WHERE season_id=%s AND lifecycle_state='completed' AND result_finalized_at IS NOT NULL""",
        (season_id,),
    ).fetchone()[0]
    if fixture_count != scope.expected_fixture_count:
        raise HistoricalLineupsError(
            "preflight requires "
            f"{scope.expected_fixture_count} completed and finalized fixtures for "
            f"league={scope.league_external_id} season={scope.season_start_year}"
        )

    rows = conn.execute(
        """SELECT fixture.id, fixture.season_id, fixture_ref.external_id,
                  fixture.home_team_id, fixture.away_team_id,
                  home_ref.external_id, away_ref.external_id,
                  fixture.kickoff_at, fixture.result_finalized_at
           FROM football.fixtures fixture
           JOIN source.fixture_provider_refs fixture_ref
             ON fixture_ref.fixture_id=fixture.id AND fixture_ref.provider_id=%s
           JOIN source.team_provider_refs home_ref
             ON home_ref.team_id=fixture.home_team_id AND home_ref.provider_id=%s
           JOIN source.team_provider_refs away_ref
             ON away_ref.team_id=fixture.away_team_id AND away_ref.provider_id=%s
           WHERE fixture.season_id=%s
             AND fixture.lifecycle_state='completed'
             AND fixture.result_finalized_at IS NOT NULL
             AND NOT EXISTS (
               SELECT 1 FROM source.provider_fetches provider_fetch
               WHERE provider_fetch.provider_id=%s
                 AND provider_fetch.endpoint=%s
                 AND provider_fetch.purpose=%s
                 AND provider_fetch.subject_fixture_id=fixture.id
             )
           ORDER BY fixture.kickoff_at ASC, fixture.id ASC
           LIMIT %s""",
        (provider_id, provider_id, provider_id, season_id, provider_id, ENDPOINT, PURPOSE, limit),
    ).fetchall()
    return [
        FixtureTarget(
            fixture_id=int(row[0]),
            season_id=int(row[1]),
            external_id=int(row[2]),
            home_team_id=int(row[3]),
            away_team_id=int(row[4]),
            home_external_id=int(row[5]),
            away_external_id=int(row[6]),
            kickoff_at=row[7],
            result_finalized_at=row[8],
        )
        for row in rows
    ]


def _target_for_fixture(
    conn: Connection[Any], *, provider_id: int, season_id: int, fixture_id: int
) -> FixtureTarget:
    row = conn.execute(
        """SELECT fixture.id, fixture.season_id, fixture_ref.external_id,
                  fixture.home_team_id, fixture.away_team_id,
                  home_ref.external_id, away_ref.external_id,
                  fixture.kickoff_at, fixture.result_finalized_at
           FROM football.fixtures fixture
           JOIN source.fixture_provider_refs fixture_ref
             ON fixture_ref.fixture_id=fixture.id AND fixture_ref.provider_id=%s
           JOIN source.team_provider_refs home_ref
             ON home_ref.team_id=fixture.home_team_id AND home_ref.provider_id=%s
           JOIN source.team_provider_refs away_ref
             ON away_ref.team_id=fixture.away_team_id AND away_ref.provider_id=%s
           WHERE fixture.id=%s AND fixture.season_id=%s
             AND fixture.lifecycle_state='completed' AND fixture.result_finalized_at IS NOT NULL""",
        (provider_id, provider_id, provider_id, fixture_id, season_id),
    ).fetchone()
    if row is None:
        raise HistoricalLineupsError("unfinished historical-lineup fetch has an invalid fixture context")
    return FixtureTarget(
        fixture_id=int(row[0]), season_id=int(row[1]), external_id=int(row[2]),
        home_team_id=int(row[3]), away_team_id=int(row[4]),
        home_external_id=int(row[5]), away_external_id=int(row[6]),
        kickoff_at=row[7], result_finalized_at=row[8],
    )


def _resume_unfinished_success_fetches(
    conn: Connection[Any], *, provider_id: int, season_id: int, clock: Clock
) -> list[NormalizationResult]:
    """Replay only recoverable raw success responses before any new API call.

    A failed provider/HTTP/contract fetch is deliberately not retried here:
    it is durable evidence requiring explicit operator review.  A successful
    raw body without a snapshot is the sole automatically recoverable state.
    """
    rows = conn.execute(
        """SELECT provider_fetch.id, provider_fetch.subject_fixture_id,
                  provider_fetch.outcome::text, provider_fetch.normalized_at,
                  raw.purged_at, raw.inline_body IS NOT NULL
           FROM source.provider_fetches provider_fetch
           LEFT JOIN football.fixture_historical_lineup_snapshots snapshot
             ON snapshot.source_fetch_id=provider_fetch.id
           LEFT JOIN source.provider_raw_payloads raw ON raw.fetch_id=provider_fetch.id
           WHERE provider_fetch.provider_id=%s
             AND provider_fetch.endpoint=%s
             AND provider_fetch.purpose=%s
             AND provider_fetch.subject_season_id=%s
             AND snapshot.id IS NULL
           ORDER BY provider_fetch.id""",
        (provider_id, ENDPOINT, PURPOSE, season_id),
    ).fetchall()
    results: list[NormalizationResult] = []
    for fetch_id, fixture_id, outcome, normalized_at, purged_at, has_inline_body in rows:
        if outcome != "success":
            raise HistoricalLineupsError(
                f"historical-lineup fetch {fetch_id} has terminal {outcome} state; operator review is required"
            )
        if normalized_at is not None:
            raise HistoricalLineupsError(
                f"historical-lineup fetch {fetch_id} is normalized without a canonical snapshot"
            )
        if fixture_id is None or purged_at is not None or not has_inline_body:
            raise HistoricalLineupsError(
                f"historical-lineup fetch {fetch_id} lacks a reusable retained raw payload"
            )
        target = _target_for_fixture(
            conn, provider_id=provider_id, season_id=season_id, fixture_id=int(fixture_id)
        )
        raw = _load_retained_raw(
            conn, provider_id=provider_id, target=target, fetch_id=int(fetch_id)
        )
        results.append(normalize_raw(conn, provider_id=provider_id, target=target, raw=raw, clock=clock))
    return results


def _parse_raw_body(raw: bytes) -> dict[str, Any]:
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError) as error:
        raise HistoricalLineupsContractError("retained lineup raw payload is not valid JSON") from error
    if not isinstance(payload, dict):
        raise HistoricalLineupsContractError("retained lineup raw payload must be a JSON object")
    return payload


def _persist_success_fetch(
    conn: Connection[Any], *, provider_id: int, target: FixtureTarget,
    response: APIFootballResponse, request_started_at: datetime, response_received_at: datetime
) -> StoredRaw:
    payload = response.data
    candidate_results = payload.get("results")
    results = candidate_results if isinstance(candidate_results, int) and not isinstance(candidate_results, bool) and candidate_results >= 0 else None
    paging = payload.get("paging")
    current = paging.get("current") if isinstance(paging, Mapping) and isinstance(paging.get("current"), int) and not isinstance(paging.get("current"), bool) else None
    total = paging.get("total") if isinstance(paging, Mapping) and isinstance(paging.get("total"), int) and not isinstance(paging.get("total"), bool) else None
    with conn.transaction():
        fetch_id = conn.execute(
            """INSERT INTO source.provider_fetches(
                 provider_id, endpoint, request_params, request_params_sha256, purpose,
                 request_started_at, response_received_at, http_status, outcome,
                 provider_results, paging_current, paging_total, content_sha256,
                 subject_fixture_id, subject_season_id
               ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'success',%s,%s,%s,%s,%s,%s)
               RETURNING id""",
            (
                provider_id, ENDPOINT, Jsonb(_params(target.external_id)),
                request_params_sha256(_params(target.external_id)), PURPOSE,
                request_started_at, response_received_at, response.status_code,
                results, current, total, hashlib.sha256(response.raw_body).digest(),
                target.fixture_id, target.season_id,
            ),
        ).fetchone()[0]
        conn.execute(
            """INSERT INTO source.provider_raw_payloads(
                 fetch_id, inline_body, content_type, byte_count, retention_class, expires_at
               ) VALUES (%s,%s,'application/json',%s,'standard',%s)""",
            (fetch_id, response.raw_body, len(response.raw_body), response_received_at + timedelta(days=RAW_RETENTION_DAYS)),
        )
    return StoredRaw(
        fetch_id=int(fetch_id), raw_body=response.raw_body,
        request_started_at=request_started_at, response_received_at=response_received_at,
        status_code=response.status_code, provider_results=results,
    )


def _record_http_failure(
    conn: Connection[Any], *, provider_id: int, target: FixtureTarget,
    request_started_at: datetime, response_received_at: datetime,
    status_code: int | None, outcome: str, error_class: str,
) -> None:
    conn.execute(
        """INSERT INTO source.provider_fetches(
             provider_id, endpoint, request_params, request_params_sha256, purpose,
             request_started_at, response_received_at, http_status, outcome,
             sanitized_error_class, sanitized_error_text, subject_fixture_id, subject_season_id
           ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
             'controlled historical lineup request failed',%s,%s)""",
        (
            provider_id, ENDPOINT, Jsonb(_params(target.external_id)),
            request_params_sha256(_params(target.external_id)), PURPOSE,
            request_started_at, response_received_at, status_code, outcome,
            error_class, target.fixture_id, target.season_id,
        ),
    )


def _persist_provider_error(
    conn: Connection[Any], *, provider_id: int, target: FixtureTarget,
    request_started_at: datetime, response_received_at: datetime, error: APIFootballAPIError,
) -> None:
    if error.raw_body is None:
        _record_http_failure(
            conn, provider_id=provider_id, target=target, request_started_at=request_started_at,
            response_received_at=response_received_at, status_code=error.status_code,
            outcome="provider_error", error_class=type(error).__name__,
        )
        return
    raw = bytes(error.raw_body)
    payload: Mapping[str, Any] | None
    try:
        decoded = json.loads(raw)
        payload = decoded if isinstance(decoded, Mapping) else None
    except ValueError:
        payload = None
    results = payload.get("results") if payload and isinstance(payload.get("results"), int) and not isinstance(payload.get("results"), bool) else None
    paging = payload.get("paging") if payload else None
    current = paging.get("current") if isinstance(paging, Mapping) and isinstance(paging.get("current"), int) and not isinstance(paging.get("current"), bool) else None
    total = paging.get("total") if isinstance(paging, Mapping) and isinstance(paging.get("total"), int) and not isinstance(paging.get("total"), bool) else None
    with conn.transaction():
        fetch_id = conn.execute(
            """INSERT INTO source.provider_fetches(
                 provider_id, endpoint, request_params, request_params_sha256, purpose,
                 request_started_at, response_received_at, http_status, outcome,
                 provider_results, paging_current, paging_total, content_sha256,
                 sanitized_error_class, sanitized_error_text, subject_fixture_id, subject_season_id
               ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'provider_error',%s,%s,%s,%s,%s,
                 'provider returned an invalid or error lineup response',%s,%s)
               RETURNING id""",
            (
                provider_id, ENDPOINT, Jsonb(_params(target.external_id)),
                request_params_sha256(_params(target.external_id)), PURPOSE,
                request_started_at, response_received_at, error.status_code or 200,
                results, current, total, hashlib.sha256(raw).digest(),
                type(error).__name__, target.fixture_id, target.season_id,
            ),
        ).fetchone()[0]
        conn.execute(
            """INSERT INTO source.provider_raw_payloads(
                 fetch_id, inline_body, content_type, byte_count, retention_class, expires_at
               ) VALUES (%s,%s,'application/json',%s,'anomaly',%s)""",
            (fetch_id, raw, len(raw), response_received_at + timedelta(days=ANOMALY_RETENTION_DAYS)),
        )


def _mark_contract_error(conn: Connection[Any], *, fetch_id: int) -> None:
    with conn.transaction():
        conn.execute(
            """UPDATE source.provider_fetches
               SET outcome='provider_error',
                   sanitized_error_class='HistoricalLineupsContractError',
                   sanitized_error_text='lineup response failed controlled validation'
               WHERE id=%s AND normalized_at IS NULL""",
            (fetch_id,),
        )
        conn.execute(
            """UPDATE source.provider_raw_payloads
               SET retention_class='anomaly', expires_at=clock_timestamp()+interval '90 days'
               WHERE fetch_id=%s""",
            (fetch_id,),
        )


def approve_contract_replays(
    conn: Connection[Any], *, provider_id: int, season_id: int, fetch_ids: frozenset[int]
) -> int:
    """Explicitly reopen hash-verified raw bodies after an importer contract fix.

    This is intentionally not automatic: only an operator-selected historical
    lineup contract anomaly for the requested provider season can be replayed.
    The raw body is never refetched or altered.
    """
    if not fetch_ids:
        return 0
    reopened = 0
    with conn.transaction():
        for fetch_id in sorted(fetch_ids):
            row = conn.execute(
                """SELECT provider_fetch.outcome::text,provider_fetch.normalized_at,
                          provider_fetch.sanitized_error_class,provider_fetch.subject_fixture_id,
                          provider_fetch.subject_season_id,provider_fetch.http_status,
                          provider_fetch.request_params_sha256,provider_fetch.content_sha256,
                          raw.inline_body,raw.purged_at
                   FROM source.provider_fetches provider_fetch
                   JOIN source.provider_raw_payloads raw ON raw.fetch_id=provider_fetch.id
                   WHERE provider_fetch.id=%s AND provider_fetch.provider_id=%s
                     AND provider_fetch.endpoint=%s AND provider_fetch.purpose=%s
                   FOR UPDATE OF provider_fetch,raw""",
                (fetch_id, provider_id, ENDPOINT, PURPOSE),
            ).fetchone()
            if row is None:
                raise HistoricalLineupsError("approved historical-lineup replay fetch is missing")
            (
                outcome, normalized_at, error_class, fixture_id, actual_season_id,
                http_status, params_digest, content_digest, body, purged_at,
            ) = row
            if (
                outcome != "provider_error"
                or normalized_at is not None
                or error_class != "HistoricalLineupsContractError"
                or fixture_id is None
                or actual_season_id != season_id
                or http_status != 200
                or body is None
                or purged_at is not None
                or content_digest is None
                or params_digest is None
            ):
                raise HistoricalLineupsError("approved historical-lineup replay is not eligible")
            target = _target_for_fixture(
                conn, provider_id=provider_id, season_id=season_id, fixture_id=int(fixture_id)
            )
            raw = bytes(body)
            if (
                hashlib.sha256(raw).digest() != bytes(content_digest)
                or bytes(params_digest) != request_params_sha256(_params(target.external_id))
            ):
                raise HistoricalLineupsError("approved historical-lineup replay provenance is invalid")
            conn.execute(
                """UPDATE source.provider_fetches
                   SET outcome='success',
                       sanitized_error_class='ApprovedHistoricalLineupContractReplay',
                       sanitized_error_text='explicitly approved replay from retained anomaly raw'
                   WHERE id=%s""",
                (fetch_id,),
            )
            reopened += 1
    return reopened


def _load_retained_raw(conn: Connection[Any], *, provider_id: int, target: FixtureTarget, fetch_id: int) -> StoredRaw:
    row = conn.execute(
        """SELECT provider_fetch.id, provider_fetch.request_started_at, provider_fetch.response_received_at,
                  provider_fetch.http_status, provider_fetch.provider_results, provider_fetch.content_sha256,
                  provider_fetch.request_params_sha256, raw.inline_body
           FROM source.provider_fetches provider_fetch
           JOIN source.provider_raw_payloads raw ON raw.fetch_id=provider_fetch.id
           WHERE provider_fetch.id=%s AND provider_fetch.provider_id=%s AND provider_fetch.endpoint=%s
             AND provider_fetch.purpose=%s AND provider_fetch.outcome='success'
             AND provider_fetch.subject_fixture_id=%s AND provider_fetch.subject_season_id=%s
             AND raw.purged_at IS NULL AND raw.inline_body IS NOT NULL""",
        (fetch_id, provider_id, ENDPOINT, PURPOSE, target.fixture_id, target.season_id),
    ).fetchone()
    if row is None:
        raise HistoricalLineupsError("retained historical-lineup raw fetch is unavailable")
    fetched_id, started, received, status, results, digest, params_digest, body = row
    raw = bytes(body)
    if digest is None or hashlib.sha256(raw).digest() != bytes(digest):
        raise HistoricalLineupsError("retained historical-lineup raw hash mismatch")
    if params_digest is None or bytes(params_digest) != request_params_sha256(_params(target.external_id)):
        raise HistoricalLineupsError("retained historical-lineup request provenance mismatch")
    if received is None:
        raise HistoricalLineupsError("retained historical-lineup response timestamp is missing")
    return StoredRaw(int(fetched_id), raw, started, received, int(status), results)


def _resolve_entity(
    conn: Connection[Any], *, kind: Literal["player", "coach"], provider_id: int,
    external_id: int, display_name: str, photo_url: str | None,
) -> tuple[int, bool]:
    entity_table = "players" if kind == "player" else "coaches"
    ref_table = "player_provider_refs" if kind == "player" else "coach_provider_refs"
    internal_column = "player_id" if kind == "player" else "coach_id"
    row = conn.execute(
        sql.SQL("SELECT {} FROM source.{} WHERE provider_id=%s AND external_id=%s FOR UPDATE").format(
            sql.Identifier(internal_column), sql.Identifier(ref_table)
        ),
        (provider_id, str(external_id)),
    ).fetchone()
    if row is not None:
        return int(row[0]), False
    entity_id = conn.execute(
        sql.SQL("INSERT INTO football.{} (display_name, photo_url) VALUES (%s,%s) RETURNING id").format(
            sql.Identifier(entity_table)
        ),
        (display_name, photo_url),
    ).fetchone()[0]
    conn.execute(
        sql.SQL("INSERT INTO source.{} (provider_id, external_id, {}) VALUES (%s,%s,%s)").format(
            sql.Identifier(ref_table), sql.Identifier(internal_column)
        ),
        (provider_id, str(external_id), entity_id),
    )
    return int(entity_id), True


def normalize_raw(
    conn: Connection[Any], *, provider_id: int, target: FixtureTarget, raw: StoredRaw,
    clock: Clock = _utcnow,
) -> NormalizationResult:
    """Normalize one retained raw response atomically, or mark its anomaly state."""
    fetch_row = conn.execute(
        "SELECT content_sha256 FROM source.provider_fetches WHERE id=%s", (raw.fetch_id,)
    ).fetchone()
    if fetch_row is None or fetch_row[0] is None or hashlib.sha256(raw.raw_body).digest() != bytes(fetch_row[0]):
        raise HistoricalLineupsError("stored historical-lineup raw hash mismatch before normalization")
    try:
        parsed = classify_response(_parse_raw_body(raw.raw_body), target)
        with conn.transaction():
            existing = conn.execute(
                """SELECT id, source_fetch_id, coverage_state::text
                   FROM football.fixture_historical_lineup_snapshots
                   WHERE fixture_id=%s AND content_sha256=(SELECT content_sha256 FROM source.provider_fetches WHERE id=%s)
                   FOR UPDATE""",
                (target.fixture_id, raw.fetch_id),
            ).fetchone()
            if existing is not None:
                conn.execute(
                    "UPDATE source.provider_fetches SET normalized_at=coalesce(normalized_at,%s) WHERE id=%s",
                    (clock(), raw.fetch_id),
                )
                return NormalizationResult(
                    coverage_state=existing[2], snapshot_created=False, team_lineups_created=0,
                    lineup_players_created=0, entities=EntityCounts(), replayed=True,
                )

            snapshot_id = conn.execute(
                """INSERT INTO football.fixture_historical_lineup_snapshots(
                     fixture_id, source_fetch_id, content_sha256, captured_at, available_at,
                     availability_basis, coverage_state, team_count, mapping_version
                   ) VALUES (%s,%s,(SELECT content_sha256 FROM source.provider_fetches WHERE id=%s),%s,%s,
                     'reconstructed_conservative',%s,%s,%s)
                   RETURNING id""",
                (
                    target.fixture_id, raw.fetch_id, raw.fetch_id,
                    raw.response_received_at, raw.response_received_at,
                    parsed.coverage_state, len(parsed.lineups), MAPPING_VERSION,
                ),
            ).fetchone()[0]
            entities = EntityCounts()
            player_count = 0
            for lineup in parsed.lineups:
                team_id = (
                    target.home_team_id if lineup.external_team_id == target.home_external_id
                    else target.away_team_id if lineup.external_team_id == target.away_external_id
                    else _raise_nonparticipant()
                )
                coach_id: int | None = None
                if lineup.coach_external_id is not None:
                    assert lineup.coach_name is not None
                    coach_id, coach_created = _resolve_entity(
                        conn, kind="coach", provider_id=provider_id,
                        external_id=lineup.coach_external_id, display_name=lineup.coach_name,
                        photo_url=lineup.coach_photo_url,
                    )
                    entities = entities.plus(kind="coach", created=coach_created)
                starter_count = sum(player.role == "starter" for player in lineup.players)
                substitute_count = sum(player.role == "substitute" for player in lineup.players)
                conn.execute(
                    """INSERT INTO football.fixture_historical_lineups(
                         snapshot_id, team_id, coach_id, formation, starter_count, substitute_count
                       ) VALUES (%s,%s,%s,%s,%s,%s)""",
                    (snapshot_id, team_id, coach_id, lineup.formation, starter_count, substitute_count),
                )
                for player in lineup.players:
                    player_id, player_created = _resolve_entity(
                        conn, kind="player", provider_id=provider_id,
                        external_id=player.external_id, display_name=player.display_name, photo_url=None,
                    )
                    entities = entities.plus(kind="player", created=player_created)
                    conn.execute(
                        """INSERT INTO football.fixture_historical_lineup_players(
                             snapshot_id, team_id, player_id, lineup_role, position, shirt_number, grid
                           ) VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                        (
                            snapshot_id, team_id, player_id, player.role,
                            player.position, player.shirt_number, player.grid,
                        ),
                    )
                    player_count += 1
            conn.execute(
                "UPDATE source.provider_fetches SET normalized_at=coalesce(normalized_at,%s) WHERE id=%s",
                (clock(), raw.fetch_id),
            )
        return NormalizationResult(
            coverage_state=parsed.coverage_state, snapshot_created=True,
            team_lineups_created=len(parsed.lineups), lineup_players_created=player_count,
            entities=entities,
        )
    except HistoricalLineupsContractError:
        _mark_contract_error(conn, fetch_id=raw.fetch_id)
        raise


def _raise_nonparticipant() -> int:
    raise HistoricalLineupsContractError("lineup response contains a non-participant team")


def _fetch_and_normalize(
    conn: Connection[Any], *, client: APIFootballClient, provider_id: int,
    target: FixtureTarget, clock: Clock,
) -> tuple[NormalizationResult, Mapping[str, str]]:
    started = clock()
    try:
        response = asyncio.run(client.get(ENDPOINT, params=_params(target.external_id)))
    except APIFootballHTTPError as error:
        received = clock()
        _record_http_failure(
            conn, provider_id=provider_id, target=target, request_started_at=started,
            response_received_at=received, status_code=error.status_code or None,
            outcome="transport_error" if error.status_code == 0 else "http_error",
            error_class=type(error).__name__,
        )
        if error.status_code == 429:
            raise HistoricalLineupsError("provider rate limit reached; campaign stopped") from error
        raise HistoricalLineupsError("provider HTTP failure; campaign stopped") from error
    except APIFootballAPIError as error:
        received = clock()
        if error.raw_body is not None and client.response_contains_api_key(error.raw_body):
            raise HistoricalLineupsError("provider error response contains API key") from error
        _persist_provider_error(
            conn, provider_id=provider_id, target=target, request_started_at=started,
            response_received_at=received, error=error,
        )
        raise HistoricalLineupsError("provider error response; campaign stopped") from error

    received = clock()
    if client.response_contains_api_key(response.raw_body):
        raise HistoricalLineupsError("provider response contains API key")
    stored = _persist_success_fetch(
        conn, provider_id=provider_id, target=target, response=response,
        request_started_at=started, response_received_at=received,
    )
    return normalize_raw(conn, provider_id=provider_id, target=target, raw=stored, clock=clock), safe_rate_limit_headers(response.headers)


def _table_fingerprint(conn: Connection[Any], table: str) -> str:
    schema_name, table_name = table.split(".", 1)
    row = conn.execute(
        sql.SQL(
            "SELECT md5(coalesce(string_agg(row_to_json(value)::text, '' ORDER BY row_to_json(value)::text), '')) "
            "FROM (SELECT * FROM {}.{}) AS value"
        ).format(sql.Identifier(schema_name), sql.Identifier(table_name))
    ).fetchone()
    return str(row[0])


OUT_OF_SCOPE_TABLES = (
    "football.fixtures",
    "football.fixture_team_statistics",
    "football.standings_snapshots",
    "football.standings_snapshot_groups",
    "football.standings_snapshot_rows",
    "football.fixture_availability_snapshots",
    "football.fixture_player_availability",
    "football.fixture_lineup_snapshots",
    "football.fixture_lineups",
    "football.fixture_lineup_players",
    "ml.predictions",
    "ml.prediction_feature_snapshots",
    "ml.prediction_fixture_inputs",
)


def _expected_player_values(
    parsed: ParsedLineups,
) -> frozenset[tuple[int, int, str, str | None, int | None, str | None]]:
    """Build an order-independent, numeric provider-ID representation."""
    return frozenset(
        (
            lineup.external_team_id,
            player.external_id,
            player.role,
            player.position,
            player.shirt_number,
            player.grid,
        )
        for lineup in parsed.lineups
        for player in lineup.players
    )


def _verify_fixture(conn: Connection[Any], *, provider_id: int, target: FixtureTarget) -> dict[str, Any]:
    row = conn.execute(
        """SELECT snapshot.id, snapshot.content_sha256, snapshot.coverage_state::text, snapshot.team_count,
                  snapshot.captured_at, snapshot.available_at, snapshot.availability_basis::text,
                  provider_fetch.id, provider_fetch.endpoint, provider_fetch.purpose::text, provider_fetch.subject_fixture_id,
                  provider_fetch.subject_season_id, provider_fetch.request_params, provider_fetch.content_sha256,
                  provider_fetch.response_received_at, provider_fetch.normalized_at, raw.inline_body,
                  raw.byte_count, raw.purged_at
           FROM football.fixture_historical_lineup_snapshots snapshot
           JOIN source.provider_fetches provider_fetch ON provider_fetch.id=snapshot.source_fetch_id
           JOIN source.provider_raw_payloads raw ON raw.fetch_id=provider_fetch.id
           WHERE snapshot.fixture_id=%s
           ORDER BY snapshot.id""",
        (target.fixture_id,),
    ).fetchall()
    if len(row) != 1:
        raise AssertionError("canary fixture must have exactly one historical lineup snapshot")
    (
        snapshot_id, snapshot_hash, coverage, team_count, captured_at, available_at, basis,
        fetch_id, endpoint, purpose, subject_fixture_id, subject_season_id,
        request_params, content_hash, response_received_at, normalized_at,
        body, byte_count, purged_at,
    ) = row[0]
    raw = bytes(body)
    if (
        endpoint != ENDPOINT or purpose != PURPOSE or subject_fixture_id != target.fixture_id
        or subject_season_id != target.season_id or request_params != _params(target.external_id)
        or bytes(snapshot_hash) != bytes(content_hash)
        or bytes(content_hash) != hashlib.sha256(raw).digest() or byte_count != len(raw)
        or purged_at is not None or normalized_at is None or captured_at != response_received_at
        or available_at != response_received_at or basis != "reconstructed_conservative"
    ):
        raise AssertionError("historical lineup raw provenance mismatch")
    parsed = classify_response(_parse_raw_body(raw), target)
    if coverage != parsed.coverage_state or team_count != len(parsed.lineups):
        raise AssertionError("historical lineup coverage mapping mismatch")

    headers = conn.execute(
        """SELECT header.team_id, team_ref.external_id, coach_ref.external_id,
                  header.formation, header.starter_count, header.substitute_count
           FROM football.fixture_historical_lineups header
           JOIN source.team_provider_refs team_ref
             ON team_ref.provider_id=%s AND team_ref.team_id=header.team_id
           LEFT JOIN source.coach_provider_refs coach_ref
             ON coach_ref.provider_id=%s AND coach_ref.coach_id=header.coach_id
           WHERE header.snapshot_id=%s
           ORDER BY header.team_id""",
        (provider_id, provider_id, snapshot_id),
    ).fetchall()
    expected_headers = sorted(
        (
            target.home_team_id if lineup.external_team_id == target.home_external_id else target.away_team_id,
            str(lineup.external_team_id),
            str(lineup.coach_external_id) if lineup.coach_external_id is not None else None,
            lineup.formation,
            sum(player.role == "starter" for player in lineup.players),
            sum(player.role == "substitute" for player in lineup.players),
        )
        for lineup in parsed.lineups
    )
    if headers != expected_headers:
        raise AssertionError("historical lineup team/coach/formation mapping mismatch")

    players = conn.execute(
        """SELECT team_ref.external_id::bigint, player_ref.external_id::bigint, child.lineup_role::text,
                  child.position, child.shirt_number, child.grid
           FROM football.fixture_historical_lineup_players child
           JOIN source.team_provider_refs team_ref
             ON team_ref.provider_id=%s AND team_ref.team_id=child.team_id
           JOIN source.player_provider_refs player_ref
             ON player_ref.provider_id=%s AND player_ref.player_id=child.player_id
           WHERE child.snapshot_id=%s
           ORDER BY team_ref.external_id::bigint, player_ref.external_id::bigint""",
        (provider_id, provider_id, snapshot_id),
    ).fetchall()
    actual_players = frozenset(
        (
            int(team_external_id), int(player_external_id), role,
            position, shirt_number, grid,
        )
        for team_external_id, player_external_id, role, position, shirt_number, grid in players
    )
    expected_players = _expected_player_values(parsed)
    if len(players) != len(expected_players) or actual_players != expected_players:
        raise AssertionError("historical lineup player/null mapping mismatch")
    return {
        "fixture_id": target.fixture_id,
        "external_fixture_id": target.external_id,
        "snapshot_id": int(snapshot_id),
        "coverage_state": coverage,
        "team_lineups": len(headers),
        "lineup_players": len(players),
        "source_fetch_id": int(fetch_id),
    }


def _verify_canary(
    conn: Connection[Any], *, provider_id: int, targets: tuple[FixtureTarget, ...],
    out_of_scope_before: Mapping[str, str],
) -> dict[str, Any]:
    fixtures = [_verify_fixture(conn, provider_id=provider_id, target=target) for target in targets]
    duplicate_headers = conn.execute(
        """SELECT count(*) FROM (
             SELECT snapshot_id, team_id FROM football.fixture_historical_lineups
             GROUP BY snapshot_id, team_id HAVING count(*) > 1
           ) duplicates"""
    ).fetchone()[0]
    duplicate_players = conn.execute(
        """SELECT count(*) FROM (
             SELECT snapshot_id, player_id FROM football.fixture_historical_lineup_players
             GROUP BY snapshot_id, player_id HAVING count(*) > 1
           ) duplicates"""
    ).fetchone()[0]
    orphan_or_nonparticipant = conn.execute(
        """SELECT count(*)
           FROM football.fixture_historical_lineups header
           JOIN football.fixture_historical_lineup_snapshots snapshot ON snapshot.id=header.snapshot_id
           JOIN football.fixtures fixture ON fixture.id=snapshot.fixture_id
           LEFT JOIN football.teams team ON team.id=header.team_id
           WHERE team.id IS NULL OR header.team_id NOT IN (fixture.home_team_id, fixture.away_team_id)"""
    ).fetchone()[0]
    count_mismatch = conn.execute(
        """SELECT count(*)
           FROM football.fixture_historical_lineups header
           WHERE header.starter_count <> (
                   SELECT count(*) FROM football.fixture_historical_lineup_players player
                   WHERE player.snapshot_id=header.snapshot_id AND player.team_id=header.team_id
                     AND player.lineup_role='starter'
                 )
              OR header.substitute_count <> (
                   SELECT count(*) FROM football.fixture_historical_lineup_players player
                   WHERE player.snapshot_id=header.snapshot_id AND player.team_id=header.team_id
                     AND player.lineup_role='substitute'
                 )"""
    ).fetchone()[0]
    provider_orphans = conn.execute(
        """SELECT
             (SELECT count(*) FROM source.player_provider_refs ref LEFT JOIN football.players player ON player.id=ref.player_id WHERE player.id IS NULL) +
             (SELECT count(*) FROM source.coach_provider_refs ref LEFT JOIN football.coaches coach ON coach.id=ref.coach_id WHERE coach.id IS NULL)"""
    ).fetchone()[0]
    after = {table: _table_fingerprint(conn, table) for table in out_of_scope_before}
    verification = {
        "fixtures": fixtures,
        "duplicate_headers": int(duplicate_headers),
        "duplicate_players": int(duplicate_players),
        "orphan_or_nonparticipant_rows": int(orphan_or_nonparticipant),
        "role_count_mismatches": int(count_mismatch),
        "provider_mapping_orphans": int(provider_orphans),
        "out_of_scope_fingerprints_unchanged": {
            table: out_of_scope_before[table] == after[table] for table in out_of_scope_before
        },
    }
    if any(verification[key] != 0 for key in (
        "duplicate_headers", "duplicate_players", "orphan_or_nonparticipant_rows",
        "role_count_mismatches", "provider_mapping_orphans",
    )) or not all(verification["out_of_scope_fingerprints_unchanged"].values()):
        raise AssertionError("historical lineup canary verification failed")
    return verification


def _add_result(
    aggregate: dict[str, int], result: NormalizationResult) -> None:
    aggregate[result.coverage_state] += 1
    aggregate["snapshots_created"] += int(result.snapshot_created)
    aggregate["team_lineups_created"] += result.team_lineups_created
    aggregate["lineup_players_created"] += result.lineup_players_created
    aggregate["players_created"] += result.entities.players_created
    aggregate["players_reused"] += result.entities.players_reused
    aggregate["coaches_created"] += result.entities.coaches_created
    aggregate["coaches_reused"] += result.entities.coaches_reused


def _first_batch_is_fully_complete(verification: Mapping[str, Any]) -> bool:
    fixtures = verification.get("fixtures")
    return (
        isinstance(fixtures, list)
        and len(fixtures) == FIRST_BATCH_CALLS
        and all(
            isinstance(fixture, Mapping)
            and fixture.get("coverage_state") == "complete"
            and fixture.get("team_lineups") == 2
            for fixture in fixtures
        )
    )


def run_controlled_canary(
    *, client: APIFootballClient | None = None, clock: Clock = _utcnow,
    sleep: Sleep = asyncio.sleep, first_batch_calls: int = FIRST_BATCH_CALLS,
    second_batch_calls: int = FIRST_BATCH_CALLS,
    scope: HistoricalLineupsScope = DEFAULT_HISTORICAL_LINEUPS_SCOPE,
) -> HistoricalLineupsCanaryReport:
    """Run exactly five calls, verify, replay one raw payload, then five more.

    The caller should treat every raised exception as a hard stop.  There are
    intentionally no automatic retries and no iteration beyond the ten-call
    controlled canary.
    """
    if first_batch_calls != FIRST_BATCH_CALLS or second_batch_calls != FIRST_BATCH_CALLS:
        raise ValueError("controlled historical-lineups canary requires two batches of exactly five calls")
    api = client or APIFootballClient.from_environment()
    with psycopg.connect(_database_url(), autocommit=True) as conn:
        provider_id, season_id = acquire_context_and_lock(conn, scope=scope)
        try:
            out_of_scope_before = {table: _table_fingerprint(conn, table) for table in OUT_OF_SCOPE_TABLES}
            resumed = _resume_unfinished_success_fetches(
                conn, provider_id=provider_id, season_id=season_id, clock=clock
            )
            aggregate = {
                "complete": 0, "empty": 0, "partial": 0, "snapshots_created": 0,
                "team_lineups_created": 0, "lineup_players_created": 0,
                "players_created": 0, "players_reused": 0, "coaches_created": 0, "coaches_reused": 0,
            }
            quota: dict[str, str] = {}
            selected: list[FixtureTarget] = []
            physical_calls = 0

            first_targets = _targets_without_historical_attempts(
                conn, provider_id=provider_id, season_id=season_id, limit=first_batch_calls, scope=scope
            )
            if len(first_targets) != first_batch_calls:
                raise HistoricalLineupsError("fewer than five unattempted fixtures are available for first canary batch")
            for target in first_targets:
                result, quota = _fetch_and_normalize(
                    conn, client=api, provider_id=provider_id, target=target, clock=clock
                )
                physical_calls += 1
                selected.append(target)
                _add_result(aggregate, result)
                remaining = quota.get("x-ratelimit-requests-remaining")
                if remaining is not None and remaining.isdigit() and int(remaining) <= QUOTA_RESERVE:
                    raise HistoricalLineupsError("provider daily quota reserve reached; campaign stopped")
                if physical_calls < first_batch_calls:
                    asyncio.run(sleep(1.0))

            first_verification = _verify_canary(
                conn, provider_id=provider_id, targets=tuple(first_targets), out_of_scope_before=out_of_scope_before
            )
            if not _first_batch_is_fully_complete(first_verification):
                first_verification["second_batch_not_run"] = "first batch did not have five complete two-team lineups"
                return HistoricalLineupsCanaryReport(
                    physical_api_calls=physical_calls,
                    retained_raw_replays=len(resumed),
                    complete=aggregate["complete"], empty=aggregate["empty"], partial=aggregate["partial"],
                    errors=0, snapshots_created=aggregate["snapshots_created"],
                    team_lineups_created=aggregate["team_lineups_created"],
                    lineup_players_created=aggregate["lineup_players_created"],
                    players_created=aggregate["players_created"], players_reused=aggregate["players_reused"],
                    coaches_created=aggregate["coaches_created"], coaches_reused=aggregate["coaches_reused"],
                    quota=quota, selected_fixture_external_ids=tuple(target.external_id for target in selected),
                    replay_fixture_external_id=None, verification={"first_batch": first_verification},
                )
            replay_target = first_targets[0]
            replay_fetch_id = first_verification["fixtures"][0]["source_fetch_id"]
            replay_raw = _load_retained_raw(
                conn, provider_id=provider_id, target=replay_target, fetch_id=replay_fetch_id
            )
            replay_before = _table_fingerprint(conn, "football.fixture_historical_lineup_snapshots")
            replay_result = normalize_raw(
                conn, provider_id=provider_id, target=replay_target, raw=replay_raw, clock=clock
            )
            if not replay_result.replayed or _table_fingerprint(conn, "football.fixture_historical_lineup_snapshots") != replay_before:
                raise AssertionError("retained raw replay created or changed canonical snapshot")

            second_targets = _targets_without_historical_attempts(
                conn, provider_id=provider_id, season_id=season_id, limit=second_batch_calls, scope=scope
            )
            if len(second_targets) != second_batch_calls:
                raise HistoricalLineupsError("fewer than five unattempted fixtures are available for second canary batch")
            for target in second_targets:
                result, quota = _fetch_and_normalize(
                    conn, client=api, provider_id=provider_id, target=target, clock=clock
                )
                physical_calls += 1
                selected.append(target)
                _add_result(aggregate, result)
                remaining = quota.get("x-ratelimit-requests-remaining")
                if remaining is not None and remaining.isdigit() and int(remaining) <= QUOTA_RESERVE:
                    raise HistoricalLineupsError("provider daily quota reserve reached; campaign stopped")
                if physical_calls < MAX_CANARY_CALLS:
                    asyncio.run(sleep(1.0))

            verification = _verify_canary(
                conn, provider_id=provider_id, targets=tuple(selected), out_of_scope_before=out_of_scope_before
            )
            verification["first_batch"] = first_verification
            verification["replay"] = {
                "fixture_id": replay_target.fixture_id,
                "external_fixture_id": replay_target.external_id,
                "physical_api_calls_added": 0,
                "canonical_snapshot_unchanged": True,
            }
            return HistoricalLineupsCanaryReport(
                physical_api_calls=physical_calls,
                retained_raw_replays=len(resumed) + 1,
                complete=aggregate["complete"], empty=aggregate["empty"], partial=aggregate["partial"],
                errors=0, snapshots_created=aggregate["snapshots_created"],
                team_lineups_created=aggregate["team_lineups_created"],
                lineup_players_created=aggregate["lineup_players_created"],
                players_created=aggregate["players_created"], players_reused=aggregate["players_reused"],
                coaches_created=aggregate["coaches_created"], coaches_reused=aggregate["coaches_reused"],
                quota=quota, selected_fixture_external_ids=tuple(target.external_id for target in selected),
                replay_fixture_external_id=replay_target.external_id, verification=verification,
            )
        finally:
            release_lock(conn, scope=scope)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a ten-call historical lineup canary")
    parser.add_argument("--league-external-id", default=LEAGUE_EXTERNAL_ID)
    parser.add_argument("--season-start-year", type=int, default=SEASON_START_YEAR)
    parser.add_argument("--expected-fixture-count", type=int, default=EXPECTED_FIXTURE_COUNT)
    args = parser.parse_args()
    report = run_controlled_canary(
        scope=HistoricalLineupsScope(
            league_external_id=args.league_external_id,
            season_start_year=args.season_start_year,
            expected_fixture_count=args.expected_fixture_count,
        )
    )
    print(json.dumps(asdict(report), default=str, sort_keys=True))


if __name__ == "__main__":
    main()
