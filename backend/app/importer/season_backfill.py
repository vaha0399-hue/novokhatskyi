"""Controlled fixtures-only API-Football completed-season backfill.

The job is intentionally manual, quota-bounded, non-live, and development-only.
It stores one season-wide raw provider response before atomically normalizing
the expected completed fixtures. A valid stored raw response is always preferred over a
new API request so interrupted normalization resumes without spending quota.
"""

from __future__ import annotations

import asyncio
import argparse
import hashlib
import json
import os
import math
import random
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
from psycopg import Connection
from psycopg.types.json import Jsonb

from app.api_football import APIFootballClient, APIFootballResponse
from app.api_football.client import safe_rate_limit_headers
from app.api_football.errors import APIFootballAPIError, APIFootballHTTPError
from app.importer.canary import parse_datetime, request_params_sha256
from app.importer.fixture_status_contract import (
    FixtureStatusObservation,
    validate_fixture_status_response,
)

PROVIDER_CODE = "api-football"
BACKFILL_PURPOSE = "bootstrap"
RAW_RETENTION_DAYS = 30
BATCH_SIZE = 50
MAX_API_ATTEMPTS = 3
RETRYABLE_HTTP_STATUSES = frozenset({408, 500, 502, 503, 504})


@dataclass(frozen=True)
class SeasonBackfillScope:
    """Immutable provider scope for one controlled, completed-season import."""

    league_external_id: int
    season_start_year: int
    expected_fixture_count: int
    preexisting_canary_fixture_external_id: int | None = None

    def __post_init__(self) -> None:
        if self.league_external_id <= 0 or self.season_start_year <= 0:
            raise ValueError("league external ID and season start year must be positive")
        if self.expected_fixture_count <= 0:
            raise ValueError("expected fixture count must be positive")
        # A double round-robin season has n * (n - 1) fixtures. Keep the
        # schedule invariant scope-driven rather than EPL-specific.
        team_count = (1 + math.isqrt(1 + 4 * self.expected_fixture_count)) // 2
        if team_count < 2 or team_count * (team_count - 1) != self.expected_fixture_count:
            raise ValueError("expected fixture count must form a complete double round-robin schedule")

    @property
    def request_params(self) -> dict[str, int]:
        return {"league": self.league_external_id, "season": self.season_start_year}

    @property
    def lock_key(self) -> str:
        return f"api-football:fixtures:{self.league_external_id}:{self.season_start_year}:v1"

    @property
    def expected_team_count(self) -> int:
        return (1 + math.isqrt(1 + 4 * self.expected_fixture_count)) // 2


DEFAULT_SCOPE = SeasonBackfillScope(
    league_external_id=39,
    season_start_year=2024,
    expected_fixture_count=380,
    preexisting_canary_fixture_external_id=1208021,
)

# Backward-compatible exports for the established EPL 2024 operational path.
LEAGUE_EXTERNAL_ID = DEFAULT_SCOPE.league_external_id
SEASON_START_YEAR = DEFAULT_SCOPE.season_start_year
EXPECTED_FIXTURE_COUNT = DEFAULT_SCOPE.expected_fixture_count
CANARY_FIXTURE_EXTERNAL_ID = DEFAULT_SCOPE.preexisting_canary_fixture_external_id
LOCK_KEY = DEFAULT_SCOPE.lock_key
REQUEST_PARAMS = DEFAULT_SCOPE.request_params


@dataclass(frozen=True)
class SeasonContext:
    provider_id: int
    league_id: int
    season_id: int
    team_ids: Mapping[int, int]
    scope: SeasonBackfillScope


@dataclass(frozen=True)
class StoredFetch:
    fetch_id: int
    response: APIFootballResponse
    request_started_at: datetime
    response_received_at: datetime
    normalized_at: datetime | None
    reused: bool


@dataclass(frozen=True)
class AttemptFailure:
    request_started_at: datetime
    response_received_at: datetime
    status_code: int | None
    outcome: str
    error_class: str
    safe_headers: Mapping[str, str]


@dataclass(frozen=True)
class CollectedFetch:
    response: APIFootballResponse
    request_started_at: datetime
    response_received_at: datetime
    attempts: int


@dataclass(frozen=True)
class FixtureRecord:
    external_id: int
    home_external_id: int
    away_external_id: int
    venue_external_id: int | None
    venue_name: str | None
    venue_city: str | None
    round_label: str | None
    kickoff_at: datetime
    source_timezone: str
    referee_name: str | None
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


FailureRecorder = Callable[[AttemptFailure], None]
Sleep = Callable[[float], Awaitable[None]]


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _database_url() -> str:
    value = os.environ.get("SUPABASE_DB_URL")
    if not value:
        raise RuntimeError("SUPABASE_DB_URL is required")
    return value


def chunked(items: Sequence[FixtureRecord], size: int = BATCH_SIZE) -> list[list[FixtureRecord]]:
    if size < 1:
        raise ValueError("chunk size must be positive")
    return [list(items[index : index + size]) for index in range(0, len(items), size)]


def _optional_text(value: Any, *, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string or null")
    return value


def _required_positive_integer(value: Any, *, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _score_value(score: Mapping[str, Any], section: str, side: str) -> int | None:
    part = score.get(section)
    if part is None:
        return None
    if not isinstance(part, Mapping):
        raise ValueError(f"score.{section} must be an object or null")
    value = part.get(side)
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"score.{section}.{side} must be a non-negative integer or null")
    return value


def validate_fixture_season_response(
    response: APIFootballResponse,
    *,
    allowed_team_external_ids: Iterable[int],
    scope: SeasonBackfillScope = DEFAULT_SCOPE,
    excluded_fixture_external_ids: Iterable[int] = (),
) -> list[FixtureRecord]:
    """Validate canonical regular-season fixtures before normalized DML.

    Most league-season payloads contain exactly the canonical double round-
    robin schedule. A separately reviewed bootstrap projection may identify a
    small set of provenance-only fixture IDs; they stay in raw provider bytes
    but cannot become canonical fixtures for this season scope.
    """
    payload = response.data
    expected_parameters = {key: str(value) for key, value in scope.request_params.items()}
    if payload.get("parameters") != expected_parameters:
        raise ValueError("provider parameters mismatch for season fixtures")
    if payload.get("errors") not in ([], {}, None):
        raise ValueError("provider returned errors for season fixtures")
    paging = payload.get("paging")
    if paging != {"current": 1, "total": 1}:
        raise ValueError("season fixtures must be a single complete page")
    entries = payload.get("response")
    if not isinstance(entries, list):
        raise ValueError("season fixtures response must be an array")
    allowed_teams = set(allowed_team_external_ids)
    excluded_ids = frozenset(excluded_fixture_external_ids)
    if any(
        not isinstance(external_id, int) or isinstance(external_id, bool) or external_id <= 0
        for external_id in excluded_ids
    ):
        raise ValueError("excluded fixture IDs must be positive integers")
    records: list[FixtureRecord] = []
    seen_ids: set[int] = set()
    seen_excluded_ids: set[int] = set()
    for entry in entries:
        if not isinstance(entry, Mapping):
            raise ValueError("fixture entry must be an object")
        fixture = entry.get("fixture")
        league = entry.get("league")
        teams = entry.get("teams")
        goals = entry.get("goals")
        score = entry.get("score")
        if not all(isinstance(value, Mapping) for value in (fixture, league, teams, goals, score)):
            raise ValueError("fixture entry is missing required objects")
        assert isinstance(fixture, Mapping)
        assert isinstance(league, Mapping)
        assert isinstance(teams, Mapping)
        assert isinstance(goals, Mapping)
        assert isinstance(score, Mapping)

        external_id = _required_positive_integer(fixture.get("id"), field="fixture.id")
        if external_id in seen_ids:
            raise ValueError("duplicate fixture.id in season response")
        seen_ids.add(external_id)
        if (
            league.get("id") != scope.league_external_id
            or league.get("season") != scope.season_start_year
        ):
            raise ValueError("fixture belongs to an unexpected league or season")
        home = teams.get("home")
        away = teams.get("away")
        if not isinstance(home, Mapping) or not isinstance(away, Mapping):
            raise ValueError("fixture teams must contain home and away objects")
        home_external_id = _required_positive_integer(home.get("id"), field="teams.home.id")
        away_external_id = _required_positive_integer(away.get("id"), field="teams.away.id")
        if home_external_id == away_external_id:
            raise ValueError("fixture home and away teams must differ")
        if external_id in excluded_ids:
            if home_external_id in allowed_teams and away_external_id in allowed_teams:
                raise ValueError("reviewed excluded fixture has only canonical season participants")
            seen_excluded_ids.add(external_id)
            continue
        if home_external_id not in allowed_teams or away_external_id not in allowed_teams:
            raise ValueError("fixture participant is not mapped to the season")
        status = fixture.get("status")
        if not isinstance(status, Mapping) or status.get("short") != "FT":
            raise ValueError("historical season backfill accepts only FT fixtures")

        kickoff_raw = fixture.get("date")
        if not isinstance(kickoff_raw, str):
            raise ValueError("fixture.date must be a timezone-aware string")
        kickoff_at = parse_datetime(kickoff_raw)
        timezone = fixture.get("timezone")
        if not isinstance(timezone, str) or not timezone:
            raise ValueError("fixture.timezone must be a non-empty string")
        home_goals = goals.get("home")
        away_goals = goals.get("away")
        if (
            not isinstance(home_goals, int)
            or isinstance(home_goals, bool)
            or home_goals < 0
            or not isinstance(away_goals, int)
            or isinstance(away_goals, bool)
            or away_goals < 0
        ):
            raise ValueError("completed fixture goals must be non-negative integers")

        venue = fixture.get("venue")
        if venue is None:
            venue = {}
        if not isinstance(venue, Mapping):
            raise ValueError("fixture.venue must be an object or null")
        venue_external_raw = venue.get("id")
        venue_external_id = None
        if venue_external_raw is not None:
            venue_external_id = _required_positive_integer(venue_external_raw, field="fixture.venue.id")

        records.append(
            FixtureRecord(
                external_id=external_id,
                home_external_id=home_external_id,
                away_external_id=away_external_id,
                venue_external_id=venue_external_id,
                venue_name=_optional_text(venue.get("name"), field="fixture.venue.name"),
                venue_city=_optional_text(venue.get("city"), field="fixture.venue.city"),
                round_label=_optional_text(league.get("round"), field="league.round"),
                kickoff_at=kickoff_at,
                source_timezone=timezone,
                referee_name=_optional_text(fixture.get("referee"), field="fixture.referee"),
                home_goals=home_goals,
                away_goals=away_goals,
                home_halftime_goals=_score_value(score, "halftime", "home"),
                away_halftime_goals=_score_value(score, "halftime", "away"),
                home_fulltime_goals=_score_value(score, "fulltime", "home"),
                away_fulltime_goals=_score_value(score, "fulltime", "away"),
                home_extratime_goals=_score_value(score, "extratime", "home"),
                away_extratime_goals=_score_value(score, "extratime", "away"),
                home_penalty_goals=_score_value(score, "penalty", "home"),
                away_penalty_goals=_score_value(score, "penalty", "away"),
            )
        )

    if seen_excluded_ids != excluded_ids:
        raise ValueError("reviewed excluded fixtures are missing from season response")
    if payload.get("results") != len(entries) or len(records) != scope.expected_fixture_count:
        raise ValueError(
            f"season fixtures response must contain exactly {scope.expected_fixture_count} canonical fixtures"
        )

    records.sort(key=lambda item: item.external_id)
    home_counts = {team_id: 0 for team_id in allowed_teams}
    away_counts = {team_id: 0 for team_id in allowed_teams}
    directed_pairs: set[tuple[int, int]] = set()
    for record in records:
        pair = (record.home_external_id, record.away_external_id)
        if pair in directed_pairs:
            raise ValueError("season fixtures contain a duplicate directed team pairing")
        directed_pairs.add(pair)
        home_counts[record.home_external_id] += 1
        away_counts[record.away_external_id] += 1
    expected_home_or_away = scope.expected_team_count - 1
    if (
        len(allowed_teams) != scope.expected_team_count
        or len(directed_pairs) != scope.expected_fixture_count
        or set(home_counts.values()) != {expected_home_or_away}
        or set(away_counts.values()) != {expected_home_or_away}
    ):
        raise ValueError(
            "season fixtures do not form a complete "
            f"{scope.expected_team_count}-team home/away schedule"
        )
    return records


def _retry_delay(attempt: int) -> float:
    return (2 ** (attempt - 1)) + random.uniform(0.0, 0.25)


async def collect_fixture_season(
    client: APIFootballClient,
    *,
    record_failure: FailureRecorder,
    scope: SeasonBackfillScope = DEFAULT_SCOPE,
    sleep: Sleep = asyncio.sleep,
) -> CollectedFetch:
    """Execute one logical request with a hard cap of three physical attempts."""
    for attempt in range(1, MAX_API_ATTEMPTS + 1):
        started_at = _utcnow()
        try:
            response = await client.get("/fixtures", params=scope.request_params)
        except APIFootballHTTPError as error:
            received_at = _utcnow()
            failure = AttemptFailure(
                request_started_at=started_at,
                response_received_at=received_at,
                status_code=error.status_code or None,
                outcome="transport_error" if error.status_code == 0 else "http_error",
                error_class=type(error).__name__,
                safe_headers=error.safe_headers,
            )
            record_failure(failure)
            if error.status_code not in ({0} | RETRYABLE_HTTP_STATUSES) or attempt == MAX_API_ATTEMPTS:
                raise
            await sleep(_retry_delay(attempt))
        except APIFootballAPIError as error:
            record_failure(
                AttemptFailure(
                    request_started_at=started_at,
                    response_received_at=_utcnow(),
                    status_code=200,
                    outcome="provider_error",
                    error_class=type(error).__name__,
                    safe_headers={},
                )
            )
            raise
        else:
            return CollectedFetch(
                response=response,
                request_started_at=started_at,
                response_received_at=_utcnow(),
                attempts=attempt,
            )
    raise AssertionError("unreachable API attempt loop")


def acquire_context_and_lock(
    conn: Connection[Any],
    *,
    scope: SeasonBackfillScope = DEFAULT_SCOPE,
) -> SeasonContext:
    locked = conn.execute(
        "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))",
        (scope.lock_key,),
    ).fetchone()[0]
    if not locked:
        raise RuntimeError("season backfill is already running")
    row = conn.execute(
        """
        SELECT p.id, lr.league_id, sr.season_id
        FROM source.providers p
        JOIN source.league_provider_refs lr
          ON lr.provider_id = p.id AND lr.external_id = %s
        JOIN source.season_provider_refs sr
          ON sr.provider_id = p.id
         AND sr.league_external_id = lr.external_id
         AND sr.external_season = %s
        WHERE p.code = %s
        """,
        (str(scope.league_external_id), scope.season_start_year, PROVIDER_CODE),
    ).fetchone()
    if row is None:
        raise RuntimeError("provider/league/season mappings are required before season backfill")
    provider_id, league_id, season_id = row
    team_rows = conn.execute(
        """
        SELECT tr.external_id, tr.team_id
        FROM source.team_provider_refs tr
        JOIN football.season_teams st
          ON st.team_id = tr.team_id AND st.season_id = %s
        WHERE tr.provider_id = %s
        ORDER BY tr.external_id
        """,
        (season_id, provider_id),
    ).fetchall()
    try:
        team_ids = {int(external_id): team_id for external_id, team_id in team_rows}
    except ValueError as error:
        raise RuntimeError("season team provider IDs must be integers") from error
    if len(team_ids) != scope.expected_team_count:
        raise RuntimeError(
            f"exactly {scope.expected_team_count} mapped season teams are required before backfill"
        )
    return SeasonContext(provider_id, league_id, season_id, team_ids, scope)


def load_reusable_fetch(conn: Connection[Any], context: SeasonContext) -> StoredFetch | None:
    row = conn.execute(
        """
        SELECT f.id, f.request_started_at, f.response_received_at, f.http_status,
               f.content_sha256, f.normalized_at, r.inline_body
        FROM source.provider_fetches f
        JOIN source.provider_raw_payloads r ON r.fetch_id = f.id
        WHERE f.provider_id = %s
          AND f.endpoint = '/fixtures'
          AND f.request_params_sha256 = %s
          AND f.purpose = %s
          AND f.subject_season_id = %s
          AND f.outcome = 'success'
          AND f.response_received_at IS NOT NULL
          AND r.purged_at IS NULL
          AND r.inline_body IS NOT NULL
        ORDER BY (f.normalized_at IS NOT NULL) DESC, f.response_received_at DESC
        LIMIT 1
        """,
        (
            context.provider_id,
            request_params_sha256(context.scope.request_params),
            BACKFILL_PURPOSE,
            context.season_id,
        ),
    ).fetchone()
    if row is None:
        return None
    fetch_id, started_at, received_at, status_code, expected_hash, normalized_at, raw_body = row
    raw_bytes = bytes(raw_body)
    if expected_hash is None or hashlib.sha256(raw_bytes).digest() != bytes(expected_hash):
        raise ValueError("stored season fixture raw payload hash mismatch")
    try:
        payload = json.loads(raw_bytes)
    except ValueError as error:
        raise ValueError("stored season fixture raw payload is invalid JSON") from error
    if not isinstance(payload, dict):
        raise ValueError("stored season fixture raw payload has invalid top level")
    return StoredFetch(
        fetch_id=fetch_id,
        response=APIFootballResponse(
            data=payload,
            raw_body=raw_bytes,
            status_code=status_code,
            headers={},
        ),
        request_started_at=started_at,
        response_received_at=received_at,
        normalized_at=normalized_at,
        reused=True,
    )


def persist_failed_attempt(
    conn: Connection[Any],
    *,
    context: SeasonContext,
    failure: AttemptFailure,
) -> None:
    with conn.transaction():
        conn.execute(
            """
            INSERT INTO source.provider_fetches (
              provider_id, endpoint, request_params, request_params_sha256, purpose,
              request_started_at, response_received_at, http_status, outcome,
              sanitized_error_class, sanitized_error_text, subject_season_id
            ) VALUES (%s, '/fixtures', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                context.provider_id,
                Jsonb(context.scope.request_params),
                request_params_sha256(context.scope.request_params),
                BACKFILL_PURPOSE,
                failure.request_started_at,
                None if failure.outcome == "transport_error" else failure.response_received_at,
                failure.status_code,
                failure.outcome,
                failure.error_class,
                "controlled season backfill request failed",
                context.season_id,
            ),
        )


def persist_success_fetch(
    conn: Connection[Any],
    *,
    context: SeasonContext,
    collected: CollectedFetch,
) -> StoredFetch:
    payload = collected.response.data
    paging = payload.get("paging")
    results = payload.get("results")
    paging_current = paging.get("current") if isinstance(paging, Mapping) else None
    paging_total = paging.get("total") if isinstance(paging, Mapping) else None
    content_hash = hashlib.sha256(collected.response.raw_body).digest()
    with conn.transaction():
        fetch_id = conn.execute(
            """
            INSERT INTO source.provider_fetches (
              provider_id, endpoint, request_params, request_params_sha256, purpose,
              request_started_at, response_received_at, http_status, outcome,
              provider_results, paging_current, paging_total, content_sha256,
              subject_season_id
            ) VALUES (
              %s, '/fixtures', %s, %s, %s, %s, %s, %s, 'success',
              %s, %s, %s, %s, %s
            ) RETURNING id
            """,
            (
                context.provider_id,
                Jsonb(context.scope.request_params),
                request_params_sha256(context.scope.request_params),
                BACKFILL_PURPOSE,
                collected.request_started_at,
                collected.response_received_at,
                collected.response.status_code,
                results if isinstance(results, int) and results >= 0 else None,
                paging_current if isinstance(paging_current, int) and paging_current >= 1 else None,
                paging_total if isinstance(paging_total, int) and paging_total >= 1 else None,
                content_hash,
                context.season_id,
            ),
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO source.provider_raw_payloads (
              fetch_id, inline_body, content_type, byte_count, retention_class, expires_at
            ) VALUES (%s, %s, 'application/json', %s, 'standard', %s)
            """,
            (
                fetch_id,
                collected.response.raw_body,
                len(collected.response.raw_body),
                _utcnow() + timedelta(days=RAW_RETENTION_DAYS),
            ),
        )
    return StoredFetch(
        fetch_id=fetch_id,
        response=collected.response,
        request_started_at=collected.request_started_at,
        response_received_at=collected.response_received_at,
        normalized_at=None,
        reused=False,
    )


def mark_fetch_contract_error(conn: Connection[Any], fetch_id: int) -> None:
    with conn.transaction():
        conn.execute(
            """
            UPDATE source.provider_fetches
            SET outcome = 'provider_error',
                sanitized_error_class = 'SeasonFixtureContractError',
                sanitized_error_text = 'season fixture response failed controlled validation'
            WHERE id = %s AND normalized_at IS NULL
            """,
            (fetch_id,),
        )


def _resolve_venue(
    conn: Connection[Any],
    *,
    context: SeasonContext,
    record: FixtureRecord,
    seen_at: datetime,
) -> int | None:
    if record.venue_external_id is None:
        return None
    external_id = str(record.venue_external_id)
    row = conn.execute(
        """
        SELECT venue_id FROM source.venue_provider_refs
        WHERE provider_id = %s AND external_id = %s FOR UPDATE
        """,
        (context.provider_id, external_id),
    ).fetchone()
    if row is not None:
        conn.execute(
            """
            UPDATE source.venue_provider_refs
            SET last_seen_at = greatest(last_seen_at, %s)
            WHERE provider_id = %s AND external_id = %s
            """,
            (seen_at, context.provider_id, external_id),
        )
        return row[0]
    venue_id = conn.execute(
        "INSERT INTO football.venues (name, city) VALUES (%s, %s) RETURNING id",
        (record.venue_name or "Unknown venue", record.venue_city),
    ).fetchone()[0]
    conn.execute(
        """
        INSERT INTO source.venue_provider_refs
          (provider_id, external_id, venue_id, first_seen_at, last_seen_at)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (context.provider_id, external_id, venue_id, seen_at, seen_at),
    )
    return venue_id


def _existing_fixture_is_exact(
    actual: Sequence[Any],
    *,
    context: SeasonContext,
    record: FixtureRecord,
) -> bool:
    expected = (
        context.season_id,
        context.team_ids[record.home_external_id],
        context.team_ids[record.away_external_id],
        record.kickoff_at,
        "completed",
        record.home_goals,
        record.away_goals,
        record.home_halftime_goals,
        record.away_halftime_goals,
        record.home_fulltime_goals,
        record.away_fulltime_goals,
        record.home_extratime_goals,
        record.away_extratime_goals,
        record.home_penalty_goals,
        record.away_penalty_goals,
    )
    return tuple(actual[:15]) == expected


def _normalize_fixture(
    conn: Connection[Any],
    *,
    context: SeasonContext,
    fetch: StoredFetch,
    record: FixtureRecord,
) -> tuple[int, bool]:
    row = conn.execute(
        """
        SELECT fixture_id FROM source.fixture_provider_refs
        WHERE provider_id = %s AND external_id = %s FOR UPDATE
        """,
        (context.provider_id, str(record.external_id)),
    ).fetchone()
    home_team_id = context.team_ids[record.home_external_id]
    away_team_id = context.team_ids[record.away_external_id]
    if row is not None:
        fixture_id = row[0]
        actual = conn.execute(
            """
            SELECT season_id, home_team_id, away_team_id, kickoff_at, lifecycle_state::text,
                   home_goals, away_goals, home_halftime_goals, away_halftime_goals,
                   home_fulltime_goals, away_fulltime_goals,
                   home_extratime_goals, away_extratime_goals,
                   home_penalty_goals, away_penalty_goals,
                   result_finalized_at
            FROM football.fixtures WHERE id = %s FOR UPDATE
            """,
            (fixture_id,),
        ).fetchone()
        if not _existing_fixture_is_exact(actual, context=context, record=record):
            raise ValueError(f"existing fixture identity or result conflict for {record.external_id}")
        if (
            context.scope.preexisting_canary_fixture_external_id == record.external_id
            and actual[15] is None
        ):
            raise ValueError("preexisting canary fixture must already be finalized and immutable")
        if actual[15] is None:
            availability_at = record.kickoff_at + timedelta(hours=3)
            conn.execute(
                """
                UPDATE football.fixtures SET
                  venue_id = %s, round_label = %s, source_timezone = %s, referee_name = %s,
                  terminal_status_observed_at = %s, result_available_at = %s,
                  availability_basis = 'reconstructed_conservative', result_finalized_at = %s,
                  last_seen_at = greatest(last_seen_at, %s), last_source_fetch_id = %s
                WHERE id = %s
                """,
                (
                    _resolve_venue(conn, context=context, record=record, seen_at=fetch.response_received_at),
                    record.round_label,
                    record.source_timezone,
                    record.referee_name,
                    availability_at,
                    availability_at,
                    fetch.response_received_at,
                    fetch.response_received_at,
                    fetch.fetch_id,
                    fixture_id,
                ),
            )
        return fixture_id, False

    if context.scope.preexisting_canary_fixture_external_id == record.external_id:
        raise ValueError("finalized preexisting canary fixture provider mapping is missing")

    availability_at = record.kickoff_at + timedelta(hours=3)
    venue_id = _resolve_venue(conn, context=context, record=record, seen_at=fetch.response_received_at)
    fixture_id = conn.execute(
        """
        INSERT INTO football.fixtures (
          season_id, home_team_id, away_team_id, venue_id, round_label, kickoff_at,
          source_timezone, referee_name, lifecycle_state,
          home_goals, away_goals,
          home_halftime_goals, away_halftime_goals,
          home_fulltime_goals, away_fulltime_goals,
          home_extratime_goals, away_extratime_goals,
          home_penalty_goals, away_penalty_goals,
          terminal_status_observed_at, result_available_at, availability_basis,
          result_finalized_at, first_seen_at, last_seen_at, last_source_fetch_id
        ) VALUES (
          %s, %s, %s, %s, %s, %s, %s, %s, 'completed',
          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
          %s, %s, 'reconstructed_conservative', %s, %s, %s, %s
        ) RETURNING id
        """,
        (
            context.season_id,
            home_team_id,
            away_team_id,
            venue_id,
            record.round_label,
            record.kickoff_at,
            record.source_timezone,
            record.referee_name,
            record.home_goals,
            record.away_goals,
            record.home_halftime_goals,
            record.away_halftime_goals,
            record.home_fulltime_goals,
            record.away_fulltime_goals,
            record.home_extratime_goals,
            record.away_extratime_goals,
            record.home_penalty_goals,
            record.away_penalty_goals,
            availability_at,
            availability_at,
            fetch.response_received_at,
            fetch.response_received_at,
            fetch.response_received_at,
            fetch.fetch_id,
        ),
    ).fetchone()[0]
    conn.execute(
        """
        INSERT INTO source.fixture_provider_refs
          (provider_id, external_id, fixture_id, first_seen_at, last_seen_at)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            context.provider_id,
            str(record.external_id),
            fixture_id,
            fetch.response_received_at,
            fetch.response_received_at,
        ),
    )
    return fixture_id, True


def normalize_fixture_season(
    conn: Connection[Any],
    *,
    context: SeasonContext,
    fetch: StoredFetch,
    records: Sequence[FixtureRecord],
    status_observations: Sequence[FixtureStatusObservation],
    fail_after_chunk: int | None = None,
) -> dict[str, int]:
    created = 0
    processed = 0
    batches = chunked(records)
    with conn.transaction():
        conn.execute("SET LOCAL statement_timeout = '120s'")
        conn.execute("SET LOCAL lock_timeout = '10s'")
        for chunk_number, batch in enumerate(batches, start=1):
            for record in batch:
                _, was_created = _normalize_fixture(
                    conn,
                    context=context,
                    fetch=fetch,
                    record=record,
                )
                created += int(was_created)
                processed += 1
            if fail_after_chunk == chunk_number:
                raise RuntimeError("injected season backfill chunk failure")
        count = conn.execute(
            """
            SELECT count(*)
            FROM source.fixture_provider_refs r
            JOIN football.fixtures f ON f.id = r.fixture_id
            WHERE r.provider_id = %s AND f.season_id = %s
            """,
            (context.provider_id, context.season_id),
        ).fetchone()[0]
        if count != context.scope.expected_fixture_count:
            raise AssertionError(
                f"season fixture mapping count is not {context.scope.expected_fixture_count}"
            )
        _insert_missing_provider_statuses(
            conn,
            context=context,
            fetch=fetch,
            observations=status_observations,
        )
        conn.execute(
            """
            UPDATE source.provider_fetches
            SET normalized_at = coalesce(normalized_at, clock_timestamp())
            WHERE id = %s AND outcome = 'success'
            """,
            (fetch.fetch_id,),
        )
    return {"processed": processed, "created": created, "batches": len(batches)}


def _insert_missing_provider_statuses(
    conn: Connection[Any],
    *,
    context: SeasonContext,
    fetch: StoredFetch,
    observations: Sequence[FixtureStatusObservation],
) -> None:
    """Persist the current exact provider status for newly imported fixtures.

    Historical backfills must never rewrite an existing status/provenance row.
    A later controlled reconciliation job is the only lane that may advance a
    provider status using a newer confirmed observation (for example NS -> FT).
    """

    expected_external_ids = {
        row[0]
        for row in conn.execute(
            """SELECT ref.external_id
               FROM source.fixture_provider_refs ref
               JOIN football.fixtures fixture ON fixture.id = ref.fixture_id
               WHERE ref.provider_id = %s AND fixture.season_id = %s""",
            (context.provider_id, context.season_id),
        ).fetchall()
    }
    observation_by_external_id = {
        str(observation.external_fixture_id): observation for observation in observations
    }
    if set(observation_by_external_id) != expected_external_ids:
        raise AssertionError("provider status observations do not match normalized season fixtures")

    for external_id, observation in observation_by_external_id.items():
        fixture_row = conn.execute(
            """SELECT fixture.id, fixture.lifecycle_state::text
               FROM source.fixture_provider_refs ref
               JOIN football.fixtures fixture ON fixture.id = ref.fixture_id
               WHERE ref.provider_id = %s AND ref.external_id = %s
               FOR KEY SHARE""",
            (context.provider_id, external_id),
        ).fetchone()
        if fixture_row is None:
            raise AssertionError("normalized fixture provider mapping is missing")
        fixture_id, lifecycle_state = fixture_row
        mapping_row = conn.execute(
            """SELECT canonical_state::text
               FROM source.fixture_status_code_mappings
               WHERE provider_id = %s AND external_code = %s""",
            (context.provider_id, observation.status_code),
        ).fetchone()
        if mapping_row is None or mapping_row[0] != lifecycle_state:
            raise AssertionError("provider status mapping conflicts with canonical fixture lifecycle")
        conn.execute(
            """INSERT INTO source.fixture_provider_status (
                    provider_id, fixture_id, status_code, observed_at, source_fetch_id
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (provider_id, fixture_id) DO NOTHING""",
            (
                context.provider_id,
                fixture_id,
                observation.status_code,
                fetch.response_received_at,
                fetch.fetch_id,
            ),
        )


def _validate_provider_statuses(
    conn: Connection[Any],
    *,
    context: SeasonContext,
    fetch: StoredFetch,
    records: Sequence[FixtureRecord],
    excluded_fixture_status_codes: Mapping[int, str] | None = None,
) -> tuple[FixtureStatusObservation, ...]:
    """Validate exact status membership against the reviewed DB mapping."""

    allowed_status_codes = {
        str(row[0])
        for row in conn.execute(
            """SELECT external_code
               FROM source.fixture_status_code_mappings
               WHERE provider_id = %s""",
            (context.provider_id,),
        ).fetchall()
    }
    if not allowed_status_codes:
        raise RuntimeError("no reviewed provider fixture-status mappings are configured")
    return validate_fixture_status_response(
        fetch.response,
        expected_content_sha256=hashlib.sha256(fetch.response.raw_body).digest(),
        expected_fixture_ids={record.external_id for record in records},
        allowed_status_codes=allowed_status_codes,
        excluded_fixture_status_codes=excluded_fixture_status_codes,
    )


UNTOUCHED_TABLES = (
    "football.fixture_team_statistics",
    "football.fixture_availability_snapshots",
    "football.fixture_player_availability",
    "football.fixture_lineup_snapshots",
    "football.fixture_lineups",
    "football.fixture_lineup_players",
    "football.standings_snapshots",
    "football.standings_snapshot_groups",
    "football.standings_snapshot_rows",
    "ml.predictions",
    "ml.prediction_feature_snapshots",
)


def table_counts(conn: Connection[Any], tables: Iterable[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in tables:
        schema, name = table.split(".", 1)
        counts[table] = conn.execute(
            "SELECT count(*) FROM {}.{}".format(
                psycopg.sql.Identifier(schema).as_string(conn),
                psycopg.sql.Identifier(name).as_string(conn),
            )
        ).fetchone()[0]
    return counts


def canary_fixture_snapshot(conn: Connection[Any], context: SeasonContext) -> tuple[Any, ...] | None:
    """Capture an optional pre-existing finalized fixture for preservation checks.

    New-season imports do not assume any provider fixture mapping exists. The
    established EPL 2024 scope retains its canary invariant through DEFAULT_SCOPE.
    """
    external_id = context.scope.preexisting_canary_fixture_external_id
    if external_id is None:
        return None
    row = conn.execute(
        """
        SELECT f.*
        FROM source.fixture_provider_refs r
        JOIN football.fixtures f ON f.id = r.fixture_id
        WHERE r.provider_id = %s AND r.external_id = %s
        """,
        (context.provider_id, str(external_id)),
    ).fetchone()
    if row is None:
        raise RuntimeError("configured preexisting canary fixture is required before season backfill")
    lifecycle_state, result_finalized_at, availability_basis = conn.execute(
        """
        SELECT f.lifecycle_state::text, f.result_finalized_at, f.availability_basis::text
        FROM source.fixture_provider_refs r
        JOIN football.fixtures f ON f.id = r.fixture_id
        WHERE r.provider_id = %s AND r.external_id = %s
        """,
        (context.provider_id, str(external_id)),
    ).fetchone()
    if lifecycle_state != "completed" or result_finalized_at is None or availability_basis != "observed":
        raise RuntimeError(
            "configured preexisting canary fixture must be completed, observed, and finalized"
        )
    return tuple(row)


def verify_remote(
    conn: Connection[Any],
    *,
    context: SeasonContext,
    fetch_id: int,
    canary_before: tuple[Any, ...] | None,
    untouched_before: Mapping[str, int],
) -> dict[str, Any]:
    fixture_count, mapping_count, completed_count = conn.execute(
        """
        SELECT count(DISTINCT f.id), count(DISTINCT r.external_id),
               count(*) FILTER (WHERE f.lifecycle_state = 'completed' AND f.result_finalized_at IS NOT NULL)
        FROM source.fixture_provider_refs r
        JOIN football.fixtures f ON f.id = r.fixture_id
        WHERE r.provider_id = %s AND f.season_id = %s
        """,
        (context.provider_id, context.season_id),
    ).fetchone()
    status_count, lifecycle_mismatch_count = conn.execute(
        """SELECT count(*), count(*) FILTER (
                WHERE status.status_code <> 'FT' OR fixture.lifecycle_state <> 'completed'
            )
           FROM source.fixture_provider_status status
           JOIN football.fixtures fixture ON fixture.id = status.fixture_id
           WHERE status.provider_id = %s AND fixture.season_id = %s""",
        (context.provider_id, context.season_id),
    ).fetchone()
    orphan_count = conn.execute(
        """
        SELECT count(*) FROM source.fixture_provider_refs r
        LEFT JOIN football.fixtures f ON f.id = r.fixture_id
        WHERE r.provider_id = %s AND f.id IS NULL
        """,
        (context.provider_id,),
    ).fetchone()[0]
    team_counts = conn.execute(
        """
        WITH participants AS (
          SELECT home_team_id AS team_id, 1 AS home_count, 0 AS away_count
          FROM football.fixtures WHERE season_id = %s
          UNION ALL
          SELECT away_team_id, 0, 1 FROM football.fixtures WHERE season_id = %s
        )
        SELECT count(*), min(total), max(total), min(home), max(home), min(away), max(away)
        FROM (
          SELECT team_id, count(*) AS total, sum(home_count) AS home, sum(away_count) AS away
          FROM participants GROUP BY team_id
        ) counts
        """,
        (context.season_id, context.season_id),
    ).fetchone()
    conservative_errors = conn.execute(
        """
        SELECT count(*)
        FROM source.fixture_provider_refs r
        JOIN football.fixtures f ON f.id = r.fixture_id
        WHERE r.provider_id = %s AND f.season_id = %s
          AND (%s IS NULL OR r.external_id <> %s)
          AND (
            f.availability_basis <> 'reconstructed_conservative'
            OR f.terminal_status_observed_at <> f.kickoff_at + interval '3 hours'
            OR f.result_available_at <> f.kickoff_at + interval '3 hours'
          )
        """,
        (
            context.provider_id,
            context.season_id,
            context.scope.preexisting_canary_fixture_external_id,
            str(context.scope.preexisting_canary_fixture_external_id)
            if context.scope.preexisting_canary_fixture_external_id is not None
            else None,
        ),
    ).fetchone()[0]
    fetch_row = conn.execute(
        """
        SELECT provider_results, paging_current, paging_total, normalized_at,
               subject_season_id, content_sha256
        FROM source.provider_fetches WHERE id = %s
        """,
        (fetch_id,),
    ).fetchone()
    raw_row = conn.execute(
        """
        SELECT f.content_sha256, r.inline_body, r.byte_count
        FROM source.provider_fetches f
        JOIN source.provider_raw_payloads r ON r.fetch_id = f.id
        WHERE f.id = %s AND r.purged_at IS NULL AND r.inline_body IS NOT NULL
        """,
        (fetch_id,),
    ).fetchone()
    raw_verified = 0
    if raw_row is not None:
        content_hash, raw_body, byte_count = raw_row
        raw_bytes = bytes(raw_body)
        raw_verified = int(
            bytes(content_hash) == hashlib.sha256(raw_bytes).digest()
            and byte_count == len(raw_bytes)
        )
    direct_dml_grants = conn.execute(
        """
        SELECT count(*)
        FROM (VALUES ('anon'), ('authenticated')) roles(role_name)
        CROSS JOIN pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname IN ('source', 'football', 'ml', 'ops')
          AND c.relkind IN ('r', 'p')
          AND (
            has_table_privilege(role_name, c.oid, 'INSERT')
            OR has_table_privilege(role_name, c.oid, 'UPDATE')
            OR has_table_privilege(role_name, c.oid, 'DELETE')
            OR has_table_privilege(role_name, c.oid, 'TRUNCATE')
          )
        """
    ).fetchone()[0]
    untouched_after = table_counts(conn, UNTOUCHED_TABLES)
    canary_unchanged = canary_fixture_snapshot(conn, context) == canary_before
    expected_count = context.scope.expected_fixture_count
    expected_schedule = (
        context.scope.expected_team_count,
        2 * (context.scope.expected_team_count - 1),
        2 * (context.scope.expected_team_count - 1),
        context.scope.expected_team_count - 1,
        context.scope.expected_team_count - 1,
        context.scope.expected_team_count - 1,
        context.scope.expected_team_count - 1,
    )

    if (fixture_count, mapping_count, completed_count) != (expected_count, expected_count, expected_count):
        raise AssertionError("remote season fixture counts are invalid")
    if orphan_count != 0:
        raise AssertionError("orphan fixture provider mappings detected")
    if (status_count, lifecycle_mismatch_count) != (expected_count, 0):
        raise AssertionError("exact provider fixture statuses are incomplete or inconsistent")
    if team_counts != expected_schedule:
        raise AssertionError("home/away season schedule verification failed")
    if conservative_errors != 0:
        raise AssertionError("historical availability reconstruction is invalid")
    if (
        fetch_row[:3] != (expected_count, 1, 1)
        or fetch_row[3] is None
        or fetch_row[4] != context.season_id
    ):
        raise AssertionError("normalized fetch metadata is invalid")
    if raw_verified != 1:
        raise AssertionError("raw payload integrity verification failed")
    if direct_dml_grants != 0:
        raise AssertionError("anon/authenticated direct DML grant detected")
    if untouched_after != dict(untouched_before):
        raise AssertionError("fixtures-only backfill changed out-of-scope tables")
    if not canary_unchanged:
        raise AssertionError("finalized canary fixture changed")
    return {
        "fixtures": fixture_count,
        "fixture_provider_mappings": mapping_count,
        "completed_fixtures": completed_count,
        "exact_provider_statuses": status_count,
        "provider_status_lifecycle_mismatches": lifecycle_mismatch_count,
        "orphan_mappings": orphan_count,
        "team_schedule": {
            "teams": team_counts[0],
            "matches_per_team": team_counts[1],
            "home_per_team": team_counts[3],
            "away_per_team": team_counts[5],
        },
        "conservative_availability_errors": conservative_errors,
        "raw_payloads_verified": raw_verified,
        "canary_unchanged": canary_unchanged,
        "out_of_scope_counts_unchanged": untouched_after == dict(untouched_before),
        "anon_authenticated_dml_grants": direct_dml_grants,
    }


def run_backfill(
    *,
    client: APIFootballClient | None = None,
    scope: SeasonBackfillScope = DEFAULT_SCOPE,
) -> dict[str, Any]:
    database_url = _database_url()
    api_client = client or APIFootballClient.from_environment()
    with psycopg.connect(database_url, autocommit=True) as conn:
        conn.execute("SET statement_timeout = '30s'")
        context = acquire_context_and_lock(conn, scope=scope)
        untouched_before = table_counts(conn, UNTOUCHED_TABLES)
        canary_before = canary_fixture_snapshot(conn, context)
        reusable = load_reusable_fetch(conn, context)
        attempts = 0
        quota: dict[str, str] = {}
        fetch = reusable
        if fetch is None:
            collected = asyncio.run(
                collect_fixture_season(
                    api_client,
                    record_failure=lambda failure: persist_failed_attempt(
                        conn,
                        context=context,
                        failure=failure,
                    ),
                    scope=scope,
                )
            )
            attempts = collected.attempts
            quota = safe_rate_limit_headers(collected.response.headers)
            if api_client.response_contains_api_key(collected.response.raw_body):
                raise RuntimeError("API key detected in provider response; refusing persistence")
            fetch = persist_success_fetch(conn, context=context, collected=collected)
        elif api_client.response_contains_api_key(fetch.response.raw_body):
            raise RuntimeError("API key detected in stored provider response")

        try:
            records = validate_fixture_season_response(
                fetch.response,
                allowed_team_external_ids=context.team_ids,
                scope=scope,
            )
        except ValueError:
            if fetch.normalized_at is None:
                mark_fetch_contract_error(conn, fetch.fetch_id)
            raise

        statuses = _validate_provider_statuses(
            conn,
            context=context,
            fetch=fetch,
            records=records,
        )

        normalization = {"processed": 0, "created": 0, "batches": len(chunked(records))}
        if fetch.normalized_at is None:
            normalization = normalize_fixture_season(
                conn,
                context=context,
                fetch=fetch,
                records=records,
                status_observations=statuses,
            )
            fetch = replace(fetch, normalized_at=_utcnow())

        verification = verify_remote(
            conn,
            context=context,
            fetch_id=fetch.fetch_id,
            canary_before=canary_before,
            untouched_before=untouched_before,
        )
        return {
            "league_external_id": scope.league_external_id,
            "research_season": scope.season_start_year,
            "api_attempts": attempts,
            "reused_raw_fetch": reusable is not None,
            "fetch_id": fetch.fetch_id,
            "safe_rate_limit": quota,
            "normalization": normalization,
            "verification": verification,
        }


def main() -> None:
    parser = argparse.ArgumentParser(description="Controlled completed-season fixtures backfill")
    parser.add_argument("--league-external-id", type=int, default=DEFAULT_SCOPE.league_external_id)
    parser.add_argument("--season-start-year", type=int, default=DEFAULT_SCOPE.season_start_year)
    parser.add_argument("--expected-fixture-count", type=int, default=DEFAULT_SCOPE.expected_fixture_count)
    parser.add_argument(
        "--preexisting-canary-fixture-external-id",
        type=int,
        default=DEFAULT_SCOPE.preexisting_canary_fixture_external_id,
        help="Existing finalized fixture preserved by the legacy 2024 path; omit with --no-preexisting-canary",
    )
    parser.add_argument(
        "--no-preexisting-canary",
        action="store_true",
        help="Do not require a pre-existing finalized fixture (required for a new season bootstrap)",
    )
    args = parser.parse_args()
    scope = SeasonBackfillScope(
        league_external_id=args.league_external_id,
        season_start_year=args.season_start_year,
        expected_fixture_count=args.expected_fixture_count,
        preexisting_canary_fixture_external_id=(
            None if args.no_preexisting_canary else args.preexisting_canary_fixture_external_id
        ),
    )
    print(json.dumps(run_backfill(scope=scope), indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
