"""Controlled fixtures-only Premier League 2024 season backfill.

The job is intentionally manual, quota-bounded, non-live, and development-only.
It stores one season-wide raw provider response before atomically normalizing the
380 completed fixtures. A valid stored raw response is always preferred over a
new API request so interrupted normalization resumes without spending quota.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
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

PROVIDER_CODE = "api-football"
LEAGUE_EXTERNAL_ID = 39
SEASON_START_YEAR = 2024
EXPECTED_FIXTURE_COUNT = 380
BACKFILL_PURPOSE = "bootstrap"
RAW_RETENTION_DAYS = 30
BATCH_SIZE = 50
MAX_API_ATTEMPTS = 3
LOCK_KEY = "api-football:fixtures:39:2024:v1"
CANARY_FIXTURE_EXTERNAL_ID = 1208021
REQUEST_PARAMS = {"league": LEAGUE_EXTERNAL_ID, "season": SEASON_START_YEAR}
RETRYABLE_HTTP_STATUSES = frozenset({408, 500, 502, 503, 504})


@dataclass(frozen=True)
class SeasonContext:
    provider_id: int
    league_id: int
    season_id: int
    team_ids: Mapping[int, int]


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
) -> list[FixtureRecord]:
    """Validate the entire season response before any normalized database DML."""
    payload = response.data
    if payload.get("parameters") != {"league": "39", "season": "2024"}:
        raise ValueError("provider parameters mismatch for season fixtures")
    if payload.get("errors") not in ([], {}, None):
        raise ValueError("provider returned errors for season fixtures")
    paging = payload.get("paging")
    if paging != {"current": 1, "total": 1}:
        raise ValueError("season fixtures must be a single complete page")
    entries = payload.get("response")
    if not isinstance(entries, list):
        raise ValueError("season fixtures response must be an array")
    if payload.get("results") != EXPECTED_FIXTURE_COUNT or len(entries) != EXPECTED_FIXTURE_COUNT:
        raise ValueError("season fixtures response must contain exactly 380 fixtures")

    allowed_teams = set(allowed_team_external_ids)
    records: list[FixtureRecord] = []
    seen_ids: set[int] = set()
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
        if league.get("id") != LEAGUE_EXTERNAL_ID or league.get("season") != SEASON_START_YEAR:
            raise ValueError("fixture belongs to an unexpected league or season")
        status = fixture.get("status")
        if not isinstance(status, Mapping) or status.get("short") != "FT":
            raise ValueError("historical season backfill accepts only FT fixtures")
        home = teams.get("home")
        away = teams.get("away")
        if not isinstance(home, Mapping) or not isinstance(away, Mapping):
            raise ValueError("fixture teams must contain home and away objects")
        home_external_id = _required_positive_integer(home.get("id"), field="teams.home.id")
        away_external_id = _required_positive_integer(away.get("id"), field="teams.away.id")
        if home_external_id == away_external_id:
            raise ValueError("fixture home and away teams must differ")
        if home_external_id not in allowed_teams or away_external_id not in allowed_teams:
            raise ValueError("fixture participant is not mapped to the season")

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

    records.sort(key=lambda item: item.external_id)
    home_counts = {team_id: 0 for team_id in allowed_teams}
    away_counts = {team_id: 0 for team_id in allowed_teams}
    for record in records:
        home_counts[record.home_external_id] += 1
        away_counts[record.away_external_id] += 1
    if len(allowed_teams) != 20 or set(home_counts.values()) != {19} or set(away_counts.values()) != {19}:
        raise ValueError("season fixtures do not form a complete 20-team home/away schedule")
    return records


def _retry_delay(attempt: int) -> float:
    return (2 ** (attempt - 1)) + random.uniform(0.0, 0.25)


async def collect_fixture_season(
    client: APIFootballClient,
    *,
    record_failure: FailureRecorder,
    sleep: Sleep = asyncio.sleep,
) -> CollectedFetch:
    """Execute one logical request with a hard cap of three physical attempts."""
    for attempt in range(1, MAX_API_ATTEMPTS + 1):
        started_at = _utcnow()
        try:
            response = await client.get("/fixtures", params=REQUEST_PARAMS)
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


def acquire_context_and_lock(conn: Connection[Any]) -> SeasonContext:
    locked = conn.execute(
        "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))",
        (LOCK_KEY,),
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
        (str(LEAGUE_EXTERNAL_ID), SEASON_START_YEAR, PROVIDER_CODE),
    ).fetchone()
    if row is None:
        raise RuntimeError("canary provider/league/season mappings are required before backfill")
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
    if len(team_ids) != 20:
        raise RuntimeError("exactly 20 mapped season teams are required before backfill")
    return SeasonContext(provider_id, league_id, season_id, team_ids)


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
            request_params_sha256(REQUEST_PARAMS),
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
                Jsonb(REQUEST_PARAMS),
                request_params_sha256(REQUEST_PARAMS),
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
                Jsonb(REQUEST_PARAMS),
                request_params_sha256(REQUEST_PARAMS),
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
        if record.external_id == CANARY_FIXTURE_EXTERNAL_ID and actual[15] is None:
            raise ValueError("canary fixture must already be finalized and immutable")
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

    if record.external_id == CANARY_FIXTURE_EXTERNAL_ID:
        raise ValueError("finalized canary fixture provider mapping is missing")

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
        if count != EXPECTED_FIXTURE_COUNT:
            raise AssertionError("season fixture mapping count is not 380")
        conn.execute(
            """
            UPDATE source.provider_fetches
            SET normalized_at = coalesce(normalized_at, clock_timestamp())
            WHERE id = %s AND outcome = 'success'
            """,
            (fetch.fetch_id,),
        )
    return {"processed": processed, "created": created, "batches": len(batches)}


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


def canary_fixture_snapshot(conn: Connection[Any], context: SeasonContext) -> tuple[Any, ...]:
    row = conn.execute(
        """
        SELECT f.*
        FROM source.fixture_provider_refs r
        JOIN football.fixtures f ON f.id = r.fixture_id
        WHERE r.provider_id = %s AND r.external_id = %s
        """,
        (context.provider_id, str(CANARY_FIXTURE_EXTERNAL_ID)),
    ).fetchone()
    if row is None:
        raise RuntimeError("finalized canary fixture is required before season backfill")
    lifecycle_state, result_finalized_at, availability_basis = conn.execute(
        """
        SELECT f.lifecycle_state::text, f.result_finalized_at, f.availability_basis::text
        FROM source.fixture_provider_refs r
        JOIN football.fixtures f ON f.id = r.fixture_id
        WHERE r.provider_id = %s AND r.external_id = %s
        """,
        (context.provider_id, str(CANARY_FIXTURE_EXTERNAL_ID)),
    ).fetchone()
    if lifecycle_state != "completed" or result_finalized_at is None or availability_basis != "observed":
        raise RuntimeError("canary fixture must be completed, observed, and finalized before backfill")
    return tuple(row)


def verify_remote(
    conn: Connection[Any],
    *,
    context: SeasonContext,
    fetch_id: int,
    canary_before: tuple[Any, ...],
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
        WHERE r.provider_id = %s AND f.season_id = %s AND r.external_id <> %s
          AND (
            f.availability_basis <> 'reconstructed_conservative'
            OR f.terminal_status_observed_at <> f.kickoff_at + interval '3 hours'
            OR f.result_available_at <> f.kickoff_at + interval '3 hours'
          )
        """,
        (context.provider_id, context.season_id, str(CANARY_FIXTURE_EXTERNAL_ID)),
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

    if (fixture_count, mapping_count, completed_count) != (380, 380, 380):
        raise AssertionError("remote season fixture counts are invalid")
    if orphan_count != 0:
        raise AssertionError("orphan fixture provider mappings detected")
    if team_counts != (20, 38, 38, 19, 19, 19, 19):
        raise AssertionError("home/away season schedule verification failed")
    if conservative_errors != 0:
        raise AssertionError("historical availability reconstruction is invalid")
    if fetch_row[:3] != (380, 1, 1) or fetch_row[3] is None or fetch_row[4] != context.season_id:
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


def run_backfill(*, client: APIFootballClient | None = None) -> dict[str, Any]:
    database_url = _database_url()
    api_client = client or APIFootballClient.from_environment()
    with psycopg.connect(database_url, autocommit=True) as conn:
        conn.execute("SET statement_timeout = '30s'")
        context = acquire_context_and_lock(conn)
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
            )
        except ValueError:
            if fetch.normalized_at is None:
                mark_fetch_contract_error(conn, fetch.fetch_id)
            raise

        normalization = {"processed": 0, "created": 0, "batches": len(chunked(records))}
        if fetch.normalized_at is None:
            normalization = normalize_fixture_season(
                conn,
                context=context,
                fetch=fetch,
                records=records,
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
            "league_external_id": LEAGUE_EXTERNAL_ID,
            "research_season": SEASON_START_YEAR,
            "api_attempts": attempts,
            "reused_raw_fetch": reusable is not None,
            "fetch_id": fetch.fetch_id,
            "safe_rate_limit": quota,
            "normalization": normalization,
            "verification": verification,
        }


def main() -> None:
    print(json.dumps(run_backfill(), indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
