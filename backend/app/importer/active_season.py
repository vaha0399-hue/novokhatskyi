"""Fail-closed canonical base import for an active provider season.

Unlike :mod:`season_bootstrap`, this narrow path accepts a complete schedule
whose fixtures are either not started (``NS``) or finished (``FT``).  It is
provider-call free: callers must collect and retain the four base responses
before asking this module to validate and normalize them.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from psycopg import Connection
from psycopg.types.json import Jsonb

from app.api_football import APIFootballResponse
from app.importer.canary import _normalize_standings, parse_datetime
from app.importer.fixture_status_contract import FixtureStatusObservation, validate_fixture_status_response
from app.importer.season_backfill import PROVIDER_CODE, SeasonBackfillScope, SeasonContext, StoredFetch, _resolve_venue
from app.importer.season_bootstrap import (
    BaseRequest,
    CollectedBaseResponse,
    LeagueRecord,
    SeasonBootstrapError,
    _insert_coverage_snapshot,
    _league_record,
    _persist_fetch,
    _provider_id,
    _resolve_country,
    _resolve_league,
    _resolve_season,
    _resolve_team_and_venue,
    _team_records,
    _validate_envelope,
    _validate_standings,
)
from app.importer.season_coverage_contract import SeasonCoverageObservation, validate_season_coverage_response


class ActiveSeasonImportError(SeasonBootstrapError):
    """An active-season response cannot safely become canonical data."""


@dataclass(frozen=True)
class ActiveSeasonScope:
    """Generic complete-schedule identity for one currently active league."""

    league_external_id: int
    season_start_year: int
    expected_fixture_count: int

    def __post_init__(self) -> None:
        SeasonBackfillScope(
            league_external_id=self.league_external_id,
            season_start_year=self.season_start_year,
            expected_fixture_count=self.expected_fixture_count,
        )

    @property
    def season_scope(self) -> SeasonBackfillScope:
        return SeasonBackfillScope(
            league_external_id=self.league_external_id,
            season_start_year=self.season_start_year,
            expected_fixture_count=self.expected_fixture_count,
        )

    @property
    def expected_team_count(self) -> int:
        return self.season_scope.expected_team_count

    @property
    def projection(self) -> None:
        """Active-season imports intentionally do not project raw fixtures."""
        return None

    @property
    def lock_key(self) -> str:
        return f"{PROVIDER_CODE}:active-season:{self.league_external_id}:{self.season_start_year}:v1"


@dataclass(frozen=True)
class ActiveFixtureRecord:
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
    status_code: str
    home_goals: int | None
    away_goals: int | None
    home_halftime_goals: int | None
    away_halftime_goals: int | None
    home_fulltime_goals: int | None
    away_fulltime_goals: int | None
    home_extratime_goals: int | None
    away_extratime_goals: int | None
    home_penalty_goals: int | None
    away_penalty_goals: int | None


@dataclass(frozen=True)
class ValidatedActiveBase:
    scope: ActiveSeasonScope
    league: LeagueRecord
    coverage: SeasonCoverageObservation
    teams: tuple[Any, ...]
    fixtures: tuple[ActiveFixtureRecord, ...]
    statuses: tuple[FixtureStatusObservation, ...]
    standings_payload: dict[str, Any]


@dataclass(frozen=True)
class ActiveSeasonVerificationReport:
    season_id: int
    team_count: int
    fixture_count: int
    fixture_mapping_count: int
    standing_row_count: int

    @property
    def is_complete(self) -> bool:
        return (
            self.team_count > 0
            and self.fixture_count == self.fixture_mapping_count
            and self.standing_row_count == self.team_count
        )


def load_replay_collected(
    replay_directory: Path, *, scope: ActiveSeasonScope
) -> tuple[CollectedBaseResponse, ...]:
    """Load exactly four retained request/raw pairs without contacting a provider."""
    if not replay_directory.is_dir():
        raise ActiveSeasonImportError("replay directory does not exist")
    expected = {request.endpoint: request for request in base_requests(scope)}
    collected: dict[str, CollectedBaseResponse] = {}
    request_files = sorted(replay_directory.glob("*.request.json"))
    if len(request_files) != len(expected):
        raise ActiveSeasonImportError("replay directory must contain exactly four request artifacts")
    for request_file in request_files:
        raw_file = request_file.with_name(request_file.name.removesuffix(".request.json") + ".raw.json")
        if not raw_file.is_file():
            raise ActiveSeasonImportError(f"replay raw artifact is missing for {request_file.name}")
        try:
            request_artifact = json.loads(request_file.read_text(encoding="utf-8"))
            raw_body = raw_file.read_bytes()
            payload = json.loads(raw_body)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ActiveSeasonImportError(f"replay artifact is not valid JSON: {request_file.name}") from error
        if not isinstance(request_artifact, Mapping) or not isinstance(payload, dict):
            raise ActiveSeasonImportError("replay artifacts must be JSON objects")
        endpoint = request_artifact.get("endpoint")
        if endpoint not in expected or endpoint in collected:
            raise ActiveSeasonImportError("replay request endpoints are incomplete or duplicated")
        request = expected[endpoint]
        if request_artifact.get("parameters") != request.params:
            raise ActiveSeasonImportError(f"replay request parameters do not match scope for {endpoint}")
        if request_artifact.get("http_status") != 200:
            raise ActiveSeasonImportError(f"replay request is not a successful response for {endpoint}")
        if request_artifact.get("byte_count") != len(raw_body):
            raise ActiveSeasonImportError(f"replay byte count mismatch for {endpoint}")
        expected_hash = request_artifact.get("content_sha256")
        if not isinstance(expected_hash, str) or hashlib.sha256(raw_body).hexdigest() != expected_hash:
            raise ActiveSeasonImportError(f"replay content SHA-256 mismatch for {endpoint}")
        try:
            started_at = parse_datetime(str(request_artifact["request_started_at"]))
            received_at = parse_datetime(str(request_artifact["response_received_at"]))
        except (KeyError, ValueError) as error:
            raise ActiveSeasonImportError(f"replay timestamps are invalid for {endpoint}") from error
        if received_at < started_at:
            raise ActiveSeasonImportError(f"replay response precedes request for {endpoint}")
        collected[endpoint] = CollectedBaseResponse(
            request=request,
            response=APIFootballResponse(payload, raw_body, 200, {}),
            request_started_at=started_at,
            response_received_at=received_at,
        )
    if set(collected) != set(expected):
        raise ActiveSeasonImportError("replay request endpoint set is incomplete")
    return tuple(collected[request.endpoint] for request in base_requests(scope))


def base_requests(scope: ActiveSeasonScope) -> tuple[BaseRequest, ...]:
    return (
        BaseRequest("/leagues", {"id": scope.league_external_id, "season": scope.season_start_year}),
        BaseRequest("/teams", scope.season_scope.request_params),
        BaseRequest("/standings", scope.season_scope.request_params),
        BaseRequest("/fixtures", scope.season_scope.request_params),
    )


def _required_positive(value: Any, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ActiveSeasonImportError(f"{field} must be a positive integer")
    return value


def _optional_text(value: Any, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ActiveSeasonImportError(f"{field} must be a string or null")
    return value


def _optional_score(value: Any, field: str) -> int | None:
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ActiveSeasonImportError(f"{field} must be a non-negative integer or null")
    return value


def _score(score: Mapping[str, Any], period: str, side: str) -> int | None:
    item = score.get(period)
    if not isinstance(item, Mapping):
        raise ActiveSeasonImportError(f"score.{period} must be an object")
    return _optional_score(item.get(side), f"score.{period}.{side}")


def _active_fixture_records(
    payload: Mapping[str, Any], *, scope: ActiveSeasonScope, allowed_team_ids: Iterable[int]
) -> tuple[ActiveFixtureRecord, ...]:
    if payload.get("parameters") != {key: str(value) for key, value in scope.season_scope.request_params.items()}:
        raise ActiveSeasonImportError("provider parameters mismatch for active season fixtures")
    if payload.get("errors") not in ({}, [], None) or payload.get("paging") != {"current": 1, "total": 1}:
        raise ActiveSeasonImportError("active season fixtures response is incomplete")
    entries = payload.get("response")
    if not isinstance(entries, list) or payload.get("results") != len(entries):
        raise ActiveSeasonImportError("active season fixtures response has an invalid result count")
    allowed = set(allowed_team_ids)
    records: list[ActiveFixtureRecord] = []
    seen_ids: set[int] = set()
    pairs: set[tuple[int, int]] = set()
    home_counts = {team_id: 0 for team_id in allowed}
    away_counts = {team_id: 0 for team_id in allowed}
    for entry in entries:
        if not isinstance(entry, Mapping):
            raise ActiveSeasonImportError("fixture entry must be an object")
        fixture, league, teams, goals, score = (entry.get(key) for key in ("fixture", "league", "teams", "goals", "score"))
        if not all(isinstance(value, Mapping) for value in (fixture, league, teams, goals, score)):
            raise ActiveSeasonImportError("fixture entry is missing required objects")
        assert isinstance(fixture, Mapping) and isinstance(league, Mapping) and isinstance(teams, Mapping)
        assert isinstance(goals, Mapping) and isinstance(score, Mapping)
        external_id = _required_positive(fixture.get("id"), "fixture.id")
        if external_id in seen_ids:
            raise ActiveSeasonImportError("active season fixtures contain duplicate ids")
        seen_ids.add(external_id)
        if league.get("id") != scope.league_external_id or league.get("season") != scope.season_start_year:
            raise ActiveSeasonImportError("fixture belongs to an unexpected league or season")
        home, away = teams.get("home"), teams.get("away")
        if not isinstance(home, Mapping) or not isinstance(away, Mapping):
            raise ActiveSeasonImportError("fixture teams are invalid")
        home_id, away_id = _required_positive(home.get("id"), "teams.home.id"), _required_positive(away.get("id"), "teams.away.id")
        if home_id == away_id or home_id not in allowed or away_id not in allowed:
            raise ActiveSeasonImportError("fixture participant is not a distinct mapped season team")
        pair = (home_id, away_id)
        if pair in pairs:
            raise ActiveSeasonImportError("active season schedule has a duplicate directed pairing")
        pairs.add(pair)
        home_counts[home_id] += 1
        away_counts[away_id] += 1
        status = fixture.get("status")
        if not isinstance(status, Mapping) or status.get("short") not in {"NS", "FT"}:
            raise ActiveSeasonImportError("active season accepts only NS or FT fixtures")
        kickoff_raw, timezone = fixture.get("date"), fixture.get("timezone")
        if not isinstance(kickoff_raw, str) or not isinstance(timezone, str) or not timezone:
            raise ActiveSeasonImportError("fixture kickoff/date timezone is invalid")
        venue = fixture.get("venue") or {}
        if not isinstance(venue, Mapping):
            raise ActiveSeasonImportError("fixture venue must be an object or null")
        venue_id = venue.get("id")
        record = ActiveFixtureRecord(
            external_id=external_id, home_external_id=home_id, away_external_id=away_id,
            venue_external_id=None if venue_id is None else _required_positive(venue_id, "fixture.venue.id"),
            venue_name=_optional_text(venue.get("name"), "fixture.venue.name"), venue_city=_optional_text(venue.get("city"), "fixture.venue.city"),
            round_label=_optional_text(league.get("round"), "league.round"), kickoff_at=parse_datetime(kickoff_raw), source_timezone=timezone,
            referee_name=_optional_text(fixture.get("referee"), "fixture.referee"), status_code=str(status["short"]),
            home_goals=_optional_score(goals.get("home"), "goals.home"), away_goals=_optional_score(goals.get("away"), "goals.away"),
            home_halftime_goals=_score(score, "halftime", "home"), away_halftime_goals=_score(score, "halftime", "away"),
            home_fulltime_goals=_score(score, "fulltime", "home"), away_fulltime_goals=_score(score, "fulltime", "away"),
            home_extratime_goals=_score(score, "extratime", "home"), away_extratime_goals=_score(score, "extratime", "away"),
            home_penalty_goals=_score(score, "penalty", "home"), away_penalty_goals=_score(score, "penalty", "away"),
        )
        if record.status_code == "NS" and any(value is not None for value in (
            record.home_goals, record.away_goals, record.home_halftime_goals, record.away_halftime_goals,
            record.home_fulltime_goals, record.away_fulltime_goals, record.home_extratime_goals, record.away_extratime_goals,
            record.home_penalty_goals, record.away_penalty_goals,
        )):
            raise ActiveSeasonImportError("NS fixture must not contain results")
        if record.status_code == "FT" and (record.home_goals is None or record.away_goals is None):
            raise ActiveSeasonImportError("FT fixture must contain final goals")
        records.append(record)
    expected_per_side = scope.expected_team_count - 1
    if len(records) != scope.expected_fixture_count or set(home_counts.values()) != {expected_per_side} or set(away_counts.values()) != {expected_per_side}:
        raise ActiveSeasonImportError("active season fixtures do not form the expected complete schedule")
    return tuple(sorted(records, key=lambda item: item.external_id))


def validate_base_responses(collected: Sequence[CollectedBaseResponse], *, scope: ActiveSeasonScope) -> ValidatedActiveBase:
    expected = {request.endpoint: request for request in base_requests(scope)}
    by_endpoint = {item.request.endpoint: item for item in collected}
    if len(collected) != len(expected) or set(by_endpoint) != set(expected):
        raise ActiveSeasonImportError("base response set is incomplete or contains duplicates")
    for endpoint, request in expected.items():
        if by_endpoint[endpoint].request.params != request.params:
            raise ActiveSeasonImportError(f"unexpected request parameters for {endpoint}")
        _validate_envelope(by_endpoint[endpoint])
    league = _league_record(by_endpoint["/leagues"].response.data, scope)  # type: ignore[arg-type]
    coverage = validate_season_coverage_response(by_endpoint["/leagues"].response, expected_content_sha256=hashlib.sha256(by_endpoint["/leagues"].response.raw_body).digest(), external_league_id=scope.league_external_id, external_season=scope.season_start_year)
    catalog = _team_records(by_endpoint["/teams"].response.data, scope, league.country_name)  # type: ignore[arg-type]
    standings, standing_ids = _validate_standings(by_endpoint["/standings"].response.data, scope, {team.external_id for team in catalog})  # type: ignore[arg-type]
    if len(catalog) != scope.expected_team_count or {team.external_id for team in catalog} != standing_ids:
        raise ActiveSeasonImportError("active season team catalog and standings membership differ")
    fixtures = _active_fixture_records(by_endpoint["/fixtures"].response.data, scope=scope, allowed_team_ids=standing_ids)
    statuses = validate_fixture_status_response(by_endpoint["/fixtures"].response, expected_content_sha256=hashlib.sha256(by_endpoint["/fixtures"].response.raw_body).digest(), expected_fixture_ids={item.external_id for item in fixtures}, allowed_status_codes={"NS", "FT"})
    return ValidatedActiveBase(scope, league, coverage, tuple(catalog), fixtures, statuses, standings)


def _normalize_fixture(conn: Connection[Any], *, context: SeasonContext, fetch: StoredFetch, record: ActiveFixtureRecord) -> int:
    state = "completed" if record.status_code == "FT" else "scheduled"
    terminal = fetch.response_received_at if state == "completed" else None
    row = conn.execute("SELECT fixture_id FROM source.fixture_provider_refs WHERE provider_id=%s AND external_id=%s FOR UPDATE", (context.provider_id, str(record.external_id))).fetchone()
    if row is None:
        venue_id = _resolve_venue(conn, context=context, record=record, seen_at=fetch.response_received_at)
        fixture_id = int(conn.execute(
            """INSERT INTO football.fixtures (season_id,home_team_id,away_team_id,venue_id,round_label,kickoff_at,source_timezone,referee_name,lifecycle_state,home_goals,away_goals,home_halftime_goals,away_halftime_goals,home_fulltime_goals,away_fulltime_goals,home_extratime_goals,away_extratime_goals,home_penalty_goals,away_penalty_goals,terminal_status_observed_at,result_available_at,availability_basis,first_seen_at,last_seen_at,last_source_fetch_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'observed',%s,%s,%s) RETURNING id""",
            (context.season_id, context.team_ids[record.home_external_id], context.team_ids[record.away_external_id], venue_id, record.round_label, record.kickoff_at, record.source_timezone, record.referee_name, state, record.home_goals, record.away_goals, record.home_halftime_goals, record.away_halftime_goals, record.home_fulltime_goals, record.away_fulltime_goals, record.home_extratime_goals, record.away_extratime_goals, record.home_penalty_goals, record.away_penalty_goals, terminal, terminal, fetch.response_received_at, fetch.response_received_at, fetch.fetch_id)).fetchone()[0])
        conn.execute("INSERT INTO source.fixture_provider_refs(provider_id,external_id,fixture_id,first_seen_at,last_seen_at) VALUES(%s,%s,%s,%s,%s)", (context.provider_id, str(record.external_id), fixture_id, fetch.response_received_at, fetch.response_received_at))
        return fixture_id
    fixture_id = int(row[0])
    existing = conn.execute("""SELECT season_id,home_team_id,away_team_id,kickoff_at,lifecycle_state::text,
               home_goals,away_goals,home_halftime_goals,away_halftime_goals,
               home_fulltime_goals,away_fulltime_goals,home_extratime_goals,away_extratime_goals,
               home_penalty_goals,away_penalty_goals,result_finalized_at
        FROM football.fixtures WHERE id=%s FOR UPDATE""", (fixture_id,)).fetchone()
    expected = (context.season_id, context.team_ids[record.home_external_id], context.team_ids[record.away_external_id], record.kickoff_at)
    if existing is None or tuple(existing[:4]) != expected:
        raise ActiveSeasonImportError(f"existing fixture identity conflict for {record.external_id}")
    existing_result = tuple(existing[5:15])
    expected_result = (
        record.home_goals, record.away_goals, record.home_halftime_goals, record.away_halftime_goals,
        record.home_fulltime_goals, record.away_fulltime_goals, record.home_extratime_goals,
        record.away_extratime_goals, record.home_penalty_goals, record.away_penalty_goals,
    )
    if existing[15] is not None:
        if existing[4] != state or existing_result != expected_result:
            raise ActiveSeasonImportError(f"finalized fixture result conflict for {record.external_id}")
        return fixture_id
    if existing[4] == "completed" and state != "completed":
        raise ActiveSeasonImportError(f"completed fixture regressed to NS for {record.external_id}")
    venue_id = _resolve_venue(conn, context=context, record=record, seen_at=fetch.response_received_at)
    conn.execute("""UPDATE football.fixtures SET venue_id=%s,round_label=%s,source_timezone=%s,referee_name=%s,lifecycle_state=%s,home_goals=%s,away_goals=%s,home_halftime_goals=%s,away_halftime_goals=%s,home_fulltime_goals=%s,away_fulltime_goals=%s,home_extratime_goals=%s,away_extratime_goals=%s,home_penalty_goals=%s,away_penalty_goals=%s,terminal_status_observed_at=%s,result_available_at=%s,last_seen_at=greatest(last_seen_at,%s),last_source_fetch_id=%s WHERE id=%s""", (venue_id, record.round_label, record.source_timezone, record.referee_name, state, record.home_goals, record.away_goals, record.home_halftime_goals, record.away_halftime_goals, record.home_fulltime_goals, record.away_fulltime_goals, record.home_extratime_goals, record.away_extratime_goals, record.home_penalty_goals, record.away_penalty_goals, terminal, terminal, fetch.response_received_at, fetch.fetch_id, fixture_id))
    return fixture_id


def _initial_fixture_venue_ids(
    conn: Connection[Any], *, context: SeasonContext, records: Sequence[ActiveFixtureRecord], seen_at: datetime
) -> dict[int, int | None] | None:
    """Return fixture-venue mappings when a new season can be inserted in bulk.

    Team normalization normally creates the same provider venue mappings used by
    fixtures.  A missing mapping means this is an unusual provider response, so
    the caller deliberately falls back to the conservative per-fixture path.
    """
    external_ids = sorted({str(record.venue_external_id) for record in records if record.venue_external_id is not None})
    if not external_ids:
        return {record.external_id: None for record in records}
    rows = conn.execute(
        """SELECT external_id, venue_id FROM source.venue_provider_refs
           WHERE provider_id=%s AND external_id=ANY(%s)""",
        (context.provider_id, external_ids),
    ).fetchall()
    mapped = {str(external_id): int(venue_id) for external_id, venue_id in rows}
    if set(mapped) != set(external_ids):
        return None
    conn.execute(
        """UPDATE source.venue_provider_refs
           SET last_seen_at=greatest(last_seen_at,%s)
           WHERE provider_id=%s AND external_id=ANY(%s)""",
        (seen_at, context.provider_id, external_ids),
    )
    return {
        record.external_id: None if record.venue_external_id is None else mapped[str(record.venue_external_id)]
        for record in records
    }


def _bulk_insert_initial_fixtures(
    conn: Connection[Any], *, context: SeasonContext, fetch: StoredFetch,
    records: Sequence[ActiveFixtureRecord], venue_ids: Mapping[int, int | None],
) -> dict[int, int]:
    """Insert an entirely new season's fixtures with set-based writes.

    This path is intentionally available only after the caller proves that the
    season has neither canonical fixtures nor provider fixture mappings.  That
    keeps all refresh/finalization checks in :func:`_normalize_fixture`.
    """
    rows = [
        {
            "external_id": str(record.external_id),
            "home_team_id": context.team_ids[record.home_external_id],
            "away_team_id": context.team_ids[record.away_external_id],
            "venue_id": venue_ids[record.external_id],
            "round_label": record.round_label,
            "kickoff_at": record.kickoff_at.isoformat(),
            "source_timezone": record.source_timezone,
            "referee_name": record.referee_name,
            "lifecycle_state": "completed" if record.status_code == "FT" else "scheduled",
            "home_goals": record.home_goals,
            "away_goals": record.away_goals,
            "home_halftime_goals": record.home_halftime_goals,
            "away_halftime_goals": record.away_halftime_goals,
            "home_fulltime_goals": record.home_fulltime_goals,
            "away_fulltime_goals": record.away_fulltime_goals,
            "home_extratime_goals": record.home_extratime_goals,
            "away_extratime_goals": record.away_extratime_goals,
            "home_penalty_goals": record.home_penalty_goals,
            "away_penalty_goals": record.away_penalty_goals,
            "terminal_status_observed_at": fetch.response_received_at.isoformat() if record.status_code == "FT" else None,
        }
        for record in records
    ]
    cursor = conn.execute(
        """WITH input AS (
                SELECT * FROM jsonb_to_recordset(%s::jsonb) AS item(
                    external_id text, home_team_id bigint, away_team_id bigint, venue_id bigint,
                    round_label text, kickoff_at timestamptz, source_timezone text, referee_name text,
                    lifecycle_state text, home_goals smallint, away_goals smallint,
                    home_halftime_goals smallint, away_halftime_goals smallint,
                    home_fulltime_goals smallint, away_fulltime_goals smallint,
                    home_extratime_goals smallint, away_extratime_goals smallint,
                    home_penalty_goals smallint, away_penalty_goals smallint,
                    terminal_status_observed_at timestamptz
                )
            ), inserted AS (
                INSERT INTO football.fixtures (
                    season_id,home_team_id,away_team_id,venue_id,round_label,kickoff_at,source_timezone,
                    referee_name,lifecycle_state,home_goals,away_goals,home_halftime_goals,
                    away_halftime_goals,home_fulltime_goals,away_fulltime_goals,home_extratime_goals,
                    away_extratime_goals,home_penalty_goals,away_penalty_goals,terminal_status_observed_at,
                    result_available_at,availability_basis,first_seen_at,last_seen_at,last_source_fetch_id
                )
                SELECT %s,home_team_id,away_team_id,venue_id,round_label,kickoff_at,source_timezone,
                    referee_name,lifecycle_state::football.fixture_lifecycle_state,home_goals,away_goals,
                    home_halftime_goals,away_halftime_goals,home_fulltime_goals,away_fulltime_goals,
                    home_extratime_goals,away_extratime_goals,home_penalty_goals,away_penalty_goals,
                    terminal_status_observed_at,terminal_status_observed_at,'observed',%s,%s,%s
                FROM input
                RETURNING id,home_team_id,away_team_id,kickoff_at
            )
            INSERT INTO source.fixture_provider_refs(
                provider_id,external_id,fixture_id,first_seen_at,last_seen_at
            )
            SELECT %s,input.external_id,inserted.id,%s,%s
            FROM input JOIN inserted USING(home_team_id,away_team_id,kickoff_at)""",
        (
            Jsonb(rows), context.season_id, fetch.response_received_at, fetch.response_received_at,
            fetch.fetch_id, context.provider_id, fetch.response_received_at, fetch.response_received_at,
        ),
    )
    if cursor.rowcount != len(records):
        raise ActiveSeasonImportError("bulk fixture insert did not create every provider mapping")
    status_rows = [{"external_id": str(record.external_id), "status_code": record.status_code} for record in records]
    cursor = conn.execute(
        """WITH input AS (
                SELECT * FROM jsonb_to_recordset(%s::jsonb) AS item(external_id text,status_code text)
            )
            INSERT INTO source.fixture_provider_status(
                provider_id,fixture_id,status_code,observed_at,source_fetch_id
            )
            SELECT %s,ref.fixture_id,input.status_code,%s,%s
            FROM input JOIN source.fixture_provider_refs ref
              ON ref.provider_id=%s AND ref.external_id=input.external_id""",
        (Jsonb(status_rows), context.provider_id, fetch.response_received_at, fetch.fetch_id, context.provider_id),
    )
    if cursor.rowcount != len(records):
        raise ActiveSeasonImportError("bulk fixture status insert did not create every provider status")
    mappings = conn.execute(
        """SELECT external_id,fixture_id FROM source.fixture_provider_refs
           WHERE provider_id=%s AND external_id=ANY(%s)""",
        (context.provider_id, [str(record.external_id) for record in records]),
    ).fetchall()
    if len(mappings) != len(records):
        raise ActiveSeasonImportError("bulk fixture mapping lookup is incomplete")
    return {int(external_id): int(fixture_id) for external_id, fixture_id in mappings}


def import_active_base(conn: Connection[Any], *, collected: Sequence[CollectedBaseResponse], scope: ActiveSeasonScope) -> SeasonContext:
    """Atomically create or refresh canonical scheduled/finished season data."""
    validated = validate_base_responses(collected, scope=scope)
    by_endpoint = {item.request.endpoint: item for item in collected}
    with conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (scope.lock_key,))
        provider_id = _provider_id(conn)
        country_id = _resolve_country(conn, provider_id=provider_id, league=validated.league)
        league_id = _resolve_league(conn, provider_id=provider_id, league=validated.league, country_id=country_id)
        season_id = _resolve_season(conn, provider_id=provider_id, league_id=league_id, league=validated.league, scope=scope)  # type: ignore[arg-type]
        fetch_ids = {endpoint: _persist_fetch(conn, provider_id=provider_id, collected=by_endpoint[endpoint], season_id=season_id) for endpoint in by_endpoint}
        _insert_coverage_snapshot(conn, provider_id=provider_id, season_id=season_id, fetch_id=fetch_ids["/leagues"], captured_at=by_endpoint["/leagues"].response_received_at, coverage=validated.coverage)
        team_ids: dict[int, int] = {}
        for team in validated.teams:
            team_id, venue_id = _resolve_team_and_venue(conn, provider_id=provider_id, country_id=country_id, record=team)
            team_ids[team.external_id] = team_id
            conn.execute("""INSERT INTO football.season_teams(season_id,team_id,default_venue_id,first_seen_at,last_seen_at,last_source_fetch_id) VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT(season_id,team_id) DO UPDATE SET default_venue_id=excluded.default_venue_id,last_seen_at=greatest(football.season_teams.last_seen_at,excluded.last_seen_at),last_source_fetch_id=excluded.last_source_fetch_id""", (season_id, team_id, venue_id, by_endpoint["/teams"].response_received_at, by_endpoint["/teams"].response_received_at, fetch_ids["/teams"]))
        context = SeasonContext(provider_id, league_id, season_id, team_ids, scope.season_scope)
        fixture_item = by_endpoint["/fixtures"]
        fetch = StoredFetch(fetch_ids["/fixtures"], fixture_item.response, fixture_item.request_started_at, fixture_item.response_received_at, None, False)
        existing = conn.execute(
            """SELECT
                    EXISTS(SELECT 1 FROM football.fixtures WHERE season_id=%s),
                    EXISTS(
                        SELECT 1 FROM source.fixture_provider_refs ref
                        JOIN football.fixtures fixture ON fixture.id=ref.fixture_id
                        WHERE ref.provider_id=%s AND fixture.season_id=%s
                    )""",
            (season_id, provider_id, season_id),
        ).fetchone()
        assert existing is not None
        venue_ids = None if any(existing) else _initial_fixture_venue_ids(
            conn, context=context, records=validated.fixtures, seen_at=fixture_item.response_received_at
        )
        if venue_ids is not None:
            _bulk_insert_initial_fixtures(
                conn, context=context, fetch=fetch, records=validated.fixtures, venue_ids=venue_ids
            )
        else:
            fixture_ids = {
                record.external_id: _normalize_fixture(conn, context=context, fetch=fetch, record=record)
                for record in validated.fixtures
            }
            for status in validated.statuses:
                conn.execute(
                    """INSERT INTO source.fixture_provider_status(provider_id,fixture_id,status_code,observed_at,source_fetch_id)
                       VALUES(%s,%s,%s,%s,%s)
                       ON CONFLICT(provider_id,fixture_id) DO UPDATE
                       SET status_code=excluded.status_code,observed_at=excluded.observed_at,
                           source_fetch_id=excluded.source_fetch_id
                       WHERE source.fixture_provider_status.observed_at < excluded.observed_at""",
                    (provider_id, fixture_ids[status.external_fixture_id], status.status_code,
                     fetch.response_received_at, fetch.fetch_id),
                )
        _normalize_standings(conn, provider_id=provider_id, season_id=season_id, fetch_id=fetch_ids["/standings"], captured_at=by_endpoint["/standings"].response_received_at, payload=validated.standings_payload)
        conn.execute("UPDATE source.provider_fetches SET normalized_at=coalesce(normalized_at,clock_timestamp()) WHERE id=ANY(%s)", (list(fetch_ids.values()),))
        return context


def verify_active_season(conn: Connection[Any], *, scope: ActiveSeasonScope) -> ActiveSeasonVerificationReport:
    """Return canonical counts needed before enabling a league's live worker."""
    row = conn.execute("""SELECT ref.season_id,(SELECT count(*) FROM football.season_teams st WHERE st.season_id=ref.season_id),(SELECT count(*) FROM football.fixtures f WHERE f.season_id=ref.season_id),(SELECT count(*) FROM source.fixture_provider_refs mapping JOIN football.fixtures f ON f.id=mapping.fixture_id WHERE mapping.provider_id=provider.id AND f.season_id=ref.season_id),(SELECT count(*) FROM football.standings_snapshot_rows rows WHERE rows.snapshot_id=(SELECT snapshots.id FROM football.standings_snapshots snapshots WHERE snapshots.season_id=ref.season_id ORDER BY snapshots.captured_at DESC,snapshots.id DESC LIMIT 1)) FROM source.providers provider JOIN source.season_provider_refs ref ON ref.provider_id=provider.id WHERE provider.code=%s AND ref.league_external_id=%s AND ref.external_season=%s""", (PROVIDER_CODE, str(scope.league_external_id), scope.season_start_year)).fetchone()
    if row is None:
        raise ActiveSeasonImportError("canonical active season mapping is missing")
    report = ActiveSeasonVerificationReport(*(int(value) for value in row))
    if report.team_count != scope.expected_team_count or report.fixture_count != scope.expected_fixture_count or report.fixture_mapping_count != scope.expected_fixture_count or report.standing_row_count != scope.expected_team_count:
        raise ActiveSeasonImportError("canonical active season counts do not match the requested scope")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay a retained active-season canonical base import")
    parser.add_argument("--replay-directory", type=Path, required=True)
    parser.add_argument("--league-external-id", type=int, required=True)
    parser.add_argument("--season-start-year", type=int, required=True)
    parser.add_argument("--expected-fixture-count", type=int, required=True)
    args = parser.parse_args()
    scope = ActiveSeasonScope(
        league_external_id=args.league_external_id,
        season_start_year=args.season_start_year,
        expected_fixture_count=args.expected_fixture_count,
    )
    database_url = os.environ.get("SUPABASE_DB_URL")
    if not database_url:
        raise ActiveSeasonImportError("SUPABASE_DB_URL is required")
    collected = load_replay_collected(args.replay_directory, scope=scope)
    with Connection.connect(database_url) as conn:
        import_active_base(conn, collected=collected, scope=scope)
        report = verify_active_season(conn, scope=scope)
    print(json.dumps(report.__dict__, sort_keys=True))


if __name__ == "__main__":
    main()
