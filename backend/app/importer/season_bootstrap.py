"""Fail-closed canonical bootstrap for one completed provider season.

This module is deliberately provider-call free.  A controlled canary collects
the four bounded base responses first, validates their raw contracts, then
passes them here for one atomic bootstrap transaction.  It never touches an
existing season's fixtures, standings, statistics, historical lineups, or raw
payloads.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from psycopg import Connection
from psycopg.types.json import Jsonb

from app.api_football import APIFootballResponse
from app.importer.canary import _normalize_standings, parse_datetime, request_params_sha256
from app.importer.fixture_status_contract import FixtureStatusObservation, validate_fixture_status_response
from app.importer.season_backfill import (
    PROVIDER_CODE,
    SeasonBackfillScope,
    SeasonContext,
    StoredFetch,
    _validate_provider_statuses,
    normalize_fixture_season,
    validate_fixture_season_response,
)
from app.importer.season_coverage_contract import (
    SeasonCoverageObservation,
    validate_season_coverage_response,
)

RAW_RETENTION_DAYS = 30
BOOTSTRAP_PURPOSE = "bootstrap"


class SeasonBootstrapError(RuntimeError):
    """A response or canonical identity is unsafe for bootstrap."""


@dataclass(frozen=True)
class BootstrapScope:
    """Explicit, immutable identity for one completed-season bootstrap."""

    league_external_id: int
    season_start_year: int
    expected_fixture_count: int

    def __post_init__(self) -> None:
        # Reuse the established completed-league schedule validation.
        SeasonBackfillScope(
            league_external_id=self.league_external_id,
            season_start_year=self.season_start_year,
            expected_fixture_count=self.expected_fixture_count,
            preexisting_canary_fixture_external_id=None,
        )

    @property
    def season_scope(self) -> SeasonBackfillScope:
        return SeasonBackfillScope(
            league_external_id=self.league_external_id,
            season_start_year=self.season_start_year,
            expected_fixture_count=self.expected_fixture_count,
            preexisting_canary_fixture_external_id=None,
        )

    @property
    def lock_key(self) -> str:
        return f"{PROVIDER_CODE}:season-bootstrap:{self.league_external_id}:{self.season_start_year}:v1"

    @property
    def expected_team_count(self) -> int:
        return self.season_scope.expected_team_count


@dataclass(frozen=True)
class BaseRequest:
    endpoint: str
    params: dict[str, int]


@dataclass(frozen=True)
class CollectedBaseResponse:
    request: BaseRequest
    response: APIFootballResponse
    request_started_at: datetime
    response_received_at: datetime


@dataclass(frozen=True)
class TeamRecord:
    external_id: int
    name: str
    code: str | None
    country_name: str
    founded_year: int | None
    is_national: bool
    logo_url: str | None
    venue_external_id: int | None
    venue_name: str | None
    venue_address: str | None
    venue_city: str | None
    venue_capacity: int | None
    venue_surface: str | None
    venue_image_url: str | None


@dataclass(frozen=True)
class LeagueRecord:
    external_id: int
    name: str
    country_name: str
    country_external_code: str
    country_flag_url: str | None
    competition_type: str
    logo_url: str | None
    starts_on: datetime
    ends_on: datetime


@dataclass(frozen=True)
class ValidatedBase:
    scope: BootstrapScope
    league: LeagueRecord
    coverage: SeasonCoverageObservation
    teams: tuple[TeamRecord, ...]
    fixtures: tuple[Any, ...]
    statuses: tuple[FixtureStatusObservation, ...]
    standings_payload: dict[str, Any]


def base_requests(scope: BootstrapScope) -> tuple[BaseRequest, ...]:
    return (
        BaseRequest("/leagues", {"id": scope.league_external_id, "season": scope.season_start_year}),
        BaseRequest("/teams", {"league": scope.league_external_id, "season": scope.season_start_year}),
        BaseRequest("/standings", {"league": scope.league_external_id, "season": scope.season_start_year}),
        BaseRequest("/fixtures", scope.season_scope.request_params),
    )


def _require_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SeasonBootstrapError(f"{field} must be a non-blank string")
    return value


def _optional_string(value: Any, field: str) -> str | None:
    if value is None:
        return None
    return _require_string(value, field)


def _optional_nonnegative_int(value: Any, field: str) -> int | None:
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise SeasonBootstrapError(f"{field} must be a non-negative integer or null")
    return value


def _require_positive_int(value: Any, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise SeasonBootstrapError(f"{field} must be a positive integer")
    return value


def _validate_envelope(collected: CollectedBaseResponse) -> dict[str, Any]:
    payload = collected.response.data
    expected_params = {key: str(value) for key, value in collected.request.params.items()}
    if not isinstance(payload, dict) or payload.get("get") != collected.request.endpoint.removeprefix("/"):
        raise SeasonBootstrapError(f"unexpected provider endpoint envelope for {collected.request.endpoint}")
    if payload.get("parameters") != expected_params:
        raise SeasonBootstrapError(f"provider parameters mismatch for {collected.request.endpoint}")
    if payload.get("errors") not in ({}, [], None):
        raise SeasonBootstrapError(f"provider returned errors for {collected.request.endpoint}")
    response = payload.get("response")
    paging = payload.get("paging")
    if not isinstance(response, list) or payload.get("results") != len(response):
        raise SeasonBootstrapError(f"results/response mismatch for {collected.request.endpoint}")
    if paging != {"current": 1, "total": 1}:
        raise SeasonBootstrapError(f"base response must be one complete page: {collected.request.endpoint}")
    if hashlib.sha256(collected.response.raw_body).digest() == b"":
        raise AssertionError("unreachable raw SHA-256 guard")
    return payload


def _league_record(payload: Mapping[str, Any], scope: BootstrapScope) -> LeagueRecord:
    response = payload["response"]
    if len(response) != 1 or not isinstance(response[0], Mapping):
        raise SeasonBootstrapError("/leagues must return exactly one league")
    item = response[0]
    league = item.get("league")
    country = item.get("country")
    seasons = item.get("seasons")
    if not isinstance(league, Mapping) or not isinstance(country, Mapping) or not isinstance(seasons, list):
        raise SeasonBootstrapError("/leagues has an invalid object structure")
    if _require_positive_int(league.get("id"), "league.id") != scope.league_external_id:
        raise SeasonBootstrapError("/leagues returned an unexpected league")
    provider_type = _require_string(league.get("type"), "league.type")
    if provider_type != "League":
        raise SeasonBootstrapError(f"unreviewed provider competition type: {provider_type}")
    matching = [item for item in seasons if isinstance(item, Mapping) and item.get("year") == scope.season_start_year]
    if len(matching) != 1:
        raise SeasonBootstrapError("/leagues must contain exactly the requested season")
    season = matching[0]
    start = _require_string(season.get("start"), "seasons.start")
    end = _require_string(season.get("end"), "seasons.end")
    try:
        starts_on = datetime.fromisoformat(start).replace(tzinfo=UTC)
        ends_on = datetime.fromisoformat(end).replace(tzinfo=UTC)
    except ValueError as error:
        raise SeasonBootstrapError("provider season dates are invalid") from error
    if ends_on < starts_on:
        raise SeasonBootstrapError("provider season end precedes start")
    return LeagueRecord(
        external_id=scope.league_external_id,
        name=_require_string(league.get("name"), "league.name"),
        country_name=_require_string(country.get("name"), "country.name"),
        country_external_code=_require_string(country.get("code"), "country.code"),
        country_flag_url=_optional_string(country.get("flag"), "country.flag"),
        competition_type="league",
        logo_url=_optional_string(league.get("logo"), "league.logo"),
        starts_on=starts_on,
        ends_on=ends_on,
    )


def _team_records(payload: Mapping[str, Any], scope: BootstrapScope, country_name: str) -> tuple[TeamRecord, ...]:
    response = payload["response"]
    if len(response) != scope.expected_team_count:
        raise SeasonBootstrapError(f"/teams must contain exactly {scope.expected_team_count} teams")
    records: list[TeamRecord] = []
    seen: set[int] = set()
    for item in response:
        if not isinstance(item, Mapping) or not isinstance(item.get("team"), Mapping):
            raise SeasonBootstrapError("/teams item must contain a team object")
        team = item["team"]
        venue = item.get("venue")
        if venue is not None and not isinstance(venue, Mapping):
            raise SeasonBootstrapError("team venue must be an object or null")
        external_id = _require_positive_int(team.get("id"), "team.id")
        if external_id in seen:
            raise SeasonBootstrapError("/teams contains a duplicate provider team ID")
        seen.add(external_id)
        team_country = _require_string(team.get("country"), "team.country")
        if team_country.casefold() != country_name.casefold():
            raise SeasonBootstrapError("team country does not match league country")
        founded = _optional_nonnegative_int(team.get("founded"), "team.founded")
        if founded is not None and founded < 1800:
            raise SeasonBootstrapError("team.founded is below the canonical minimum")
        national = team.get("national")
        if not isinstance(national, bool):
            raise SeasonBootstrapError("team.national must be boolean")
        venue = venue or {}
        records.append(
            TeamRecord(
                external_id=external_id,
                name=_require_string(team.get("name"), "team.name"),
                code=_optional_string(team.get("code"), "team.code"),
                country_name=team_country,
                founded_year=founded,
                is_national=national,
                logo_url=_optional_string(team.get("logo"), "team.logo"),
                venue_external_id=(
                    _require_positive_int(venue.get("id"), "venue.id") if venue.get("id") is not None else None
                ),
                venue_name=_optional_string(venue.get("name"), "venue.name"),
                venue_address=_optional_string(venue.get("address"), "venue.address"),
                venue_city=_optional_string(venue.get("city"), "venue.city"),
                venue_capacity=_optional_nonnegative_int(venue.get("capacity"), "venue.capacity"),
                venue_surface=_optional_string(venue.get("surface"), "venue.surface"),
                venue_image_url=_optional_string(venue.get("image"), "venue.image"),
            )
        )
    return tuple(sorted(records, key=lambda record: record.external_id))


def _validate_standings(payload: Mapping[str, Any], scope: BootstrapScope, team_external_ids: set[int]) -> dict[str, Any]:
    response = payload["response"]
    if len(response) != 1 or not isinstance(response[0], Mapping):
        raise SeasonBootstrapError("/standings must return exactly one league")
    league = response[0].get("league")
    if not isinstance(league, Mapping) or league.get("id") != scope.league_external_id or league.get("season") != scope.season_start_year:
        raise SeasonBootstrapError("standings league/season mismatch")
    groups = league.get("standings")
    if not isinstance(groups, list) or len(groups) != 1 or not isinstance(groups[0], list):
        raise SeasonBootstrapError("completed league standings must contain exactly one group")
    rows = groups[0]
    if len(rows) != scope.expected_team_count:
        raise SeasonBootstrapError("standings team count mismatch")
    provider_ids: set[int] = set()
    ranks: set[int] = set()
    for row in rows:
        if not isinstance(row, Mapping) or not isinstance(row.get("team"), Mapping):
            raise SeasonBootstrapError("standings row has invalid team")
        provider_ids.add(_require_positive_int(row["team"].get("id"), "standings.team.id"))
        ranks.add(_require_positive_int(row.get("rank"), "standings.rank"))
        for record_name in ("all", "home", "away"):
            record = row.get(record_name)
            if not isinstance(record, Mapping) or not isinstance(record.get("goals"), Mapping):
                raise SeasonBootstrapError(f"standings.{record_name} has invalid structure")
    if provider_ids != team_external_ids or ranks != set(range(1, scope.expected_team_count + 1)):
        raise SeasonBootstrapError("standings membership or ranks do not match season teams")
    return dict(payload)


def validate_base_responses(
    collected: Sequence[CollectedBaseResponse], *, scope: BootstrapScope
) -> ValidatedBase:
    """Validate all four retained raw contracts before any canonical DML."""

    expected = {request.endpoint: request for request in base_requests(scope)}
    by_endpoint = {item.request.endpoint: item for item in collected}
    if set(by_endpoint) != set(expected) or len(collected) != len(expected):
        raise SeasonBootstrapError("base response set is incomplete or contains duplicates")
    for endpoint, request in expected.items():
        item = by_endpoint[endpoint]
        if item.request.params != request.params:
            raise SeasonBootstrapError(f"unexpected request parameters for {endpoint}")
        _validate_envelope(item)

    leagues_payload = by_endpoint["/leagues"].response.data
    league = _league_record(leagues_payload, scope)
    coverage = validate_season_coverage_response(
        by_endpoint["/leagues"].response,
        expected_content_sha256=hashlib.sha256(by_endpoint["/leagues"].response.raw_body).digest(),
        external_league_id=scope.league_external_id,
        external_season=scope.season_start_year,
    )
    teams = _team_records(by_endpoint["/teams"].response.data, scope, league.country_name)
    team_ids = {team.external_id for team in teams}
    fixtures = validate_fixture_season_response(
        by_endpoint["/fixtures"].response,
        allowed_team_external_ids=team_ids,
        scope=scope.season_scope,
    )
    statuses = validate_fixture_status_response(
        by_endpoint["/fixtures"].response,
        expected_content_sha256=hashlib.sha256(by_endpoint["/fixtures"].response.raw_body).digest(),
        expected_fixture_ids={fixture.external_id for fixture in fixtures},
        allowed_status_codes={"FT"},
    )
    standings = _validate_standings(by_endpoint["/standings"].response.data, scope, team_ids)
    return ValidatedBase(scope, league, coverage, teams, tuple(fixtures), statuses, standings)


def _provider_id(conn: Connection[Any]) -> int:
    row = conn.execute("SELECT id FROM source.providers WHERE code=%s", (PROVIDER_CODE,)).fetchone()
    if row is None:
        raise SeasonBootstrapError("API-Football provider mapping is required")
    return int(row[0])


def _resolve_country(conn: Connection[Any], *, provider_id: int, league: LeagueRecord) -> int:
    row = conn.execute(
        """SELECT country_id FROM source.country_provider_refs
           WHERE provider_id=%s AND external_code=%s FOR UPDATE""",
        (provider_id, league.country_external_code),
    ).fetchone()
    if row is not None:
        country_id = int(row[0])
        name = conn.execute("SELECT name FROM football.countries WHERE id=%s", (country_id,)).fetchone()
        if name is None or str(name[0]).casefold() != league.country_name.casefold():
            raise SeasonBootstrapError("provider country mapping conflicts with canonical country")
        return country_id
    row = conn.execute(
        """SELECT id FROM football.countries
           WHERE lower(btrim(name))=lower(btrim(%s)) AND retired_at IS NULL FOR UPDATE""",
        (league.country_name,),
    ).fetchone()
    country_id = int(row[0]) if row is not None else int(
        conn.execute(
            "INSERT INTO football.countries(name,flag_url) VALUES(%s,%s) RETURNING id",
            (league.country_name, league.country_flag_url),
        ).fetchone()[0]
    )
    conn.execute(
        """INSERT INTO source.country_provider_refs(provider_id,external_code,country_id)
           VALUES(%s,%s,%s)""",
        (provider_id, league.country_external_code, country_id),
    )
    return country_id


def _resolve_league(conn: Connection[Any], *, provider_id: int, league: LeagueRecord, country_id: int) -> int:
    row = conn.execute(
        """SELECT ref.league_id, target.name, target.country_id, target.competition_type
           FROM source.league_provider_refs ref
           JOIN football.leagues target ON target.id=ref.league_id
           WHERE ref.provider_id=%s AND ref.external_id=%s FOR UPDATE OF ref,target""",
        (provider_id, str(league.external_id)),
    ).fetchone()
    if row is not None:
        league_id, name, actual_country_id, competition_type = row
        if (str(name), int(actual_country_id), str(competition_type)) != (
            league.name,
            country_id,
            league.competition_type,
        ):
            raise SeasonBootstrapError("provider league mapping conflicts with canonical league")
        return int(league_id)
    league_id = int(
        conn.execute(
            """INSERT INTO football.leagues(name,country_name,logo_url,flag_url,country_id,competition_type)
               VALUES(%s,%s,%s,%s,%s,%s) RETURNING id""",
            (league.name, league.country_name, league.logo_url, league.country_flag_url, country_id, league.competition_type),
        ).fetchone()[0]
    )
    conn.execute(
        """INSERT INTO source.league_provider_refs(provider_id,external_id,league_id)
           VALUES(%s,%s,%s)""",
        (provider_id, str(league.external_id), league_id),
    )
    return league_id


def _resolve_season(
    conn: Connection[Any], *, provider_id: int, league_id: int, league: LeagueRecord, scope: BootstrapScope
) -> int:
    row = conn.execute(
        """SELECT ref.season_id, season.league_id, season.starts_on, season.ends_on
           FROM source.season_provider_refs ref
           JOIN football.seasons season ON season.id=ref.season_id
           WHERE ref.provider_id=%s AND ref.league_external_id=%s AND ref.external_season=%s
           FOR UPDATE OF ref,season""",
        (provider_id, str(scope.league_external_id), scope.season_start_year),
    ).fetchone()
    expected_dates = (league.starts_on.date(), league.ends_on.date())
    if row is not None:
        season_id, actual_league_id, starts_on, ends_on = row
        if (int(actual_league_id), starts_on, ends_on) != (league_id, *expected_dates):
            raise SeasonBootstrapError("provider season mapping conflicts with canonical season")
        return int(season_id)
    label = f"{scope.season_start_year}/{str(scope.season_start_year + 1)[-2:]}"
    season_id = int(
        conn.execute(
            """INSERT INTO football.seasons(league_id,start_year,label,starts_on,ends_on)
               VALUES(%s,%s,%s,%s,%s) RETURNING id""",
            (league_id, scope.season_start_year, label, *expected_dates),
        ).fetchone()[0]
    )
    conn.execute(
        """INSERT INTO source.season_provider_refs(provider_id,league_external_id,external_season,season_id)
           VALUES(%s,%s,%s,%s)""",
        (provider_id, str(scope.league_external_id), scope.season_start_year, season_id),
    )
    return season_id


def _persist_fetch(
    conn: Connection[Any],
    *,
    provider_id: int,
    collected: CollectedBaseResponse,
    season_id: int,
) -> int:
    payload = collected.response.data
    paging = payload["paging"]
    fetch_id = int(
        conn.execute(
            """INSERT INTO source.provider_fetches(
                    provider_id,endpoint,request_params,request_params_sha256,purpose,
                    request_started_at,response_received_at,http_status,outcome,
                    provider_results,paging_current,paging_total,content_sha256,subject_season_id
                ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,'success',%s,%s,%s,%s,%s)
                RETURNING id""",
            (
                provider_id,
                collected.request.endpoint,
                Jsonb(collected.request.params),
                request_params_sha256(collected.request.params),
                BOOTSTRAP_PURPOSE,
                collected.request_started_at,
                collected.response_received_at,
                collected.response.status_code,
                payload["results"],
                paging["current"],
                paging["total"],
                hashlib.sha256(collected.response.raw_body).digest(),
                season_id,
            ),
        ).fetchone()[0]
    )
    conn.execute(
        """INSERT INTO source.provider_raw_payloads(
                fetch_id,inline_body,content_type,byte_count,retention_class,expires_at
            ) VALUES(%s,%s,'application/json',%s,'standard',%s)""",
        (
            fetch_id,
            collected.response.raw_body,
            len(collected.response.raw_body),
            collected.response_received_at + timedelta(days=RAW_RETENTION_DAYS),
        ),
    )
    return fetch_id


def _resolve_team_and_venue(
    conn: Connection[Any], *, provider_id: int, country_id: int, record: TeamRecord
) -> tuple[int, int | None]:
    row = conn.execute(
        """SELECT ref.team_id,target.name,target.country_id
           FROM source.team_provider_refs ref
           JOIN football.teams target ON target.id=ref.team_id
           WHERE ref.provider_id=%s AND ref.external_id=%s FOR UPDATE OF ref,target""",
        (provider_id, str(record.external_id)),
    ).fetchone()
    if row is not None:
        team_id, name, actual_country_id = row
        if str(name) != record.name or actual_country_id != country_id:
            raise SeasonBootstrapError("provider team mapping conflicts with canonical team")
        team_id = int(team_id)
    else:
        team_id = int(
            conn.execute(
                """INSERT INTO football.teams(
                        name,code,country_name,founded_year,is_national,logo_url,country_id
                    ) VALUES(%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                (
                    record.name,
                    record.code,
                    record.country_name,
                    record.founded_year,
                    record.is_national,
                    record.logo_url,
                    country_id,
                ),
            ).fetchone()[0]
        )
        conn.execute(
            """INSERT INTO source.team_provider_refs(provider_id,external_id,team_id)
               VALUES(%s,%s,%s)""",
            (provider_id, str(record.external_id), team_id),
        )

    if record.venue_external_id is None:
        return team_id, None
    row = conn.execute(
        """SELECT ref.venue_id,target.name
           FROM source.venue_provider_refs ref
           JOIN football.venues target ON target.id=ref.venue_id
           WHERE ref.provider_id=%s AND ref.external_id=%s FOR UPDATE OF ref,target""",
        (provider_id, str(record.venue_external_id)),
    ).fetchone()
    if row is not None:
        venue_id, name = row
        if record.venue_name is not None and str(name) != record.venue_name:
            raise SeasonBootstrapError("provider venue mapping conflicts with canonical venue")
        return team_id, int(venue_id)
    venue_id = int(
        conn.execute(
            """INSERT INTO football.venues(name,address,city,capacity,surface,image_url)
               VALUES(%s,%s,%s,%s,%s,%s) RETURNING id""",
            (
                record.venue_name or "Unknown venue",
                record.venue_address,
                record.venue_city,
                record.venue_capacity,
                record.venue_surface,
                record.venue_image_url,
            ),
        ).fetchone()[0]
    )
    conn.execute(
        """INSERT INTO source.venue_provider_refs(provider_id,external_id,venue_id)
           VALUES(%s,%s,%s)""",
        (provider_id, str(record.venue_external_id), venue_id),
    )
    return team_id, venue_id


def _insert_coverage_snapshot(
    conn: Connection[Any], *, provider_id: int, season_id: int, fetch_id: int,
    captured_at: datetime, coverage: SeasonCoverageObservation,
) -> None:
    conn.execute(
        """INSERT INTO source.season_coverage_snapshots(
                provider_id,season_id,captured_at,fixture_statistics_supported,lineups_supported,
                standings_supported,injuries_supported,mapping_version,source_fetch_id
            ) VALUES(%s,%s,%s,%s,%s,%s,%s,'api-football-v1',%s)
            ON CONFLICT(provider_id,season_id,source_fetch_id) DO NOTHING""",
        (
            provider_id,
            season_id,
            captured_at,
            coverage.fixture_statistics_supported,
            coverage.lineups_supported,
            coverage.standings_supported,
            coverage.injuries_supported,
            fetch_id,
        ),
    )


def bootstrap_base(
    conn: Connection[Any], *, collected: Sequence[CollectedBaseResponse], scope: BootstrapScope
) -> SeasonContext:
    """Atomically create the canonical base for a validated completed season."""

    validated = validate_base_responses(collected, scope=scope)
    by_endpoint = {item.request.endpoint: item for item in collected}
    with conn.transaction():
        conn.execute("SET LOCAL statement_timeout='120s'")
        conn.execute("SET LOCAL lock_timeout='10s'")
        conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (scope.lock_key,))
        provider_id = _provider_id(conn)
        country_id = _resolve_country(conn, provider_id=provider_id, league=validated.league)
        league_id = _resolve_league(conn, provider_id=provider_id, league=validated.league, country_id=country_id)
        season_id = _resolve_season(
            conn, provider_id=provider_id, league_id=league_id, league=validated.league, scope=scope
        )

        fetch_ids = {
            endpoint: _persist_fetch(
                conn, provider_id=provider_id, collected=by_endpoint[endpoint], season_id=season_id
            )
            for endpoint in ("/leagues", "/teams", "/standings", "/fixtures")
        }
        _insert_coverage_snapshot(
            conn,
            provider_id=provider_id,
            season_id=season_id,
            fetch_id=fetch_ids["/leagues"],
            captured_at=by_endpoint["/leagues"].response_received_at,
            coverage=validated.coverage,
        )

        team_ids: dict[int, int] = {}
        for team in validated.teams:
            team_id, venue_id = _resolve_team_and_venue(
                conn, provider_id=provider_id, country_id=country_id, record=team
            )
            team_ids[team.external_id] = team_id
            conn.execute(
                """INSERT INTO football.season_teams(
                        season_id,team_id,default_venue_id,first_seen_at,last_seen_at,last_source_fetch_id
                    ) VALUES(%s,%s,%s,%s,%s,%s)
                    ON CONFLICT(season_id,team_id) DO NOTHING""",
                (
                    season_id,
                    team_id,
                    venue_id,
                    by_endpoint["/teams"].response_received_at,
                    by_endpoint["/teams"].response_received_at,
                    fetch_ids["/teams"],
                ),
            )

        context = SeasonContext(provider_id, league_id, season_id, team_ids, scope.season_scope)
        fixture_response = by_endpoint["/fixtures"]
        fixture_fetch = StoredFetch(
            fetch_id=fetch_ids["/fixtures"],
            response=fixture_response.response,
            request_started_at=fixture_response.request_started_at,
            response_received_at=fixture_response.response_received_at,
            normalized_at=None,
            reused=False,
        )
        statuses = _validate_provider_statuses(
            conn,
            context=context,
            fetch=fixture_fetch,
            records=validated.fixtures,
        )
        if statuses != validated.statuses:
            raise SeasonBootstrapError("provider status contract changed after base validation")
        normalize_fixture_season(
            conn,
            context=context,
            fetch=fixture_fetch,
            records=validated.fixtures,
            status_observations=statuses,
        )
        _normalize_standings(
            conn,
            provider_id=provider_id,
            season_id=season_id,
            fetch_id=fetch_ids["/standings"],
            captured_at=by_endpoint["/standings"].response_received_at,
            payload=validated.standings_payload,
        )
        conn.execute(
            """UPDATE source.provider_fetches
               SET normalized_at=coalesce(normalized_at,clock_timestamp())
               WHERE id = ANY(%s)""",
            (list(fetch_ids.values()),),
        )
        return context
