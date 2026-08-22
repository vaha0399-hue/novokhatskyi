"""Deterministic six-request API-Football canary importer.

This is deliberately not a scheduler or production backfill. It imports one
known historical fixture and its smallest useful league context, then replays
the same persisted payloads to prove normalized-row idempotency.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

import psycopg
from psycopg import Connection, sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app.api_football import APIFootballClient, APIFootballResponse

CANARY_KEY = "pl-2024-fixture-1208021-v1"
PROVIDER_CODE = "api-football"
PROVIDER_NAME = "API-Football"
MAPPING_VERSION = "api-football-v1"
RAW_RETENTION_DAYS = 30
MAX_API_ATTEMPTS = 6


@dataclass(frozen=True)
class RequestSpec:
    name: str
    endpoint: str
    params: dict[str, int]
    purpose: str


CANARY_REQUESTS = (
    RequestSpec("fixture", "/fixtures", {"id": 1208021}, "bootstrap"),
    RequestSpec("teams", "/teams", {"league": 39, "season": 2024}, "bootstrap"),
    RequestSpec("standings", "/standings", {"league": 39, "season": 2024}, "bootstrap"),
    RequestSpec("fixture_statistics", "/fixtures/statistics", {"fixture": 1208021}, "bootstrap"),
    RequestSpec("injuries", "/injuries", {"fixture": 1208021}, "research"),
    RequestSpec("lineups", "/fixtures/lineups", {"fixture": 1208021}, "research"),
)


@dataclass(frozen=True)
class CollectedResponse:
    spec: RequestSpec
    response: APIFootballResponse
    request_started_at: datetime
    response_received_at: datetime
    fetch_id: int | None = None
    reused: bool = False


TOUCHED_TABLES = (
    "source.providers",
    "source.league_provider_refs",
    "source.season_provider_refs",
    "source.team_provider_refs",
    "source.venue_provider_refs",
    "source.fixture_provider_refs",
    "source.player_provider_refs",
    "source.coach_provider_refs",
    "source.provider_fetches",
    "source.provider_raw_payloads",
    "football.leagues",
    "football.seasons",
    "football.teams",
    "football.venues",
    "football.players",
    "football.coaches",
    "football.season_teams",
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
)


STATISTIC_COLUMNS = {
    "Shots on Goal": "shots_on_goal",
    "Shots off Goal": "shots_off_goal",
    "Total Shots": "total_shots",
    "Blocked Shots": "blocked_shots",
    "Shots insidebox": "shots_inside_box",
    "Shots outsidebox": "shots_outside_box",
    "Fouls": "fouls",
    "Corner Kicks": "corner_kicks",
    "Offsides": "offsides",
    "Yellow Cards": "yellow_cards",
    "Red Cards": "red_cards",
    "Goalkeeper Saves": "goalkeeper_saves",
    "Total passes": "total_passes",
    "Passes accurate": "passes_accurate",
    "Ball Possession": "possession_pct",
    "Passes %": "pass_accuracy_pct",
    "expected_goals": "expected_goals",
    "goals_prevented": "goals_prevented",
}

INTEGER_STATISTICS = {
    "shots_on_goal",
    "shots_off_goal",
    "total_shots",
    "blocked_shots",
    "shots_inside_box",
    "shots_outside_box",
    "fouls",
    "corner_kicks",
    "offsides",
    "yellow_cards",
    "red_cards",
    "goalkeeper_saves",
    "total_passes",
    "passes_accurate",
}


def canonical_params_bytes(params: dict[str, int]) -> bytes:
    return json.dumps(params, sort_keys=True, separators=(",", ":")).encode()


def request_params_sha256(params: dict[str, int]) -> bytes:
    return hashlib.sha256(canonical_params_bytes(params)).digest()


def parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("provider datetime must include a timezone")
    return parsed


def parse_integer(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("boolean is not an integer statistic")
    if isinstance(value, int):
        result = value
    elif isinstance(value, str) and value.strip().lstrip("+").isdigit():
        result = int(value)
    else:
        raise ValueError(f"unsupported integer statistic type: {type(value).__name__}")
    if result < 0:
        raise ValueError("negative statistic")
    return result


def parse_decimal(value: Any, *, percentage: bool = False) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("boolean is not a decimal statistic")
    raw = str(value).strip()
    if raw.endswith("%"):
        raw = raw[:-1].strip()
    elif percentage and isinstance(value, str) and not raw:
        raise ValueError("empty percentage")
    try:
        result = Decimal(raw)
    except InvalidOperation as error:
        raise ValueError(f"invalid decimal statistic: {value!r}") from error
    if result < 0:
        raise ValueError("negative statistic")
    if percentage and result > 100:
        raise ValueError("percentage exceeds 100")
    return result


def map_fixture_statistics(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    mapped: list[dict[str, Any]] = []
    for item in items:
        team = item.get("team") or {}
        team_id = team.get("id")
        if not isinstance(team_id, int):
            raise ValueError("fixture statistics require an integer team id")

        values: dict[str, Any] = {column: None for column in STATISTIC_COLUMNS.values()}
        extra: dict[str, Any] = {}
        for statistic in item.get("statistics") or []:
            label = statistic.get("type")
            if not isinstance(label, str):
                raise ValueError("statistic label must be a string")
            value = statistic.get("value")
            column = STATISTIC_COLUMNS.get(label)
            if column is None:
                extra[label] = value
            elif column in INTEGER_STATISTICS:
                values[column] = parse_integer(value)
            else:
                values[column] = parse_decimal(
                    value,
                    percentage=column in {"possession_pct", "pass_accuracy_pct"},
                )
        mapped.append({"external_team_id": team_id, **values, "extra_metrics": extra})
    return mapped


def validate_wrapper(spec: RequestSpec, response: APIFootballResponse) -> None:
    payload = response.data
    expected_parameters = {key: str(value) for key, value in spec.params.items()}
    if payload.get("parameters") != expected_parameters:
        raise ValueError(f"provider parameters mismatch for {spec.endpoint}")
    if payload.get("errors") not in ([], {}, None):
        raise ValueError(f"provider returned errors for {spec.endpoint}")
    if not isinstance(payload.get("results"), int) or payload["results"] < 0:
        raise ValueError(f"invalid results count for {spec.endpoint}")
    paging = payload.get("paging")
    if not isinstance(paging, dict) or not isinstance(paging.get("current"), int) or not isinstance(paging.get("total"), int):
        raise ValueError(f"invalid paging for {spec.endpoint}")
    if spec.name == "fixture" and payload.get("results") != 1:
        raise ValueError("canary fixture request must return exactly one fixture")
    if spec.name == "fixture_statistics" and payload.get("results") != 2:
        raise ValueError("canary statistics must return both fixture teams")
    if spec.name == "lineups" and payload.get("results") not in (0, 1, 2):
        raise ValueError("unexpected lineup team count")


def _entity_config(kind: str) -> tuple[str, str, str, tuple[str, ...]]:
    configs = {
        "league": ("football", "leagues", "league_id", ("name", "country_name", "logo_url", "flag_url")),
        "team": ("football", "teams", "team_id", ("name", "code", "country_name", "founded_year", "is_national", "logo_url")),
        "venue": ("football", "venues", "venue_id", ("name", "address", "city", "capacity", "surface", "image_url")),
        "player": ("football", "players", "player_id", ("display_name", "photo_url")),
        "coach": ("football", "coaches", "coach_id", ("display_name", "photo_url")),
    }
    return configs[kind]


def resolve_entity(
    conn: Connection[Any],
    *,
    kind: str,
    provider_id: int,
    external_id: int,
    values: tuple[Any, ...],
    seen_at: datetime,
) -> int:
    schema_name, table_name, internal_column, columns = _entity_config(kind)
    ref_table = f"{kind}_provider_refs"
    external = str(external_id)
    row = conn.execute(
        sql.SQL("SELECT {} FROM source.{} WHERE provider_id = %s AND external_id = %s FOR UPDATE").format(
            sql.Identifier(internal_column), sql.Identifier(ref_table)
        ),
        (provider_id, external),
    ).fetchone()

    if row is None:
        identifiers = sql.SQL(", ").join(map(sql.Identifier, columns))
        placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in columns)
        entity_id = conn.execute(
            sql.SQL("INSERT INTO {}.{} ({}) VALUES ({}) RETURNING id").format(
                sql.Identifier(schema_name), sql.Identifier(table_name), identifiers, placeholders
            ),
            values,
        ).fetchone()[0]
        conn.execute(
            sql.SQL(
                "INSERT INTO source.{} (provider_id, external_id, {}, first_seen_at, last_seen_at) "
                "VALUES (%s, %s, %s, %s, %s)"
            ).format(sql.Identifier(ref_table), sql.Identifier(internal_column)),
            (provider_id, external, entity_id, seen_at, seen_at),
        )
    else:
        entity_id = row[0]
        assignments = sql.SQL(", ").join(
            sql.SQL("{} = {}").format(sql.Identifier(column), sql.Placeholder()) for column in columns
        )
        differences = sql.SQL(" OR ").join(
            sql.SQL("{} IS DISTINCT FROM {}").format(sql.Identifier(column), sql.Placeholder())
            for column in columns
        )
        conn.execute(
            sql.SQL("UPDATE {}.{} SET {} WHERE id = %s AND ({})").format(
                sql.Identifier(schema_name), sql.Identifier(table_name), assignments, differences
            ),
            (*values, entity_id, *values),
        )
        conn.execute(
            sql.SQL("UPDATE source.{} SET last_seen_at = greatest(last_seen_at, %s) WHERE provider_id = %s AND external_id = %s").format(
                sql.Identifier(ref_table)
            ),
            (seen_at, provider_id, external),
        )
    return entity_id


def resolve_season(
    conn: Connection[Any],
    *,
    provider_id: int,
    league_external_id: int,
    league_id: int,
    start_year: int,
    seen_at: datetime,
) -> int:
    row = conn.execute(
        """
        SELECT season_id FROM source.season_provider_refs
        WHERE provider_id = %s AND league_external_id = %s AND external_season = %s
        FOR UPDATE
        """,
        (provider_id, str(league_external_id), start_year),
    ).fetchone()
    if row is not None:
        season_id = row[0]
        actual_league = conn.execute("SELECT league_id FROM football.seasons WHERE id = %s", (season_id,)).fetchone()[0]
        if actual_league != league_id:
            raise ValueError("season provider mapping points to a different league")
        conn.execute(
            """
            UPDATE source.season_provider_refs SET last_seen_at = greatest(last_seen_at, %s)
            WHERE provider_id = %s AND league_external_id = %s AND external_season = %s
            """,
            (seen_at, provider_id, str(league_external_id), start_year),
        )
        return season_id

    row = conn.execute(
        "SELECT id FROM football.seasons WHERE league_id = %s AND start_year = %s FOR UPDATE",
        (league_id, start_year),
    ).fetchone()
    season_id = row[0] if row else conn.execute(
        "INSERT INTO football.seasons (league_id, start_year, label) VALUES (%s, %s, %s) RETURNING id",
        (league_id, start_year, str(start_year)),
    ).fetchone()[0]
    conn.execute(
        """
        INSERT INTO source.season_provider_refs
          (provider_id, league_external_id, external_season, season_id, first_seen_at, last_seen_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (provider_id, str(league_external_id), start_year, season_id, seen_at, seen_at),
    )
    return season_id


def _provider_id(conn: Connection[Any], *, create: bool) -> int | None:
    row = conn.execute("SELECT id FROM source.providers WHERE code = %s", (PROVIDER_CODE,)).fetchone()
    if row is not None:
        return row[0]
    if not create:
        return None
    return conn.execute(
        "INSERT INTO source.providers (code, name) VALUES (%s, %s) RETURNING id",
        (PROVIDER_CODE, PROVIDER_NAME),
    ).fetchone()[0]


def load_reusable_responses(conn: Connection[Any]) -> dict[str, CollectedResponse]:
    provider_id = _provider_id(conn, create=False)
    if provider_id is None:
        return {}
    reusable: dict[str, CollectedResponse] = {}
    for spec in CANARY_REQUESTS:
        row = conn.execute(
            """
            SELECT f.id, f.request_started_at, f.response_received_at, f.http_status,
                   f.content_sha256, r.inline_body
            FROM source.provider_fetches f
            JOIN source.provider_raw_payloads r ON r.fetch_id = f.id
            WHERE f.provider_id = %s AND f.endpoint = %s
              AND f.request_params_sha256 = %s AND f.purpose = %s
              AND f.outcome = 'success' AND f.normalized_at IS NOT NULL
              AND r.purged_at IS NULL AND r.inline_body IS NOT NULL
            ORDER BY f.response_received_at DESC LIMIT 1
            """,
            (provider_id, spec.endpoint, request_params_sha256(spec.params), spec.purpose),
        ).fetchone()
        if row is None:
            continue
        fetch_id, started_at, received_at, status_code, expected_hash, raw_body = row
        raw_bytes = bytes(raw_body)
        if hashlib.sha256(raw_bytes).digest() != bytes(expected_hash):
            raise ValueError(f"stored raw payload hash mismatch for {spec.endpoint}")
        payload = json.loads(raw_bytes)
        response = APIFootballResponse(data=payload, raw_body=raw_bytes, status_code=status_code, headers={})
        validate_wrapper(spec, response)
        reusable[spec.name] = CollectedResponse(
            spec=spec,
            response=response,
            request_started_at=started_at,
            response_received_at=received_at,
            fetch_id=fetch_id,
            reused=True,
        )
    return reusable


async def collect_responses(reusable: dict[str, CollectedResponse]) -> tuple[list[CollectedResponse], int]:
    client = APIFootballClient.from_environment()
    collected: list[CollectedResponse] = []
    attempts = 0
    for spec in CANARY_REQUESTS:
        if spec.name in reusable:
            if client.response_contains_api_key(reusable[spec.name].response.raw_body):
                raise RuntimeError("API key detected in stored provider response")
            collected.append(reusable[spec.name])
            continue
        if attempts >= MAX_API_ATTEMPTS:
            raise RuntimeError("canary API attempt limit reached")
        attempts += 1
        started_at = datetime.now(UTC)
        response = await client.get(spec.endpoint, params=spec.params)
        received_at = datetime.now(UTC)
        if client.response_contains_api_key(response.raw_body):
            raise RuntimeError("API key detected in provider response; refusing persistence")
        validate_wrapper(spec, response)
        collected.append(
            CollectedResponse(
                spec=spec,
                response=response,
                request_started_at=started_at,
                response_received_at=received_at,
            )
        )
    return collected, attempts


def persist_fetch(
    conn: Connection[Any],
    *,
    provider_id: int,
    collected: CollectedResponse,
    season_id: int | None = None,
    fixture_id: int | None = None,
    team_id: int | None = None,
) -> int:
    if collected.fetch_id is not None:
        return collected.fetch_id
    payload = collected.response.data
    paging = payload["paging"]
    content_hash = hashlib.sha256(collected.response.raw_body).digest()
    fetch_id = conn.execute(
        """
        INSERT INTO source.provider_fetches (
          provider_id, endpoint, request_params, request_params_sha256, purpose,
          request_started_at, response_received_at, http_status, outcome,
          provider_results, paging_current, paging_total, content_sha256,
          subject_fixture_id, subject_season_id, subject_team_id
        ) VALUES (
          %s, %s, %s, %s, %s, %s, %s, %s, 'success', %s, %s, %s, %s, %s, %s, %s
        ) RETURNING id
        """,
        (
            provider_id,
            collected.spec.endpoint,
            Jsonb(collected.spec.params),
            request_params_sha256(collected.spec.params),
            collected.spec.purpose,
            collected.request_started_at,
            collected.response_received_at,
            collected.response.status_code,
            payload["results"],
            paging["current"],
            paging["total"],
            content_hash,
            fixture_id,
            season_id,
            team_id,
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
            datetime.now(UTC) + timedelta(days=RAW_RETENTION_DAYS),
        ),
    )
    return fetch_id


def _response_by_name(collected: Iterable[CollectedResponse]) -> dict[str, CollectedResponse]:
    result = {item.spec.name: item for item in collected}
    if set(result) != {spec.name for spec in CANARY_REQUESTS}:
        raise ValueError("canary response set is incomplete")
    return result


def _resolve_team_id(conn: Connection[Any], provider_id: int, external_id: int) -> int:
    row = conn.execute(
        "SELECT team_id FROM source.team_provider_refs WHERE provider_id = %s AND external_id = %s",
        (provider_id, str(external_id)),
    ).fetchone()
    if row is None:
        raise ValueError(f"team provider mapping missing for {external_id}")
    return row[0]


def _normalize_standings(
    conn: Connection[Any],
    *,
    provider_id: int,
    season_id: int,
    fetch_id: int,
    captured_at: datetime,
    payload: dict[str, Any],
) -> None:
    existing = conn.execute(
        "SELECT id FROM football.standings_snapshots WHERE season_id = %s AND source_fetch_id = %s",
        (season_id, fetch_id),
    ).fetchone()
    if existing is not None:
        return
    response = payload.get("response") or []
    if len(response) != 1:
        raise ValueError("standings canary expects one league response")
    groups = response[0]["league"].get("standings") or []
    snapshot_id = conn.execute(
        """
        INSERT INTO football.standings_snapshots (season_id, captured_at, source_fetch_id, group_count)
        VALUES (%s, %s, %s, %s) RETURNING id
        """,
        (season_id, captured_at, fetch_id, len(groups)),
    ).fetchone()[0]
    for group_index, rows in enumerate(groups):
        group_name = rows[0].get("group") if rows else None
        conn.execute(
            """
            INSERT INTO football.standings_snapshot_groups (snapshot_id, group_index, group_name, row_count)
            VALUES (%s, %s, %s, %s)
            """,
            (snapshot_id, group_index, group_name, len(rows)),
        )
        for row in rows:
            team_id = _resolve_team_id(conn, provider_id, row["team"]["id"])
            all_record, home, away = row["all"], row["home"], row["away"]
            conn.execute(
                """
                INSERT INTO football.standings_snapshot_rows (
                  snapshot_id, group_index, team_id, rank, points, goals_diff, form, status, description,
                  played, wins, draws, losses, goals_for, goals_against,
                  home_played, home_wins, home_draws, home_losses, home_goals_for, home_goals_against,
                  away_played, away_wins, away_draws, away_losses, away_goals_for, away_goals_against,
                  provider_updated_at
                ) VALUES (
                  %s, %s, %s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    snapshot_id,
                    group_index,
                    team_id,
                    row["rank"],
                    row["points"],
                    row["goalsDiff"],
                    row.get("form"),
                    row.get("status"),
                    row.get("description"),
                    all_record["played"], all_record["win"], all_record["draw"], all_record["lose"],
                    all_record["goals"]["for"], all_record["goals"]["against"],
                    home["played"], home["win"], home["draw"], home["lose"],
                    home["goals"]["for"], home["goals"]["against"],
                    away["played"], away["win"], away["draw"], away["lose"],
                    away["goals"]["for"], away["goals"]["against"],
                    parse_datetime(row["update"]) if row.get("update") else None,
                ),
            )


def _normalize_statistics(
    conn: Connection[Any],
    *,
    provider_id: int,
    fixture_id: int,
    fetch_id: int,
    available_at: datetime,
    payload: dict[str, Any],
) -> None:
    columns = list(STATISTIC_COLUMNS.values())
    for mapped in map_fixture_statistics(payload.get("response") or []):
        team_id = _resolve_team_id(conn, provider_id, mapped["external_team_id"])
        existing = conn.execute(
            "SELECT 1 FROM football.fixture_team_statistics WHERE fixture_id = %s AND team_id = %s",
            (fixture_id, team_id),
        ).fetchone()
        if existing is not None:
            continue
        conn.execute(
            sql.SQL(
                "INSERT INTO football.fixture_team_statistics "
                "(fixture_id, team_id, {}, extra_metrics, mapping_version, observed_at, available_at, "
                "availability_basis, last_source_fetch_id, finalized_at) "
                "VALUES (%s, %s, {}, %s, %s, %s, %s, 'observed', %s, %s)"
            ).format(
                sql.SQL(", ").join(map(sql.Identifier, columns)),
                sql.SQL(", ").join(sql.Placeholder() for _ in columns),
            ),
            (
                fixture_id,
                team_id,
                *(mapped[column] for column in columns),
                Jsonb(mapped["extra_metrics"]),
                MAPPING_VERSION,
                available_at,
                available_at,
                fetch_id,
                available_at,
            ),
        )


def normalize_canary(
    conn: Connection[Any], collected: list[CollectedResponse]
) -> list[CollectedResponse]:
    items = _response_by_name(collected)
    with conn.transaction():
        conn.execute("SET LOCAL statement_timeout = '60s'")
        conn.execute("SET LOCAL lock_timeout = '10s'")
        conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (CANARY_KEY,))
        provider_id = _provider_id(conn, create=True)
        assert provider_id is not None

        fixture_item = items["fixture"]
        fixture_payload = fixture_item.response.data["response"][0]
        league_data = fixture_payload["league"]
        league_id = resolve_entity(
            conn,
            kind="league",
            provider_id=provider_id,
            external_id=league_data["id"],
            values=(league_data["name"], league_data.get("country"), league_data.get("logo"), league_data.get("flag")),
            seen_at=fixture_item.response_received_at,
        )
        season_id = resolve_season(
            conn,
            provider_id=provider_id,
            league_external_id=league_data["id"],
            league_id=league_id,
            start_year=league_data["season"],
            seen_at=fixture_item.response_received_at,
        )

        teams_item = items["teams"]
        team_mappings: dict[int, tuple[int, int | None]] = {}
        for entry in teams_item.response.data.get("response") or []:
            team = entry["team"]
            venue = entry.get("venue") or {}
            team_id = resolve_entity(
                conn,
                kind="team",
                provider_id=provider_id,
                external_id=team["id"],
                values=(team["name"], team.get("code"), team.get("country"), team.get("founded"), team.get("national"), team.get("logo")),
                seen_at=teams_item.response_received_at,
            )
            venue_id = None
            if isinstance(venue.get("id"), int):
                venue_id = resolve_entity(
                    conn,
                    kind="venue",
                    provider_id=provider_id,
                    external_id=venue["id"],
                    values=(venue.get("name") or "Unknown venue", venue.get("address"), venue.get("city"), venue.get("capacity"), venue.get("surface"), venue.get("image")),
                    seen_at=teams_item.response_received_at,
                )
            team_mappings[team["id"]] = (team_id, venue_id)

        required_team_ids = {fixture_payload["teams"]["home"]["id"], fixture_payload["teams"]["away"]["id"]}
        if not required_team_ids.issubset(team_mappings):
            raise ValueError("fixture participants are missing from the league teams response")

        fetch_ids: dict[str, int] = {}
        for name in ("fixture", "teams", "standings"):
            fetch_ids[name] = persist_fetch(
                conn,
                provider_id=provider_id,
                collected=items[name],
                season_id=season_id,
            )

        for external_team_id, (team_id, venue_id) in team_mappings.items():
            conn.execute(
                """
                INSERT INTO football.season_teams (
                  season_id, team_id, default_venue_id, first_seen_at, last_seen_at, last_source_fetch_id
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (season_id, team_id) DO UPDATE SET
                  default_venue_id = EXCLUDED.default_venue_id,
                  first_seen_at = least(football.season_teams.first_seen_at, EXCLUDED.first_seen_at),
                  last_seen_at = greatest(football.season_teams.last_seen_at, EXCLUDED.last_seen_at),
                  last_source_fetch_id = EXCLUDED.last_source_fetch_id
                """,
                (season_id, team_id, venue_id, teams_item.response_received_at, teams_item.response_received_at, fetch_ids["teams"]),
            )

        fixture_external_id = fixture_payload["fixture"]["id"]
        fixture_ref = conn.execute(
            """
            SELECT fixture_id FROM source.fixture_provider_refs
            WHERE provider_id = %s AND external_id = %s FOR UPDATE
            """,
            (provider_id, str(fixture_external_id)),
        ).fetchone()
        home_team_id = team_mappings[fixture_payload["teams"]["home"]["id"]][0]
        away_team_id = team_mappings[fixture_payload["teams"]["away"]["id"]][0]
        venue_external_id = (fixture_payload["fixture"].get("venue") or {}).get("id")
        venue_id = None
        if isinstance(venue_external_id, int):
            row = conn.execute(
                "SELECT venue_id FROM source.venue_provider_refs WHERE provider_id = %s AND external_id = %s",
                (provider_id, str(venue_external_id)),
            ).fetchone()
            venue_id = row[0] if row else None
        kickoff_at = parse_datetime(fixture_payload["fixture"]["date"])
        if fixture_ref is None:
            fixture_id = conn.execute(
                """
                INSERT INTO football.fixtures (
                  season_id, home_team_id, away_team_id, venue_id, round_label, kickoff_at,
                  source_timezone, referee_name, lifecycle_state, first_seen_at, last_seen_at,
                  last_source_fetch_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'scheduled', %s, %s, %s)
                RETURNING id
                """,
                (
                    season_id, home_team_id, away_team_id, venue_id, league_data.get("round"), kickoff_at,
                    fixture_payload["fixture"].get("timezone"), fixture_payload["fixture"].get("referee"),
                    fixture_item.response_received_at, fixture_item.response_received_at, fetch_ids["fixture"],
                ),
            ).fetchone()[0]
            conn.execute(
                """
                INSERT INTO source.fixture_provider_refs
                  (provider_id, external_id, fixture_id, first_seen_at, last_seen_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (provider_id, str(fixture_external_id), fixture_id, fixture_item.response_received_at, fixture_item.response_received_at),
            )
        else:
            fixture_id = fixture_ref[0]
            actual = conn.execute(
                "SELECT season_id, home_team_id, away_team_id, kickoff_at FROM football.fixtures WHERE id = %s",
                (fixture_id,),
            ).fetchone()
            if actual != (season_id, home_team_id, away_team_id, kickoff_at):
                raise ValueError("existing fixture identity differs from canary payload")

        for name in ("fixture_statistics", "injuries", "lineups"):
            fetch_ids[name] = persist_fetch(
                conn,
                provider_id=provider_id,
                collected=items[name],
                season_id=season_id,
                fixture_id=fixture_id,
            )

        status = fixture_payload["fixture"]["status"]["short"]
        if status != "FT":
            raise ValueError(f"canary fixture must be FT, got {status}")
        score = fixture_payload["score"]
        goals = fixture_payload["goals"]
        existing_fixture = conn.execute(
            "SELECT lifecycle_state::text, result_finalized_at FROM football.fixtures WHERE id = %s",
            (fixture_id,),
        ).fetchone()
        if existing_fixture[1] is None:
            conn.execute(
                """
                UPDATE football.fixtures SET
                  lifecycle_state = 'completed', home_goals = %s, away_goals = %s,
                  home_halftime_goals = %s, away_halftime_goals = %s,
                  home_fulltime_goals = %s, away_fulltime_goals = %s,
                  home_extratime_goals = %s, away_extratime_goals = %s,
                  home_penalty_goals = %s, away_penalty_goals = %s,
                  terminal_status_observed_at = %s, result_available_at = %s,
                  availability_basis = 'observed', result_finalized_at = %s,
                  last_seen_at = %s, last_source_fetch_id = %s
                WHERE id = %s
                """,
                (
                    goals.get("home"), goals.get("away"),
                    score["halftime"].get("home"), score["halftime"].get("away"),
                    score["fulltime"].get("home"), score["fulltime"].get("away"),
                    score["extratime"].get("home"), score["extratime"].get("away"),
                    score["penalty"].get("home"), score["penalty"].get("away"),
                    fixture_item.response_received_at, fixture_item.response_received_at,
                    fixture_item.response_received_at, fixture_item.response_received_at,
                    fetch_ids["fixture"], fixture_id,
                ),
            )
        elif existing_fixture[0] != "completed":
            raise ValueError("finalized canary fixture is not completed")

        _normalize_standings(
            conn,
            provider_id=provider_id,
            season_id=season_id,
            fetch_id=fetch_ids["standings"],
            captured_at=items["standings"].response_received_at,
            payload=items["standings"].response.data,
        )
        _normalize_statistics(
            conn,
            provider_id=provider_id,
            fixture_id=fixture_id,
            fetch_id=fetch_ids["fixture_statistics"],
            available_at=items["fixture_statistics"].response_received_at,
            payload=items["fixture_statistics"].response.data,
        )

        for entry in items["injuries"].response.data.get("response") or []:
            if entry["fixture"]["id"] != fixture_external_id:
                raise ValueError("injury response contains a different fixture")
            player = entry["player"]
            resolve_entity(
                conn,
                kind="player",
                provider_id=provider_id,
                external_id=player["id"],
                values=(player["name"], player.get("photo")),
                seen_at=items["injuries"].response_received_at,
            )

        for entry in items["lineups"].response.data.get("response") or []:
            external_team_id = entry["team"]["id"]
            if external_team_id not in required_team_ids:
                raise ValueError("lineup response contains a non-participant team")
            coach = entry.get("coach") or {}
            if isinstance(coach.get("id"), int):
                resolve_entity(
                    conn,
                    kind="coach",
                    provider_id=provider_id,
                    external_id=coach["id"],
                    values=(coach.get("name") or "Unknown coach", coach.get("photo")),
                    seen_at=items["lineups"].response_received_at,
                )
            for group in ("startXI", "substitutes"):
                for wrapper in entry.get(group) or []:
                    player = wrapper["player"]
                    resolve_entity(
                        conn,
                        kind="player",
                        provider_id=provider_id,
                        external_id=player["id"],
                        values=(player["name"], player.get("photo")),
                        seen_at=items["lineups"].response_received_at,
                    )

        for name, fetch_id in fetch_ids.items():
            conn.execute(
                "UPDATE source.provider_fetches SET normalized_at = coalesce(normalized_at, clock_timestamp()) WHERE id = %s",
                (fetch_id,),
            )
            items[name] = replace(items[name], fetch_id=fetch_id)

    return [items[spec.name] for spec in CANARY_REQUESTS]


def table_counts(conn: Connection[Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in TOUCHED_TABLES:
        schema_name, table_name = table.split(".")
        counts[table] = conn.execute(
            sql.SQL("SELECT count(*) FROM {}.{}").format(sql.Identifier(schema_name), sql.Identifier(table_name))
        ).fetchone()[0]
    return counts


def verify_remote(conn: Connection[Any], fetch_ids: list[int]) -> dict[str, Any]:
    provider_id = _provider_id(conn, create=False)
    if provider_id is None:
        raise AssertionError("provider row missing")

    duplicate_refs = 0
    for kind in ("league", "team", "venue", "fixture", "player", "coach"):
        _, _, internal_column, _ = _entity_config(kind) if kind != "fixture" else ("football", "fixtures", "fixture_id", ())
        table_name = f"{kind}_provider_refs"
        duplicate_refs += conn.execute(
            sql.SQL(
                "SELECT count(*) FROM (SELECT provider_id, {}, count(*) FROM source.{} "
                "GROUP BY provider_id, {} HAVING count(*) > 1) duplicates"
            ).format(sql.Identifier(internal_column), sql.Identifier(table_name), sql.Identifier(internal_column))
        ).fetchone()[0]
    duplicate_refs += conn.execute(
        """
        SELECT count(*) FROM (
          SELECT provider_id, season_id, count(*) FROM source.season_provider_refs
          GROUP BY provider_id, season_id HAVING count(*) > 1
        ) duplicates
        """
    ).fetchone()[0]

    fixture = conn.execute(
        """
        SELECT f.id, hs.external_id, asr.external_id, lr.external_id, s.start_year,
               f.lifecycle_state::text, f.home_goals, f.away_goals,
               f.availability_basis::text, f.last_source_fetch_id
        FROM source.fixture_provider_refs fr
        JOIN football.fixtures f ON f.id = fr.fixture_id
        JOIN source.team_provider_refs hs ON hs.provider_id = fr.provider_id AND hs.team_id = f.home_team_id
        JOIN source.team_provider_refs asr ON asr.provider_id = fr.provider_id AND asr.team_id = f.away_team_id
        JOIN football.seasons s ON s.id = f.season_id
        JOIN source.league_provider_refs lr ON lr.provider_id = fr.provider_id AND lr.league_id = s.league_id
        WHERE fr.provider_id = %s AND fr.external_id = '1208021'
        """,
        (provider_id,),
    ).fetchone()
    if fixture is None or fixture[1:5] != ("33", "36", "39", 2024):
        raise AssertionError(f"fixture home/away/season mapping mismatch: {fixture}")
    if fixture[5:9] != ("completed", 1, 0, "observed"):
        raise AssertionError(f"fixture terminal mapping mismatch: {fixture[5:9]}")
    fixture_id = fixture[0]

    nonparticipant_statistics = conn.execute(
        """
        SELECT count(*) FROM football.fixture_team_statistics s
        JOIN football.fixtures f ON f.id = s.fixture_id
        WHERE s.fixture_id = %s AND s.team_id NOT IN (f.home_team_id, f.away_team_id)
        """,
        (fixture_id,),
    ).fetchone()[0]
    if nonparticipant_statistics:
        raise AssertionError("fixture statistics contain a non-participant")

    statistics = conn.execute(
        """
        SELECT tr.external_id, s.possession_pct, s.pass_accuracy_pct, s.expected_goals,
               s.goals_prevented, s.red_cards
        FROM football.fixture_team_statistics s
        JOIN source.team_provider_refs tr ON tr.provider_id = %s AND tr.team_id = s.team_id
        WHERE s.fixture_id = %s ORDER BY tr.external_id
        """,
        (provider_id, fixture_id),
    ).fetchall()
    expected_statistics = [
        ("33", Decimal("55.00"), Decimal("85.00"), Decimal("2.430"), Decimal("1.070"), None),
        ("36", Decimal("45.00"), Decimal("80.00"), Decimal("0.440"), Decimal("1.070"), None),
    ]
    if statistics != expected_statistics:
        raise AssertionError(f"typed statistic mapping mismatch: {statistics}")

    raw_rows = conn.execute(
        """
        SELECT f.endpoint, f.content_sha256, r.inline_body, r.byte_count,
               f.normalized_at IS NOT NULL,
               source.jsonb_contains_forbidden_metadata_key(f.request_params),
               r.retention_class::text, r.expires_at IS NOT NULL, r.purged_at IS NULL,
               f.subject_fixture_id, f.subject_season_id
        FROM source.provider_fetches f
        JOIN source.provider_raw_payloads r ON r.fetch_id = f.id
        WHERE f.id = ANY(%s)
        """,
        (fetch_ids,),
    ).fetchall()
    if len(raw_rows) != 6:
        raise AssertionError(f"expected 6 raw payloads, found {len(raw_rows)}")
    for row in raw_rows:
        endpoint, content_hash, body, byte_count, normalized, forbidden, retention, expires, unpurged, subject_fixture, subject_season = row
        body_bytes = bytes(body)
        if bytes(content_hash) != hashlib.sha256(body_bytes).digest() or byte_count != len(body_bytes):
            raise AssertionError(f"raw content integrity mismatch for {endpoint}")
        if not normalized or forbidden or retention != "standard" or not expires or not unpurged:
            raise AssertionError(f"raw provenance metadata mismatch for {endpoint}")
        if endpoint in {"/fixtures/statistics", "/injuries", "/fixtures/lineups"} and subject_fixture != fixture_id:
            raise AssertionError(f"fixture subject missing for {endpoint}")
        if subject_season is None:
            raise AssertionError(f"season subject missing for {endpoint}")

    orphan_count = conn.execute(
        """
        SELECT
          (SELECT count(*) FROM source.league_provider_refs r LEFT JOIN football.leagues x ON x.id = r.league_id WHERE x.id IS NULL) +
          (SELECT count(*) FROM source.season_provider_refs r LEFT JOIN football.seasons x ON x.id = r.season_id WHERE x.id IS NULL) +
          (SELECT count(*) FROM source.team_provider_refs r LEFT JOIN football.teams x ON x.id = r.team_id WHERE x.id IS NULL) +
          (SELECT count(*) FROM source.venue_provider_refs r LEFT JOIN football.venues x ON x.id = r.venue_id WHERE x.id IS NULL) +
          (SELECT count(*) FROM source.fixture_provider_refs r LEFT JOIN football.fixtures x ON x.id = r.fixture_id WHERE x.id IS NULL) +
          (SELECT count(*) FROM source.player_provider_refs r LEFT JOIN football.players x ON x.id = r.player_id WHERE x.id IS NULL) +
          (SELECT count(*) FROM source.coach_provider_refs r LEFT JOIN football.coaches x ON x.id = r.coach_id WHERE x.id IS NULL) +
          (SELECT count(*) FROM football.season_teams st LEFT JOIN football.seasons s ON s.id = st.season_id LEFT JOIN football.teams t ON t.id = st.team_id WHERE s.id IS NULL OR t.id IS NULL) +
          (SELECT count(*) FROM football.standings_snapshot_rows r JOIN football.standings_snapshots s ON s.id = r.snapshot_id LEFT JOIN football.season_teams st ON st.season_id = s.season_id AND st.team_id = r.team_id WHERE st.team_id IS NULL)
        """
    ).fetchone()[0]
    if orphan_count != 0:
        raise AssertionError(f"orphan relationship count is {orphan_count}")

    historical_snapshot_rows = conn.execute(
        """
        SELECT
          (SELECT count(*) FROM football.fixture_availability_snapshots WHERE fixture_id = %s) +
          (SELECT count(*) FROM football.fixture_lineup_snapshots WHERE fixture_id = %s)
        """,
        (fixture_id, fixture_id),
    ).fetchone()[0]
    if historical_snapshot_rows != 0:
        raise AssertionError("historical injuries/lineups were incorrectly stored as pre-match snapshots")

    direct_dml_grants = conn.execute(
        """
        SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        CROSS JOIN (VALUES ('anon'), ('authenticated')) roles(role_name)
        WHERE n.nspname IN ('source', 'football', 'ml', 'ops') AND c.relkind IN ('r', 'p')
          AND (has_table_privilege(role_name, c.oid, 'INSERT')
            OR has_table_privilege(role_name, c.oid, 'UPDATE')
            OR has_table_privilege(role_name, c.oid, 'DELETE')
            OR has_table_privilege(role_name, c.oid, 'TRUNCATE'))
        """
    ).fetchone()[0]
    if direct_dml_grants != 0:
        raise AssertionError("anon/authenticated direct DML grant detected")

    return {
        "duplicate_provider_mappings": duplicate_refs,
        "fixture_internal_id": fixture_id,
        "home_external_id": fixture[1],
        "away_external_id": fixture[2],
        "league_external_id": fixture[3],
        "season_start_year": fixture[4],
        "statistics_rows": len(statistics),
        "statistics_nonparticipants": nonparticipant_statistics,
        "raw_payloads_verified": len(raw_rows),
        "orphan_relationships": orphan_count,
        "historical_prematch_snapshots": historical_snapshot_rows,
        "anon_authenticated_dml_grants": direct_dml_grants,
    }


def _database_url() -> str:
    value = os.environ.get("SUPABASE_DB_URL")
    if not value:
        raise RuntimeError("SUPABASE_DB_URL is required")
    return value


def main() -> None:
    database_url = _database_url()
    with psycopg.connect(database_url, autocommit=True) as connection:
        connection.execute("SET statement_timeout = '30s'")
        baseline = table_counts(connection)
        reusable = load_reusable_responses(connection)

    collected, api_attempts = asyncio.run(collect_responses(reusable))
    if api_attempts > MAX_API_ATTEMPTS:
        raise RuntimeError("API request cap exceeded")

    with psycopg.connect(database_url) as connection:
        first_pass = normalize_canary(connection, collected)

    with psycopg.connect(database_url, autocommit=True) as connection:
        after_first = table_counts(connection)

    with psycopg.connect(database_url) as connection:
        second_pass = normalize_canary(connection, first_pass)

    with psycopg.connect(database_url, autocommit=True) as connection:
        after_second = table_counts(connection)
        if after_second != after_first:
            raise AssertionError("idempotency replay changed table counts")
        fetch_ids = [item.fetch_id for item in second_pass]
        if any(fetch_id is None for fetch_id in fetch_ids):
            raise AssertionError("missing fetch id after normalization")
        verification = verify_remote(connection, [int(fetch_id) for fetch_id in fetch_ids])

    report = {
        "canary_key": CANARY_KEY,
        "api_attempts": api_attempts,
        "reused_fetches": len(reusable),
        "fetch_ids": [item.fetch_id for item in second_pass],
        "row_counts_before": baseline,
        "row_counts_after_first": after_first,
        "row_counts_after_replay": after_second,
        "idempotent_row_counts": after_first == after_second,
        "verification": verification,
    }
    print(json.dumps(report, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
