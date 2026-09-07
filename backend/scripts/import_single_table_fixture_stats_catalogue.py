#!/usr/bin/env python3
"""One-off, restart-safe import for the classified single-table catalogue slice.

The script reads retained catalogue/standings bodies first.  It does not call
``/standings`` and it never deletes canonical data.  Missing teams and fixtures
are fetched only when neither canonical state nor retained raw can satisfy the
existing atomic active-season importer.  Completed fixture statistics use the
existing batch importer and therefore upsert an exact two-team pair.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
from psycopg import Connection

from app.api_football import APIFootballAPIError, APIFootballClient, APIFootballHTTPError, APIFootballResponse
from app.api_football.client import safe_rate_limit_headers
from app.importer.active_season import ActiveSeasonImportError, ActiveSeasonScope, import_active_base
from app.importer.current_season_statistics import (
    CurrentSeasonStatisticsError,
    CurrentSeasonStatisticsScope,
    load_completed_targets,
    run_current_season_statistics_backfill_async,
)
from app.importer.season_bootstrap import BaseRequest, CollectedBaseResponse, SeasonBootstrapError


PROVIDER_CODE = "api-football"
DEFAULT_CATALOGUE_RAW = Path(
    "/var/lib/football-analytics/catalogue-bootstrap/catalogue/"
    "aa5292f720afe6fa13f0a04a3398a5630d52b0d9886325e82b831bcc14244d7b/leagues.raw.json"
)
TERMINAL_STATUSES = frozenset({"FT", "AET", "PEN"})


class CatalogueImportError(RuntimeError):
    """A single scope cannot be safely imported in this one-off run."""


@dataclass(frozen=True)
class Competition:
    league_id: int
    name: str
    country: str | None
    provider_type: str
    season: int
    coverage: dict[str, Any]

    @classmethod
    def from_json(cls, value: object) -> "Competition":
        if not isinstance(value, Mapping):
            raise ValueError("competition must be an object")
        league_id, name, provider_type, season, coverage = (
            value.get("league_id"), value.get("name"), value.get("type"), value.get("season"), value.get("coverage")
        )
        if not isinstance(league_id, int) or league_id <= 0:
            raise ValueError("competition league_id must be positive")
        if not isinstance(name, str) or not name:
            raise ValueError(f"league {league_id}: invalid name")
        if not isinstance(provider_type, str) or not provider_type:
            raise ValueError(f"league {league_id}: invalid type")
        if not isinstance(season, int):
            raise ValueError(f"league {league_id}: invalid season")
        if not isinstance(coverage, dict):
            raise ValueError(f"league {league_id}: invalid coverage")
        country = value.get("country")
        if country is not None and not isinstance(country, str):
            raise ValueError(f"league {league_id}: invalid country")
        return cls(league_id, name, country, provider_type, season, coverage)

    @property
    def key(self) -> str:
        return f"{self.league_id}:{self.season}"

    @property
    def raw_stem(self) -> str:
        return f"league-{self.league_id}-season-{self.season}"


@dataclass(frozen=True)
class ScopeState:
    season_id: int | None
    provider_id: int
    teams_ready: bool
    fixtures_ready: bool
    standings_ready: bool
    completed_total: int
    statistics_complete: int

    @property
    def base_ready(self) -> bool:
        return self.teams_ready and self.fixtures_ready and self.standings_ready

    @property
    def complete(self) -> bool:
        return self.base_ready and self.completed_total == self.statistics_complete


@dataclass
class Counters:
    competitions_total: int = 0
    competitions_already_complete: int = 0
    competitions_updated: int = 0
    teams_skipped: int = 0
    teams_fetched: int = 0
    fixtures_skipped: int = 0
    fixtures_fetched: int = 0
    standings_skipped: int = 0
    standings_imported_from_raw: int = 0
    completed_fixtures_total: int = 0
    fixture_stats_already_complete: int = 0
    fixture_stats_fetched: int = 0
    fixture_stats_imported: int = 0
    provider_calls_used: int = 0
    errors: int = 0

    def output(self) -> dict[str, int]:
        return {
            "COMPETITIONS_TOTAL": self.competitions_total,
            "COMPETITIONS_ALREADY_COMPLETE": self.competitions_already_complete,
            "COMPETITIONS_UPDATED": self.competitions_updated,
            "TEAMS_SKIPPED": self.teams_skipped,
            "TEAMS_FETCHED": self.teams_fetched,
            "FIXTURES_SKIPPED": self.fixtures_skipped,
            "FIXTURES_FETCHED": self.fixtures_fetched,
            "STANDINGS_SKIPPED": self.standings_skipped,
            "STANDINGS_IMPORTED_FROM_RAW": self.standings_imported_from_raw,
            "COMPLETED_FIXTURES_TOTAL": self.completed_fixtures_total,
            "FIXTURE_STATS_ALREADY_COMPLETE": self.fixture_stats_already_complete,
            "FIXTURE_STATS_FETCHED": self.fixture_stats_fetched,
            "FIXTURE_STATS_IMPORTED": self.fixture_stats_imported,
            "PROVIDER_CALLS_USED": self.provider_calls_used,
            "ERRORS": self.errors,
        }


def _counters_from_checkpoint(value: object, *, expected_total: int) -> Counters:
    """Restore a previously persisted queue position without replaying scopes."""
    if not isinstance(value, Mapping):
        raise ValueError("checkpoint counters must be an object")
    mappings = {
        "COMPETITIONS_TOTAL": "competitions_total",
        "COMPETITIONS_ALREADY_COMPLETE": "competitions_already_complete",
        "COMPETITIONS_UPDATED": "competitions_updated",
        "TEAMS_SKIPPED": "teams_skipped",
        "TEAMS_FETCHED": "teams_fetched",
        "FIXTURES_SKIPPED": "fixtures_skipped",
        "FIXTURES_FETCHED": "fixtures_fetched",
        "STANDINGS_SKIPPED": "standings_skipped",
        "STANDINGS_IMPORTED_FROM_RAW": "standings_imported_from_raw",
        "COMPLETED_FIXTURES_TOTAL": "completed_fixtures_total",
        "FIXTURE_STATS_ALREADY_COMPLETE": "fixture_stats_already_complete",
        "FIXTURE_STATS_FETCHED": "fixture_stats_fetched",
        "FIXTURE_STATS_IMPORTED": "fixture_stats_imported",
        "PROVIDER_CALLS_USED": "provider_calls_used",
        "ERRORS": "errors",
    }
    restored: dict[str, int] = {}
    for checkpoint_key, field_name in mappings.items():
        raw = value.get(checkpoint_key, 0)
        if not isinstance(raw, int) or raw < 0:
            raise ValueError(f"checkpoint counter {checkpoint_key} must be a non-negative integer")
        restored[field_name] = raw
    if restored["competitions_total"] != expected_total:
        raise ValueError("checkpoint belongs to a different input scope")
    return Counters(**restored)


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _read_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_competitions(path: Path) -> list[Competition]:
    value = _read_json(path)
    if not isinstance(value, list):
        raise ValueError("single_table input must be an array")
    items = [Competition.from_json(item) for item in value]
    keys = [(item.league_id, item.season) for item in items]
    if len(set(keys)) != len(keys):
        raise ValueError("single_table input contains duplicate league_id + season")
    return sorted(items, key=lambda item: (item.league_id, item.season))


def _parse_time(value: object) -> datetime:
    if not isinstance(value, str):
        return _utcnow()
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return _utcnow()
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _response_from_raw(raw: bytes, *, status_code: int = 200) -> APIFootballResponse:
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise CatalogueImportError("retained raw payload has an invalid top-level shape")
    return APIFootballResponse(payload, raw, status_code, {})


def _collected(request: BaseRequest, response: APIFootballResponse, *, started: datetime, received: datetime) -> CollectedBaseResponse:
    return CollectedBaseResponse(request=request, response=response, request_started_at=started, response_received_at=received)


def _standings_team_count(raw: bytes, competition: Competition) -> int:
    response = _response_from_raw(raw).data.get("response")
    if not isinstance(response, list) or len(response) != 1 or not isinstance(response[0], Mapping):
        raise CatalogueImportError("saved standings response is not a one-league response")
    league = response[0].get("league")
    if not isinstance(league, Mapping) or league.get("id") != competition.league_id or league.get("season") != competition.season:
        raise CatalogueImportError("saved standings response league/season mismatch")
    groups = league.get("standings")
    if not isinstance(groups, list) or len(groups) != 1 or not isinstance(groups[0], list) or not groups[0]:
        raise CatalogueImportError("saved standings response is no longer a non-empty single table")
    return len(groups[0])


def _catalogue_projection(catalogue_raw: Path, competition: Competition) -> APIFootballResponse:
    payload = _read_json(catalogue_raw)
    if not isinstance(payload, Mapping) or not isinstance(payload.get("response"), list):
        raise CatalogueImportError("saved catalogue is invalid")
    matches = []
    for item in payload["response"]:
        if not isinstance(item, Mapping):
            continue
        league = item.get("league")
        seasons = item.get("seasons")
        if not isinstance(league, Mapping) or league.get("id") != competition.league_id or not isinstance(seasons, list):
            continue
        if any(isinstance(season, Mapping) and season.get("year") == competition.season for season in seasons):
            matches.append(dict(item))
    if len(matches) != 1:
        raise CatalogueImportError("saved catalogue has no unique league + season record")
    # This deterministic projection preserves the provider's league/season data
    # while adapting the envelope to the existing base-import contract.
    projected = {"get": "leagues", "parameters": {"id": str(competition.league_id), "season": str(competition.season)}, "errors": {}, "results": 1, "paging": {"current": 1, "total": 1}, "response": matches}
    raw = json.dumps(projected, separators=(",", ":"), ensure_ascii=False).encode()
    return _response_from_raw(raw)


def _load_local_raw(output_dir: Path, competition: Competition, endpoint: str) -> CollectedBaseResponse | None:
    label = endpoint.strip("/").replace("/", "_")
    base = output_dir / "raw_base" / f"{competition.raw_stem}-{label}"
    raw_path, request_path = base.with_suffix(".raw.json"), base.with_suffix(".request.json")
    if not raw_path.exists() and not request_path.exists():
        return None
    if not raw_path.is_file() or not request_path.is_file():
        raise CatalogueImportError(f"partial retained {endpoint} raw artifact")
    metadata = _read_json(request_path)
    if not isinstance(metadata, Mapping) or metadata.get("endpoint") != endpoint:
        raise CatalogueImportError(f"retained {endpoint} request metadata mismatch")
    params = {"league": competition.league_id, "season": competition.season}
    if metadata.get("parameters") != params:
        raise CatalogueImportError(f"retained {endpoint} request parameters mismatch")
    received = _parse_time(metadata.get("response_received_at"))
    return _collected(BaseRequest(endpoint, params), _response_from_raw(raw_path.read_bytes(), status_code=int(metadata.get("http_status", 200))), started=_parse_time(metadata.get("request_started_at")), received=received)


def _load_database_raw(conn: Connection[Any], *, state: ScopeState, competition: Competition, endpoint: str) -> CollectedBaseResponse | None:
    if state.season_id is None:
        return None
    row = conn.execute(
        """SELECT provider_fetch.request_started_at,provider_fetch.response_received_at,
                  provider_fetch.http_status,payload.inline_body
           FROM source.provider_fetches provider_fetch
           JOIN source.provider_raw_payloads payload ON payload.fetch_id=provider_fetch.id
           WHERE provider_fetch.provider_id=%s AND provider_fetch.subject_season_id=%s
             AND provider_fetch.endpoint=%s AND provider_fetch.outcome='success'
             AND provider_fetch.request_params->>'league'=%s
             AND provider_fetch.request_params->>'season'=%s
             AND NOT (provider_fetch.request_params ? 'status')
             AND NOT (provider_fetch.request_params ? 'ids')
             AND payload.purged_at IS NULL AND payload.inline_body IS NOT NULL
           ORDER BY provider_fetch.response_received_at DESC NULLS LAST,provider_fetch.id DESC LIMIT 1""",
        (state.provider_id, state.season_id, endpoint, str(competition.league_id), str(competition.season)),
    ).fetchone()
    if row is None:
        return None
    started, received, status, body = row
    if received is None:
        return None
    return _collected(
        BaseRequest(endpoint, {"league": competition.league_id, "season": competition.season}),
        _response_from_raw(bytes(body), status_code=int(status or 200)),
        started=started,
        received=received,
    )


def _scope_state(conn: Connection[Any], competition: Competition, standings_team_count: int) -> ScopeState:
    provider = conn.execute("SELECT id FROM source.providers WHERE code=%s", (PROVIDER_CODE,)).fetchone()
    if provider is None:
        raise CatalogueImportError("API-Football provider is not configured")
    provider_id = int(provider[0])
    row = conn.execute(
        """SELECT season_ref.season_id
           FROM source.season_provider_refs season_ref
           WHERE season_ref.provider_id=%s AND season_ref.league_external_id=%s AND season_ref.external_season=%s""",
        (provider_id, str(competition.league_id), competition.season),
    ).fetchone()
    if row is None:
        return ScopeState(None, provider_id, False, False, False, 0, 0)
    season_id = int(row[0])
    teams, fixtures, mappings = conn.execute(
        """SELECT
             (SELECT count(*) FROM football.season_teams WHERE season_id=%s),
             (SELECT count(*) FROM football.fixtures WHERE season_id=%s),
             (SELECT count(*) FROM source.fixture_provider_refs ref JOIN football.fixtures fixture ON fixture.id=ref.fixture_id WHERE ref.provider_id=%s AND fixture.season_id=%s)""",
        (season_id, season_id, provider_id, season_id),
    ).fetchone()
    team_result = conn.execute(
        """SELECT provider_results FROM source.provider_fetches
           WHERE provider_id=%s AND subject_season_id=%s AND endpoint='/teams' AND outcome='success' AND normalized_at IS NOT NULL
             AND request_params->>'league'=%s AND request_params->>'season'=%s
           ORDER BY response_received_at DESC NULLS LAST,id DESC LIMIT 1""",
        (provider_id, season_id, str(competition.league_id), str(competition.season)),
    ).fetchone()
    fixture_result = conn.execute(
        """SELECT provider_results FROM source.provider_fetches
           WHERE provider_id=%s AND subject_season_id=%s AND endpoint='/fixtures' AND outcome='success' AND normalized_at IS NOT NULL
             AND request_params->>'league'=%s AND request_params->>'season'=%s
             AND NOT (request_params ? 'status') AND NOT (request_params ? 'ids')
           ORDER BY response_received_at DESC NULLS LAST,id DESC LIMIT 1""",
        (provider_id, season_id, str(competition.league_id), str(competition.season)),
    ).fetchone()
    snapshot = conn.execute(
        """SELECT snapshot.group_count,(SELECT count(*) FROM football.standings_snapshot_rows row WHERE row.snapshot_id=snapshot.id)
           FROM football.standings_snapshots snapshot WHERE snapshot.season_id=%s
           ORDER BY snapshot.captured_at DESC,snapshot.id DESC LIMIT 1""",
        (season_id,),
    ).fetchone()
    completed_total, statistics_complete = conn.execute(
        """SELECT count(*),count(*) FILTER (WHERE stat_count=2 AND participant_count=2)
           FROM (
             SELECT fixture.id,count(stat.*) AS stat_count,
                    count(stat.*) FILTER (WHERE stat.team_id IN (fixture.home_team_id,fixture.away_team_id)) AS participant_count
             FROM football.fixtures fixture LEFT JOIN football.fixture_team_statistics stat ON stat.fixture_id=fixture.id
             WHERE fixture.season_id=%s AND fixture.lifecycle_state='completed'
             GROUP BY fixture.id
           ) pairs""",
        (season_id,),
    ).fetchone()
    teams_ready = int(teams) == standings_team_count and team_result is not None and int(team_result[0]) == int(teams)
    fixtures_ready = int(fixtures) == int(mappings) and fixture_result is not None and int(fixture_result[0]) == int(fixtures)
    standings_ready = snapshot is not None and int(snapshot[0]) == 1 and int(snapshot[1]) == standings_team_count
    return ScopeState(season_id, provider_id, teams_ready, fixtures_ready, standings_ready, int(completed_total), int(statistics_complete))


async def _fetch_base(
    client: APIFootballClient, *, output_dir: Path, competition: Competition, endpoint: str, counters: Counters
) -> CollectedBaseResponse:
    params = {"league": competition.league_id, "season": competition.season}
    started = _utcnow()
    response = await client.get(endpoint, params=params)
    received = _utcnow()
    counters.provider_calls_used += 1
    if client.response_contains_api_key(response.raw_body):
        raise CatalogueImportError("provider response contains API key")
    label = endpoint.strip("/").replace("/", "_")
    base = output_dir / "raw_base" / f"{competition.raw_stem}-{label}"
    base.parent.mkdir(parents=True, exist_ok=True)
    base.with_suffix(".raw.json").write_bytes(response.raw_body)
    _write_json(base.with_suffix(".request.json"), {"endpoint": endpoint, "parameters": params, "request_started_at": started.isoformat(), "response_received_at": received.isoformat(), "http_status": response.status_code, "rate_limit": safe_rate_limit_headers(response.headers)})
    return _collected(BaseRequest(endpoint, params), response, started=started, received=received)


def _saved_standings(raw_dir: Path, competition: Competition) -> CollectedBaseResponse:
    base = raw_dir / f"{competition.raw_stem}"
    raw_path, request_path = base.with_suffix(".raw.json"), base.with_suffix(".request.json")
    if not raw_path.is_file() or not request_path.is_file():
        raise CatalogueImportError("saved /standings raw response is missing")
    metadata = _read_json(request_path)
    if not isinstance(metadata, Mapping) or metadata.get("endpoint") != "/standings" or metadata.get("parameters") != {"league": competition.league_id, "season": competition.season}:
        raise CatalogueImportError("saved /standings request metadata mismatch")
    received = _parse_time(metadata.get("fetched_at"))
    return _collected(BaseRequest("/standings", {"league": competition.league_id, "season": competition.season}), _response_from_raw(raw_path.read_bytes(), status_code=int(metadata.get("http_status", 200))), started=received, received=received)


def _checkpoint(output_dir: Path, *, status: str, results: list[dict[str, object]], counters: Counters) -> None:
    _write_json(output_dir / "checkpoint.json", {"status": status, "updated_at": _utcnow().isoformat(), "counters": counters.output(), "scopes": results})


async def _process_one(
    *, conn: Connection[Any], client: APIFootballClient, competition: Competition, catalogue_raw: Path,
    standings_dir: Path, output_dir: Path, counters: Counters, raw_only: bool = False,
    base_raw_dir: Path | None = None,
) -> dict[str, object]:
    standings = _saved_standings(standings_dir, competition)
    standings_team_count = _standings_team_count(standings.response.raw_body, competition)
    before = _scope_state(conn, competition, standings_team_count)
    counters.completed_fixtures_total += before.completed_total
    counters.fixture_stats_already_complete += before.statistics_complete
    if before.complete:
        counters.competitions_already_complete += 1
        counters.teams_skipped += 1
        counters.fixtures_skipped += 1
        counters.standings_skipped += 1
        return {"league_id": competition.league_id, "season": competition.season, "status": "already_complete"}

    if competition.provider_type != "League":
        raise CatalogueImportError("single-table cup import is deferred: canonical regular-season importer requires provider type League")

    base_changed = False
    if not before.base_ready:
        league_response = _catalogue_projection(catalogue_raw, competition)
        now = _utcnow()
        league = _collected(BaseRequest("/leagues", {"id": competition.league_id, "season": competition.season}), league_response, started=now, received=now)
        loaded: dict[str, CollectedBaseResponse] = {"/leagues": league, "/standings": standings}
        for endpoint, ready, counter_skipped, counter_fetched in (
            ("/teams", before.teams_ready, "teams_skipped", "teams_fetched"),
            ("/fixtures", before.fixtures_ready, "fixtures_skipped", "fixtures_fetched"),
        ):
            retained = _load_local_raw(base_raw_dir or output_dir, competition, endpoint) or _load_database_raw(conn, state=before, competition=competition, endpoint=endpoint)
            if retained is not None:
                loaded[endpoint] = retained
                setattr(counters, counter_skipped, getattr(counters, counter_skipped) + 1)
            elif ready:
                raise CatalogueImportError(f"{endpoint} is canonical but no retained raw is available for atomic replay")
            else:
                loaded[endpoint] = await _fetch_base(client, output_dir=output_dir, competition=competition, endpoint=endpoint, counters=counters)
                setattr(counters, counter_fetched, getattr(counters, counter_fetched) + 1)
        scope = ActiveSeasonScope(
            league_external_id=competition.league_id,
            season_start_year=competition.season,
            expected_fixture_count=standings_team_count * (standings_team_count - 1),
            require_complete_schedule=False,
        )
        try:
            import_active_base(conn, collected=(loaded["/leagues"], loaded["/teams"], loaded["/standings"], loaded["/fixtures"]), scope=scope)
        except (ActiveSeasonImportError, SeasonBootstrapError, ValueError) as error:
            raise CatalogueImportError(str(error)) from error
        if before.standings_ready:
            counters.standings_skipped += 1
        else:
            counters.standings_imported_from_raw += 1
        base_changed = True
    else:
        counters.teams_skipped += 1
        counters.fixtures_skipped += 1
        counters.standings_skipped += 1

    after_base = _scope_state(conn, competition, standings_team_count)
    if not after_base.base_ready or after_base.season_id is None:
        raise CatalogueImportError("canonical base remains incomplete after import")

    if raw_only:
        if base_changed:
            counters.competitions_updated += 1
        return {
            "league_id": competition.league_id,
            "season": competition.season,
            "status": "base_complete_statistics_pending" if after_base.completed_total > after_base.statistics_complete else "updated",
            "completed_fixtures_total": after_base.completed_total,
            "fixture_statistics_already_complete": after_base.statistics_complete,
            "fixture_statistics_complete_after": after_base.statistics_complete,
            "statistics_stop_reason": "raw_only",
            "statistics_errors": [],
        }

    targets = load_completed_targets(conn, provider_id=after_base.provider_id, season_id=after_base.season_id)
    completed_total = len(targets)
    complete_before_stats = sum(target.statistics_complete for target in targets)
    report = await run_current_season_statistics_backfill_async(
        scope=CurrentSeasonStatisticsScope(
            league_external_id=competition.league_id,
            season_start_year=competition.season,
            project_discovery=False,
            select_all_completed=True,
        ),
        client=client,
    )
    counters.provider_calls_used += report.api_requests
    counters.fixture_stats_fetched += report.unique_fixtures_selected
    counters.fixture_stats_imported += report.fixtures_normalized
    if base_changed or report.api_requests or report.statistics_rows_written:
        counters.competitions_updated += 1
    final = _scope_state(conn, competition, standings_team_count)
    statistics_pending = final.statistics_complete < completed_total
    return {
        "league_id": competition.league_id,
        "season": competition.season,
        "status": (
            "base_complete_statistics_pending"
            if statistics_pending
            else "updated"
            if base_changed or report.api_requests or report.statistics_rows_written
            else "already_complete"
        ),
        "completed_fixtures_total": completed_total,
        "fixture_statistics_already_complete": complete_before_stats,
        "fixture_statistics_complete_after": final.statistics_complete,
        "statistics_stop_reason": report.stopped_reason,
        "statistics_errors": list(report.errors),
    }


async def run(
    *, input_json: Path, catalogue_raw: Path, standings_dir: Path, output_dir: Path,
    raw_only: bool = False, base_raw_dir: Path | None = None, resume: bool = False,
    retry_failed: bool = False,
) -> dict[str, object]:
    competitions = _read_competitions(input_json)
    if output_dir.exists() and not output_dir.is_dir():
        raise ValueError("output path must be a directory")
    output_dir.mkdir(parents=True, exist_ok=True)
    counters = Counters(competitions_total=len(competitions))
    results: list[dict[str, object]] = []
    processed: set[tuple[int, int]] = set()
    if resume:
        checkpoint_path = output_dir / "checkpoint.json"
        if not checkpoint_path.is_file():
            raise ValueError("--resume requires an existing checkpoint.json")
        checkpoint = _read_json(checkpoint_path)
        if not isinstance(checkpoint, Mapping) or not isinstance(checkpoint.get("scopes"), list):
            raise ValueError("checkpoint has an invalid shape")
        counters = _counters_from_checkpoint(checkpoint.get("counters"), expected_total=len(competitions))
        retried_failures = 0
        for result in checkpoint["scopes"]:
            if not isinstance(result, dict) or not isinstance(result.get("league_id"), int) or not isinstance(result.get("season"), int):
                raise ValueError("checkpoint scope result has an invalid shape")
            if retry_failed and result.get("status") == "incomplete":
                retried_failures += 1
                continue
            key = (int(result["league_id"]), int(result["season"]))
            if key in processed:
                raise ValueError("checkpoint contains a duplicate scope")
            processed.add(key)
            results.append(result)
        if retried_failures > counters.errors:
            raise ValueError("checkpoint error counter is inconsistent with failed scopes")
        counters.errors -= retried_failures
        expected_keys = {(item.league_id, item.season) for item in competitions}
        if not processed.issubset(expected_keys):
            raise ValueError("checkpoint includes a scope outside the input queue")

    database_url = os.environ.get("SUPABASE_DB_URL")
    if not database_url:
        raise CatalogueImportError("SUPABASE_DB_URL is required")
    with psycopg.connect(database_url, autocommit=True) as conn:
        async with APIFootballClient.from_environment() as client:
            for competition in competitions:
                if (competition.league_id, competition.season) in processed:
                    continue
                try:
                    result = await _process_one(conn=conn, client=client, competition=competition, catalogue_raw=catalogue_raw, standings_dir=standings_dir, output_dir=output_dir, counters=counters, raw_only=raw_only, base_raw_dir=base_raw_dir)
                    results.append(result)
                except (CatalogueImportError, CurrentSeasonStatisticsError, APIFootballAPIError, APIFootballHTTPError) as error:
                    counters.errors += 1
                    result = {
                        "league_id": competition.league_id,
                        "season": competition.season,
                        "status": "incomplete",
                        "error": type(error).__name__,
                        "error_detail": str(error),
                    }
                    results.append(result)
                except Exception as error:
                    # A one-off classifier/importer must finish the remaining
                    # queue even if one provider shape triggers an unforeseen
                    # database or normalization exception. The saved raw and
                    # per-scope checkpoint retain the evidence for review.
                    counters.errors += 1
                    result = {
                        "league_id": competition.league_id,
                        "season": competition.season,
                        "status": "incomplete",
                        "error": type(error).__name__,
                        "error_detail": str(error),
                    }
                    results.append(result)
                _checkpoint(output_dir, status="running", results=results, counters=counters)

    incomplete = [
        result for result in results
        if result.get("status") not in {"already_complete", "updated"}
        or result.get("statistics_stop_reason")
        or result.get("statistics_errors")
    ]
    _write_json(output_dir / "incomplete.json", incomplete)
    summary = {**counters.output(), "INCOMPLETE_SCOPES": [{"league_id": item["league_id"], "season": item["season"]} for item in incomplete]}
    _write_json(output_dir / "summary.json", summary)
    _checkpoint(output_dir, status="complete", results=results, counters=counters)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_json", type=Path)
    parser.add_argument("--catalogue-raw", type=Path, default=DEFAULT_CATALOGUE_RAW)
    parser.add_argument("--standings-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--raw-only", action="store_true", help="Import only retained base raw; never request statistics.")
    parser.add_argument("--base-raw-dir", type=Path, help="Read retained /teams and /fixtures raw from another output directory.")
    parser.add_argument("--resume", action="store_true", help="Continue from this output directory's checkpoint without replaying saved scopes.")
    parser.add_argument("--retry-failed", action="store_true", help="With --resume, retry only checkpoint scopes marked incomplete.")
    args = parser.parse_args()
    if args.retry_failed and not args.resume:
        parser.error("--retry-failed requires --resume")
    summary = asyncio.run(
        run(
            input_json=args.input_json.resolve(strict=True),
            catalogue_raw=args.catalogue_raw.resolve(strict=True),
            standings_dir=args.standings_dir.resolve(strict=True),
            output_dir=args.output_dir.resolve(),
            raw_only=args.raw_only,
            base_raw_dir=(args.base_raw_dir.resolve(strict=True) if args.base_raw_dir else None),
            resume=args.resume,
            retry_failed=args.retry_failed,
        )
    )
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
