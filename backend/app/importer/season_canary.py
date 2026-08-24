"""Controlled, no-concurrency canary for one completed provider season.

The runner is intentionally narrow: four base requests, then five fixture
statistics and five historical lineups.  It has a hard 14-call ceiling and no
automatic retry.  Full seasonal backfills remain separate jobs after this
canary has been explicitly reviewed.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg import Connection

from app.api_football import APIFootballClient, APIFootballResponse
from app.api_football.client import safe_rate_limit_headers
from app.api_football.errors import APIFootballAPIError, APIFootballHTTPError
from app.importer import historical_lineups as lineups
from app.importer import statistics_backfill as statistics
from app.importer.canary import request_params_sha256
from app.importer.season_backfill import PROVIDER_CODE, SeasonContext
from app.importer.season_bootstrap import (
    BOOTSTRAP_PURPOSE,
    BaseRequest,
    BootstrapScope,
    CollectedBaseResponse,
    SeasonBootstrapError,
    base_requests,
    bootstrap_base,
)

MAX_PHYSICAL_CALLS = 14
FIXTURE_CALL_COUNT = 5
QUOTA_RESERVE = 50
# The first fully completed historical season is the preservation baseline for
# every subsequent multi-competition canary, not merely for new EPL seasons.
EPL_2024_LEAGUE_EXTERNAL_ID = "39"


class SeasonCanaryError(RuntimeError):
    """A controlled-stop condition; the caller must not continue the campaign."""


@dataclass(frozen=True)
class SeasonCanaryScope:
    league_external_id: int
    season_start_year: int
    expected_fixture_count: int
    selected_fixture_external_ids: tuple[int, ...]
    selected_fixture_expectations: tuple["FixtureExpectation", ...] = ()

    def __post_init__(self) -> None:
        BootstrapScope(
            league_external_id=self.league_external_id,
            season_start_year=self.season_start_year,
            expected_fixture_count=self.expected_fixture_count,
        )
        if len(self.selected_fixture_external_ids) != FIXTURE_CALL_COUNT:
            raise ValueError("canary requires exactly five selected fixtures")
        if len(set(self.selected_fixture_external_ids)) != FIXTURE_CALL_COUNT:
            raise ValueError("canary selected fixtures must be distinct")
        if any(value <= 0 for value in self.selected_fixture_external_ids):
            raise ValueError("canary selected fixture IDs must be positive")
        if self.selected_fixture_expectations:
            if len(self.selected_fixture_expectations) != FIXTURE_CALL_COUNT:
                raise ValueError("canary requires expectations for all five selected fixtures")
            if {item.external_id for item in self.selected_fixture_expectations} != set(self.selected_fixture_external_ids):
                raise ValueError("canary fixture expectations must match selected fixture IDs")

    @property
    def bootstrap_scope(self) -> BootstrapScope:
        return BootstrapScope(
            league_external_id=self.league_external_id,
            season_start_year=self.season_start_year,
            expected_fixture_count=self.expected_fixture_count,
        )

    @property
    def statistics_scope(self) -> statistics.StatisticsImportScope:
        return statistics.StatisticsImportScope(
            league_external_id=self.league_external_id,
            season_start_year=self.season_start_year,
            expected_fixture_count=self.expected_fixture_count,
            canary_fixture_external_id=None,
        )

    @property
    def lineups_scope(self) -> lineups.HistoricalLineupsScope:
        return lineups.HistoricalLineupsScope(
            league_external_id=str(self.league_external_id),
            season_start_year=self.season_start_year,
            expected_fixture_count=self.expected_fixture_count,
        )


@dataclass(frozen=True)
class FixtureExpectation:
    """Stable fixture identity shown to the operator before API calls."""

    external_id: int
    kickoff_at: datetime
    home_external_id: int
    away_external_id: int

    def __post_init__(self) -> None:
        if self.external_id <= 0 or self.home_external_id <= 0 or self.away_external_id <= 0:
            raise ValueError("fixture expectation provider IDs must be positive")
        if self.home_external_id == self.away_external_id or self.kickoff_at.tzinfo is None:
            raise ValueError("fixture expectation is not a valid timezone-aware home/away identity")


@dataclass(frozen=True)
class SeasonCanaryReport:
    physical_api_calls: int
    reused_raw_fetches: int
    quota: Mapping[str, str]
    season_id: int
    selected_fixture_external_ids: tuple[int, ...]
    verification: Mapping[str, Any]


def _database_url() -> str:
    value = os.environ.get("SUPABASE_DB_URL")
    if not value:
        raise SeasonCanaryError("SUPABASE_DB_URL is required")
    return value


def _provider_and_season_id(conn: Connection[Any], *, scope: SeasonCanaryScope) -> tuple[int, int] | None:
    row = conn.execute(
        """SELECT provider.id, ref.season_id
           FROM source.providers provider
           JOIN source.season_provider_refs ref ON ref.provider_id=provider.id
           WHERE provider.code=%s AND ref.league_external_id=%s AND ref.external_season=%s""",
        (PROVIDER_CODE, str(scope.league_external_id), scope.season_start_year),
    ).fetchone()
    return None if row is None else (int(row[0]), int(row[1]))


def _assert_quota_reserve(quota: Mapping[str, str], *, future_calls: int) -> None:
    remaining = quota.get("x-ratelimit-requests-remaining")
    if remaining is None:
        return
    if not remaining.isdigit():
        raise SeasonCanaryError("provider rate-limit header is malformed")
    if int(remaining) < future_calls + QUOTA_RESERVE:
        raise SeasonCanaryError("provider daily quota reserve would be breached; campaign stopped")


def _canary_lock_keys(scope: SeasonCanaryScope) -> tuple[str, ...]:
    """Return the importer locks in a deterministic order.

    The canary runs the bootstrap, statistics, and historical-lineups lanes as
    one campaign. Holding their existing per-season locks for its lifetime
    prevents a separately started bulk job from consuming or normalizing the
    same raw/canonical rows concurrently.
    """
    return tuple(
        sorted(
            {
                scope.bootstrap_scope.lock_key,
                scope.statistics_scope.lock_key,
                scope.lineups_scope.lock_key,
            }
        )
    )


def _acquire_canary_locks(conn: Connection[Any], *, scope: SeasonCanaryScope) -> tuple[str, ...]:
    acquired: list[str] = []
    for key in _canary_lock_keys(scope):
        locked = conn.execute(
            "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))", (key,)
        ).fetchone()[0]
        if not locked:
            for held_key in reversed(acquired):
                conn.execute(
                    "SELECT pg_advisory_unlock(hashtextextended(%s, 0))", (held_key,)
                )
            raise SeasonCanaryError("a bootstrap/statistics/lineups import is already running")
        acquired.append(key)
    return tuple(acquired)


def _release_canary_locks(conn: Connection[Any], *, keys: Sequence[str]) -> None:
    for key in reversed(keys):
        conn.execute("SELECT pg_advisory_unlock(hashtextextended(%s, 0))", (key,))


def _load_reusable_base(
    conn: Connection[Any], *, scope: SeasonCanaryScope
) -> tuple[CollectedBaseResponse, ...] | None:
    context = _provider_and_season_id(conn, scope=scope)
    if context is None:
        return None
    provider_id, season_id = context
    loaded: list[CollectedBaseResponse] = []
    for request in base_requests(scope.bootstrap_scope):
        row = conn.execute(
            """SELECT provider_fetch.request_started_at,provider_fetch.response_received_at,provider_fetch.http_status,
                      provider_fetch.content_sha256,raw.inline_body
               FROM source.provider_fetches provider_fetch
               JOIN source.provider_raw_payloads raw ON raw.fetch_id=provider_fetch.id
               WHERE provider_fetch.provider_id=%s AND provider_fetch.endpoint=%s
                 AND provider_fetch.request_params_sha256=%s AND provider_fetch.purpose=%s
                 AND provider_fetch.subject_season_id=%s AND provider_fetch.outcome='success'
                 AND provider_fetch.normalized_at IS NOT NULL
                 AND raw.purged_at IS NULL AND raw.inline_body IS NOT NULL
               ORDER BY provider_fetch.response_received_at DESC,provider_fetch.id DESC LIMIT 1""",
            (provider_id, request.endpoint, request_params_sha256(request.params), BOOTSTRAP_PURPOSE, season_id),
        ).fetchone()
        if row is None:
            raise SeasonCanaryError("existing season has incomplete retained bootstrap provenance")
        started, received, status, expected_hash, body = row
        raw = bytes(body)
        if expected_hash is None or hashlib.sha256(raw).digest() != bytes(expected_hash):
            raise SeasonCanaryError("retained bootstrap raw SHA-256 mismatch")
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError) as error:
            raise SeasonCanaryError("retained bootstrap raw is invalid JSON") from error
        if not isinstance(payload, dict) or received is None:
            raise SeasonCanaryError("retained bootstrap raw has an invalid contract")
        loaded.append(
            CollectedBaseResponse(
                request=request,
                response=APIFootballResponse(payload, raw, int(status), {}),
                request_started_at=started,
                response_received_at=received,
            )
        )
    return tuple(loaded)


def _collect_base(
    *, client: APIFootballClient, scope: SeasonCanaryScope
) -> tuple[tuple[CollectedBaseResponse, ...], int, Mapping[str, str]]:
    collected: list[CollectedBaseResponse] = []
    quota: Mapping[str, str] = {}
    for offset, request in enumerate(base_requests(scope.bootstrap_scope), start=1):
        started = datetime.now(UTC)
        try:
            response = asyncio.run(client.get(request.endpoint, params=request.params))
        except (APIFootballHTTPError, APIFootballAPIError) as error:
            raise SeasonCanaryError("base provider request failed; campaign stopped") from error
        received = datetime.now(UTC)
        if client.response_contains_api_key(response.raw_body):
            raise SeasonCanaryError("provider response contains API key; refusing persistence")
        quota = safe_rate_limit_headers(response.headers)
        _assert_quota_reserve(quota, future_calls=MAX_PHYSICAL_CALLS - offset)
        collected.append(CollectedBaseResponse(request, response, started, received))
    return tuple(collected), len(collected), quota


def _season_context(conn: Connection[Any], *, scope: SeasonCanaryScope) -> SeasonContext:
    row = conn.execute(
        """SELECT provider.id,league_ref.league_id,season_ref.season_id
           FROM source.providers provider
           JOIN source.league_provider_refs league_ref
             ON league_ref.provider_id=provider.id AND league_ref.external_id=%s
           JOIN source.season_provider_refs season_ref
             ON season_ref.provider_id=provider.id
            AND season_ref.league_external_id=league_ref.external_id
            AND season_ref.external_season=%s
           WHERE provider.code=%s""",
        (str(scope.league_external_id), scope.season_start_year, PROVIDER_CODE),
    ).fetchone()
    if row is None:
        raise SeasonCanaryError("bootstrap did not create the provider season mapping")
    provider_id, league_id, season_id = map(int, row)
    teams = {
        int(external_id): int(team_id)
        for external_id, team_id in conn.execute(
            """SELECT ref.external_id,ref.team_id
               FROM source.team_provider_refs ref
               JOIN football.season_teams season_team
                 ON season_team.team_id=ref.team_id AND season_team.season_id=%s
               WHERE ref.provider_id=%s""",
            (season_id, provider_id),
        ).fetchall()
    }
    if len(teams) != scope.bootstrap_scope.expected_team_count:
        raise SeasonCanaryError("bootstrap did not create the expected season-team mappings")
    return SeasonContext(provider_id, league_id, season_id, teams, scope.bootstrap_scope.season_scope)


def _selected_statistics_targets(
    conn: Connection[Any], *, context: SeasonContext, scope: SeasonCanaryScope
) -> tuple[statistics.FixtureTarget, ...]:
    targets = {
        target.external_id: target
        for target in statistics.load_targets(
            conn, provider_id=context.provider_id, season_id=context.season_id, scope=scope.statistics_scope
        )
    }
    if not set(scope.selected_fixture_external_ids).issubset(targets):
        raise SeasonCanaryError("selected fixture is not a completed/finalized canonical fixture")
    expectations = {item.external_id: item for item in scope.selected_fixture_expectations}
    for external_id, expected in expectations.items():
        target = targets[external_id]
        actual = (target.kickoff_at, target.home_team_id, target.away_team_id)
        # Resolve the provider team IDs from the exact canonical fixture rather
        # than trusting an in-memory response or ordering convention.
        row = conn.execute(
            """SELECT home_ref.external_id,away_ref.external_id
               FROM football.fixtures fixture
               JOIN source.team_provider_refs home_ref
                 ON home_ref.provider_id=%s AND home_ref.team_id=fixture.home_team_id
               JOIN source.team_provider_refs away_ref
                 ON away_ref.provider_id=%s AND away_ref.team_id=fixture.away_team_id
               WHERE fixture.id=%s""",
            (context.provider_id, context.provider_id, target.fixture_id),
        ).fetchone()
        if row is None or actual[0] != expected.kickoff_at or tuple(map(int, row)) != (
            expected.home_external_id,
            expected.away_external_id,
        ):
            raise SeasonCanaryError("selected fixture differs from the operator-reviewed identity")
    return tuple(targets[value] for value in scope.selected_fixture_external_ids)


def _run_one_statistic(
    conn: Connection[Any], *, client: APIFootballClient, provider_id: int,
    target: statistics.FixtureTarget,
) -> tuple[bool, Mapping[str, str]]:
    state = statistics._existing_pair_state(conn, provider_id=provider_id, target=target)
    if state == "done":
        return False, {}
    raw = statistics._find_reusable_raw(conn, provider_id=provider_id, target=target)
    if raw is not None:
        mapped_state, _ = statistics.normalize_raw(conn, provider_id=provider_id, target=target, raw=raw)
        if mapped_state != "complete":
            raise SeasonCanaryError("canary statistics response is not a complete two-team pair")
        return False, {}
    started = datetime.now(UTC)
    try:
        response = asyncio.run(client.get(statistics.ENDPOINT, params={"fixture": target.external_id}))
    except APIFootballHTTPError as error:
        statistics._record_failure(
            conn,
            provider_id=provider_id,
            target=target,
            started=started,
            received=datetime.now(UTC),
            status=error.status_code or None,
            outcome="transport_error" if error.status_code == 0 else "http_error",
            error_class=type(error).__name__,
        )
        raise SeasonCanaryError("fixture statistics provider request failed; campaign stopped") from error
    except APIFootballAPIError as error:
        received = datetime.now(UTC)
        if error.raw_body is not None and client.response_contains_api_key(error.raw_body):
            statistics._record_failure(
                conn,
                provider_id=provider_id,
                target=target,
                started=started,
                received=received,
                status=error.status_code or 200,
                outcome="provider_error",
                error_class=type(error).__name__,
            )
            raise SeasonCanaryError("provider error response contains API key") from error
        statistics._persist_api_error_response(
            conn,
            provider_id=provider_id,
            target=target,
            started=started,
            received=received,
            error=error,
        )
        raise SeasonCanaryError("fixture statistics provider request failed; campaign stopped") from error
    received = datetime.now(UTC)
    if client.response_contains_api_key(response.raw_body):
        raise SeasonCanaryError("provider response contains API key; refusing persistence")
    quota = safe_rate_limit_headers(response.headers)
    raw = statistics._persist_fetch(
        conn, provider_id=provider_id, target=target, response=response, started=started, received=received
    )
    mapped_state, _ = statistics.normalize_raw(conn, provider_id=provider_id, target=target, raw=raw)
    if mapped_state != "complete":
        raise SeasonCanaryError("canary statistics response is not a complete two-team pair")
    return True, quota


def _lineup_target(
    conn: Connection[Any], *, context: SeasonContext, fixture_id: int
) -> lineups.FixtureTarget:
    return lineups._target_for_fixture(
        conn, provider_id=context.provider_id, season_id=context.season_id, fixture_id=fixture_id
    )


def _run_one_lineup(
    conn: Connection[Any], *, client: APIFootballClient, provider_id: int,
    target: lineups.FixtureTarget,
) -> tuple[bool, Mapping[str, str]]:
    existing = conn.execute(
        "SELECT 1 FROM football.fixture_historical_lineup_snapshots WHERE fixture_id=%s",
        (target.fixture_id,),
    ).fetchone()
    if existing is not None:
        lineups._verify_fixture(conn, provider_id=provider_id, target=target)
        return False, {}
    row = conn.execute(
        """SELECT id FROM source.provider_fetches
           WHERE provider_id=%s AND endpoint=%s AND purpose=%s
             AND subject_fixture_id=%s AND subject_season_id=%s
             AND outcome='success' AND normalized_at IS NULL
           ORDER BY id DESC LIMIT 1""",
        (provider_id, lineups.ENDPOINT, lineups.PURPOSE, target.fixture_id, target.season_id),
    ).fetchone()
    if row is not None:
        raw = lineups._load_retained_raw(conn, provider_id=provider_id, target=target, fetch_id=int(row[0]))
        result = lineups.normalize_raw(conn, provider_id=provider_id, target=target, raw=raw)
        if result.coverage_state != "complete":
            raise SeasonCanaryError("canary historical lineup response is not complete")
        return False, {}
    result, quota = lineups._fetch_and_normalize(
        conn, client=client, provider_id=provider_id, target=target, clock=lambda: datetime.now(UTC)
    )
    if result.coverage_state != "complete":
        raise SeasonCanaryError("canary historical lineup response is not complete")
    return True, quota


def _epl_2024_fingerprint(conn: Connection[Any], *, provider_id: int) -> str:
    row = conn.execute(
        """WITH season AS (
                SELECT ref.season_id FROM source.season_provider_refs ref
                WHERE ref.provider_id=%s AND ref.league_external_id=%s AND ref.external_season=2024
            ), pieces AS (
                SELECT 'fixture:' || row_to_json(fixture)::text AS value
                FROM football.fixtures fixture WHERE fixture.season_id=(SELECT season_id FROM season)
                UNION ALL SELECT 'fixture_ref:' || row_to_json(ref)::text
                FROM source.fixture_provider_refs ref
                JOIN football.fixtures fixture ON fixture.id=ref.fixture_id
                WHERE fixture.season_id=(SELECT season_id FROM season)
                UNION ALL SELECT 'stat:' || row_to_json(stat)::text
                FROM football.fixture_team_statistics stat
                JOIN football.fixtures fixture ON fixture.id=stat.fixture_id
                WHERE fixture.season_id=(SELECT season_id FROM season)
                UNION ALL SELECT 'status:' || row_to_json(status)::text
                FROM source.fixture_provider_status status
                JOIN football.fixtures fixture ON fixture.id=status.fixture_id
                WHERE fixture.season_id=(SELECT season_id FROM season)
                UNION ALL SELECT 'standing_snapshot:' || row_to_json(snapshot)::text
                FROM football.standings_snapshots snapshot WHERE snapshot.season_id=(SELECT season_id FROM season)
                UNION ALL SELECT 'standing_group:' || row_to_json(group_row)::text
                FROM football.standings_snapshot_groups group_row
                JOIN football.standings_snapshots snapshot ON snapshot.id=group_row.snapshot_id
                WHERE snapshot.season_id=(SELECT season_id FROM season)
                UNION ALL SELECT 'standing_row:' || row_to_json(standing_row)::text
                FROM football.standings_snapshot_rows standing_row
                JOIN football.standings_snapshots snapshot ON snapshot.id=standing_row.snapshot_id
                WHERE snapshot.season_id=(SELECT season_id FROM season)
                UNION ALL SELECT 'historical_snapshot:' || row_to_json(snapshot)::text
                FROM football.fixture_historical_lineup_snapshots snapshot
                JOIN football.fixtures fixture ON fixture.id=snapshot.fixture_id
                WHERE fixture.season_id=(SELECT season_id FROM season)
                UNION ALL SELECT 'historical_lineup:' || row_to_json(lineup)::text
                FROM football.fixture_historical_lineups lineup
                JOIN football.fixture_historical_lineup_snapshots snapshot ON snapshot.id=lineup.snapshot_id
                JOIN football.fixtures fixture ON fixture.id=snapshot.fixture_id
                WHERE fixture.season_id=(SELECT season_id FROM season)
                UNION ALL SELECT 'historical_player:' || row_to_json(player)::text
                FROM football.fixture_historical_lineup_players player
                JOIN football.fixture_historical_lineup_snapshots snapshot ON snapshot.id=player.snapshot_id
                JOIN football.fixtures fixture ON fixture.id=snapshot.fixture_id
                WHERE fixture.season_id=(SELECT season_id FROM season)
                UNION ALL SELECT 'fetch:' || row_to_json(provider_fetch)::text
                FROM source.provider_fetches provider_fetch
                WHERE provider_fetch.subject_season_id=(SELECT season_id FROM season)
                UNION ALL SELECT 'raw_hash:' || encode(provider_fetch.content_sha256,'hex')
                FROM source.provider_fetches provider_fetch
                JOIN source.provider_raw_payloads raw ON raw.fetch_id=provider_fetch.id
                WHERE provider_fetch.subject_season_id=(SELECT season_id FROM season)
            ) SELECT md5(coalesce(string_agg(value,'' ORDER BY value),'')) FROM pieces""",
        (provider_id, EPL_2024_LEAGUE_EXTERNAL_ID),
    ).fetchone()
    return str(row[0])


def _verify(
    conn: Connection[Any], *, context: SeasonContext, scope: SeasonCanaryScope,
    epl_2024_before: str, player_refs_before: frozenset[str], coach_refs_before: frozenset[str],
) -> dict[str, Any]:
    fixtures, mappings, statuses = conn.execute(
        """SELECT count(*),count(ref.external_id),count(status.fixture_id)
           FROM football.fixtures fixture
           LEFT JOIN source.fixture_provider_refs ref
             ON ref.fixture_id=fixture.id AND ref.provider_id=%s
           LEFT JOIN source.fixture_provider_status status
             ON status.fixture_id=fixture.id AND status.provider_id=%s
           WHERE fixture.season_id=%s""",
        (context.provider_id, context.provider_id, context.season_id),
    ).fetchone()
    stats_complete = 0
    lineups_complete = 0
    selected_fixture_ids: list[int] = []
    for target in _selected_statistics_targets(conn, context=context, scope=scope):
        if statistics._existing_pair_state(conn, provider_id=context.provider_id, target=target) == "done":
            stats_complete += 1
        selected_fixture_ids.append(target.fixture_id)
        lineup_target = _lineup_target(conn, context=context, fixture_id=target.fixture_id)
        verified = lineups._verify_fixture(conn, provider_id=context.provider_id, target=lineup_target)
        if verified["coverage_state"] == "complete" and verified["team_lineups"] == 2:
            lineups_complete += 1
    orphan_or_nonparticipant = conn.execute(
        """SELECT
              (SELECT count(*) FROM football.fixture_team_statistics stat
               JOIN football.fixtures fixture ON fixture.id=stat.fixture_id
               WHERE fixture.season_id=%s AND stat.team_id NOT IN(fixture.home_team_id,fixture.away_team_id)) +
              (SELECT count(*) FROM football.fixture_historical_lineups lineup
               JOIN football.fixture_historical_lineup_snapshots snapshot ON snapshot.id=lineup.snapshot_id
               JOIN football.fixtures fixture ON fixture.id=snapshot.fixture_id
               WHERE fixture.season_id=%s AND lineup.team_id NOT IN(fixture.home_team_id,fixture.away_team_id))""",
        (context.season_id, context.season_id),
    ).fetchone()[0]
    duplicates = conn.execute(
        """SELECT
              (SELECT count(*) FROM (SELECT fixture_id,team_id FROM football.fixture_team_statistics GROUP BY fixture_id,team_id HAVING count(*)>1) x) +
              (SELECT count(*) FROM (SELECT snapshot_id,team_id FROM football.fixture_historical_lineups GROUP BY snapshot_id,team_id HAVING count(*)>1) x)"""
    ).fetchone()[0]
    prematch_rows = conn.execute(
        """SELECT (SELECT count(*) FROM football.fixture_lineup_snapshots) +
                  (SELECT count(*) FROM football.fixture_availability_snapshots)"""
    ).fetchone()[0]
    raw_rows = conn.execute(
        """SELECT provider_fetch.content_sha256,raw.inline_body,raw.byte_count
           FROM source.provider_fetches provider_fetch
           JOIN source.provider_raw_payloads raw ON raw.fetch_id=provider_fetch.id
           WHERE provider_fetch.provider_id=%s AND provider_fetch.subject_season_id=%s
             AND provider_fetch.endpoint IN('/leagues','/teams','/standings','/fixtures','/fixtures/statistics','/fixtures/lineups')
             AND raw.purged_at IS NULL AND raw.inline_body IS NOT NULL""",
        (context.provider_id, context.season_id),
    ).fetchall()
    raw_verified = sum(
        int(bytes(digest) == hashlib.sha256(bytes(body)).digest() and int(count) == len(bytes(body)))
        for digest, body, count in raw_rows
    )
    player_refs_after = {
        str(row[0])
        for row in conn.execute(
            """SELECT DISTINCT ref.external_id
               FROM football.fixture_historical_lineup_players player
               JOIN football.fixture_historical_lineup_snapshots snapshot ON snapshot.id=player.snapshot_id
               JOIN source.player_provider_refs ref ON ref.provider_id=%s AND ref.player_id=player.player_id
               WHERE snapshot.fixture_id=ANY(%s)""",
            (context.provider_id, selected_fixture_ids),
        ).fetchall()
    }
    coach_refs_after = {
        str(row[0])
        for row in conn.execute(
            """SELECT DISTINCT ref.external_id
               FROM football.fixture_historical_lineups lineup
               JOIN football.fixture_historical_lineup_snapshots snapshot ON snapshot.id=lineup.snapshot_id
               JOIN source.coach_provider_refs ref ON ref.provider_id=%s AND ref.coach_id=lineup.coach_id
               WHERE snapshot.fixture_id=ANY(%s) AND lineup.coach_id IS NOT NULL""",
            (context.provider_id, selected_fixture_ids),
        ).fetchall()
    }
    after = _epl_2024_fingerprint(conn, provider_id=context.provider_id)
    result = {
        "fixtures": int(fixtures),
        "fixture_provider_mappings": int(mappings),
        "exact_provider_statuses": int(statuses),
        "season_teams": int(conn.execute("SELECT count(*) FROM football.season_teams WHERE season_id=%s", (context.season_id,)).fetchone()[0]),
        "standings_rows": int(conn.execute("""SELECT count(*) FROM football.standings_snapshot_rows row JOIN football.standings_snapshots snapshot ON snapshot.id=row.snapshot_id WHERE snapshot.season_id=%s""", (context.season_id,)).fetchone()[0]),
        "coverage_snapshots": int(conn.execute("SELECT count(*) FROM source.season_coverage_snapshots WHERE season_id=%s", (context.season_id,)).fetchone()[0]),
        "statistics_complete": stats_complete,
        "historical_lineups_complete": lineups_complete,
        "duplicates": int(duplicates),
        "orphans_or_nonparticipants": int(orphan_or_nonparticipant),
        "prematch_rows": int(prematch_rows),
        "raw_payloads_verified": raw_verified,
        "selected_lineup_players_created": len(player_refs_after - player_refs_before),
        "selected_lineup_players_reused": len(player_refs_after & player_refs_before),
        "selected_lineup_coaches_created": len(coach_refs_after - coach_refs_before),
        "selected_lineup_coaches_reused": len(coach_refs_after & coach_refs_before),
        "epl_2024_fingerprint_unchanged": after == epl_2024_before,
    }
    if (
        (result["fixtures"], result["fixture_provider_mappings"], result["exact_provider_statuses"], result["season_teams"])
        != (scope.expected_fixture_count, scope.expected_fixture_count, scope.expected_fixture_count, scope.bootstrap_scope.expected_team_count)
        or result["standings_rows"] != scope.bootstrap_scope.expected_team_count
        or result["coverage_snapshots"] != 1
        or result["statistics_complete"] != FIXTURE_CALL_COUNT
        or result["historical_lineups_complete"] != FIXTURE_CALL_COUNT
        or result["duplicates"] != 0
        or result["orphans_or_nonparticipants"] != 0
        or result["prematch_rows"] != 0
        or result["raw_payloads_verified"] < MAX_PHYSICAL_CALLS
        or not result["epl_2024_fingerprint_unchanged"]
    ):
        raise AssertionError("season canary remote verification failed")
    return result


def run_controlled_canary(
    *, client: APIFootballClient | None = None, scope: SeasonCanaryScope
) -> SeasonCanaryReport:
    """Run the approved 4 + 5 + 5 controlled canary and then stop."""

    api = client or APIFootballClient.from_environment()
    with psycopg.connect(_database_url(), autocommit=True) as conn:
        lock_keys = _acquire_canary_locks(conn, scope=scope)
        try:
            return _run_controlled_canary_locked(conn=conn, api=api, scope=scope)
        finally:
            _release_canary_locks(conn, keys=lock_keys)


def _run_controlled_canary_locked(
    *, conn: Connection[Any], api: APIFootballClient, scope: SeasonCanaryScope
) -> SeasonCanaryReport:
    """Run the canary while the three affected importer lanes are locked."""

    physical_calls = 0
    reused_raw = 0
    quota: Mapping[str, str] = {}
    provider_row = conn.execute("SELECT id FROM source.providers WHERE code=%s", (PROVIDER_CODE,)).fetchone()
    if provider_row is None:
        raise SeasonCanaryError("API-Football provider mapping is required")
    epl_2024_before = _epl_2024_fingerprint(conn, provider_id=int(provider_row[0]))
    player_refs_before = frozenset(
        str(row[0])
        for row in conn.execute(
            "SELECT external_id FROM source.player_provider_refs WHERE provider_id=%s", (int(provider_row[0]),)
        ).fetchall()
    )
    coach_refs_before = frozenset(
        str(row[0])
        for row in conn.execute(
            "SELECT external_id FROM source.coach_provider_refs WHERE provider_id=%s", (int(provider_row[0]),)
        ).fetchall()
    )
    reusable_base = _load_reusable_base(conn, scope=scope)

    if reusable_base is None:
        base, calls, quota = _collect_base(client=api, scope=scope)
        physical_calls += calls
        try:
            context = bootstrap_base(conn, collected=base, scope=scope.bootstrap_scope)
        except SeasonBootstrapError as error:
            raise SeasonCanaryError("base bootstrap contract/integrity failure; campaign stopped") from error
    else:
        reused_raw += len(reusable_base)
        context = _season_context(conn, scope=scope)

    targets = _selected_statistics_targets(conn, context=context, scope=scope)
    for index, target in enumerate(targets, start=1):
        used_network, headers = _run_one_statistic(
            conn, client=api, provider_id=context.provider_id, target=target
        )
        physical_calls += int(used_network)
        reused_raw += int(not used_network)
        if headers:
            quota = headers
            _assert_quota_reserve(quota, future_calls=(FIXTURE_CALL_COUNT - index) + FIXTURE_CALL_COUNT)
        if physical_calls > MAX_PHYSICAL_CALLS:
            raise AssertionError("canary physical call cap exceeded")

    for index, stat_target in enumerate(targets, start=1):
        target = _lineup_target(conn, context=context, fixture_id=stat_target.fixture_id)
        used_network, headers = _run_one_lineup(
            conn, client=api, provider_id=context.provider_id, target=target
        )
        physical_calls += int(used_network)
        reused_raw += int(not used_network)
        if headers:
            quota = headers
            _assert_quota_reserve(quota, future_calls=FIXTURE_CALL_COUNT - index)
        if physical_calls > MAX_PHYSICAL_CALLS:
            raise AssertionError("canary physical call cap exceeded")

    verification = _verify(
        conn,
        context=context,
        scope=scope,
        epl_2024_before=epl_2024_before,
        player_refs_before=player_refs_before,
        coach_refs_before=coach_refs_before,
    )
    return SeasonCanaryReport(
        physical_api_calls=physical_calls,
        reused_raw_fetches=reused_raw,
        quota=quota,
        season_id=context.season_id,
        selected_fixture_external_ids=scope.selected_fixture_external_ids,
        verification=verification,
    )


def main() -> None:
    raise SystemExit("Use run_controlled_canary(scope=...) from an approved operator command")


if __name__ == "__main__":
    main()
