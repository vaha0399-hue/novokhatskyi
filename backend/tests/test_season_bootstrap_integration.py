from __future__ import annotations

import copy
import hashlib
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
import pytest

from app.api_football import APIFootballResponse
from app.importer.season_bootstrap import (
    BootstrapScope,
    CollectedBaseResponse,
    ExcludedFixtureContract,
    RegularSeasonProjection,
    SeasonBootstrapError,
    base_requests,
    bootstrap_base,
)


TEST_DB_URL = os.environ.get("SEASON_BOOTSTRAP_TEST_DB_URL")
pytestmark = pytest.mark.skipif(not TEST_DB_URL, reason="SEASON_BOOTSTRAP_TEST_DB_URL is not configured")
SAMPLES = Path(__file__).parents[2] / "samples" / "api-football" / "pro-canary-2026-08-22"


def _payload(name: str) -> dict:
    return json.loads((SAMPLES / name).read_text())


def _response(payload: dict) -> APIFootballResponse:
    raw = json.dumps(payload, separators=(",", ":")).encode()
    return APIFootballResponse(payload, raw, 200, {})


def _collected() -> tuple[CollectedBaseResponse, ...]:
    scope = BootstrapScope(league_external_id=39, season_start_year=2025, expected_fixture_count=380)
    payloads = {
        "/leagues": _payload("01-leagues-epl-2025.raw.json"),
        "/teams": _payload("03-teams-epl-2025.raw.json"),
        "/standings": _payload("04-standings-epl-2025.raw.json"),
        "/fixtures": _payload("06-fixtures-epl-2025-completed.raw.json"),
    }
    payloads["/fixtures"] = copy.deepcopy(payloads["/fixtures"])
    payloads["/fixtures"]["parameters"] = {"league": "39", "season": "2025"}
    now = datetime.now(UTC)
    return tuple(
        CollectedBaseResponse(
            request=request,
            response=_response(payloads[request.endpoint]),
            request_started_at=now - timedelta(seconds=1),
            response_received_at=now,
        )
        for request in base_requests(scope)
    )


def _projected_scope() -> BootstrapScope:
    return BootstrapScope(
        league_external_id=39,
        season_start_year=2026,
        expected_fixture_count=380,
        projection=RegularSeasonProjection(
            expected_raw_team_count=21,
            expected_raw_fixture_count=382,
            excluded_team_external_ids=frozenset({999_185}),
            excluded_fixtures=(
                ExcludedFixtureContract(9_001, 33, 999_185, "Final", "FT"),
                ExcludedFixtureContract(9_002, 999_185, 33, "Final", "AET"),
            ),
        ),
    )


def _projected_collected() -> tuple[CollectedBaseResponse, ...]:
    scope = _projected_scope()
    baseline = {item.request.endpoint: copy.deepcopy(item.response.data) for item in _collected()}
    leagues = baseline["/leagues"]
    leagues["parameters"] = {"id": "39", "season": "2026"}
    season = next(item for item in leagues["response"][0]["seasons"] if item["year"] == 2025)
    projected_season = copy.deepcopy(season)
    projected_season["year"] = 2026
    leagues["response"][0]["seasons"].append(projected_season)

    teams = baseline["/teams"]
    teams["parameters"] = {"league": "39", "season": "2026"}
    extra_team = copy.deepcopy(teams["response"][0])
    extra_team["team"].update({"id": 999_185, "name": "Raw-only playoff club"})
    teams["response"].append(extra_team)
    teams["results"] = len(teams["response"])

    fixtures = baseline["/fixtures"]
    fixtures["parameters"] = {"league": "39", "season": "2026"}
    for fixture in fixtures["response"]:
        fixture["league"]["season"] = 2026
        fixture["fixture"]["id"] += 20_000_000
    fixture_template = copy.deepcopy(fixtures["response"][0])
    for fixture_id, home_id, away_id, status in (
        (9_001, 33, 999_185, "FT"),
        (9_002, 999_185, 33, "AET"),
    ):
        extra_fixture = copy.deepcopy(fixture_template)
        extra_fixture["fixture"]["id"] = fixture_id
        extra_fixture["fixture"]["status"]["short"] = status
        extra_fixture["league"]["round"] = "Final"
        extra_fixture["teams"]["home"]["id"] = home_id
        extra_fixture["teams"]["away"]["id"] = away_id
        fixtures["response"].append(extra_fixture)
    fixtures["results"] = len(fixtures["response"])

    standings = baseline["/standings"]
    standings["parameters"] = {"league": "39", "season": "2026"}
    standings["response"][0]["league"]["season"] = 2026

    now = datetime.now(UTC)
    return tuple(
        CollectedBaseResponse(
            request=request,
            response=_response(baseline[request.endpoint]),
            request_started_at=now - timedelta(seconds=1),
            response_received_at=now,
        )
        for request in base_requests(scope)
    )


def _epl_2024_fingerprint(conn: psycopg.Connection) -> str:
    row = conn.execute(
        """WITH season AS (
                SELECT ref.season_id
                FROM source.season_provider_refs ref
                WHERE ref.provider_id=3 AND ref.league_external_id='39' AND ref.external_season=2024
            ), pieces AS (
                SELECT 'fixture:' || row_to_json(f)::text AS value
                FROM football.fixtures f WHERE f.season_id=(SELECT season_id FROM season)
                UNION ALL
                SELECT 'stat:' || row_to_json(s)::text
                FROM football.fixture_team_statistics s
                JOIN football.fixtures f ON f.id=s.fixture_id
                WHERE f.season_id=(SELECT season_id FROM season)
                UNION ALL
                SELECT 'status:' || row_to_json(status)::text
                FROM source.fixture_provider_status status
                JOIN football.fixtures f ON f.id=status.fixture_id
                WHERE f.season_id=(SELECT season_id FROM season)
            ) SELECT md5(coalesce(string_agg(value,'' ORDER BY value),'')) FROM pieces"""
    ).fetchone()
    return str(row[0])


def test_bootstrap_creates_2025_base_without_mutating_epl_2024() -> None:
    assert TEST_DB_URL is not None
    scope = BootstrapScope(league_external_id=39, season_start_year=2025, expected_fixture_count=380)
    with psycopg.connect(TEST_DB_URL) as conn:
        before = _epl_2024_fingerprint(conn)
        context = bootstrap_base(conn, collected=_collected(), scope=scope)
        assert context.scope == scope.season_scope

        assert conn.execute("SELECT count(*) FROM football.season_teams WHERE season_id=%s", (context.season_id,)).fetchone()[0] == 20
        assert conn.execute("SELECT count(*) FROM football.fixtures WHERE season_id=%s", (context.season_id,)).fetchone()[0] == 380
        assert conn.execute(
            "SELECT count(*) FROM source.fixture_provider_status WHERE provider_id=%s AND status_code='FT'",
            (context.provider_id,),
        ).fetchone()[0] == 760
        assert conn.execute(
            "SELECT count(*) FROM source.fixture_provider_status status JOIN football.fixtures fixture ON fixture.id=status.fixture_id WHERE fixture.season_id=%s",
            (context.season_id,),
        ).fetchone()[0] == 380
        assert conn.execute("SELECT count(*) FROM source.season_coverage_snapshots WHERE season_id=%s", (context.season_id,)).fetchone()[0] == 1
        assert conn.execute("SELECT count(*) FROM football.standings_snapshots WHERE season_id=%s", (context.season_id,)).fetchone()[0] == 1
        assert conn.execute(
            """SELECT count(*) FROM football.standings_snapshot_rows row
               JOIN football.standings_snapshots snapshot ON snapshot.id=row.snapshot_id
               WHERE snapshot.season_id=%s""",
            (context.season_id,),
        ).fetchone()[0] == 20
        hashes = conn.execute(
            """SELECT provider_fetch.content_sha256, raw.inline_body, raw.byte_count
               FROM source.provider_fetches provider_fetch
               JOIN source.provider_raw_payloads raw ON raw.fetch_id=provider_fetch.id
               WHERE provider_fetch.subject_season_id=%s ORDER BY provider_fetch.id""",
            (context.season_id,),
        ).fetchall()
        assert len(hashes) == 4
        assert all(bytes(digest) == hashlib.sha256(bytes(body)).digest() and count == len(bytes(body)) for digest, body, count in hashes)
        assert conn.execute("SELECT count(*) FROM football.fixture_lineup_snapshots").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM football.fixture_availability_snapshots").fetchone()[0] == 0
        assert _epl_2024_fingerprint(conn) == before


def test_projection_keeps_postseason_only_in_immutable_raw_provenance() -> None:
    assert TEST_DB_URL is not None
    scope = _projected_scope()
    with psycopg.connect(TEST_DB_URL) as conn:
        before = _epl_2024_fingerprint(conn)
        context = bootstrap_base(conn, collected=_projected_collected(), scope=scope)

        assert conn.execute(
            "SELECT count(*) FROM football.season_teams WHERE season_id=%s", (context.season_id,)
        ).fetchone()[0] == 20
        assert conn.execute(
            "SELECT count(*) FROM football.fixtures WHERE season_id=%s", (context.season_id,)
        ).fetchone()[0] == 380
        assert conn.execute(
            "SELECT count(*) FROM source.fixture_provider_refs WHERE provider_id=%s AND external_id IN ('9001','9002')",
            (context.provider_id,),
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT count(*) FROM source.team_provider_refs WHERE provider_id=%s AND external_id='999185'",
            (context.provider_id,),
        ).fetchone()[0] == 0
        assert conn.execute(
            """SELECT count(*) FROM source.fixture_provider_status status
               JOIN football.fixtures fixture ON fixture.id=status.fixture_id
               WHERE fixture.season_id=%s""",
            (context.season_id,),
        ).fetchone()[0] == 380
        raw = conn.execute(
            """SELECT provider_fetch.provider_results, raw.retention_class::text, raw.expires_at,
                      provider_fetch.content_sha256, raw.inline_body, raw.byte_count
               FROM source.provider_fetches provider_fetch
               JOIN source.provider_raw_payloads raw ON raw.fetch_id=provider_fetch.id
               WHERE provider_fetch.subject_season_id=%s AND provider_fetch.endpoint='/fixtures'""",
            (context.season_id,),
        ).fetchone()
        assert raw is not None
        results, retention, expires_at, digest, body, byte_count = raw
        assert (results, retention, expires_at) == (382, "contract_sample", None)
        assert bytes(digest) == hashlib.sha256(bytes(body)).digest()
        assert byte_count == len(bytes(body))
        assert _epl_2024_fingerprint(conn) == before


def test_contract_failure_rolls_back_every_2025_canonical_row(monkeypatch: pytest.MonkeyPatch) -> None:
    assert TEST_DB_URL is not None
    scope = BootstrapScope(league_external_id=39, season_start_year=2025, expected_fixture_count=380)
    import app.importer.season_bootstrap as bootstrap_module

    def fail_after_mappings(*args, **kwargs):
        raise RuntimeError("injected fixture normalization failure")

    monkeypatch.setattr(bootstrap_module, "normalize_fixture_season", fail_after_mappings)
    with psycopg.connect(TEST_DB_URL) as conn:
        before_seasons = conn.execute(
            "SELECT count(*) FROM source.season_provider_refs WHERE provider_id=3 AND league_external_id='39' AND external_season=2025"
        ).fetchone()[0]
        before_fetches = conn.execute(
            "SELECT count(*) FROM source.provider_fetches WHERE request_params @> '{\"season\": 2025}'::jsonb"
        ).fetchone()[0]
        with pytest.raises(RuntimeError, match="injected"):
            bootstrap_base(conn, collected=_collected(), scope=scope)
        assert conn.execute(
            "SELECT count(*) FROM source.season_provider_refs WHERE provider_id=3 AND league_external_id='39' AND external_season=2025"
        ).fetchone()[0] == before_seasons
        assert conn.execute(
            "SELECT count(*) FROM source.provider_fetches WHERE request_params @> '{\"season\": 2025}'::jsonb"
        ).fetchone()[0] == before_fetches
