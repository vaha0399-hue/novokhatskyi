"""Optional read-only contract checks against a configured development database."""

from __future__ import annotations

import asyncio
import os

import httpx
import psycopg
import pytest

from app.main import app


TEST_DB_URL = os.environ.get("READ_API_TEST_DB_URL")
pytestmark = pytest.mark.skipif(not TEST_DB_URL, reason="READ_API_TEST_DB_URL is not configured")


async def _get(path: str) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        return await client.get(path)


def test_development_read_api_contract_is_deterministic_and_cutoff_safe(monkeypatch: pytest.MonkeyPatch) -> None:
    assert TEST_DB_URL is not None
    monkeypatch.setenv("SUPABASE_DB_URL", TEST_DB_URL)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        season_id = connection.execute(
            """SELECT season_id FROM football.fixtures GROUP BY season_id
               HAVING count(*) >= 2 ORDER BY season_id LIMIT 1"""
        ).fetchone()[0]
        team_id = connection.execute(
            """SELECT home_team_id FROM football.fixtures WHERE season_id=%s
               ORDER BY kickoff_at DESC,id DESC LIMIT 1""", (season_id,)
        ).fetchone()[0]
        fixture_id, target_kickoff = connection.execute(
            """SELECT id,kickoff_at FROM football.fixtures WHERE season_id=%s
               ORDER BY kickoff_at DESC,id DESC LIMIT 1""", (season_id,)
        ).fetchone()
        home_team_id = connection.execute(
            "SELECT home_team_id FROM football.fixtures WHERE id=%s", (fixture_id,)
        ).fetchone()[0]
        expected_home_history = connection.execute(
            """SELECT count(*) FROM football.fixtures
               WHERE season_id=%s AND lifecycle_state='completed' AND result_finalized_at IS NOT NULL
                 AND %s IN (home_team_id, away_team_id) AND kickoff_at < %s""",
            (season_id, home_team_id, target_kickoff),
        ).fetchone()[0]

    fixtures = asyncio.run(_get(f"/web/v1/seasons/{season_id}/fixtures?limit=2&offset=0"))
    team = asyncio.run(_get(f"/web/v1/teams/{team_id}/analytics?season_id={season_id}&scope=overall&window=10"))
    fixture = asyncio.run(_get(f"/web/v1/fixtures/{fixture_id}/analytics?window=5"))

    assert fixtures.status_code == team.status_code == fixture.status_code == 200
    fixture_rows = fixtures.json()["fixtures"]
    assert [(row["kickoff_at"], row["id"]) for row in fixture_rows] == sorted(
        (row["kickoff_at"], row["id"]) for row in fixture_rows
    )
    assert team.json()["season_id"] == season_id
    assert team.json()["team"]["id"] == team_id
    body = fixture.json()
    assert body["fixture"]["id"] == fixture_id
    assert body["historical_cutoff_at"] == target_kickoff.isoformat().replace("+00:00", "Z")
    assert body["home"]["overall"]["matches"] == min(5, expected_home_history)
    assert body["away"]["overall"]["matches"] <= 5


def test_development_discovery_standings_and_statistics_are_read_only(monkeypatch: pytest.MonkeyPatch) -> None:
    assert TEST_DB_URL is not None
    monkeypatch.setenv("SUPABASE_DB_URL", TEST_DB_URL)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        league_id, season_id = connection.execute(
            """SELECT l.id,s.id FROM football.leagues l
                 JOIN football.seasons s ON s.league_id=l.id
                 JOIN football.standings_snapshots snapshot ON snapshot.season_id=s.id
                 ORDER BY l.id,s.id LIMIT 1"""
        ).fetchone()
        fixture_id = connection.execute(
            """SELECT f.id FROM football.fixtures f
                 WHERE f.season_id=%s
                   AND EXISTS (SELECT 1 FROM football.fixture_team_statistics stat WHERE stat.fixture_id=f.id)
                 ORDER BY f.kickoff_at ASC,f.id ASC LIMIT 1""",
            (season_id,),
        ).fetchone()[0]

    leagues = asyncio.run(_get("/web/v1/leagues"))
    seasons = asyncio.run(_get(f"/web/v1/leagues/{league_id}/seasons"))
    standings = asyncio.run(_get(f"/web/v1/seasons/{season_id}/standings"))
    statistics = asyncio.run(_get(f"/web/v1/fixtures/{fixture_id}/statistics"))

    assert leagues.status_code == seasons.status_code == standings.status_code == statistics.status_code == 200
    assert any(row["id"] == league_id for row in leagues.json()["leagues"])
    assert any(row["id"] == season_id for row in seasons.json()["seasons"])
    assert standings.json()["groups"]
    assert len(statistics.json()["home"]["metrics"]) == 18
    assert len(statistics.json()["away"]["metrics"]) == 18
