from __future__ import annotations

import copy
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import psycopg
import pytest

from app.api_football import APIFootballResponse
from app.importer.season_canary import FixtureExpectation, SeasonCanaryScope, run_controlled_canary


TEST_DB_URL = os.environ.get("SEASON_BOOTSTRAP_TEST_DB_URL")
pytestmark = pytest.mark.skipif(not TEST_DB_URL, reason="SEASON_BOOTSTRAP_TEST_DB_URL is not configured")
SAMPLES = Path(__file__).parents[2] / "samples" / "api-football"
PRO_SAMPLES = SAMPLES / "pro-canary-2026-08-22"
SELECTED = (1378969, 1378970, 1378974, 1378971, 1378973)


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def _response(payload: dict[str, Any]) -> APIFootballResponse:
    raw = json.dumps(payload, separators=(",", ":")).encode()
    return APIFootballResponse(payload, raw, 200, {"x-ratelimit-requests-remaining": "7000"})


def _fixture_team_info() -> dict[int, tuple[dict[str, Any], dict[str, Any]]]:
    payload = _read(PRO_SAMPLES / "06-fixtures-epl-2025-completed.raw.json")
    return {
        item["fixture"]["id"]: (item["teams"]["home"], item["teams"]["away"])
        for item in payload["response"]
        if item["fixture"]["id"] in SELECTED
    }


def _expectations() -> tuple[FixtureExpectation, ...]:
    payload = _read(PRO_SAMPLES / "06-fixtures-epl-2025-completed.raw.json")
    rows = {item["fixture"]["id"]: item for item in payload["response"]}
    return tuple(
        FixtureExpectation(
            external_id=fixture_id,
            kickoff_at=datetime.fromisoformat(rows[fixture_id]["fixture"]["date"]),
            home_external_id=rows[fixture_id]["teams"]["home"]["id"],
            away_external_id=rows[fixture_id]["teams"]["away"]["id"],
        )
        for fixture_id in SELECTED
    )


def _statistics(fixture_id: int, home: dict[str, Any], away: dict[str, Any]) -> APIFootballResponse:
    payload = copy.deepcopy(_read(PRO_SAMPLES / "09-fixture-statistics-single.raw.json"))
    payload["parameters"] = {"fixture": str(fixture_id)}
    payload["response"][0]["team"].update({"id": home["id"], "name": home["name"]})
    payload["response"][1]["team"].update({"id": away["id"], "name": away["name"]})
    return _response(payload)


def _lineups(fixture_id: int, home: dict[str, Any], away: dict[str, Any], index: int) -> APIFootballResponse:
    payload = copy.deepcopy(_read(SAMPLES / "lineups.raw.json"))
    payload["parameters"] = {"fixture": str(fixture_id)}
    for team_index, (entry, team) in enumerate(zip(payload["response"], (home, away), strict=True)):
        entry["team"].update({"id": team["id"], "name": team["name"]})
        entry["coach"]["id"] = 900_000 + index * 10 + team_index
        for role_index, role in enumerate(("startXI", "substitutes")):
            for player_index, wrapper in enumerate(entry[role]):
                wrapper["player"]["id"] = (
                    1_000_000 + index * 10_000 + team_index * 1_000 + role_index * 100 + player_index
                )
    return _response(payload)


class QueuedClient:
    def __init__(self, responses: dict[tuple[str, tuple[tuple[str, int], ...]], APIFootballResponse]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, dict[str, int]]] = []

    async def get(self, endpoint: str, *, params: dict[str, int]) -> APIFootballResponse:
        self.calls.append((endpoint, dict(params)))
        return self.responses[(endpoint, tuple(sorted(params.items())))]

    @staticmethod
    def response_contains_api_key(_: bytes) -> bool:
        return False


class NoNetworkClient:
    async def get(self, endpoint: str, *, params: dict[str, int]) -> APIFootballResponse:
        raise AssertionError(f"replay must not call API-Football: {endpoint} {params}")

    @staticmethod
    def response_contains_api_key(_: bytes) -> bool:
        return False


def test_full_14_call_canary_is_atomic_per_fixture_and_preserves_epl_2024(monkeypatch: pytest.MonkeyPatch) -> None:
    assert TEST_DB_URL is not None
    monkeypatch.setenv("SUPABASE_DB_URL", TEST_DB_URL)
    teams = _fixture_team_info()
    fixtures = _read(PRO_SAMPLES / "06-fixtures-epl-2025-completed.raw.json")
    fixtures["parameters"] = {"league": "39", "season": "2025"}
    base_payloads = {
        "/leagues": _read(PRO_SAMPLES / "01-leagues-epl-2025.raw.json"),
        "/teams": _read(PRO_SAMPLES / "03-teams-epl-2025.raw.json"),
        "/standings": _read(PRO_SAMPLES / "04-standings-epl-2025.raw.json"),
        "/fixtures": fixtures,
    }
    responses: dict[tuple[str, tuple[tuple[str, int], ...]], APIFootballResponse] = {
        ("/leagues", (("id", 39), ("season", 2025))): _response(base_payloads["/leagues"]),
        ("/teams", (("league", 39), ("season", 2025))): _response(base_payloads["/teams"]),
        ("/standings", (("league", 39), ("season", 2025))): _response(base_payloads["/standings"]),
        ("/fixtures", (("league", 39), ("season", 2025))): _response(base_payloads["/fixtures"]),
    }
    for index, fixture_id in enumerate(SELECTED):
        home, away = teams[fixture_id]
        responses[("/fixtures/statistics", (("fixture", fixture_id),))] = _statistics(fixture_id, home, away)
        responses[("/fixtures/lineups", (("fixture", fixture_id),))] = _lineups(fixture_id, home, away, index)

    client = QueuedClient(responses)
    report = run_controlled_canary(
        client=client,  # type: ignore[arg-type]
        scope=SeasonCanaryScope(
            league_external_id=39,
            season_start_year=2025,
            expected_fixture_count=380,
            selected_fixture_external_ids=SELECTED,
            selected_fixture_expectations=_expectations(),
        ),
    )

    assert report.physical_api_calls == 14
    assert len(client.calls) == 14
    assert report.verification["fixtures"] == 380
    assert report.verification["exact_provider_statuses"] == 380
    assert report.verification["statistics_complete"] == 5
    assert report.verification["historical_lineups_complete"] == 5
    assert report.verification["duplicates"] == 0
    assert report.verification["orphans_or_nonparticipants"] == 0
    assert report.verification["prematch_rows"] == 0
    assert report.verification["epl_2024_fingerprint_unchanged"] is True

    with psycopg.connect(TEST_DB_URL) as conn:
        assert conn.execute(
            "SELECT count(*) FROM football.fixture_historical_lineup_snapshots snapshot JOIN football.fixtures fixture ON fixture.id=snapshot.fixture_id WHERE fixture.season_id=%s",
            (report.season_id,),
        ).fetchone()[0] == 5
        assert conn.execute(
            "SELECT count(*) FROM football.fixture_team_statistics stat JOIN football.fixtures fixture ON fixture.id=stat.fixture_id WHERE fixture.season_id=%s",
            (report.season_id,),
        ).fetchone()[0] == 10

    replay = run_controlled_canary(
        client=NoNetworkClient(),  # type: ignore[arg-type]
        scope=SeasonCanaryScope(
            league_external_id=39,
            season_start_year=2025,
            expected_fixture_count=380,
            selected_fixture_external_ids=SELECTED,
            selected_fixture_expectations=_expectations(),
        ),
    )
    assert replay.physical_api_calls == 0
    assert replay.reused_raw_fetches == 14
    assert replay.verification["epl_2024_fingerprint_unchanged"] is True
