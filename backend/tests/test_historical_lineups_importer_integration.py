import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
import pytest

from app.api_football import APIFootballResponse
from app.importer.historical_lineups import (
    HistoricalLineupsError,
    HistoricalLineupsContractError,
    PURPOSE,
    FixtureTarget,
    _load_retained_raw,
    _persist_success_fetch,
    _resume_unfinished_success_fetches,
    approve_contract_replays,
    normalize_raw,
    run_historical_lineups_backfill,
    run_controlled_canary,
)

SAMPLE = Path(__file__).parents[2] / "samples" / "api-football" / "lineups.raw.json"
TEST_DB_URL = os.environ.get("HISTORICAL_LINEUPS_TEST_DB_URL")
pytestmark = pytest.mark.skipif(not TEST_DB_URL, reason="HISTORICAL_LINEUPS_TEST_DB_URL is not configured")


def _target(conn: psycopg.Connection, provider_id: int) -> FixtureTarget:
    fixture_id = 1
    conn.execute("UPDATE source.fixture_provider_refs SET external_id='1208021' WHERE provider_id=%s AND fixture_id=%s", (provider_id, fixture_id))
    fixture = conn.execute("SELECT home_team_id,away_team_id,season_id,kickoff_at,result_finalized_at FROM football.fixtures WHERE id=%s", (fixture_id,)).fetchone()
    assert fixture is not None
    home_id, away_id, season_id, kickoff_at, finalized_at = fixture
    conn.execute("UPDATE source.team_provider_refs SET external_id='33' WHERE provider_id=%s AND team_id=%s", (provider_id, home_id))
    conn.execute("UPDATE source.team_provider_refs SET external_id='36' WHERE provider_id=%s AND team_id=%s", (provider_id, away_id))
    return FixtureTarget(fixture_id, season_id, 1208021, home_id, away_id, 33, 36, kickoff_at, finalized_at)


def _existing_target(conn: psycopg.Connection, provider_id: int, fixture_id: int) -> FixtureTarget:
    row = conn.execute(
        """SELECT fixture.season_id, fixture_ref.external_id, fixture.home_team_id, fixture.away_team_id,
                  home_ref.external_id, away_ref.external_id, fixture.kickoff_at, fixture.result_finalized_at
           FROM football.fixtures fixture
           JOIN source.fixture_provider_refs fixture_ref
             ON fixture_ref.provider_id=%s AND fixture_ref.fixture_id=fixture.id
           JOIN source.team_provider_refs home_ref
             ON home_ref.provider_id=%s AND home_ref.team_id=fixture.home_team_id
           JOIN source.team_provider_refs away_ref
             ON away_ref.provider_id=%s AND away_ref.team_id=fixture.away_team_id
           WHERE fixture.id=%s""",
        (provider_id, provider_id, provider_id, fixture_id),
    ).fetchone()
    assert row is not None
    return FixtureTarget(fixture_id, row[0], int(row[1]), row[2], row[3], int(row[4]), int(row[5]), row[6], row[7])


def _after_finalization(target: FixtureTarget) -> datetime:
    """A deterministic historical observation time that satisfies the DB guard."""
    return max(
        target.result_finalized_at + timedelta(days=1),
        datetime.now(UTC) + timedelta(minutes=1),
    )


def _response(payload: dict) -> APIFootballResponse:
    raw_body = json.dumps(payload, separators=(",", ":")).encode()
    return APIFootballResponse(payload, raw_body, 200, {})


async def _no_sleep(_: float) -> None:
    return None


class PartialLineupClient:
    def __init__(self, home_teams: dict[int, int]) -> None:
        self.home_teams = home_teams
        self.calls: list[int] = []

    async def get(self, endpoint: str, *, params: dict[str, int]) -> APIFootballResponse:
        assert endpoint == "/fixtures/lineups"
        external_fixture_id = params["fixture"]
        self.calls.append(external_fixture_id)
        return _response(
            {
                "get": "fixtures/lineups",
                "parameters": {"fixture": str(external_fixture_id)},
                "errors": [],
                "results": 1,
                "paging": {"current": 1, "total": 1},
                "response": [{
                    "team": {"id": self.home_teams[external_fixture_id]},
                    "coach": None,
                    "formation": None,
                    "startXI": [],
                    "substitutes": [],
                }],
            }
        )

    def response_contains_api_key(self, body: bytes) -> bool:
        return False


def test_real_sample_normalizes_raw_provenance_mappings_and_replay(monkeypatch: pytest.MonkeyPatch) -> None:
    assert TEST_DB_URL is not None
    monkeypatch.setenv("SUPABASE_DB_URL", TEST_DB_URL)
    payload = json.loads(SAMPLE.read_text())
    response = _response(payload)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as conn:
        provider_id = conn.execute("SELECT id FROM source.providers WHERE code='api-football'").fetchone()[0]
        target = _target(conn, provider_id)
        before_prematch = {
            table: conn.execute(f"SELECT count(*) FROM football.{table}").fetchone()[0]
            for table in ("fixture_lineup_snapshots", "fixture_lineups", "fixture_lineup_players")
        }
        received = _after_finalization(target)
        stored = _persist_success_fetch(
            conn, provider_id=provider_id, target=target, response=response,
            request_started_at=received - timedelta(seconds=1), response_received_at=received,
        )
        result = normalize_raw(conn, provider_id=provider_id, target=target, raw=stored)
        assert result.coverage_state == "complete"
        assert result.snapshot_created is True
        assert result.team_lineups_created == 2
        assert result.lineup_players_created == 40
        assert result.entities.players_created == 40
        assert result.entities.coaches_created == 2
        assert conn.execute("SELECT count(*) FROM football.fixture_historical_lineup_snapshots WHERE fixture_id=%s", (target.fixture_id,)).fetchone()[0] == 1
        assert conn.execute("SELECT count(*) FROM football.fixture_historical_lineups").fetchone()[0] == 2
        assert conn.execute("SELECT count(*) FROM football.fixture_historical_lineup_players").fetchone()[0] == 40
        assert conn.execute("SELECT count(*) FROM football.fixture_historical_lineup_players WHERE grid IS NULL").fetchone()[0] == 18
        assert conn.execute("SELECT count(*) FROM source.provider_fetches WHERE id=%s AND purpose=%s AND normalized_at IS NOT NULL", (stored.fetch_id, PURPOSE)).fetchone()[0] == 1
        assert conn.execute("SELECT count(*) FROM source.player_provider_refs WHERE provider_id=%s", (provider_id,)).fetchone()[0] == 40
        assert conn.execute("SELECT count(*) FROM source.coach_provider_refs WHERE provider_id=%s", (provider_id,)).fetchone()[0] == 2
        assert {table: conn.execute(f"SELECT count(*) FROM football.{table}").fetchone()[0] for table in before_prematch} == before_prematch
        replay = normalize_raw(conn, provider_id=provider_id, target=target, raw=_load_retained_raw(conn, provider_id=provider_id, target=target, fetch_id=stored.fetch_id))
        assert replay.replayed is True
        assert replay.snapshot_created is False
        assert conn.execute("SELECT count(*) FROM football.fixture_historical_lineup_snapshots WHERE fixture_id=%s", (target.fixture_id,)).fetchone()[0] == 1


def test_null_coach_identity_replays_retained_contract_anomaly_without_network() -> None:
    """A provider coach object with null identity is factual absence, not a failure."""
    assert TEST_DB_URL is not None
    payload = json.loads(SAMPLE.read_text())
    payload["response"][1]["coach"] = {"id": None, "name": None, "photo": None}
    payload["response"][1]["formation"] = None

    with psycopg.connect(TEST_DB_URL, autocommit=True) as conn:
        provider_id = conn.execute("SELECT id FROM source.providers WHERE code='api-football'").fetchone()[0]
        target = _existing_target(conn, provider_id, 11)
        payload["parameters"] = {"fixture": str(target.external_id)}
        payload["response"][0]["team"]["id"] = target.home_external_id
        payload["response"][1]["team"]["id"] = target.away_external_id
        received = _after_finalization(target)
        stored = _persist_success_fetch(
            conn, provider_id=provider_id, target=target, response=_response(payload),
            request_started_at=received - timedelta(seconds=1), response_received_at=received,
        )
        # Simulate the durable state left by the older strict parser. Reopening
        # is explicit, hash-checked, and the raw response is then replayed.
        conn.execute(
            """UPDATE source.provider_fetches
               SET outcome='provider_error', sanitized_error_class='HistoricalLineupsContractError'
               WHERE id=%s""",
            (stored.fetch_id,),
        )
        conn.execute(
            "UPDATE source.provider_raw_payloads SET retention_class='anomaly' WHERE fetch_id=%s",
            (stored.fetch_id,),
        )

        assert approve_contract_replays(
            conn, provider_id=provider_id, season_id=target.season_id,
            fetch_ids=frozenset({stored.fetch_id}),
        ) == 1
        resumed = _resume_unfinished_success_fetches(
            conn, provider_id=provider_id, season_id=target.season_id, clock=lambda: received
        )

        assert len(resumed) == 1
        assert resumed[0].coverage_state == "complete"
        assert conn.execute(
            """SELECT count(*) FILTER (WHERE coach_id IS NULL),
                      count(*) FILTER (WHERE formation IS NULL)
               FROM football.fixture_historical_lineups
               WHERE snapshot_id=(SELECT id FROM football.fixture_historical_lineup_snapshots WHERE fixture_id=%s)""",
            (target.fixture_id,),
        ).fetchone() == (1, 1)
        assert conn.execute(
            "SELECT outcome::text, normalized_at IS NOT NULL FROM source.provider_fetches WHERE id=%s",
            (stored.fetch_id,),
        ).fetchone() == ("success", True)


def test_runner_continues_after_partial_lineups(monkeypatch: pytest.MonkeyPatch) -> None:
    assert TEST_DB_URL is not None
    monkeypatch.setenv("SUPABASE_DB_URL", TEST_DB_URL)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as conn:
        provider_id = conn.execute("SELECT id FROM source.providers WHERE code='api-football'").fetchone()[0]
        home_teams = {
            int(fixture_external_id): int(home_external_id)
            for fixture_external_id, home_external_id in conn.execute(
                """SELECT fixture_ref.external_id, home_ref.external_id
                   FROM football.fixtures fixture
                   JOIN source.fixture_provider_refs fixture_ref
                     ON fixture_ref.provider_id=%s AND fixture_ref.fixture_id=fixture.id
                   JOIN source.team_provider_refs home_ref
                     ON home_ref.provider_id=%s AND home_ref.team_id=fixture.home_team_id""",
                (provider_id, provider_id),
            ).fetchall()
        }
    client = PartialLineupClient(home_teams)

    safe_clock = datetime.now(UTC) + timedelta(minutes=1)
    report = run_controlled_canary(
        client=client, sleep=_no_sleep, clock=lambda: safe_clock
    )  # type: ignore[arg-type]

    assert len(client.calls) == 10
    assert report.physical_api_calls == 10
    assert report.complete == 0
    assert report.partial == 10
    assert report.retained_raw_replays == 1
    assert report.replay_fixture_external_id is not None
    assert report.verification["first_batch"]["fixtures"]
    assert report.verification["replay"]["physical_api_calls_added"] == 0


def test_bounded_bulk_persists_partial_lineups_and_keeps_other_domains_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bulk follows the same factual-coverage semantics as the canary."""
    assert TEST_DB_URL is not None
    monkeypatch.setenv("SUPABASE_DB_URL", TEST_DB_URL)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as conn:
        provider_id = conn.execute("SELECT id FROM source.providers WHERE code='api-football'").fetchone()[0]
        home_teams = {
            int(fixture_external_id): int(home_external_id)
            for fixture_external_id, home_external_id in conn.execute(
                """SELECT fixture_ref.external_id, home_ref.external_id
                   FROM football.fixtures fixture
                   JOIN source.fixture_provider_refs fixture_ref
                     ON fixture_ref.provider_id=%s AND fixture_ref.fixture_id=fixture.id
                   JOIN source.team_provider_refs home_ref
                     ON home_ref.provider_id=%s AND home_ref.team_id=fixture.home_team_id""",
                (provider_id, provider_id),
            ).fetchall()
        }

    client = PartialLineupClient(home_teams)
    report = run_historical_lineups_backfill(
        client=client,
        sleep=_no_sleep,
        clock=lambda: datetime.now(UTC) + timedelta(minutes=1),
        max_calls=3,
    )  # type: ignore[arg-type]

    assert report.physical_api_calls == 3
    assert report.partial == 3
    assert report.complete == report.empty == 0
    assert report.snapshots_created == report.team_lineups_created == 3
    assert report.lineup_players_created == 0
    assert report.stop_reason is None
    assert report.remaining_fixtures >= 1
    assert all(report.verification["out_of_scope_fingerprints_unchanged"].values())


def test_recoverable_raw_success_replays_before_new_network_work() -> None:
    assert TEST_DB_URL is not None
    with psycopg.connect(TEST_DB_URL, autocommit=True) as conn:
        provider_id = conn.execute("SELECT id FROM source.providers WHERE code='api-football'").fetchone()[0]
        target = _existing_target(conn, provider_id, 360)
        now = _after_finalization(target)
        payload = {
            "get": "fixtures/lineups", "parameters": {"fixture": str(target.external_id)},
            "errors": [], "results": 0, "paging": {"current": 1, "total": 1}, "response": [],
        }
        stored = _persist_success_fetch(
            conn, provider_id=provider_id, target=target, response=_response(payload),
            request_started_at=now - timedelta(seconds=1), response_received_at=now,
        )
        resumed = _resume_unfinished_success_fetches(
            conn, provider_id=provider_id, season_id=target.season_id, clock=lambda: now
        )
        assert len(resumed) == 1
        assert resumed[0].coverage_state == "empty"
        assert conn.execute("SELECT normalized_at IS NOT NULL FROM source.provider_fetches WHERE id=%s", (stored.fetch_id,)).fetchone()[0] is True
        assert conn.execute("SELECT count(*) FROM football.fixture_historical_lineup_snapshots WHERE fixture_id=%s", (target.fixture_id,)).fetchone()[0] == 1


def test_empty_partial_and_contract_anomaly_are_durable_without_fabricated_rows() -> None:
    assert TEST_DB_URL is not None
    with psycopg.connect(TEST_DB_URL, autocommit=True) as conn:
        provider_id = conn.execute("SELECT id FROM source.providers WHERE code='api-football'").fetchone()[0]
        empty_target = _existing_target(conn, provider_id, 361)
        now = _after_finalization(empty_target)
        empty_payload = {
            "get": "fixtures/lineups", "parameters": {"fixture": str(empty_target.external_id)},
            "errors": [], "results": 0, "paging": {"current": 1, "total": 1}, "response": [],
        }
        empty_raw = _persist_success_fetch(
            conn, provider_id=provider_id, target=empty_target, response=_response(empty_payload),
            request_started_at=now - timedelta(seconds=1), response_received_at=now,
        )
        assert normalize_raw(conn, provider_id=provider_id, target=empty_target, raw=empty_raw).coverage_state == "empty"
        assert conn.execute("SELECT count(*) FROM football.fixture_historical_lineups WHERE snapshot_id=(SELECT id FROM football.fixture_historical_lineup_snapshots WHERE fixture_id=%s)", (empty_target.fixture_id,)).fetchone()[0] == 0

        partial_target = _existing_target(conn, provider_id, 362)
        partial_payload = {
            "get": "fixtures/lineups", "parameters": {"fixture": str(partial_target.external_id)},
            "errors": [], "results": 1, "paging": {"current": 1, "total": 1},
            "response": [{
                "team": {"id": partial_target.home_external_id}, "coach": None, "formation": None,
                "startXI": [], "substitutes": [],
            }],
        }
        partial_raw = _persist_success_fetch(
            conn, provider_id=provider_id, target=partial_target, response=_response(partial_payload),
            request_started_at=now - timedelta(seconds=1), response_received_at=now,
        )
        assert normalize_raw(conn, provider_id=provider_id, target=partial_target, raw=partial_raw).coverage_state == "partial"
        assert conn.execute("SELECT team_count FROM football.fixture_historical_lineup_snapshots WHERE fixture_id=%s", (partial_target.fixture_id,)).fetchone()[0] == 1

        bad_target = _existing_target(conn, provider_id, 363)
        malformed_payload = {
            "get": "fixtures/lineups", "parameters": {"fixture": "wrong"},
            "errors": [], "results": 0, "paging": {"current": 1, "total": 1}, "response": [],
        }
        malformed_raw = _persist_success_fetch(
            conn, provider_id=provider_id, target=bad_target, response=_response(malformed_payload),
            request_started_at=now - timedelta(seconds=1), response_received_at=now,
        )
        with pytest.raises(HistoricalLineupsContractError, match="parameters"):
            normalize_raw(conn, provider_id=provider_id, target=bad_target, raw=malformed_raw)
        assert conn.execute("SELECT count(*) FROM football.fixture_historical_lineup_snapshots WHERE fixture_id=%s", (bad_target.fixture_id,)).fetchone()[0] == 0
        assert conn.execute(
            """SELECT outcome::text, normalized_at IS NULL, sanitized_error_class,
                      (SELECT retention_class::text FROM source.provider_raw_payloads WHERE fetch_id=id)
               FROM source.provider_fetches WHERE id=%s""",
            (malformed_raw.fetch_id,),
        ).fetchone() == ("provider_error", True, "HistoricalLineupsContractError", "anomaly")
        with pytest.raises(HistoricalLineupsError, match="terminal provider_error"):
            _resume_unfinished_success_fetches(
                conn, provider_id=provider_id, season_id=bad_target.season_id, clock=lambda: now
            )
