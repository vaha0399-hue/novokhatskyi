import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
import pytest

from app.api_football import APIFootballResponse
from app.importer.canary import CANARY_REQUESTS, CollectedResponse, normalize_canary
from app.importer.fixture_status_contract import FixtureStatusObservation
from app.importer.season_backfill import (
    REQUEST_PARAMS,
    CollectedFetch,
    acquire_context_and_lock,
    canary_fixture_snapshot,
    load_reusable_fetch,
    normalize_fixture_season,
    persist_success_fetch,
    run_backfill,
    table_counts,
    validate_fixture_season_response,
    verify_remote,
    UNTOUCHED_TABLES,
)

SAMPLES = Path(__file__).parents[2] / "samples" / "api-football"
TEST_DB_URL = os.environ.get("BACKFILL_TEST_DB_URL")

pytestmark = pytest.mark.skipif(not TEST_DB_URL, reason="BACKFILL_TEST_DB_URL is not configured")


def _raw(name: str) -> dict:
    return json.loads((SAMPLES / f"{name}.raw.json").read_text())


def _response(payload: dict) -> APIFootballResponse:
    body = json.dumps(payload, separators=(",", ":")).encode()
    return APIFootballResponse(payload, body, 200, {})


def _canary_collected() -> list[CollectedResponse]:
    now = datetime.now(UTC)
    fixtures = _raw("fixtures")
    fixture_entry = next(item for item in fixtures["response"] if item["fixture"]["id"] == 1208021)
    injuries = _raw("injuries")
    injury_entries = [item for item in injuries["response"] if item["fixture"]["id"] == 1208021]
    payloads = {
        "fixture": {
            **fixtures,
            "parameters": {"id": "1208021"},
            "results": 1,
            "response": [fixture_entry],
        },
        "teams": _raw("teams"),
        "standings": _raw("standings"),
        "fixture_statistics": _raw("fixture-statistics"),
        "injuries": {
            **injuries,
            "parameters": {"fixture": "1208021"},
            "results": len(injury_entries),
            "response": injury_entries,
        },
        "lineups": _raw("lineups"),
    }
    return [
        CollectedResponse(
            spec=spec,
            response=_response(payloads[spec.name]),
            request_started_at=now - timedelta(seconds=1),
            response_received_at=now,
        )
        for spec in CANARY_REQUESTS
    ]


class NoNetworkClient:
    def __init__(self) -> None:
        self.calls = 0

    async def get(self, endpoint: str, *, params):
        self.calls += 1
        raise AssertionError("replay must not call API-Football")

    def response_contains_api_key(self, body: bytes) -> bool:
        return False


def test_atomic_rollback_raw_resume_and_idempotent_replay(monkeypatch: pytest.MonkeyPatch) -> None:
    assert TEST_DB_URL is not None
    monkeypatch.setenv("SUPABASE_DB_URL", TEST_DB_URL)
    with psycopg.connect(TEST_DB_URL) as seed_conn:
        normalize_canary(seed_conn, _canary_collected())

    season_response = _response(_raw("fixtures"))
    with psycopg.connect(TEST_DB_URL, autocommit=True) as conn:
        context = acquire_context_and_lock(conn)
        canary_before = canary_fixture_snapshot(conn, context)
        untouched_before = table_counts(conn, UNTOUCHED_TABLES)
        collected = CollectedFetch(
            response=season_response,
            request_started_at=datetime.now(UTC) - timedelta(seconds=1),
            response_received_at=datetime.now(UTC),
            attempts=1,
        )
        fetch = persist_success_fetch(conn, context=context, collected=collected)
        records = validate_fixture_season_response(
            fetch.response,
            allowed_team_external_ids=context.team_ids,
        )
        statuses = tuple(
            FixtureStatusObservation(external_fixture_id=record.external_id, status_code="FT")
            for record in records
        )

        with pytest.raises(RuntimeError, match="injected"):
            normalize_fixture_season(
                conn,
                context=context,
                fetch=fetch,
                records=records,
                status_observations=statuses,
                fail_after_chunk=3,
            )

        fixture_count = conn.execute(
            "SELECT count(*) FROM football.fixtures WHERE season_id = %s",
            (context.season_id,),
        ).fetchone()[0]
        normalized_at = conn.execute(
            "SELECT normalized_at FROM source.provider_fetches WHERE id = %s",
            (fetch.fetch_id,),
        ).fetchone()[0]
        assert fixture_count == 1
        assert normalized_at is None
        assert load_reusable_fetch(conn, context).fetch_id == fetch.fetch_id

        result = normalize_fixture_season(
            conn,
            context=context,
            fetch=fetch,
            records=records,
            status_observations=statuses,
        )
        assert result == {"processed": 380, "created": 379, "batches": 8}
        verification = verify_remote(
            conn,
            context=context,
            fetch_id=fetch.fetch_id,
            canary_before=canary_before,
            untouched_before=untouched_before,
        )
        assert verification["fixtures"] == 380
        assert verification["canary_unchanged"] is True

    client = NoNetworkClient()
    replay = run_backfill(client=client)  # type: ignore[arg-type]
    assert client.calls == 0
    assert replay["api_attempts"] == 0
    assert replay["reused_raw_fetch"] is True
    assert replay["verification"]["fixtures"] == 380
