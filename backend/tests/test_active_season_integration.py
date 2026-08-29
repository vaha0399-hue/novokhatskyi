from __future__ import annotations

import json
import os
import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
import pytest

import app.importer.active_season as active_season
from app.api_football import APIFootballResponse
from app.importer.active_season import (
    ActiveSeasonScope,
    base_requests,
    import_active_base,
    verify_active_season,
)
from app.importer.season_bootstrap import CollectedBaseResponse


TEST_DB_URL = os.environ.get("ACTIVE_SEASON_TEST_DB_URL")
pytestmark = pytest.mark.skipif(
    not TEST_DB_URL, reason="ACTIVE_SEASON_TEST_DB_URL is not configured"
)
SAMPLES = Path(__file__).parents[2] / "samples" / "api-football" / "pro-canary-2026-08-29"
SCOPE = ActiveSeasonScope(league_external_id=39, season_start_year=2026, expected_fixture_count=380)


def _response(payload: dict) -> APIFootballResponse:
    raw = json.dumps(payload, separators=(",", ":")).encode()
    return APIFootballResponse(payload, raw, 200, {})


def _stored_response(path: Path) -> APIFootballResponse:
    raw = path.read_bytes()
    return APIFootballResponse(json.loads(raw), raw, 200, {})


def _collected(received_at: datetime) -> tuple[CollectedBaseResponse, ...]:
    files = {
        "/leagues": "01-leagues-epl-2026.raw.json",
        "/teams": "02-teams-epl-2026.raw.json",
        "/standings": "04-standings-epl-2026.raw.json",
        "/fixtures": "03-fixtures-epl-2026.raw.json",
    }
    return tuple(
        CollectedBaseResponse(
            request=request,
            response=_stored_response(SAMPLES / files[request.endpoint]),
            request_started_at=received_at - timedelta(seconds=1),
            response_received_at=received_at,
        )
        for request in base_requests(SCOPE)
    )


def test_real_epl_2026_active_base_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replay retained provider bytes only; never call API-Football or Supabase."""
    assert TEST_DB_URL is not None
    first_received_at = datetime.now(UTC)
    initial_bulk_calls: list[int] = []
    original_bulk_insert = active_season._bulk_insert_initial_fixtures

    def observe_initial_bulk_insert(*args: object, **kwargs: object) -> dict[int, int]:
        records = kwargs["records"]
        assert isinstance(records, tuple)
        initial_bulk_calls.append(len(records))
        return original_bulk_insert(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(active_season, "_bulk_insert_initial_fixtures", observe_initial_bulk_insert)
    with psycopg.connect(TEST_DB_URL) as conn:
        first = import_active_base(conn, collected=_collected(first_received_at), scope=SCOPE)
        report = verify_active_season(conn, scope=SCOPE)

        assert initial_bulk_calls == [380]
        assert first.season_id == report.season_id
        assert (report.team_count, report.fixture_count, report.fixture_mapping_count) == (20, 380, 380)
        assert report.standing_row_count == 20
        assert report.is_complete is True
        assert conn.execute(
            "SELECT lifecycle_state::text, count(*) FROM football.fixtures WHERE season_id=%s GROUP BY lifecycle_state ORDER BY lifecycle_state",
            (first.season_id,),
        ).fetchall() == [("completed", 11), ("scheduled", 369)]
        assert conn.execute(
            """SELECT status.status_code, count(*)
               FROM source.fixture_provider_status status
               JOIN football.fixtures fixture ON fixture.id=status.fixture_id
               WHERE fixture.season_id=%s
               GROUP BY status.status_code ORDER BY status.status_code""",
            (first.season_id,),
        ).fetchall() == [("FT", 11), ("NS", 369)]
        assert conn.execute(
            """SELECT count(*)
               FROM source.fixture_provider_refs ref
               JOIN football.fixtures fixture ON fixture.id=ref.fixture_id
               WHERE ref.provider_id=(SELECT id FROM source.providers WHERE code='api-football')
                 AND fixture.season_id=%s""",
            (first.season_id,),
        ).fetchone()[0] == 380
        assert conn.execute(
            """SELECT count(*)
               FROM source.fixture_provider_status status
               JOIN football.fixtures fixture ON fixture.id=status.fixture_id
               WHERE status.provider_id=(SELECT id FROM source.providers WHERE code='api-football')
                 AND fixture.season_id=%s""",
            (first.season_id,),
        ).fetchone()[0] == 380
        assert conn.execute(
            """SELECT count(*)
               FROM football.fixtures fixture
               JOIN source.venue_provider_refs venue_ref
                 ON venue_ref.venue_id=fixture.venue_id
                AND venue_ref.provider_id=(SELECT id FROM source.providers WHERE code='api-football')
               WHERE fixture.season_id=%s
                 AND venue_ref.last_seen_at >= %s""",
            (first.season_id, first_received_at),
        ).fetchone()[0] == 380

        second = import_active_base(
            conn,
            collected=_collected(first_received_at + timedelta(minutes=1)),
            scope=SCOPE,
        )
        replay = verify_active_season(conn, scope=SCOPE)

        assert second.season_id == first.season_id == replay.season_id
        assert (replay.team_count, replay.fixture_count, replay.fixture_mapping_count) == (20, 380, 380)
        assert replay.standing_row_count == 20
        assert conn.execute(
            "SELECT count(*) FROM source.fixture_provider_status status JOIN football.fixtures fixture ON fixture.id=status.fixture_id WHERE fixture.season_id=%s",
            (first.season_id,),
        ).fetchone()[0] == 380

        finalized_fixture_id = conn.execute(
            """WITH target AS (
                    SELECT id FROM football.fixtures
                    WHERE season_id=%s AND lifecycle_state='completed'
                    ORDER BY id LIMIT 1
                )
                UPDATE football.fixtures fixture
                SET result_finalized_at=fixture.result_available_at
                FROM target WHERE fixture.id=target.id
                RETURNING fixture.id""",
            (first.season_id,),
        ).fetchone()[0]
        identical_replay = import_active_base(
            conn, collected=_collected(first_received_at), scope=SCOPE
        )
        assert identical_replay.season_id == first.season_id
        assert conn.execute(
            "SELECT result_finalized_at FROM football.fixtures WHERE id=%s", (finalized_fixture_id,)
        ).fetchone()[0] is not None

        raw_by_endpoint = {
            endpoint: SAMPLES.joinpath(filename).read_bytes()
            for endpoint, filename in {
                "/leagues": "01-leagues-epl-2026.raw.json",
                "/teams": "02-teams-epl-2026.raw.json",
                "/standings": "04-standings-epl-2026.raw.json",
                "/fixtures": "03-fixtures-epl-2026.raw.json",
            }.items()
        }
        persisted = conn.execute(
            """SELECT fetch.endpoint,fetch.content_sha256,raw.inline_body
               FROM source.provider_fetches fetch
               JOIN source.provider_raw_payloads raw ON raw.fetch_id=fetch.id
               WHERE fetch.subject_season_id=%s ORDER BY fetch.id""",
            (first.season_id,),
        ).fetchall()
        assert len(persisted) == 12
        for endpoint, digest, body in persisted:
            expected_raw = raw_by_endpoint[endpoint]
            assert bytes(body) == expected_raw
            assert bytes(digest) == hashlib.sha256(expected_raw).digest()
