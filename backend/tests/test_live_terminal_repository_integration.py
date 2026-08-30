from __future__ import annotations

import asyncio
import json
import os
from datetime import UTC, datetime, timedelta

import pytest
from psycopg import AsyncConnection

from app.api_football import APIFootballResponse
from app.live import (
    AsyncPostgresLiveRepository,
    normalize_final_result,
    normalize_live_fixture,
)


TEST_DB_URL = os.environ.get("LIVE_WORKER_TEST_DB_URL")
pytestmark = pytest.mark.skipif(
    not TEST_DB_URL, reason="LIVE_WORKER_TEST_DB_URL is not configured"
)
KICKOFF = datetime(2026, 8, 30, 15, tzinfo=UTC)
OBSERVED = datetime(2026, 8, 30, 17, tzinfo=UTC)


def _terminal_fixture(*, fixture_id: int = 1557383, goals: tuple[int, int] = (2, 1)):
    return normalize_live_fixture(
        {
            "fixture": {
                "id": fixture_id,
                "status": {"short": "FT", "elapsed": 90, "extra": 5},
            },
            "league": {"id": 39, "season": 2026},
            "teams": {"home": {"id": 40}, "away": {"id": 65}},
            "goals": {"home": goals[0], "away": goals[1]},
            "score": {"fulltime": {"home": 99, "away": 98}},
        }
    )


def _terminal_response(
    *, fixture_id: int = 1557383, goals: tuple[int, int] = (2, 1)
) -> APIFootballResponse:
    entry = {
        "fixture": {
            "id": fixture_id,
            "status": {"short": "FT", "elapsed": 90, "extra": 5},
        },
        "league": {"id": 39, "season": 2026},
        "teams": {"home": {"id": 40}, "away": {"id": 65}},
        "goals": {"home": goals[0], "away": goals[1]},
        "score": {
            "halftime": {"home": 1, "away": 0},
            "fulltime": {"home": goals[0], "away": goals[1]},
            "extratime": {"home": None, "away": None},
            "penalty": {"home": None, "away": None},
        },
    }
    payload = {
        "get": "fixtures",
        "parameters": {"id": str(fixture_id)},
        "errors": [],
        "results": 1,
        "paging": {"current": 1, "total": 1},
        "response": [entry],
    }
    return APIFootballResponse(
        payload, json.dumps(payload, separators=(",", ":")).encode(), 200, {}
    )


async def _returning_id(connection: AsyncConnection, query: str, params: tuple) -> int:
    cursor = await connection.execute(query, params)
    row = await cursor.fetchone()
    assert row is not None
    return int(row[0])


async def _seed_scheduled_fixture(connection: AsyncConnection) -> int:
    provider_id = await _returning_id(
        connection,
        "INSERT INTO source.providers(code,name) VALUES('api-football','API-Football') RETURNING id",
        (),
    )
    country_id = await _returning_id(
        connection,
        "INSERT INTO football.countries(name) VALUES('England') RETURNING id",
        (),
    )
    league_id = await _returning_id(
        connection,
        """INSERT INTO football.leagues(name,country_name,country_id,competition_type)
           VALUES('Premier League','England',%s,'league') RETURNING id""",
        (country_id,),
    )
    await connection.execute(
        "INSERT INTO source.league_provider_refs(provider_id,external_id,league_id) VALUES(%s,'39',%s)",
        (provider_id, league_id),
    )
    season_id = await _returning_id(
        connection,
        """INSERT INTO football.seasons(league_id,start_year,label,starts_on,ends_on)
           VALUES(%s,2026,'2026/27','2026-08-01','2027-05-31') RETURNING id""",
        (league_id,),
    )
    await connection.execute(
        """INSERT INTO source.season_provider_refs(
               provider_id,league_external_id,external_season,season_id
           ) VALUES(%s,'39',2026,%s)""",
        (provider_id, season_id),
    )
    home_id = await _returning_id(
        connection,
        "INSERT INTO football.teams(name,country_name,country_id) VALUES('Liverpool','England',%s) RETURNING id",
        (country_id,),
    )
    away_id = await _returning_id(
        connection,
        "INSERT INTO football.teams(name,country_name,country_id) VALUES('Forest','England',%s) RETURNING id",
        (country_id,),
    )
    await connection.execute(
        "INSERT INTO football.season_teams(season_id,team_id) VALUES(%s,%s),(%s,%s)",
        (season_id, home_id, season_id, away_id),
    )
    await connection.execute(
        """INSERT INTO source.team_provider_refs(provider_id,external_id,team_id)
           VALUES(%s,'40',%s),(%s,'65',%s)""",
        (provider_id, home_id, provider_id, away_id),
    )
    fixture_id = await _returning_id(
        connection,
        """INSERT INTO football.fixtures(
               season_id,home_team_id,away_team_id,kickoff_at,lifecycle_state
           ) VALUES(%s,%s,%s,%s,'scheduled') RETURNING id""",
        (season_id, home_id, away_id, KICKOFF),
    )
    await connection.execute(
        """INSERT INTO source.fixture_provider_refs(provider_id,external_id,fixture_id)
           VALUES(%s,'1557383',%s)""",
        (provider_id, fixture_id),
    )
    await connection.execute(
        """INSERT INTO source.fixture_status_code_mappings(
               provider_id,external_code,canonical_state,mapping_version
           ) VALUES(%s,'NS','scheduled','test-v1'),(%s,'FT','completed','test-v1')""",
        (provider_id, provider_id),
    )
    return fixture_id


async def _seed_additional_fixture(connection: AsyncConnection) -> int:
    provider_id = await _returning_id(
        connection, "SELECT id FROM source.providers WHERE code='api-football'", ()
    )
    season_id = await _returning_id(
        connection,
        "SELECT id FROM football.seasons WHERE start_year=2026",
        (),
    )
    home_id = await _returning_id(
        connection,
        "SELECT team_id FROM source.team_provider_refs WHERE provider_id=%s AND external_id='40'",
        (provider_id,),
    )
    away_id = await _returning_id(
        connection,
        "SELECT team_id FROM source.team_provider_refs WHERE provider_id=%s AND external_id='65'",
        (provider_id,),
    )
    fixture_id = await _returning_id(
        connection,
        """INSERT INTO football.fixtures(
               season_id,home_team_id,away_team_id,kickoff_at,lifecycle_state
           ) VALUES(%s,%s,%s,%s,'scheduled') RETURNING id""",
        (season_id, home_id, away_id, KICKOFF),
    )
    await connection.execute(
        """INSERT INTO source.fixture_provider_refs(provider_id,external_id,fixture_id)
           VALUES(%s,'1557384',%s)""",
        (provider_id, fixture_id),
    )
    return fixture_id


def test_terminal_handoff_schedules_existing_ops_reconciliation_without_finalizing() -> None:
    assert TEST_DB_URL is not None

    async def exercise() -> None:
        connection = await AsyncConnection.connect(TEST_DB_URL)
        try:
            assert connection.info.dbname == "fa_live_worker", (
                "live worker integration requires its disposable database"
            )
            fixture_id = await _seed_scheduled_fixture(connection)
            failure_fixture_id = await _seed_additional_fixture(connection)
            await connection.commit()
            repository = AsyncPostgresLiveRepository(connection)

            first = await repository.ensure_terminal_reconciliation(
                _terminal_fixture(), observed_at=OBSERVED
            )
            await repository.ensure_terminal_reconciliation(
                _terminal_fixture(fixture_id=1557384, goals=(0, 0)), observed_at=OBSERVED
            )
            second = await repository.ensure_terminal_reconciliation(
                _terminal_fixture(), observed_at=OBSERVED
            )

            fixture_cursor = await connection.execute(
                """SELECT lifecycle_state::text,home_goals,away_goals,
                          terminal_status_observed_at,result_available_at,
                          result_finalized_at
                   FROM football.fixtures WHERE id=%s""",
                (fixture_id,),
            )
            assert await fixture_cursor.fetchone() == (
                "scheduled",
                None,
                None,
                None,
                None,
                None,
            )
            state_cursor = await connection.execute(
                """SELECT state::text,eligible_at,next_attempt_at,attempt_count,
                          max_attempts,last_attempt_at,last_source_fetch_id
                   FROM ops.fixture_reconciliation_state WHERE fixture_id=%s""",
                (fixture_id,),
            )
            assert await state_cursor.fetchone() == (
                "waiting",
                KICKOFF + timedelta(hours=3),
                KICKOFF + timedelta(hours=3),
                0,
                4,
                None,
                None,
            )
            fetch_count_cursor = await connection.execute(
                "SELECT count(*) FROM source.provider_fetches"
            )
            assert await fetch_count_cursor.fetchone() == (0,)
            assert first.fixture_id == second.fixture_id == fixture_id

            failure_task = await repository.next_due_reconciliation(
                league_external_ids=(39,),
                as_of=KICKOFF + timedelta(hours=3),
                exclude_fixture_ids=(fixture_id,),
            )
            assert failure_task is not None
            assert failure_task.fixture_id == failure_fixture_id
            failure_base = KICKOFF + timedelta(hours=3)
            for attempt in range(4):
                attempt_at = failure_base + timedelta(minutes=attempt * 6)
                await repository.record_reconciliation_failure(
                    failure_task,
                    request_started_at=attempt_at,
                    response_received_at=attempt_at,
                    next_attempt_at=attempt_at + timedelta(minutes=5),
                    http_status=503,
                    outcome="http_error",
                    error_class="APIFootballHTTPError",
                )
            exhausted_cursor = await connection.execute(
                """SELECT state::text,attempt_count FROM ops.fixture_reconciliation_state
                   WHERE fixture_id=%s""",
                (failure_fixture_id,),
            )
            assert await exhausted_cursor.fetchone() == ("exhausted", 4)

            task = await repository.next_due_reconciliation(
                league_external_ids=(39,),
                as_of=KICKOFF + timedelta(hours=3),
                exclude_fixture_ids=(failure_fixture_id,),
            )
            assert task is not None
            response = _terminal_response()
            received_at = KICKOFF + timedelta(hours=3, minutes=1)
            await repository.persist_reconciliation_response(
                task,
                normalize_live_fixture(response.data["response"][0]),
                response,
                request_started_at=received_at,
                response_received_at=received_at,
                result=normalize_final_result(response.data["response"][0]),
                next_attempt_at=received_at + timedelta(minutes=5),
            )

            fixture_cursor = await connection.execute(
                """SELECT lifecycle_state::text,home_goals,away_goals,
                          result_finalized_at IS NOT NULL
                   FROM football.fixtures WHERE id=%s""",
                (fixture_id,),
            )
            assert await fixture_cursor.fetchone() == ("completed", 2, 1, True)
            state_cursor = await connection.execute(
                """SELECT state::text,attempt_count,terminal_observed_at IS NOT NULL,
                          completed_at IS NOT NULL
                   FROM ops.fixture_reconciliation_state WHERE fixture_id=%s""",
                (fixture_id,),
            )
            assert await state_cursor.fetchone() == ("completed", 1, True, True)
            status_cursor = await connection.execute(
                """SELECT status_code,provider_fetch.purpose::text,
                          provider_fetch.subject_fixture_id,raw.inline_body IS NOT NULL
                   FROM source.fixture_provider_status status
                   JOIN source.provider_fetches provider_fetch
                     ON provider_fetch.id=status.source_fetch_id
                   JOIN source.provider_raw_payloads raw
                     ON raw.fetch_id=provider_fetch.id
                   WHERE status.fixture_id=%s""",
                (fixture_id,),
            )
            assert await status_cursor.fetchone() == (
                "FT",
                "postmatch_reconciliation",
                fixture_id,
                True,
            )
            assert (
                await repository.next_due_reconciliation(
                    league_external_ids=(39,), as_of=received_at
                )
            ) is None
        finally:
            await connection.close()

    asyncio.run(exercise())
