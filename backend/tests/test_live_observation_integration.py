from __future__ import annotations

import asyncio
import json
import os
from datetime import UTC, datetime, timedelta

import pytest
from psycopg import AsyncConnection

from app.api_football import APIFootballResponse
from app.importer.fixture_schedule_observations import (
    FixtureScheduleObservationConflict,
)
from app.live import (
    AsyncPostgresLiveRepository,
    LiveReconciliationError,
    LiveResolutionError,
    LiveSettings,
    bind_live_fixture,
    normalize_live_response,
)
from app.live.worker import LiveWorker, LiveWorkerError


TEST_DB_URL = os.environ.get("LIVE_WORKER_TEST_DB_URL")
pytestmark = pytest.mark.skipif(
    not TEST_DB_URL, reason="LIVE_WORKER_TEST_DB_URL is not configured"
)
KICKOFF = datetime(2027, 8, 30, 15, tzinfo=UTC)
OBSERVED = datetime(2027, 8, 30, 16, 7, tzinfo=UTC)
PROVIDER_FIXTURE_ID = 2_557_383


def _entry(
    *,
    fixture_id: int = PROVIDER_FIXTURE_ID,
    kickoff: str | None = "2027-08-30T15:00:00+00:00",
    home_team_id: int = 140,
    status: str = "1H",
) -> dict:
    return {
        "fixture": {
            "id": fixture_id,
            "date": kickoff,
            "status": {"short": status, "elapsed": 35, "extra": None},
        },
        "league": {"id": 142, "season": 2027},
        "teams": {"home": {"id": home_team_id}, "away": {"id": 165}},
        "goals": {"home": 1, "away": 0},
        "score": {"fulltime": {"home": None, "away": None}},
    }


def _response(
    entries: list[dict], *, parameters: dict[str, str] | None = None
) -> APIFootballResponse:
    payload = {
        "get": "fixtures",
        "parameters": parameters or {"live": "142"},
        "errors": [],
        "results": len(entries),
        "paging": {"current": 1, "total": 1},
        "response": entries,
    }
    raw = json.dumps(payload, separators=(",", ":")).encode()
    return APIFootballResponse(payload, raw, 200, {})


async def _returning_id(
    connection: AsyncConnection, query: str, params: tuple
) -> int:
    cursor = await connection.execute(query, params)
    row = await cursor.fetchone()
    assert row is not None
    return int(row[0])


async def _seed_fixture(connection: AsyncConnection) -> tuple[int, int]:
    provider_id = await _returning_id(
        connection,
        """INSERT INTO source.providers(code,name)
           VALUES('api-football','API-Football')
           ON CONFLICT(code) DO UPDATE SET name=excluded.name
           RETURNING id""",
        (),
    )
    country_id = await _returning_id(
        connection,
        "INSERT INTO football.countries(name) VALUES('Observationland') RETURNING id",
        (),
    )
    league_id = await _returning_id(
        connection,
        """INSERT INTO football.leagues(name,country_name,country_id,competition_type)
           VALUES('Live Observation League','Observationland',%s,'league') RETURNING id""",
        (country_id,),
    )
    await connection.execute(
        """INSERT INTO source.league_provider_refs(provider_id,external_id,league_id)
           VALUES(%s,'142',%s)""",
        (provider_id, league_id),
    )
    season_id = await _returning_id(
        connection,
        """INSERT INTO football.seasons(league_id,start_year,label,starts_on,ends_on)
           VALUES(%s,2027,'2027/28','2027-08-01','2028-05-31') RETURNING id""",
        (league_id,),
    )
    await connection.execute(
        """INSERT INTO source.season_provider_refs(
               provider_id,league_external_id,external_season,season_id
           ) VALUES(%s,'142',2027,%s)""",
        (provider_id, season_id),
    )
    home_id = await _returning_id(
        connection,
        """INSERT INTO football.teams(name,country_name,country_id)
           VALUES('Live Home','Observationland',%s) RETURNING id""",
        (country_id,),
    )
    away_id = await _returning_id(
        connection,
        """INSERT INTO football.teams(name,country_name,country_id)
           VALUES('Live Away','Observationland',%s) RETURNING id""",
        (country_id,),
    )
    await connection.execute(
        "INSERT INTO football.season_teams(season_id,team_id) VALUES(%s,%s),(%s,%s)",
        (season_id, home_id, season_id, away_id),
    )
    await connection.execute(
        """INSERT INTO source.team_provider_refs(provider_id,external_id,team_id)
           VALUES(%s,'140',%s),(%s,'165',%s)""",
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
           VALUES(%s,%s,%s)""",
        (provider_id, str(PROVIDER_FIXTURE_ID), fixture_id),
    )
    return provider_id, fixture_id


class _Provider:
    def __init__(
        self,
        responses: list[APIFootballResponse],
        *,
        credential_results: list[bool] | None = None,
    ) -> None:
        self._responses = responses
        self._credential_results = list(credential_results or [])

    async def get(self, endpoint: str, *, params: dict | None = None):
        assert endpoint == "/fixtures"
        response = self._responses.pop(0)
        assert params is not None
        assert {key: str(value) for key, value in params.items()} == response.data[
            "parameters"
        ]
        return response

    def response_contains_api_key(self, body: bytes) -> bool:
        return (
            self._credential_results.pop(0)
            if self._credential_results
            else False
        )


class _RetryStore:
    def __init__(self) -> None:
        self.fail_next = True
        self.applied = []

    async def active(self):
        return ()

    async def apply_poll(self, active_states, *, finished_fixture_ids=()):
        if self.fail_next:
            self.fail_next = False
            raise RuntimeError("redis write failed after postgres commit")
        self.applied.append((tuple(active_states), frozenset(finished_fixture_ids)))


class _StaticStore:
    def __init__(self, current=()) -> None:
        self.current = tuple(current)
        self.applied = []

    async def active(self):
        return self.current

    async def apply_poll(self, active_states, *, finished_fixture_ids=()):
        self.applied.append((tuple(active_states), frozenset(finished_fixture_ids)))


def test_live_observations_replay_rollback_and_redis_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert TEST_DB_URL is not None

    async def exercise() -> None:
        connection = await AsyncConnection.connect(TEST_DB_URL, autocommit=True)
        try:
            async with connection.transaction():
                provider_id, fixture_id = await _seed_fixture(connection)
            repository = AsyncPostgresLiveRepository(connection)
            response = _response([_entry()])
            fixtures = normalize_live_response(response.data, expected_league_ids={142})
            started_at = OBSERVED - timedelta(seconds=1)

            fetch_id = await repository.persist_live_response(
                response,
                fixtures,
                request_params={"live": "142"},
                request_started_at=started_at,
                response_received_at=OBSERVED,
            )
            cursor = await connection.execute(
                """SELECT observation.provider_id,observation.fixture_id,
                          observation.source_fetch_id,observation.observed_kickoff_at,
                          observation.observed_at,provider_fetch.endpoint,
                          provider_fetch.request_params,provider_fetch.purpose::text,
                          provider_fetch.outcome::text,provider_fetch.request_started_at,
                          provider_fetch.response_received_at,provider_fetch.normalized_at,
                          raw.inline_body
                   FROM source.fixture_schedule_observations observation
                   JOIN source.provider_fetches provider_fetch
                     ON provider_fetch.id=observation.source_fetch_id
                   JOIN source.provider_raw_payloads raw
                     ON raw.fetch_id=provider_fetch.id
                   WHERE observation.source_fetch_id=%s""",
                (fetch_id,),
            )
            assert await cursor.fetchone() == (
                provider_id,
                fixture_id,
                fetch_id,
                KICKOFF,
                OBSERVED,
                "/fixtures",
                {"live": "142"},
                "scheduled_refresh",
                "success",
                started_at,
                OBSERVED,
                OBSERVED,
                response.raw_body,
            )
            original_persist_fetch = repository._persist_successful_fixture_fetch

            async def reuse_fetch(**kwargs: object) -> int:
                return fetch_id

            monkeypatch.setattr(
                repository, "_persist_successful_fixture_fetch", reuse_fetch
            )
            assert await repository.persist_live_response(
                response,
                fixtures,
                request_params={"live": "142"},
                request_started_at=started_at,
                response_received_at=OBSERVED,
            ) == fetch_id
            cursor = await connection.execute(
                """SELECT count(*) FROM source.fixture_schedule_observations
                   WHERE source_fetch_id=%s""",
                (fetch_id,),
            )
            assert await cursor.fetchone() == (1,)
            conflict_response = _response(
                [_entry(kickoff="2027-08-30T15:30:00+00:00")]
            )
            conflict_fixtures = normalize_live_response(
                conflict_response.data, expected_league_ids={142}
            )
            with pytest.raises(
                FixtureScheduleObservationConflict,
                match="conflicting fixture schedule observation",
            ):
                await repository.persist_live_response(
                    conflict_response,
                    conflict_fixtures,
                    request_params={"live": "142"},
                    request_started_at=started_at,
                    response_received_at=OBSERVED,
                )
            cursor = await connection.execute(
                """SELECT observed_kickoff_at
                   FROM source.fixture_schedule_observations
                   WHERE source_fetch_id=%s""",
                (fetch_id,),
            )
            assert await cursor.fetchall() == [(KICKOFF,)]
            monkeypatch.setattr(
                repository,
                "_persist_successful_fixture_fetch",
                original_persist_fetch,
            )
            null_observed_at = OBSERVED + timedelta(minutes=1)
            null_response = _response([_entry(kickoff=None)])
            null_fetch_id = await repository.persist_live_response(
                null_response,
                normalize_live_response(null_response.data, expected_league_ids={142}),
                request_params={"live": "142"},
                request_started_at=null_observed_at - timedelta(seconds=1),
                response_received_at=null_observed_at,
            )
            cursor = await connection.execute(
                """SELECT observed_kickoff_at,observed_at
                   FROM source.fixture_schedule_observations
                   WHERE source_fetch_id=%s""",
                (null_fetch_id,),
            )
            assert await cursor.fetchone() == (None, null_observed_at)
            fetch_count_cursor = await connection.execute(
                "SELECT count(*) FROM source.provider_fetches"
            )
            fetch_count_before = (await fetch_count_cursor.fetchone())[0]
            missing_response = _response([_entry(fixture_id=9_999_999)])
            with pytest.raises(LiveResolutionError, match="mapping is missing"):
                await repository.persist_live_response(
                    missing_response,
                    normalize_live_response(
                        missing_response.data, expected_league_ids={142}
                    ),
                    request_params={"live": "142"},
                    request_started_at=null_observed_at,
                    response_received_at=null_observed_at,
                )
            conflicting_mapping_response = _response(
                [_entry(home_team_id=999_999)]
            )
            with pytest.raises(LiveResolutionError, match="mapping is missing"):
                await repository.persist_live_response(
                    conflicting_mapping_response,
                    normalize_live_response(
                        conflicting_mapping_response.data,
                        expected_league_ids={142},
                    ),
                    request_params={"live": "142"},
                    request_started_at=null_observed_at,
                    response_received_at=null_observed_at,
                )
            with pytest.raises(
                LiveReconciliationError, match="normalized fixture membership"
            ):
                await repository.persist_live_response(
                    response,
                    (),
                    request_params={"live": "142"},
                    request_started_at=started_at,
                    response_received_at=OBSERVED,
                )
            fetch_count_cursor = await connection.execute(
                "SELECT count(*) FROM source.provider_fetches"
            )
            assert (await fetch_count_cursor.fetchone())[0] == fetch_count_before
            rollback_observed_at = OBSERVED + timedelta(minutes=2)
            rollback_response = _response(
                [_entry(kickoff="2027-08-30T15:45:00+00:00")]
            )

            async def fail_normalized_mark(
                source_fetch_id: int, normalized_at: datetime
            ) -> None:
                cursor = await connection.execute(
                    """SELECT observed_kickoff_at,observed_at
                       FROM source.fixture_schedule_observations
                       WHERE source_fetch_id=%s""",
                    (source_fetch_id,),
                )
                assert await cursor.fetchone() == (
                    datetime(2027, 8, 30, 15, 45, tzinfo=UTC),
                    rollback_observed_at,
                )
                raise RuntimeError("forced failure before normalized mark")

            original_mark_normalized = repository._mark_fetch_normalized
            monkeypatch.setattr(
                repository, "_mark_fetch_normalized", fail_normalized_mark
            )
            with pytest.raises(RuntimeError, match="forced failure"):
                await repository.persist_live_response(
                    rollback_response,
                    normalize_live_response(
                        rollback_response.data, expected_league_ids={142}
                    ),
                    request_params={"live": "142"},
                    request_started_at=rollback_observed_at - timedelta(seconds=1),
                    response_received_at=rollback_observed_at,
                )
            cursor = await connection.execute(
                """SELECT count(*) FROM source.provider_fetches
                   WHERE response_received_at=%s""",
                (rollback_observed_at,),
            )
            assert await cursor.fetchone() == (0,)
            monkeypatch.setattr(
                repository, "_mark_fetch_normalized", original_mark_normalized
            )

            redis_first_at = OBSERVED + timedelta(minutes=3)
            redis_second_at = OBSERVED + timedelta(minutes=4)
            clock_values = iter(
                (
                    redis_first_at - timedelta(seconds=1),
                    redis_first_at,
                    redis_second_at - timedelta(seconds=1),
                    redis_second_at,
                )
            )
            store = _RetryStore()
            worker = LiveWorker(
                provider=_Provider([response, response]),
                repository=repository,
                store=store,
                settings=LiveSettings(
                    redis_url="redis://unused", league_external_ids=(142,)
                ),
                clock=lambda: next(clock_values),
            )
            with pytest.raises(RuntimeError, match="redis write failed"):
                await worker.poll_once()

            observer = await AsyncConnection.connect(TEST_DB_URL)
            try:
                cursor = await observer.execute(
                    """SELECT observation.observed_at,provider_fetch.normalized_at
                       FROM source.fixture_schedule_observations observation
                       JOIN source.provider_fetches provider_fetch
                         ON provider_fetch.id=observation.source_fetch_id
                       WHERE observation.fixture_id=%s
                         AND observation.observed_at=%s""",
                    (fixture_id, redis_first_at),
                )
                assert await cursor.fetchone() == (redis_first_at, redis_first_at)
            finally:
                await observer.close()

            report = await worker.poll_once()
            assert report.active_count == 1
            assert len(store.applied) == 1
            cursor = await connection.execute(
                """SELECT observed_at,count(*)
                   FROM source.fixture_schedule_observations
                   WHERE fixture_id=%s AND observed_at IN (%s,%s)
                   GROUP BY observed_at ORDER BY observed_at""",
                (fixture_id, redis_first_at, redis_second_at),
            )
            assert await cursor.fetchall() == [
                (redis_first_at, 1),
                (redis_second_at, 1),
            ]

            async def persistence_counts() -> tuple[int, int, int, int]:
                cursor = await connection.execute(
                    """SELECT
                           (SELECT count(*) FROM source.provider_fetches),
                           (SELECT count(*) FROM source.provider_raw_payloads),
                           (SELECT count(*) FROM source.fixture_schedule_observations),
                           (SELECT count(*) FROM ops.fixture_reconciliation_state
                            WHERE fixture_id=%s)""",
                    (fixture_id,),
                )
                row = await cursor.fetchone()
                assert row is not None
                return tuple(int(value) for value in row)  # type: ignore[return-value]

            before_primary_rejection = await persistence_counts()
            primary_store = _StaticStore()
            credential_primary = LiveWorker(
                provider=_Provider(
                    [_response([_entry(status="FT")])],
                    credential_results=[True],
                ),
                repository=repository,
                store=primary_store,
                settings=LiveSettings(
                    redis_url="redis://unused", league_external_ids=(142,)
                ),
                clock=lambda: OBSERVED + timedelta(minutes=5),
            )
            with pytest.raises(LiveWorkerError, match="API key"):
                await credential_primary.poll_once()
            assert await persistence_counts() == before_primary_rejection
            assert primary_store.applied == []

            reference = await repository.resolve(fixtures[0])
            previous_state = bind_live_fixture(
                fixtures[0], reference, observed_at=redis_second_at
            )
            recheck_store = _StaticStore([previous_state])
            before_recheck_rejection = await persistence_counts()
            recheck_clock = iter(
                (
                    OBSERVED + timedelta(minutes=6),
                    OBSERVED + timedelta(minutes=6, seconds=1),
                    OBSERVED + timedelta(minutes=6, seconds=2),
                )
            )
            credential_recheck = LiveWorker(
                provider=_Provider(
                        [
                            _response([]),
                            _response(
                                [_entry(status="FT")],
                                parameters={"id": str(PROVIDER_FIXTURE_ID)},
                            ),
                    ],
                    credential_results=[False, True],
                ),
                repository=repository,
                store=recheck_store,
                settings=LiveSettings(
                    redis_url="redis://unused", league_external_ids=(142,)
                ),
                clock=lambda: next(recheck_clock),
                monotonic_clock=lambda: 100.0,
            )
            with pytest.raises(LiveWorkerError, match="API key"):
                await credential_recheck.poll_once()
            after_recheck_rejection = await persistence_counts()
            assert after_recheck_rejection == (
                before_recheck_rejection[0] + 1,
                before_recheck_rejection[1] + 1,
                before_recheck_rejection[2],
                before_recheck_rejection[3],
            )
            assert recheck_store.applied == []
            assert recheck_store.current == (previous_state,)
        finally:
            await connection.close()

    asyncio.run(exercise())
