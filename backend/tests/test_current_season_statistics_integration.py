import json
import os
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
import pytest
from psycopg.types.json import Jsonb

from app.api_football import APIFootballResponse
from app.api_football.errors import APIFootballHTTPError
from app.importer.current_season_statistics import (
    ENDPOINT,
    CurrentSeasonStatisticsError,
    CurrentSeasonStatisticsScope,
    _lock_key,
    _bind_returned_fixture_subjects,
    fixture_ids_parameter,
    load_completed_targets,
    run_current_season_statistics_backfill,
)


TEST_DB_URL = os.environ.get("CURRENT_SEASON_STATISTICS_TEST_DB_URL")
pytestmark = pytest.mark.skipif(not TEST_DB_URL, reason="CURRENT_SEASON_STATISTICS_TEST_DB_URL is not configured")


class BatchClient:
    def __init__(self, fixtures: Mapping[int, Mapping[str, Any]], *, season: int = 2024) -> None:
        self.fixtures = fixtures
        self.season = season
        self.calls: list[tuple[str, Mapping[str, str | int]]] = []

    async def get(self, endpoint: str, *, params: Mapping[str, str | int]) -> APIFootballResponse:
        assert endpoint == ENDPOINT
        self.calls.append((endpoint, params))
        fixture_ids = (
            sorted(self.fixtures)
            if "league" in params
            else [int(value) for value in str(params["ids"]).split("-")]
        )
        response = []
        for fixture_id in fixture_ids:
            fixture = self.fixtures[fixture_id]
            home_id, away_id = fixture["home_id"], fixture["away_id"]
            response.append(
                {
                    "fixture": {
                        "id": fixture_id, "date": fixture["kickoff"].isoformat(), "status": {"short": "FT"},
                    },
                    "league": {"id": 39, "season": self.season},
                    # The seeded provider-team refs use canonical team ids + 1000.
                    "teams": {
                        "home": {"id": home_id},
                        "away": {"id": away_id},
                    },
                    "goals": {"home": fixture["home_goals"], "away": fixture["away_goals"]},
                    "score": {
                        "halftime": {"home": None, "away": None},
                        "fulltime": {"home": fixture["home_goals"], "away": fixture["away_goals"]},
                        "extratime": {"home": None, "away": None},
                        "penalty": {"home": None, "away": None},
                    },
                    "statistics": ([] if "league" in params else [
                        {"team": {"id": home_id}, "statistics": []},
                        {"team": {"id": away_id}, "statistics": []},
                    ]),
                }
            )
        payload: dict[str, Any] = {
            "get": "fixtures", "parameters": (
                {"league": str(params["league"]), "season": str(params["season"]), "status": "FT-AET-PEN"}
                if "league" in params else {"ids": str(params["ids"])}
            ), "errors": {},
            "results": len(response), "paging": {"current": 1, "total": 1}, "response": response,
        }
        raw = json.dumps(payload, separators=(",", ":")).encode()
        return APIFootballResponse(payload, raw, 200, {"x-ratelimit-requests-remaining": "7000"})

    def response_contains_api_key(self, body: bytes) -> bool:
        return False


async def _no_sleep(_: float) -> None:
    return None


class RetryingFailureClient:
    def __init__(self) -> None:
        self.calls = 0

    async def get(self, endpoint: str, *, params: Mapping[str, str | int]) -> APIFootballResponse:
        self.calls += 1
        raise APIFootballHTTPError(500)

    def response_contains_api_key(self, body: bytes) -> bool:
        return False


class ConflictDiscoveryClient:
    def __init__(self, response: list[dict[str, Any]]) -> None:
        self.response = response

    async def get(self, endpoint: str, *, params: Mapping[str, str | int]) -> APIFootballResponse:
        assert params == {"league": 39, "season": 2023, "status": "FT-AET-PEN"}
        payload: dict[str, Any] = {
            "get": "fixtures",
            "parameters": {"league": "39", "season": "2023", "status": "FT-AET-PEN"},
            "errors": {}, "results": len(self.response), "paging": {"current": 1, "total": 1},
            "response": self.response,
        }
        raw = json.dumps(payload, separators=(",", ":")).encode()
        return APIFootballResponse(payload, raw, 200, {})

    def response_contains_api_key(self, body: bytes) -> bool:
        return False


def test_one_league_batch_statistics_persists_raw_provenance_pairs_and_metrics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert TEST_DB_URL is not None
    monkeypatch.setenv("SUPABASE_DB_URL", TEST_DB_URL)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as conn:
        fixtures = {
            int(fixture_external_id): {
                "home_id": int(home_external_id), "away_id": int(away_external_id), "kickoff": kickoff,
                "home_goals": int(home_goals), "away_goals": int(away_goals),
            }
            for fixture_external_id, home_external_id, away_external_id, kickoff, home_goals, away_goals in conn.execute(
                """SELECT fixture_ref.external_id,home_ref.external_id,away_ref.external_id,
                          fixture.kickoff_at,fixture.home_goals,fixture.away_goals
                   FROM football.fixtures fixture
                   JOIN source.fixture_provider_refs fixture_ref ON fixture_ref.fixture_id=fixture.id
                   JOIN source.team_provider_refs home_ref ON home_ref.team_id=fixture.home_team_id
                   JOIN source.team_provider_refs away_ref ON away_ref.team_id=fixture.away_team_id"""
            ).fetchall()
        }
    client = BatchClient(fixtures)
    scope = CurrentSeasonStatisticsScope(league_external_id=39, season_start_year=2024, max_requests=11)

    report = run_current_season_statistics_backfill(scope=scope, client=client, sleep=_no_sleep)

    assert report.fixtures_discovered == 380
    assert report.unique_fixtures_selected == 200
    assert report.fixture_discovery_requests == 1
    assert report.batch_requests == 10
    assert report.api_requests == 11
    assert report.fixtures_normalized == 200
    assert report.statistics_rows_written == 400
    assert report.teams_aggregated == 20
    assert report.errors == ()
    assert client.calls[0] == ("/fixtures", {"league": 39, "season": 2024, "status": "FT-AET-PEN"})
    assert all(len(str(call[1]["ids"]).split("-")) == 20 for call in client.calls[1:])

    with psycopg.connect(TEST_DB_URL, autocommit=True) as conn:
        fetch_id, raw_count, memberships, rows, metric_rows = conn.execute(
            """SELECT
                   (SELECT max(id) FROM source.provider_fetches WHERE endpoint='/fixtures' AND purpose='scheduled_refresh'),
                   (SELECT count(*) FROM source.provider_raw_payloads raw
                     JOIN source.provider_fetches provider_fetch ON provider_fetch.id=raw.fetch_id
                    WHERE provider_fetch.endpoint='/fixtures' AND provider_fetch.purpose='scheduled_refresh'),
                   (SELECT count(*) FROM source.provider_fetch_fixture_subjects subject
                     JOIN source.provider_fetches provider_fetch ON provider_fetch.id=subject.fetch_id
                    WHERE provider_fetch.endpoint='/fixtures' AND provider_fetch.purpose='scheduled_refresh'),
                   (SELECT count(*) FROM football.fixture_team_statistics statistics
                     JOIN source.provider_fetches provider_fetch ON provider_fetch.id=statistics.last_source_fetch_id
                    WHERE provider_fetch.endpoint='/fixtures' AND provider_fetch.purpose='scheduled_refresh'),
                   (SELECT count(*) FROM football.team_rolling_metrics WHERE season_id=(SELECT season_id FROM source.season_provider_refs WHERE external_season=2024))"""
        ).fetchone()
        assert fetch_id is not None
        assert (raw_count, memberships, rows, metric_rows) == (11, 580, 400, 120)
        assert conn.execute(
            """SELECT count(*) FROM football.fixture_team_statistics statistics
               JOIN source.provider_fetches provider_fetch ON provider_fetch.id=statistics.last_source_fetch_id
               WHERE provider_fetch.id=%s AND statistics.finalized_at IS NULL""",
            (fetch_id,),
        ).fetchone()[0] == 40

    second_client = BatchClient(fixtures)
    rerun = run_current_season_statistics_backfill(scope=scope, client=second_client, sleep=_no_sleep)
    assert rerun.api_requests == rerun.fixture_discovery_requests == 1
    assert rerun.statistics_rows_written == 0
    assert len(second_client.calls) == 1


def test_discovery_advances_scheduled_ns_status_with_fixture_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert TEST_DB_URL is not None
    monkeypatch.setenv("SUPABASE_DB_URL", TEST_DB_URL)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as conn:
        provider_id, league_id = conn.execute(
            """SELECT provider.id,league_ref.league_id FROM source.providers provider
               JOIN source.league_provider_refs league_ref ON league_ref.provider_id=provider.id
               WHERE provider.code='api-football' AND league_ref.external_id='39'"""
        ).fetchone()
        season_id = conn.execute(
            "INSERT INTO football.seasons(league_id,start_year,label) VALUES(%s,2022,'2022/23') RETURNING id",
            (league_id,),
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO source.season_provider_refs(provider_id,league_external_id,external_season,season_id) VALUES(%s,'39',2022,%s)",
            (provider_id, season_id),
        )
        teams = conn.execute(
            """SELECT ref.team_id,ref.external_id FROM source.team_provider_refs ref
               WHERE ref.provider_id=%s ORDER BY ref.team_id LIMIT 2""",
            (provider_id,),
        ).fetchall()
        (home_team_id, home_external_id), (away_team_id, away_external_id) = teams
        conn.execute(
            "INSERT INTO football.season_teams(season_id,team_id) VALUES(%s,%s),(%s,%s)",
            (season_id, home_team_id, season_id, away_team_id),
        )
        observed_at = datetime(2022, 8, 1, tzinfo=UTC)
        seed_fetch_id = conn.execute(
            """INSERT INTO source.provider_fetches(
                   provider_id,endpoint,request_params,purpose,request_started_at,response_received_at,http_status,outcome,
                   provider_results,paging_current,paging_total,subject_season_id
                 ) VALUES(%s,'/fixtures',%s,'bootstrap',%s,%s,200,'success',1,1,1,%s) RETURNING id""",
            (provider_id, Jsonb({"league": 39, "season": 2022}), observed_at, observed_at, season_id),
        ).fetchone()[0]
        fixture_id = conn.execute(
            """INSERT INTO football.fixtures(
                   season_id,home_team_id,away_team_id,kickoff_at,lifecycle_state,first_seen_at,last_seen_at,last_source_fetch_id
                 ) VALUES(%s,%s,%s,%s,'scheduled',%s,%s,%s) RETURNING id""",
            (season_id, home_team_id, away_team_id, datetime(2022, 8, 7, 15, tzinfo=UTC), observed_at, observed_at, seed_fetch_id),
        ).fetchone()[0]
        external_fixture_id = 9_100_001
        conn.execute(
            "INSERT INTO source.fixture_provider_refs(provider_id,external_id,fixture_id) VALUES(%s,%s,%s)",
            (provider_id, str(external_fixture_id), fixture_id),
        )
        conn.execute(
            """INSERT INTO source.fixture_provider_status(
                   provider_id,fixture_id,status_code,observed_at,source_fetch_id
                 ) VALUES(%s,%s,'NS',%s,%s)""",
            (provider_id, fixture_id, observed_at, seed_fetch_id),
        )

    client = BatchClient({
        external_fixture_id: {
            "home_id": int(home_external_id), "away_id": int(away_external_id),
            "kickoff": datetime(2022, 8, 7, 15, tzinfo=UTC), "home_goals": 1, "away_goals": 0,
        },
    }, season=2022)
    report = run_current_season_statistics_backfill(
        scope=CurrentSeasonStatisticsScope(league_external_id=39, season_start_year=2022, max_requests=2),
        client=client, sleep=_no_sleep,
    )

    assert (report.fixture_discovery_requests, report.batch_requests, report.statistics_rows_written) == (1, 1, 2)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as conn:
        assert conn.execute(
            "SELECT lifecycle_state::text,home_goals,away_goals,result_finalized_at IS NOT NULL FROM football.fixtures WHERE id=%s", (fixture_id,)
        ).fetchone() == ("completed", 1, 0, True)
        assert conn.execute(
            """SELECT status.status_code,provider_fetch.endpoint,provider_fetch.purpose::text
               FROM source.fixture_provider_status status
               JOIN source.provider_fetches provider_fetch ON provider_fetch.id=status.source_fetch_id
               WHERE status.provider_id=%s AND status.fixture_id=%s""",
            (provider_id, fixture_id),
        ).fetchone() == ("FT", "/fixtures", "scheduled_refresh")


def test_terminal_discovery_before_three_hour_window_keeps_fixture_scheduled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert TEST_DB_URL is not None
    monkeypatch.setenv("SUPABASE_DB_URL", TEST_DB_URL)
    kickoff = datetime(2027, 8, 7, 15, tzinfo=UTC)
    observed_at = kickoff + timedelta(hours=2)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as conn:
        provider_id, league_id = conn.execute(
            """SELECT provider.id,league_ref.league_id FROM source.providers provider
               JOIN source.league_provider_refs league_ref ON league_ref.provider_id=provider.id
               WHERE provider.code='api-football' AND league_ref.external_id='39'"""
        ).fetchone()
        season_id = conn.execute(
            "INSERT INTO football.seasons(league_id,start_year,label) VALUES(%s,2027,'2027/28') RETURNING id",
            (league_id,),
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO source.season_provider_refs(provider_id,league_external_id,external_season,season_id) VALUES(%s,'39',2027,%s)",
            (provider_id, season_id),
        )
        (home_team_id, home_external_id), (away_team_id, away_external_id) = conn.execute(
            "SELECT team_id,external_id FROM source.team_provider_refs WHERE provider_id=%s ORDER BY team_id LIMIT 2",
            (provider_id,),
        ).fetchall()
        conn.execute(
            "INSERT INTO football.season_teams(season_id,team_id) VALUES(%s,%s),(%s,%s)",
            (season_id, home_team_id, season_id, away_team_id),
        )
        seed_time = kickoff - timedelta(days=7)
        seed_fetch_id = conn.execute(
            """INSERT INTO source.provider_fetches(
                   provider_id,endpoint,request_params,purpose,request_started_at,response_received_at,http_status,outcome,
                   provider_results,paging_current,paging_total,subject_season_id
                 ) VALUES(%s,'/fixtures',%s,'bootstrap',%s,%s,200,'success',1,1,1,%s) RETURNING id""",
                (provider_id, Jsonb({"league": 39, "season": 2027}), seed_time, seed_time, season_id),
        ).fetchone()[0]
        fixture_id = conn.execute(
            """INSERT INTO football.fixtures(
                   season_id,home_team_id,away_team_id,kickoff_at,lifecycle_state,first_seen_at,last_seen_at,last_source_fetch_id
                 ) VALUES(%s,%s,%s,%s,'scheduled',%s,%s,%s) RETURNING id""",
            (season_id, home_team_id, away_team_id, kickoff, seed_time, seed_time, seed_fetch_id),
        ).fetchone()[0]
        external_fixture_id = 9_100_002
        conn.execute(
            "INSERT INTO source.fixture_provider_refs(provider_id,external_id,fixture_id) VALUES(%s,%s,%s)",
            (provider_id, str(external_fixture_id), fixture_id),
        )
        conn.execute(
            """INSERT INTO source.fixture_provider_status(provider_id,fixture_id,status_code,observed_at,source_fetch_id)
               VALUES(%s,%s,'NS',%s,%s)""",
            (provider_id, fixture_id, seed_time, seed_fetch_id),
        )

    client = BatchClient({
        external_fixture_id: {
            "home_id": int(home_external_id), "away_id": int(away_external_id),
            "kickoff": kickoff, "home_goals": 1, "away_goals": 0,
        },
    }, season=2027)
    report = run_current_season_statistics_backfill(
        scope=CurrentSeasonStatisticsScope(league_external_id=39, season_start_year=2027, max_requests=2),
        client=client, sleep=_no_sleep, clock=lambda: observed_at,
    )

    assert (report.fixture_discovery_requests, report.batch_requests, report.statistics_rows_written) == (1, 0, 0)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as conn:
        assert conn.execute(
            """SELECT lifecycle_state::text,home_goals,away_goals,result_finalized_at
               FROM football.fixtures WHERE id=%s""",
            (fixture_id,),
        ).fetchone() == ("scheduled", None, None, None)
        assert conn.execute(
            "SELECT status_code FROM source.fixture_provider_status WHERE provider_id=%s AND fixture_id=%s",
            (provider_id, fixture_id),
        ).fetchone() == ("NS",)
        assert conn.execute(
            "SELECT count(*) FROM football.fixture_team_statistics WHERE fixture_id=%s", (fixture_id,)
        ).fetchone() == (0,)


def test_retry_does_not_exceed_the_last_available_request(monkeypatch: pytest.MonkeyPatch) -> None:
    assert TEST_DB_URL is not None
    monkeypatch.setenv("SUPABASE_DB_URL", TEST_DB_URL)
    client = RetryingFailureClient()

    report = run_current_season_statistics_backfill(
        scope=CurrentSeasonStatisticsScope(league_external_id=39, season_start_year=2024, max_requests=1),
        client=client,
        sleep=_no_sleep,
    )

    assert client.calls == report.api_requests == 1
    assert report.retries == 0
    assert report.stopped_reason == "run_request_cap"


def test_active_scope_lock_prevents_a_second_run(monkeypatch: pytest.MonkeyPatch) -> None:
    assert TEST_DB_URL is not None
    monkeypatch.setenv("SUPABASE_DB_URL", TEST_DB_URL)
    scope = CurrentSeasonStatisticsScope(league_external_id=39, season_start_year=2024, max_requests=1)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(hashtextextended(%s, 0))", (_lock_key(scope),))
        try:
            with pytest.raises(CurrentSeasonStatisticsError, match="already active"):
                run_current_season_statistics_backfill(scope=scope, client=RetryingFailureClient(), sleep=_no_sleep)
        finally:
            conn.execute("SELECT pg_advisory_unlock(hashtextextended(%s, 0))", (_lock_key(scope),))


def test_db_provenance_omits_requested_fixture_absent_from_provider_response() -> None:
    assert TEST_DB_URL is not None
    with psycopg.connect(TEST_DB_URL, autocommit=True) as conn:
        provider_id, season_id = conn.execute(
            """SELECT provider.id,season_ref.season_id FROM source.providers provider
               JOIN source.season_provider_refs season_ref ON season_ref.provider_id=provider.id
               WHERE provider.code='api-football' AND season_ref.external_season=2024"""
        ).fetchone()
        targets = load_completed_targets(conn, provider_id=provider_id, season_id=season_id)[:3]
        params = {"ids": fixture_ids_parameter(target.external_fixture_id for target in targets)}
        fetch_id = conn.execute(
            """INSERT INTO source.provider_fetches(
                   provider_id,endpoint,request_params,purpose,request_started_at,response_received_at,http_status,outcome,
                   provider_results,paging_current,paging_total,subject_season_id
                 ) VALUES(%s,'/fixtures',%s,'scheduled_refresh',clock_timestamp(),clock_timestamp(),200,'success',2,1,1,%s)
                 RETURNING id""",
            (provider_id, Jsonb(params), season_id),
        ).fetchone()[0]
        _bind_returned_fixture_subjects(
            conn, fetch_id=fetch_id, returned_fixture_ids=[targets[0].fixture_id, targets[1].fixture_id], targets=targets
        )
        assert conn.execute(
            "SELECT array_agg(fixture_id ORDER BY fixture_id) FROM source.provider_fetch_fixture_subjects WHERE fetch_id=%s",
            (fetch_id,),
        ).fetchone()[0] == sorted([targets[0].fixture_id, targets[1].fixture_id])


def test_discovery_conflict_rolls_back_every_canonical_fixture_update(monkeypatch: pytest.MonkeyPatch) -> None:
    assert TEST_DB_URL is not None
    monkeypatch.setenv("SUPABASE_DB_URL", TEST_DB_URL)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as conn:
        provider_id, league_id = conn.execute(
            """SELECT provider.id,league_ref.league_id FROM source.providers provider
               JOIN source.league_provider_refs league_ref ON league_ref.provider_id=provider.id
               WHERE provider.code='api-football' AND league_ref.external_id='39'"""
        ).fetchone()
        season_id = conn.execute(
            "INSERT INTO football.seasons(league_id,start_year,label) VALUES(%s,2023,'2023/24') RETURNING id",
            (league_id,),
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO source.season_provider_refs(provider_id,league_external_id,external_season,season_id) VALUES(%s,'39',2023,%s)",
            (provider_id, season_id),
        )
        team_ids = [row[0] for row in conn.execute("SELECT id FROM football.teams ORDER BY id LIMIT 4").fetchall()]
        with conn.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO football.season_teams(season_id,team_id) VALUES(%s,%s)",
                [(season_id, team_id) for team_id in team_ids],
            )
        seed_fetch_id = conn.execute(
            """INSERT INTO source.provider_fetches(
                   provider_id,endpoint,request_params,purpose,request_started_at,response_received_at,http_status,outcome,
                   provider_results,paging_current,paging_total,subject_season_id
                 ) VALUES(%s,'/fixtures',%s,'bootstrap',%s,%s,200,'success',2,1,1,%s) RETURNING id""",
            (provider_id, Jsonb({"league": 39, "season": 2023}), datetime(2023, 1, 1, tzinfo=UTC), datetime(2023, 1, 1, 0, 1, tzinfo=UTC), season_id),
        ).fetchone()[0]
        fixture_ids = []
        for index, (home_id, away_id) in enumerate(((team_ids[0], team_ids[1]), (team_ids[2], team_ids[3])), start=1):
            fixture_id = conn.execute(
                """INSERT INTO football.fixtures(
                       season_id,home_team_id,away_team_id,kickoff_at,lifecycle_state,first_seen_at,last_seen_at,last_source_fetch_id
                     ) VALUES(%s,%s,%s,%s,'scheduled',%s,%s,%s) RETURNING id""",
                (season_id, home_id, away_id, datetime(2023, 8, index, 12, tzinfo=UTC),
                 datetime(2023, 1, 1, tzinfo=UTC), datetime(2023, 1, 1, tzinfo=UTC), seed_fetch_id),
            ).fetchone()[0]
            external_id = 9_000_000 + index
            conn.execute(
                "INSERT INTO source.fixture_provider_refs(provider_id,external_id,fixture_id) VALUES(%s,%s,%s)",
                (provider_id, str(external_id), fixture_id),
            )
            fixture_ids.append((fixture_id, external_id, home_id, away_id))
        external_teams = {
            int(team_id): int(external_id)
            for team_id, external_id in conn.execute(
                "SELECT team_id,external_id FROM source.team_provider_refs WHERE provider_id=%s", (provider_id,)
            ).fetchall()
        }

    response = []
    for index, (_, fixture_external_id, home_id, away_id) in enumerate(fixture_ids):
        response.append(
            {
                "fixture": {"id": fixture_external_id, "date": datetime(2023, 8, index + 1, 12, tzinfo=UTC).isoformat(), "status": {"short": "FT"}},
                "league": {"id": 39, "season": 2023},
                # The second entry deliberately conflicts after the first would update.
                "teams": {"home": {"id": external_teams[home_id]}, "away": {"id": external_teams[away_id if index == 0 else home_id]}},
                "goals": {"home": 1, "away": 0},
                "score": {"halftime": {"home": None, "away": None}, "fulltime": {"home": 1, "away": 0}, "extratime": {"home": None, "away": None}, "penalty": {"home": None, "away": None}},
            }
        )

    report = run_current_season_statistics_backfill(
        scope=CurrentSeasonStatisticsScope(league_external_id=39, season_start_year=2023, max_requests=1),
        client=ConflictDiscoveryClient(response), sleep=_no_sleep,
    )
    assert report.stopped_reason == "completed_fixture_discovery_error"
    with psycopg.connect(TEST_DB_URL, autocommit=True) as conn:
        states = conn.execute(
            "SELECT lifecycle_state::text,home_goals,away_goals,last_source_fetch_id FROM football.fixtures WHERE id=ANY(%s) ORDER BY id",
            ([fixture[0] for fixture in fixture_ids],),
        ).fetchall()
        assert states == [("scheduled", None, None, seed_fetch_id), ("scheduled", None, None, seed_fetch_id)]
        assert conn.execute(
            """SELECT outcome::text,normalized_at IS NULL FROM source.provider_fetches
               WHERE subject_season_id=%s AND endpoint='/fixtures' AND purpose='scheduled_refresh'
               ORDER BY id DESC LIMIT 1""",
            (season_id,),
        ).fetchone() == ("provider_error", True)
