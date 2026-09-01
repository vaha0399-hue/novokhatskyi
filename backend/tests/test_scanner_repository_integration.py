from __future__ import annotations

import os
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal

import psycopg
import pytest
from psycopg.types.json import Jsonb

from app.scanner import ScannerFilter, ScannerMetric, ScannerOperator, ScannerQuery, ScannerRepository, ScannerService, ScannerSide


TEST_DB_URL = os.environ.get("CURRENT_SEASON_STATISTICS_TEST_DB_URL")
pytestmark = pytest.mark.skipif(not TEST_DB_URL, reason="CURRENT_SEASON_STATISTICS_TEST_DB_URL is not configured")


def _insert_metrics(connection: psycopg.Connection, *, team_id: int, season_id: int, scope: str, xg: str, conceded_rate: str) -> None:
    connection.execute(
        """INSERT INTO football.team_rolling_metrics(
               team_id,season_id,scope,window_size,matches_count,
               avg_xg,xg_sample_count,avg_xga,xga_sample_count,avg_goals_for,avg_goals_against,
               scored_rate,conceded_rate,btts_rate,over_1_5_rate,over_2_5_rate,over_3_5_rate,
               avg_shots,shots_sample_count,avg_shots_on_goal,shots_on_goal_sample_count,
               avg_corners,corners_sample_count,avg_possession,possession_sample_count,
               source_last_kickoff_at,updated_at
             ) VALUES(
               %s,%s,%s,10,5,
               %s,4,1.1,4,1.8,0.8,
               0.8,%s,0.4,0.8,0.6,0.2,
               12,5,5,5,4,5,54,5,
               '2030-09-05 14:00:00+00','2030-09-05 14:01:00+00'
             )""",
        (team_id, season_id, scope, Decimal(xg), Decimal(conceded_rate)),
    )


def test_scanner_reads_only_future_scheduled_fixtures_with_complete_venue_metrics() -> None:
    assert TEST_DB_URL is not None
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, league_id = connection.execute(
            """SELECT provider.id,league_ref.league_id FROM source.providers provider
               JOIN source.league_provider_refs league_ref ON league_ref.provider_id=provider.id
               WHERE provider.code='api-football' AND league_ref.external_id='39'"""
        ).fetchone()
        season_id = connection.execute(
            "INSERT INTO football.seasons(league_id,start_year,label) VALUES(%s,2030,'2030/31') RETURNING id",
            (league_id,),
        ).fetchone()[0]
        connection.execute(
            "INSERT INTO source.season_provider_refs(provider_id,league_external_id,external_season,season_id) VALUES(%s,'39',2030,%s)",
            (provider_id, season_id),
        )
        home_team_id, away_team_id = [
            row[0] for row in connection.execute("SELECT id FROM football.teams ORDER BY id LIMIT 2").fetchall()
        ]
        connection.execute(
            "INSERT INTO football.season_teams(season_id,team_id) VALUES(%s,%s),(%s,%s)",
            (season_id, home_team_id, season_id, away_team_id),
        )
        observed_at = datetime(2030, 8, 1, tzinfo=UTC)
        fetch_id = connection.execute(
            """INSERT INTO source.provider_fetches(
                   provider_id,endpoint,request_params,purpose,request_started_at,response_received_at,http_status,outcome,
                   provider_results,paging_current,paging_total,subject_season_id
                 ) VALUES(%s,'/fixtures',%s,'bootstrap',%s,%s,200,'success',1,1,1,%s) RETURNING id""",
            (provider_id, Jsonb({"league": 39, "season": 2030}), observed_at, observed_at, season_id),
        ).fetchone()[0]
        kickoff = datetime(2030, 9, 5, 15, tzinfo=UTC)
        selected_fixture_id = connection.execute(
            """INSERT INTO football.fixtures(
                   season_id,home_team_id,away_team_id,kickoff_at,lifecycle_state,first_seen_at,last_seen_at,last_source_fetch_id
                 ) VALUES(%s,%s,%s,%s,'scheduled',%s,%s,%s) RETURNING id""",
            (season_id, home_team_id, away_team_id, kickoff, observed_at, observed_at, fetch_id),
        ).fetchone()[0]
        connection.execute(
            """INSERT INTO football.fixtures(
                   season_id,home_team_id,away_team_id,kickoff_at,lifecycle_state,first_seen_at,last_seen_at,last_source_fetch_id
                 ) VALUES(%s,%s,%s,%s,'scheduled',%s,%s,%s)""",
            (season_id, home_team_id, away_team_id, kickoff - timedelta(hours=2), observed_at, observed_at, fetch_id),
        )
        _insert_metrics(connection, team_id=home_team_id, season_id=season_id, scope="overall", xg="1.8", conceded_rate="0.6")
        _insert_metrics(connection, team_id=home_team_id, season_id=season_id, scope="home", xg="1.8", conceded_rate="0.6")
        _insert_metrics(connection, team_id=away_team_id, season_id=season_id, scope="overall", xg="1.1", conceded_rate="0.8")
        _insert_metrics(connection, team_id=away_team_id, season_id=season_id, scope="away", xg="1.1", conceded_rate="0.8")

        service = ScannerService(ScannerRepository(connection))
        query = ScannerQuery(
            match_date=date(2030, 9, 5), timezone="UTC", league_ids=(int(league_id),),
            window_size=10, min_matches=3,
            filters=(
                ScannerFilter(ScannerSide.HOME, ScannerMetric.AVG_XG, ScannerOperator.GREATER_THAN_OR_EQUAL, Decimal("1.7"), 3),
                ScannerFilter(ScannerSide.AWAY, ScannerMetric.CONCEDED_RATE, ScannerOperator.GREATER_THAN_OR_EQUAL, Decimal("0.7")),
            ),
            limit=50, offset=0,
        )
        timezone, total, fixtures = service.scan(query=query)
        _, empty_page_total, empty_page = service.scan(query=ScannerQuery(
            **{**query.__dict__, "offset": 1},
        ))

    assert timezone == "UTC"
    assert total == 1
    assert [fixture.fixture_id for fixture in fixtures] == [selected_fixture_id]
    assert fixtures[0].home_venue is not None
    assert fixtures[0].home_venue.xg_sample_count == 4
    assert fixtures[0].away_venue is not None
    assert fixtures[0].away_venue.conceded_rate == Decimal("0.8")
    assert empty_page_total == 1
    assert empty_page == []
