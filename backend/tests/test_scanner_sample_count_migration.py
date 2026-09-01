from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

import psycopg
import pytest


TEST_DB_URL = os.environ.get("CURRENT_SEASON_STATISTICS_TEST_DB_URL")
pytestmark = pytest.mark.skipif(not TEST_DB_URL, reason="CURRENT_SEASON_STATISTICS_TEST_DB_URL is not configured")
MIGRATION = Path(__file__).parents[2] / "supabase" / "migrations" / "20260901193000_scanner_metric_sample_counts.sql"


def test_sample_count_migration_backfills_legacy_rolling_rows_from_finalized_statistics() -> None:
    assert TEST_DB_URL is not None
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        assert connection.execute(
            """SELECT NOT EXISTS (
                   SELECT 1 FROM information_schema.columns
                   WHERE table_schema='football' AND table_name='team_rolling_metrics'
                     AND column_name='xg_sample_count'
                 )"""
        ).fetchone()[0] is True
        team_id, season_id = connection.execute(
            """SELECT team_id,season_id FROM (
                   SELECT home_team_id AS team_id,season_id,id,kickoff_at FROM football.fixtures
                   WHERE lifecycle_state='completed' AND result_finalized_at IS NOT NULL
                   UNION ALL
                   SELECT away_team_id AS team_id,season_id,id,kickoff_at FROM football.fixtures
                   WHERE lifecycle_state='completed' AND result_finalized_at IS NOT NULL
                 ) appearances
                 GROUP BY team_id,season_id
                 HAVING count(*) >= 2
                 ORDER BY season_id,team_id
                 LIMIT 1"""
        ).fetchone()
        fixtures = connection.execute(
            """SELECT id,home_team_id,away_team_id,kickoff_at
                 FROM football.fixtures
                 WHERE season_id=%s AND %s IN (home_team_id,away_team_id)
                   AND lifecycle_state='completed' AND result_finalized_at IS NOT NULL
                 ORDER BY kickoff_at DESC,id DESC LIMIT 2""",
            (season_id, team_id),
        ).fetchall()
        assert len(fixtures) == 2
        for index, (fixture_id, home_team_id, away_team_id, kickoff_at) in enumerate(fixtures):
            own_is_home = home_team_id == team_id
            own_team_id = home_team_id if own_is_home else away_team_id
            opponent_team_id = away_team_id if own_is_home else home_team_id
            own_xg = "1.50" if index == 0 else None
            available_at = kickoff_at + timedelta(hours=3)
            connection.execute(
                """INSERT INTO football.fixture_team_statistics(
                       fixture_id,team_id,shots_on_goal,total_shots,corner_kicks,possession_pct,expected_goals,
                       mapping_version,observed_at,available_at,availability_basis
                     ) VALUES(%s,%s,%s,%s,%s,%s,%s,'test-v1',%s,%s,'reconstructed_conservative'),
                              (%s,%s,%s,%s,%s,%s,%s,'test-v1',%s,%s,'reconstructed_conservative')""",
                (
                    fixture_id, own_team_id, 4 if index == 0 else None, 10, 5 if index == 0 else None,
                    "55.0", own_xg, kickoff_at, available_at,
                    fixture_id, opponent_team_id, 3, 9, 4, "45.0", "0.80", kickoff_at, available_at,
                ),
            )
        connection.execute(
            """INSERT INTO football.team_rolling_metrics(
                   team_id,season_id,scope,window_size,matches_count,
                   avg_xg,avg_xga,avg_goals_for,avg_goals_against,
                   scored_rate,conceded_rate,btts_rate,over_1_5_rate,over_2_5_rate,over_3_5_rate,
                   avg_shots,avg_shots_on_goal,avg_corners,avg_possession,source_last_kickoff_at
                 ) VALUES(%s,%s,'overall',10,10,
                   1.5,0.8,1.0,1.0,
                   0.5,0.5,0.5,0.5,0.5,0.0,
                   10,4,5,55,%s)""",
            (team_id, season_id, fixtures[0][3]),
        )
        connection.execute(MIGRATION.read_text())
        counts = connection.execute(
            """SELECT matches_count,xg_sample_count,xga_sample_count,shots_sample_count,
                      shots_on_goal_sample_count,corners_sample_count,possession_sample_count
                 FROM football.team_rolling_metrics
                 WHERE team_id=%s AND season_id=%s AND scope='overall' AND window_size=10""",
            (team_id, season_id),
        ).fetchone()

    # One fixture deliberately lacks own xG.  The migration retains that
    # distinction instead of claiming all ten selected matches had xG.
    assert counts == (10, 9, 10, 10, 9, 1, 10)
