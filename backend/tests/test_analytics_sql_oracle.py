"""Independent PostgreSQL-oracle validation for Analytics Engine v1.

Run intentionally only when ANALYTICS_TEST_DB_URL is supplied.  The test is
read-only and compares every v1 metric across the real EPL 2024 data rather
than duplicating the engine's Python aggregation.
"""

from __future__ import annotations

import os
from decimal import Decimal, ROUND_HALF_UP

import psycopg
import pytest
from psycopg.rows import dict_row

from app.analytics.engine import AnalyticsEngine
from app.analytics.models import AnalyticsScope, AverageMetric, RateMetric
from app.analytics.repository import PostgresAnalyticsRepository


TEST_DB_URL = os.environ.get("ANALYTICS_TEST_DB_URL")
pytestmark = pytest.mark.skipif(not TEST_DB_URL, reason="ANALYTICS_TEST_DB_URL is not configured")


SQL_ORACLE = """
WITH history AS (
    SELECT f.id AS fixture_id, f.kickoff_at,
      CASE WHEN f.home_team_id = %(team_id)s THEN f.home_goals ELSE f.away_goals END AS goals_for,
      CASE WHEN f.home_team_id = %(team_id)s THEN f.away_goals ELSE f.home_goals END AS goals_against,
      own.expected_goals AS xg, opponent.expected_goals AS xga,
      own.total_shots, own.shots_on_goal, own.possession_pct, own.corner_kicks,
      own.yellow_cards, own.red_cards
    FROM football.fixtures AS f
    LEFT JOIN football.fixture_team_statistics AS own
      ON own.fixture_id = f.id AND own.team_id = %(team_id)s
    LEFT JOIN football.fixture_team_statistics AS opponent
      ON opponent.fixture_id = f.id AND opponent.team_id = CASE
        WHEN f.home_team_id = %(team_id)s THEN f.away_team_id ELSE f.home_team_id END
    WHERE f.season_id = %(season_id)s
      AND f.lifecycle_state = 'completed'
      AND f.result_finalized_at IS NOT NULL
      AND f.kickoff_at < %(cutoff_at)s
      AND %(team_id)s IN (f.home_team_id, f.away_team_id)
      AND (%(scope)s = 'overall'
        OR (%(scope)s = 'home' AND f.home_team_id = %(team_id)s)
        OR (%(scope)s = 'away' AND f.away_team_id = %(team_id)s))
), ranked AS (
    SELECT *, row_number() OVER (ORDER BY kickoff_at DESC, fixture_id DESC) AS rn
    FROM history
), sample AS (
    SELECT * FROM ranked WHERE rn <= %(window)s
), aggregate AS (
    SELECT
      count(*)::integer AS matches,
      count(*) FILTER (WHERE goals_for > goals_against)::integer AS wins,
      count(*) FILTER (WHERE goals_for = goals_against)::integer AS draws,
      count(*) FILTER (WHERE goals_for < goals_against)::integer AS losses,
      coalesce(sum(goals_for), 0)::integer AS goals_scored,
      coalesce(sum(goals_against), 0)::integer AS goals_conceded,
      round(avg(xg), 3) AS average_xg, count(xg)::integer AS xg_samples,
      round(avg(xga), 3) AS average_xga, count(xga)::integer AS xga_samples,
      round(avg(total_shots), 3) AS average_shots, count(total_shots)::integer AS shots_samples,
      round(avg(shots_on_goal), 3) AS average_shots_on_goal, count(shots_on_goal)::integer AS sot_samples,
      round(avg(possession_pct), 3) AS average_possession, count(possession_pct)::integer AS possession_samples,
      round(avg(corner_kicks), 3) AS average_corners, count(corner_kicks)::integer AS corners_samples,
      round(avg(yellow_cards), 3) AS average_yellow_cards, count(yellow_cards)::integer AS yellow_samples,
      round(avg(red_cards), 3) AS average_red_cards, count(red_cards)::integer AS red_samples,
      count(*) FILTER (WHERE goals_against = 0)::integer AS clean_sheets,
      count(*) FILTER (WHERE goals_for = 0)::integer AS failed_to_score,
      count(*) FILTER (WHERE goals_for > 0 AND goals_against > 0)::integer AS btts,
      count(*) FILTER (WHERE goals_for + goals_against > 0.5)::integer AS over_0_5,
      count(*) FILTER (WHERE goals_for + goals_against > 1.5)::integer AS over_1_5,
      count(*) FILTER (WHERE goals_for + goals_against > 2.5)::integer AS over_2_5,
      count(*) FILTER (WHERE goals_for + goals_against > 3.5)::integer AS over_3_5
    FROM sample
)
SELECT aggregate.*,
  round((wins * 3 + draws)::numeric / nullif(matches, 0), 3) AS ppg,
  round(goals_scored::numeric / nullif(matches, 0), 3) AS average_goals_scored,
  round(goals_conceded::numeric / nullif(matches, 0), 3) AS average_goals_conceded,
  coalesce((SELECT min(rn) - 1 FROM sample WHERE NOT (goals_for > goals_against)), matches)::integer AS win_streak,
  coalesce((SELECT min(rn) - 1 FROM sample WHERE NOT (goals_for >= goals_against)), matches)::integer AS unbeaten_streak,
  coalesce((SELECT min(rn) - 1 FROM sample WHERE NOT (goals_for <= goals_against)), matches)::integer AS winless_streak,
  coalesce((SELECT min(rn) - 1 FROM sample WHERE NOT (goals_for < goals_against)), matches)::integer AS loss_streak,
  coalesce((SELECT min(rn) - 1 FROM sample WHERE NOT (goals_for > 0)), matches)::integer AS scored_streak,
  coalesce((SELECT min(rn) - 1 FROM sample WHERE NOT (goals_against = 0)), matches)::integer AS clean_sheet_streak,
  coalesce((SELECT min(rn) - 1 FROM sample WHERE NOT (goals_for > 0 AND goals_against > 0)), matches)::integer AS btts_streak
FROM aggregate
"""


def _target(connection: psycopg.Connection) -> tuple[int, int, object, int]:
    row = connection.execute(
        """SELECT id, season_id, kickoff_at, home_team_id
           FROM football.fixtures WHERE lifecycle_state='completed'
           ORDER BY kickoff_at DESC, id DESC LIMIT 1"""
    ).fetchone()
    assert row is not None
    return row


def _assert_average(actual: AverageMetric, row: dict, name: str, sample_name: str) -> None:
    assert actual.value == row[name]
    assert actual.sample_size == row[sample_name]


def _assert_rate(actual: RateMetric, count: int, matches: int) -> None:
    assert actual.count == count
    expected = None if not matches else (Decimal(count) / Decimal(matches)).quantize(
        Decimal("0.001"), rounding=ROUND_HALF_UP
    )
    assert actual.rate == expected


@pytest.mark.parametrize("scope", list(AnalyticsScope))
@pytest.mark.parametrize("window", [5, 10, 15, 20])
def test_all_v1_metrics_match_independent_sql_oracle(scope: AnalyticsScope, window: int) -> None:
    assert TEST_DB_URL is not None
    with psycopg.connect(TEST_DB_URL, autocommit=True) as engine_connection:
        _, season_id, cutoff, team_id = _target(engine_connection)
        actual = AnalyticsEngine(PostgresAnalyticsRepository(engine_connection)).team_analytics(
            team_id=team_id, season_id=season_id, as_of_kickoff=cutoff, scope=scope,
        ).windows[window]
    with psycopg.connect(TEST_DB_URL, autocommit=True, row_factory=dict_row) as oracle_connection:
        row = oracle_connection.execute(
            SQL_ORACLE,
            {"team_id": team_id, "season_id": season_id, "cutoff_at": cutoff, "scope": scope.value, "window": window},
        ).fetchone()
    assert row is not None

    assert (actual.matches, actual.wins, actual.draws, actual.losses) == (
        row["matches"], row["wins"], row["draws"], row["losses"],
    )
    assert actual.points == row["wins"] * 3 + row["draws"]
    assert actual.points_per_game == row["ppg"]
    assert (actual.goals_scored, actual.goals_conceded) == (row["goals_scored"], row["goals_conceded"])
    assert actual.average_goals_scored == row["average_goals_scored"]
    assert actual.average_goals_conceded == row["average_goals_conceded"]
    _assert_average(actual.average_xg, row, "average_xg", "xg_samples")
    _assert_average(actual.average_xga, row, "average_xga", "xga_samples")
    _assert_average(actual.average_shots, row, "average_shots", "shots_samples")
    _assert_average(actual.average_shots_on_goal, row, "average_shots_on_goal", "sot_samples")
    _assert_average(actual.average_possession_pct, row, "average_possession", "possession_samples")
    _assert_average(actual.average_corners, row, "average_corners", "corners_samples")
    _assert_average(actual.average_yellow_cards, row, "average_yellow_cards", "yellow_samples")
    _assert_average(actual.average_red_cards, row, "average_red_cards", "red_samples")
    _assert_rate(actual.clean_sheets, row["clean_sheets"], row["matches"])
    _assert_rate(actual.failed_to_score, row["failed_to_score"], row["matches"])
    _assert_rate(actual.btts, row["btts"], row["matches"])
    for threshold in ("0.5", "1.5", "2.5", "3.5"):
        over = row[f"over_{threshold.replace('.', '_')}"]
        _assert_rate(actual.total_goals[threshold].over, over, row["matches"])
        _assert_rate(actual.total_goals[threshold].under, row["matches"] - over, row["matches"])
    assert actual.streaks.wins == row["win_streak"]
    assert actual.streaks.unbeaten == row["unbeaten_streak"]
    assert actual.streaks.winless == row["winless_streak"]
    assert actual.streaks.losses == row["loss_streak"]
    assert actual.streaks.scored == row["scored_streak"]
    assert actual.streaks.clean_sheets == row["clean_sheet_streak"]
    assert actual.streaks.btts == row["btts_streak"]
