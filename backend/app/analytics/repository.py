"""PostgreSQL read access for cutoff-safe historical analytics."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from typing import Any, Protocol

import psycopg

from .models import AnalyticsScope, FixtureContext, TeamMatchRecord


class AnalyticsRepository(Protocol):
    def load_team_history(
        self,
        *,
        team_id: int,
        season_id: int,
        as_of_kickoff: datetime,
        scope: AnalyticsScope,
    ) -> Sequence[TeamMatchRecord]: ...

    def load_fixture_context(self, *, fixture_id: int) -> FixtureContext: ...


TEAM_HISTORY_SQL = """
SELECT
    f.id AS fixture_id,
    f.kickoff_at,
    (f.home_team_id = %(team_id)s) AS is_home,
    CASE WHEN f.home_team_id = %(team_id)s THEN f.home_goals ELSE f.away_goals END AS goals_for,
    CASE WHEN f.home_team_id = %(team_id)s THEN f.away_goals ELSE f.home_goals END AS goals_against,
    own.expected_goals,
    opponent.expected_goals AS expected_goals_against,
    own.total_shots,
    own.shots_on_goal,
    own.possession_pct,
    own.corner_kicks,
    own.yellow_cards,
    own.red_cards
FROM football.fixtures AS f
LEFT JOIN football.fixture_team_statistics AS own
  ON own.fixture_id = f.id AND own.team_id = %(team_id)s
LEFT JOIN football.fixture_team_statistics AS opponent
  ON opponent.fixture_id = f.id
 AND opponent.team_id = CASE
     WHEN f.home_team_id = %(team_id)s THEN f.away_team_id
     ELSE f.home_team_id
 END
WHERE f.season_id = %(season_id)s
  AND f.lifecycle_state = 'completed'
  AND f.result_finalized_at IS NOT NULL
  AND f.kickoff_at < %(as_of_kickoff)s
  AND (%(scope)s = 'overall'
       OR (%(scope)s = 'home' AND f.home_team_id = %(team_id)s)
       OR (%(scope)s = 'away' AND f.away_team_id = %(team_id)s))
  AND %(team_id)s IN (f.home_team_id, f.away_team_id)
ORDER BY f.kickoff_at DESC, f.id DESC
"""

FIXTURE_CONTEXT_SQL = """
SELECT id, season_id, kickoff_at, home_team_id, away_team_id
FROM football.fixtures
WHERE id = %(fixture_id)s
"""


class PostgresAnalyticsRepository:
    """Read-only repository. It opens no external-data connection."""

    def __init__(self, connection: psycopg.Connection[Any]) -> None:
        self._connection = connection

    def load_team_history(
        self,
        *,
        team_id: int,
        season_id: int,
        as_of_kickoff: datetime,
        scope: AnalyticsScope,
    ) -> list[TeamMatchRecord]:
        if as_of_kickoff.tzinfo is None:
            raise ValueError("as_of_kickoff must be timezone-aware")
        rows = self._connection.execute(
            TEAM_HISTORY_SQL,
            {
                "team_id": team_id,
                "season_id": season_id,
                "as_of_kickoff": as_of_kickoff,
                "scope": scope.value,
            },
        ).fetchall()
        return [self._record(row) for row in rows]

    def load_fixture_context(self, *, fixture_id: int) -> FixtureContext:
        row = self._connection.execute(FIXTURE_CONTEXT_SQL, {"fixture_id": fixture_id}).fetchone()
        if row is None:
            raise LookupError("fixture does not exist")
        return FixtureContext(
            fixture_id=int(row[0]), season_id=int(row[1]), kickoff_at=row[2],
            home_team_id=int(row[3]), away_team_id=int(row[4]),
        )

    @staticmethod
    def _record(row: tuple[Any, ...]) -> TeamMatchRecord:
        fixture_id, kickoff_at, is_home, goals_for, goals_against, *metrics = row
        if goals_for is None or goals_against is None:
            raise ValueError("completed fixture has no final score")
        return TeamMatchRecord(
            fixture_id=int(fixture_id), kickoff_at=kickoff_at, is_home=bool(is_home),
            goals_for=int(goals_for), goals_against=int(goals_against),
            expected_goals=metrics[0], expected_goals_against=metrics[1],
            total_shots=metrics[2], shots_on_goal=metrics[3], possession_pct=metrics[4],
            corner_kicks=metrics[5], yellow_cards=metrics[6], red_cards=metrics[7],
        )
