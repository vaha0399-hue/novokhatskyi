"""Read-only SQL repository for stable web DTO composition."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from psycopg import Connection

from app.analytics.models import FixtureContext


@dataclass(frozen=True)
class TeamRecord:
    id: int
    name: str


@dataclass(frozen=True)
class FixtureRecord:
    context: FixtureContext
    round_label: str | None
    lifecycle_state: str
    home_team: TeamRecord
    away_team: TeamRecord
    home_goals: int | None
    away_goals: int | None
    result_finalized_at: datetime | None


class WebReadRepository:
    """All methods are SELECT-only and accept canonical internal IDs."""

    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection

    def season_exists(self, *, season_id: int) -> bool:
        return self._connection.execute(
            "SELECT EXISTS (SELECT 1 FROM football.seasons WHERE id=%s)", (season_id,)
        ).fetchone()[0]

    def list_season_fixtures(self, *, season_id: int, limit: int, offset: int) -> tuple[int, list[FixtureRecord]]:
        total = int(self._connection.execute(
            "SELECT count(*) FROM football.fixtures WHERE season_id=%s", (season_id,)
        ).fetchone()[0])
        rows = self._connection.execute(
            """SELECT f.id,f.season_id,f.kickoff_at,f.home_team_id,f.away_team_id,
                      f.round_label,f.lifecycle_state::text,f.home_goals,f.away_goals,f.result_finalized_at,
                      home.name,away.name
                 FROM football.fixtures f
                 JOIN football.teams home ON home.id=f.home_team_id
                 JOIN football.teams away ON away.id=f.away_team_id
                 WHERE f.season_id=%s
                 ORDER BY f.kickoff_at ASC,f.id ASC LIMIT %s OFFSET %s""",
            (season_id, limit, offset),
        ).fetchall()
        return total, [self._fixture_record(row) for row in rows]

    def team_in_season(self, *, team_id: int, season_id: int) -> TeamRecord | None:
        row = self._connection.execute(
            """SELECT t.id,t.name FROM football.teams t
               JOIN football.season_teams st ON st.team_id=t.id
               WHERE t.id=%s AND st.season_id=%s""",
            (team_id, season_id),
        ).fetchone()
        return None if row is None else TeamRecord(id=int(row[0]), name=str(row[1]))

    def latest_completed_team_cutoff(self, *, team_id: int, season_id: int) -> datetime | None:
        row = self._connection.execute(
            """SELECT max(kickoff_at) FROM football.fixtures
               WHERE season_id=%s AND lifecycle_state='completed' AND result_finalized_at IS NOT NULL
                 AND %s IN (home_team_id,away_team_id)""",
            (season_id, team_id),
        ).fetchone()
        if row[0] is None:
            return None
        # A team-season summary has no target fixture. The minimal increment
        # includes all completed fixtures at the final simultaneous kickoff.
        return row[0] + timedelta(microseconds=1)

    def fixture(self, *, fixture_id: int) -> FixtureRecord | None:
        row = self._connection.execute(
            """SELECT f.id,f.season_id,f.kickoff_at,f.home_team_id,f.away_team_id,
                      f.round_label,f.lifecycle_state::text,f.home_goals,f.away_goals,f.result_finalized_at,
                      home.name,away.name
                 FROM football.fixtures f
                 JOIN football.teams home ON home.id=f.home_team_id
                 JOIN football.teams away ON away.id=f.away_team_id
                 WHERE f.id=%s""",
            (fixture_id,),
        ).fetchone()
        return None if row is None else self._fixture_record(row)

    @staticmethod
    def _fixture_record(row: tuple[Any, ...]) -> FixtureRecord:
        fixture_id, season_id, kickoff, home_id, away_id, round_label, lifecycle, home_goals, away_goals, result_finalized_at, home_name, away_name = row
        return FixtureRecord(
            context=FixtureContext(int(fixture_id), int(season_id), kickoff, int(home_id), int(away_id)),
            round_label=round_label, lifecycle_state=str(lifecycle),
            home_team=TeamRecord(int(home_id), str(home_name)),
            away_team=TeamRecord(int(away_id), str(away_name)),
            home_goals=None if home_goals is None else int(home_goals),
            away_goals=None if away_goals is None else int(away_goals),
            result_finalized_at=result_finalized_at,
        )
