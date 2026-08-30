"""Read-only SQL repository for stable web DTO composition."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

from psycopg import Connection

from app.analytics.models import FixtureContext


@dataclass(frozen=True)
class TeamRecord:
    id: int
    name: str


@dataclass(frozen=True)
class LeagueRecord:
    id: int
    name: str
    country_name: str | None
    logo_url: str | None
    competition_type: str | None


@dataclass(frozen=True)
class SeasonRecord:
    id: int
    league: LeagueRecord
    start_year: int
    label: str
    starts_on: date | None
    ends_on: date | None


@dataclass(frozen=True)
class StandingRecord:
    group_index: int
    group_name: str | None
    team: TeamRecord
    rank: int
    points: int
    played: int
    wins: int
    draws: int
    losses: int
    goals_for: int
    goals_against: int
    goals_diff: int
    form: str | None
    status: str | None
    description: str | None


@dataclass(frozen=True)
class FixtureStatisticsRecord:
    team_id: int
    shots_on_goal: int | None
    shots_off_goal: int | None
    total_shots: int | None
    blocked_shots: int | None
    shots_inside_box: int | None
    shots_outside_box: int | None
    fouls: int | None
    corner_kicks: int | None
    offsides: int | None
    yellow_cards: int | None
    red_cards: int | None
    goalkeeper_saves: int | None
    total_passes: int | None
    passes_accurate: int | None
    possession_pct: Any
    pass_accuracy_pct: Any
    expected_goals: Any
    goals_prevented: Any


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


@dataclass(frozen=True)
class MatchDateLeagueRecord:
    league: LeagueRecord
    fixture_count: int


class WebReadRepository:
    """All methods are SELECT-only and accept canonical internal IDs."""

    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection

    def season_exists(self, *, season_id: int) -> bool:
        return self._connection.execute(
            "SELECT EXISTS (SELECT 1 FROM football.seasons WHERE id=%s)", (season_id,)
        ).fetchone()[0]

    def list_leagues(self) -> list[LeagueRecord]:
        rows = self._connection.execute(
            """SELECT id,name,country_name,logo_url,competition_type
                 FROM football.leagues
                 WHERE retired_at IS NULL
                 ORDER BY country_name NULLS LAST,name ASC,id ASC"""
        ).fetchall()
        return [self._league_record(row) for row in rows]

    def league(self, *, league_id: int) -> LeagueRecord | None:
        row = self._connection.execute(
            """SELECT id,name,country_name,logo_url,competition_type
                 FROM football.leagues WHERE id=%s""", (league_id,)
        ).fetchone()
        return None if row is None else self._league_record(row)

    def list_league_seasons(self, *, league_id: int) -> list[SeasonRecord]:
        rows = self._connection.execute(
            """SELECT s.id,l.id,l.name,l.country_name,l.logo_url,l.competition_type,
                      s.start_year,s.label,s.starts_on,s.ends_on
                 FROM football.seasons s
                 JOIN football.leagues l ON l.id=s.league_id
                 WHERE s.league_id=%s
                 ORDER BY s.start_year DESC,s.id DESC""", (league_id,)
        ).fetchall()
        return [self._season_record(row) for row in rows]

    def season(self, *, season_id: int) -> SeasonRecord | None:
        row = self._connection.execute(
            """SELECT s.id,l.id,l.name,l.country_name,l.logo_url,l.competition_type,
                      s.start_year,s.label,s.starts_on,s.ends_on
                 FROM football.seasons s
                 JOIN football.leagues l ON l.id=s.league_id
                 WHERE s.id=%s""", (season_id,)
        ).fetchone()
        return None if row is None else self._season_record(row)

    def latest_season_standings(self, *, season_id: int) -> tuple[datetime, list[StandingRecord]] | None:
        snapshot = self._connection.execute(
            """SELECT id,captured_at FROM football.standings_snapshots
                 WHERE season_id=%s
                 ORDER BY captured_at DESC,id DESC LIMIT 1""", (season_id,)
        ).fetchone()
        if snapshot is None:
            return None
        snapshot_id, captured_at = snapshot
        rows = self._connection.execute(
            """SELECT row.group_index,grp.group_name,t.id,t.name,row.rank,row.points,row.played,
                      row.wins,row.draws,row.losses,row.goals_for,row.goals_against,row.goals_diff,
                      row.form,row.status,row.description
                 FROM football.standings_snapshot_rows row
                 JOIN football.standings_snapshot_groups grp
                   ON grp.snapshot_id=row.snapshot_id AND grp.group_index=row.group_index
                 JOIN football.teams t ON t.id=row.team_id
                 WHERE row.snapshot_id=%s
                 ORDER BY row.group_index ASC,row.rank ASC,t.id ASC""", (snapshot_id,)
        ).fetchall()
        return captured_at, [self._standing_record(row) for row in rows]

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

    def list_match_date_leagues(
        self, *, start_at: datetime, end_at: datetime
    ) -> list[MatchDateLeagueRecord]:
        rows = self._connection.execute(
            """SELECT l.id,l.name,l.country_name,l.logo_url,l.competition_type,count(*)
                 FROM football.fixtures f
                 JOIN football.seasons s ON s.id=f.season_id
                 JOIN football.leagues l ON l.id=s.league_id
                 WHERE f.kickoff_at >= %s AND f.kickoff_at < %s
                 GROUP BY l.id,l.name,l.country_name,l.logo_url,l.competition_type
                 ORDER BY l.country_name NULLS LAST,l.name ASC,l.id ASC""",
            (start_at, end_at),
        ).fetchall()
        return [
            MatchDateLeagueRecord(
                league=self._league_record(row[:5]), fixture_count=int(row[5])
            )
            for row in rows
        ]

    def list_league_matches(
        self, *, league_id: int, start_at: datetime, end_at: datetime
    ) -> list[FixtureRecord]:
        rows = self._connection.execute(
            """SELECT f.id,f.season_id,f.kickoff_at,f.home_team_id,f.away_team_id,
                      f.round_label,f.lifecycle_state::text,f.home_goals,f.away_goals,f.result_finalized_at,
                      home.name,away.name
                 FROM football.fixtures f
                 JOIN football.seasons s ON s.id=f.season_id
                 JOIN football.teams home ON home.id=f.home_team_id
                 JOIN football.teams away ON away.id=f.away_team_id
                 WHERE s.league_id=%s
                   AND f.kickoff_at >= %s AND f.kickoff_at < %s
                 ORDER BY f.kickoff_at ASC,f.id ASC""",
            (league_id, start_at, end_at),
        ).fetchall()
        return [self._fixture_record(row) for row in rows]

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

    def fixture_statistics(self, *, fixture_id: int) -> list[FixtureStatisticsRecord]:
        rows = self._connection.execute(
            """SELECT team_id,shots_on_goal,shots_off_goal,total_shots,blocked_shots,
                      shots_inside_box,shots_outside_box,fouls,corner_kicks,offsides,
                      yellow_cards,red_cards,goalkeeper_saves,total_passes,passes_accurate,
                      possession_pct,pass_accuracy_pct,expected_goals,goals_prevented
                 FROM football.fixture_team_statistics
                 WHERE fixture_id=%s
                 ORDER BY team_id ASC""", (fixture_id,)
        ).fetchall()
        return [self._fixture_statistics_record(row) for row in rows]

    @staticmethod
    def _league_record(row: tuple[Any, ...]) -> LeagueRecord:
        league_id, name, country_name, logo_url, competition_type = row
        return LeagueRecord(
            id=int(league_id), name=str(name), country_name=country_name,
            logo_url=logo_url, competition_type=competition_type,
        )

    @staticmethod
    def _season_record(row: tuple[Any, ...]) -> SeasonRecord:
        season_id, league_id, league_name, country_name, logo_url, competition_type, start_year, label, starts_on, ends_on = row
        return SeasonRecord(
            id=int(season_id),
            league=LeagueRecord(
                id=int(league_id), name=str(league_name), country_name=country_name,
                logo_url=logo_url, competition_type=competition_type,
            ),
            start_year=int(start_year), label=str(label), starts_on=starts_on, ends_on=ends_on,
        )

    @staticmethod
    def _standing_record(row: tuple[Any, ...]) -> StandingRecord:
        (group_index, group_name, team_id, team_name, rank, points, played, wins, draws,
         losses, goals_for, goals_against, goals_diff, form, status, description) = row
        return StandingRecord(
            group_index=int(group_index), group_name=group_name,
            team=TeamRecord(id=int(team_id), name=str(team_name)), rank=int(rank), points=int(points),
            played=int(played), wins=int(wins), draws=int(draws), losses=int(losses),
            goals_for=int(goals_for), goals_against=int(goals_against), goals_diff=int(goals_diff),
            form=form, status=status, description=description,
        )

    @staticmethod
    def _fixture_statistics_record(row: tuple[Any, ...]) -> FixtureStatisticsRecord:
        return FixtureStatisticsRecord(
            team_id=int(row[0]), shots_on_goal=row[1], shots_off_goal=row[2], total_shots=row[3],
            blocked_shots=row[4], shots_inside_box=row[5], shots_outside_box=row[6], fouls=row[7],
            corner_kicks=row[8], offsides=row[9], yellow_cards=row[10], red_cards=row[11],
            goalkeeper_saves=row[12], total_passes=row[13], passes_accurate=row[14],
            possession_pct=row[15], pass_accuracy_pct=row[16], expected_goals=row[17],
            goals_prevented=row[18],
        )

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
