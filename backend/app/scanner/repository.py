"""Single-query canonical reads for scheduled-fixture scanning."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Mapping, Sequence

from psycopg import Connection

from .models import NULLABLE_AVERAGE_SAMPLE_COLUMNS, ScannerFilter, ScannerOperator, ScannerQuery, ScannerSide


@dataclass(frozen=True)
class ScannerTeamRecord:
    id: int
    name: str


@dataclass(frozen=True)
class ScannerLeagueRecord:
    id: int
    name: str
    country_name: str | None
    logo_url: str | None
    competition_type: str | None


@dataclass(frozen=True)
class ScannerMetricRecord:
    matches_count: int
    avg_xg: Decimal | None
    xg_sample_count: int
    avg_xga: Decimal | None
    xga_sample_count: int
    avg_goals_for: Decimal
    avg_goals_against: Decimal
    scored_rate: Decimal
    conceded_rate: Decimal
    btts_rate: Decimal
    over_1_5_rate: Decimal
    over_2_5_rate: Decimal
    over_3_5_rate: Decimal
    avg_shots: Decimal | None
    shots_sample_count: int
    avg_shots_on_goal: Decimal | None
    shots_on_goal_sample_count: int
    avg_corners: Decimal | None
    corners_sample_count: int
    avg_possession: Decimal | None
    possession_sample_count: int
    source_last_kickoff_at: datetime | None
    updated_at: datetime


@dataclass(frozen=True)
class ScannerFixtureRecord:
    fixture_id: int
    season_id: int
    kickoff_at: datetime
    round_label: str | None
    league: ScannerLeagueRecord
    home_team: ScannerTeamRecord
    away_team: ScannerTeamRecord
    home_overall: ScannerMetricRecord | None
    home_venue: ScannerMetricRecord | None
    away_overall: ScannerMetricRecord | None
    away_venue: ScannerMetricRecord | None


_OPERATOR_SQL = {
    ScannerOperator.GREATER_THAN: ">",
    ScannerOperator.GREATER_THAN_OR_EQUAL: ">=",
    ScannerOperator.LESS_THAN: "<",
    ScannerOperator.LESS_THAN_OR_EQUAL: "<=",
}


def _metric_record(value: Mapping[str, Any] | None) -> ScannerMetricRecord | None:
    if value is None:
        return None
    numeric = lambda key: None if value[key] is None else Decimal(str(value[key]))
    timestamp = lambda key: (
        None if value[key] is None else (
            value[key] if isinstance(value[key], datetime)
            else datetime.fromisoformat(str(value[key]).replace("Z", "+00:00"))
        )
    )
    return ScannerMetricRecord(
        matches_count=int(value["matches_count"]),
        avg_xg=numeric("avg_xg"), xg_sample_count=int(value["xg_sample_count"]),
        avg_xga=numeric("avg_xga"), xga_sample_count=int(value["xga_sample_count"]),
        avg_goals_for=Decimal(str(value["avg_goals_for"])),
        avg_goals_against=Decimal(str(value["avg_goals_against"])),
        scored_rate=Decimal(str(value["scored_rate"])), conceded_rate=Decimal(str(value["conceded_rate"])),
        btts_rate=Decimal(str(value["btts_rate"])), over_1_5_rate=Decimal(str(value["over_1_5_rate"])),
        over_2_5_rate=Decimal(str(value["over_2_5_rate"])), over_3_5_rate=Decimal(str(value["over_3_5_rate"])),
        avg_shots=numeric("avg_shots"), shots_sample_count=int(value["shots_sample_count"]),
        avg_shots_on_goal=numeric("avg_shots_on_goal"),
        shots_on_goal_sample_count=int(value["shots_on_goal_sample_count"]),
        avg_corners=numeric("avg_corners"), corners_sample_count=int(value["corners_sample_count"]),
        avg_possession=numeric("avg_possession"), possession_sample_count=int(value["possession_sample_count"]),
        source_last_kickoff_at=timestamp("source_last_kickoff_at"), updated_at=timestamp("updated_at"),
    )


class ScannerRepository:
    """SELECT-only repository using internal canonical league IDs."""

    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection

    def missing_league_ids(self, league_ids: Sequence[int]) -> set[int]:
        if not league_ids:
            return set()
        rows = self._connection.execute(
            "SELECT id FROM football.leagues WHERE id = ANY(%s)", (list(league_ids),)
        ).fetchall()
        return set(league_ids).difference(int(row[0]) for row in rows)

    def scan_scheduled_fixtures(
        self, *, query: ScannerQuery, start_at: datetime, end_at: datetime
    ) -> tuple[int, list[ScannerFixtureRecord]]:
        candidate_sql, params = self._candidate_sql(
            query=query, start_at=start_at, end_at=end_at,
        )
        total = int(self._connection.execute(f"SELECT count(*) {candidate_sql}", params).fetchone()[0])
        if total == 0 or query.offset >= total:
            return total, []
        rows = self._connection.execute(
            f"""SELECT fixture.id,fixture.season_id,fixture.kickoff_at,fixture.round_label,
                       league.id,league.name,league.country_name,league.logo_url,league.competition_type,
                       home.id,home.name,away.id,away.name,
                       to_jsonb(home_overall),to_jsonb(home_venue),
                       to_jsonb(away_overall),to_jsonb(away_venue)
                {candidate_sql}
                ORDER BY fixture.kickoff_at ASC,fixture.id ASC
                LIMIT %(limit)s OFFSET %(offset)s""",
            {**params, "limit": query.limit, "offset": query.offset},
        ).fetchall()
        return total, [self._fixture_record(row) for row in rows]

    def _candidate_sql(
        self, *, query: ScannerQuery, start_at: datetime, end_at: datetime
    ) -> tuple[str, dict[str, object]]:
        filters_sql, filter_params = self._filters_sql(query.filters)
        league_sql = ""
        params: dict[str, object] = {
            "start_at": start_at,
            "end_at": end_at,
            "window_size": query.window_size,
            "min_matches": query.min_matches,
            **filter_params,
        }
        if query.league_ids:
            league_sql = "AND season.league_id = ANY(%(league_ids)s)"
            params["league_ids"] = list(query.league_ids)
        return f"""FROM football.fixtures fixture
                JOIN football.seasons season ON season.id=fixture.season_id
                JOIN football.leagues league ON league.id=season.league_id
                JOIN football.teams home ON home.id=fixture.home_team_id
                JOIN football.teams away ON away.id=fixture.away_team_id
                LEFT JOIN football.team_rolling_metrics home_overall
                  ON home_overall.team_id=fixture.home_team_id
                 AND home_overall.season_id=fixture.season_id
                 AND home_overall.scope='overall'
                 AND home_overall.window_size=%(window_size)s
                LEFT JOIN football.team_rolling_metrics home_venue
                  ON home_venue.team_id=fixture.home_team_id
                 AND home_venue.season_id=fixture.season_id
                 AND home_venue.scope='home'
                 AND home_venue.window_size=%(window_size)s
                LEFT JOIN football.team_rolling_metrics away_overall
                  ON away_overall.team_id=fixture.away_team_id
                 AND away_overall.season_id=fixture.season_id
                 AND away_overall.scope='overall'
                 AND away_overall.window_size=%(window_size)s
                LEFT JOIN football.team_rolling_metrics away_venue
                  ON away_venue.team_id=fixture.away_team_id
                 AND away_venue.season_id=fixture.season_id
                 AND away_venue.scope='away'
                 AND away_venue.window_size=%(window_size)s
                WHERE fixture.lifecycle_state='scheduled'
                  AND fixture.kickoff_at >= %(start_at)s
                  AND fixture.kickoff_at < %(end_at)s
                  AND fixture.kickoff_at > clock_timestamp()
                  {league_sql}
                  AND coalesce(home_venue.matches_count,0) >= %(min_matches)s
                  AND coalesce(away_venue.matches_count,0) >= %(min_matches)s
                  AND coalesce(home_overall.source_last_kickoff_at,'-infinity'::timestamptz) < fixture.kickoff_at
                  AND coalesce(home_venue.source_last_kickoff_at,'-infinity'::timestamptz) < fixture.kickoff_at
                  AND coalesce(away_overall.source_last_kickoff_at,'-infinity'::timestamptz) < fixture.kickoff_at
                  AND coalesce(away_venue.source_last_kickoff_at,'-infinity'::timestamptz) < fixture.kickoff_at
                  {filters_sql}""", params

    @staticmethod
    def _filters_sql(filters: Sequence[ScannerFilter]) -> tuple[str, dict[str, object]]:
        clauses: list[str] = []
        params: dict[str, object] = {}
        for index, filter_ in enumerate(filters):
            alias = "home_venue" if filter_.side is ScannerSide.HOME else "away_venue"
            column = filter_.metric.value
            operator = _OPERATOR_SQL[filter_.operator]
            value_name = f"filter_{index}"
            clauses.append(f"AND {alias}.{column} {operator} %({value_name})s")
            params[value_name] = filter_.value
            if filter_.min_samples is not None:
                sample_column = NULLABLE_AVERAGE_SAMPLE_COLUMNS[filter_.metric]
                sample_name = f"filter_samples_{index}"
                clauses.append(f"AND {alias}.{sample_column} >= %({sample_name})s")
                params[sample_name] = filter_.min_samples
        return "\n                  ".join(clauses), params

    @staticmethod
    def _fixture_record(row: tuple[Any, ...]) -> ScannerFixtureRecord:
        return ScannerFixtureRecord(
            fixture_id=int(row[0]), season_id=int(row[1]), kickoff_at=row[2], round_label=row[3],
            league=ScannerLeagueRecord(
                id=int(row[4]), name=str(row[5]), country_name=row[6], logo_url=row[7], competition_type=row[8],
            ),
            home_team=ScannerTeamRecord(id=int(row[9]), name=str(row[10])),
            away_team=ScannerTeamRecord(id=int(row[11]), name=str(row[12])),
            home_overall=_metric_record(row[13]), home_venue=_metric_record(row[14]),
            away_overall=_metric_record(row[15]), away_venue=_metric_record(row[16]),
        )
