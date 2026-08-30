"""Stable DTOs for /web/v1. These intentionally do not mirror SQL rows."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class WebDTO(BaseModel):
    model_config = ConfigDict(extra="forbid")


class TeamReference(WebDTO):
    id: int
    name: str


class LeagueReference(WebDTO):
    id: int
    name: str
    country_name: str | None
    logo_url: str | None
    competition_type: str | None


class LeagueListResponse(WebDTO):
    leagues: list[LeagueReference]


class SeasonReference(WebDTO):
    id: int
    league: LeagueReference
    start_year: int
    label: str
    starts_on: date | None
    ends_on: date | None


class LeagueSeasonsResponse(WebDTO):
    league: LeagueReference
    seasons: list[SeasonReference]


class SeasonStandingRow(WebDTO):
    rank: int = Field(gt=0)
    team: TeamReference
    points: int = Field(ge=0)
    played: int = Field(ge=0)
    wins: int = Field(ge=0)
    draws: int = Field(ge=0)
    losses: int = Field(ge=0)
    goals_for: int = Field(ge=0)
    goals_against: int = Field(ge=0)
    goals_diff: int
    form: str | None
    status: str | None
    description: str | None


class StandingsGroup(WebDTO):
    name: str | None
    rows: list[SeasonStandingRow]


class SeasonStandingsResponse(WebDTO):
    season: SeasonReference
    captured_at: datetime
    groups: list[StandingsGroup]


class FixtureScore(WebDTO):
    home: int = Field(ge=0)
    away: int = Field(ge=0)


class LiveFixtureDTO(WebDTO):
    fixture_id: int = Field(gt=0)
    season_id: int = Field(gt=0)
    league_id: int = Field(gt=0)
    kickoff_at: datetime
    home_team: TeamReference
    away_team: TeamReference
    status: Literal["first_half", "half_time", "second_half"]
    score: FixtureScore
    elapsed_minute: int | None = Field(default=None, ge=0)
    added_time: int | None = Field(default=None, ge=0)
    observed_at: datetime


class LiveFixturesResponse(WebDTO):
    fixtures: list[LiveFixtureDTO]


class FixtureSummary(WebDTO):
    id: int
    season_id: int
    kickoff_at: datetime
    round_label: str | None
    lifecycle_state: str
    home_team: TeamReference
    away_team: TeamReference
    final_score: FixtureScore | None


class PaginationMetadata(WebDTO):
    total: int = Field(ge=0)
    limit: int = Field(ge=1)
    offset: int = Field(ge=0)
    next_offset: int | None = Field(default=None, ge=0)


class SeasonFixturesResponse(WebDTO):
    season_id: int
    fixtures: list[FixtureSummary]
    pagination: PaginationMetadata


class AverageMetricSummary(WebDTO):
    value: float | None
    sample_size: int = Field(ge=0)


class RateMetricSummary(WebDTO):
    count: int = Field(ge=0)
    rate: float | None


class GoalTotalsRateSummary(WebDTO):
    over: RateMetricSummary
    under: RateMetricSummary


class StreakSummary(WebDTO):
    wins: int = Field(ge=0)
    unbeaten: int = Field(ge=0)
    winless: int = Field(ge=0)
    losses: int = Field(ge=0)
    scored: int = Field(ge=0)
    clean_sheets: int = Field(ge=0)
    btts: int = Field(ge=0)


class MetricSummary(WebDTO):
    matches: int = Field(ge=0)
    wins: int = Field(ge=0)
    draws: int = Field(ge=0)
    losses: int = Field(ge=0)
    points: int = Field(ge=0)
    points_per_game: float | None
    goals_scored: int = Field(ge=0)
    goals_conceded: int = Field(ge=0)
    average_goals_scored: float | None
    average_goals_conceded: float | None
    average_xg: AverageMetricSummary
    average_xga: AverageMetricSummary
    average_shots: AverageMetricSummary
    average_shots_on_goal: AverageMetricSummary
    average_possession_pct: AverageMetricSummary
    average_corners: AverageMetricSummary
    average_yellow_cards: AverageMetricSummary
    average_red_cards: AverageMetricSummary
    clean_sheets: RateMetricSummary
    failed_to_score: RateMetricSummary
    btts: RateMetricSummary
    total_goals: dict[str, GoalTotalsRateSummary]
    streaks: StreakSummary


class TeamAnalyticsResponse(WebDTO):
    team: TeamReference
    season_id: int
    scope: str
    window: int
    as_of_kickoff: datetime
    metrics: MetricSummary


class FixtureAnalyticsSide(WebDTO):
    team: TeamReference
    overall: MetricSummary
    venue_split: MetricSummary


class FixtureAnalyticsResponse(WebDTO):
    fixture: FixtureSummary
    window: int
    historical_cutoff_at: datetime
    home: FixtureAnalyticsSide
    away: FixtureAnalyticsSide


class FixtureTeamStatistics(WebDTO):
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
    possession_pct: float | None
    pass_accuracy_pct: float | None
    expected_goals: float | None
    goals_prevented: float | None


class FixtureStatisticsSide(WebDTO):
    team: TeamReference
    metrics: FixtureTeamStatistics | None


class FixtureStatisticsResponse(WebDTO):
    fixture: FixtureSummary
    home: FixtureStatisticsSide
    away: FixtureStatisticsSide
