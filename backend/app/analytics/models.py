"""Typed, database-independent contracts for Analytics Engine v1."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum


class AnalyticsScope(StrEnum):
    """Which matches contribute to a team's historical form."""

    OVERALL = "overall"
    HOME = "home"
    AWAY = "away"


@dataclass(frozen=True)
class TeamMatchRecord:
    """One completed fixture viewed from one team's perspective."""

    fixture_id: int
    kickoff_at: datetime
    is_home: bool
    goals_for: int
    goals_against: int
    expected_goals: Decimal | None
    expected_goals_against: Decimal | None
    total_shots: int | None
    shots_on_goal: int | None
    possession_pct: Decimal | None
    corner_kicks: int | None
    yellow_cards: int | None
    red_cards: int | None


@dataclass(frozen=True)
class AverageMetric:
    """An average that keeps its non-null source sample size explicit."""

    value: Decimal | None
    sample_size: int


@dataclass(frozen=True)
class RateMetric:
    """A count/rate with the fixture count used as its denominator."""

    count: int
    rate: Decimal | None


@dataclass(frozen=True)
class GoalTotalsRate:
    over: RateMetric
    under: RateMetric


@dataclass(frozen=True)
class Streaks:
    """Consecutive results ending at the most recent eligible fixture."""

    wins: int
    unbeaten: int
    winless: int
    losses: int
    scored: int
    clean_sheets: int
    btts: int


@dataclass(frozen=True)
class WindowAnalytics:
    """One Last-N view for a team and scope at a fixed historical cutoff."""

    requested_window: int
    matches: int
    wins: int
    draws: int
    losses: int
    points: int
    points_per_game: Decimal | None
    goals_scored: int
    goals_conceded: int
    average_goals_scored: Decimal | None
    average_goals_conceded: Decimal | None
    average_xg: AverageMetric
    average_xga: AverageMetric
    average_shots: AverageMetric
    average_shots_on_goal: AverageMetric
    average_possession_pct: AverageMetric
    average_corners: AverageMetric
    average_yellow_cards: AverageMetric
    average_red_cards: AverageMetric
    clean_sheets: RateMetric
    failed_to_score: RateMetric
    btts: RateMetric
    total_goals: dict[str, GoalTotalsRate]
    streaks: Streaks


@dataclass(frozen=True)
class TeamAnalyticsBundle:
    team_id: int
    season_id: int
    as_of_kickoff: datetime
    scope: AnalyticsScope
    windows: dict[int, WindowAnalytics]


@dataclass(frozen=True)
class FixtureContext:
    fixture_id: int
    season_id: int
    kickoff_at: datetime
    home_team_id: int
    away_team_id: int


@dataclass(frozen=True)
class FixtureAnalytics:
    """The pre-kickoff comparison inputs for one fixture; never a prediction."""

    fixture: FixtureContext
    home_overall: TeamAnalyticsBundle
    home_home: TeamAnalyticsBundle
    away_overall: TeamAnalyticsBundle
    away_away: TeamAnalyticsBundle
