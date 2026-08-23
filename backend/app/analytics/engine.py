"""Analytics Engine v1: factual, cutoff-safe historical calculations."""

from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal, ROUND_HALF_UP

from .models import (
    AnalyticsScope,
    AverageMetric,
    FixtureAnalytics,
    GoalTotalsRate,
    RateMetric,
    Streaks,
    TeamAnalyticsBundle,
    TeamMatchRecord,
    WindowAnalytics,
)
from .repository import AnalyticsRepository

SUPPORTED_WINDOWS = (5, 10, 15, 20)
TOTAL_GOAL_THRESHOLDS = (Decimal("0.5"), Decimal("1.5"), Decimal("2.5"), Decimal("3.5"))
_SCALE = Decimal("0.001")


class AnalyticsInvariantError(ValueError):
    """Raised if repository data could leak the target fixture's outcome."""


def _decimal(value: int | Decimal, denominator: int) -> Decimal | None:
    if not denominator:
        return None
    return (Decimal(value) / Decimal(denominator)).quantize(_SCALE, rounding=ROUND_HALF_UP)


def _average(values: Iterable[int | Decimal | None]) -> AverageMetric:
    included = [Decimal(value) for value in values if value is not None]
    return AverageMetric(_decimal(sum(included), len(included)), len(included))


def _rate(count: int, matches: int) -> RateMetric:
    return RateMetric(count=count, rate=_decimal(count, matches))


def _leading_count(records: list[TeamMatchRecord], predicate) -> int:
    count = 0
    for record in records:
        if not predicate(record):
            break
        count += 1
    return count


def _streaks(records: list[TeamMatchRecord]) -> Streaks:
    return Streaks(
        wins=_leading_count(records, lambda row: row.goals_for > row.goals_against),
        unbeaten=_leading_count(records, lambda row: row.goals_for >= row.goals_against),
        winless=_leading_count(records, lambda row: row.goals_for <= row.goals_against),
        losses=_leading_count(records, lambda row: row.goals_for < row.goals_against),
        scored=_leading_count(records, lambda row: row.goals_for > 0),
        clean_sheets=_leading_count(records, lambda row: row.goals_against == 0),
        btts=_leading_count(records, lambda row: row.goals_for > 0 and row.goals_against > 0),
    )


def calculate_window(*, records: Iterable[TeamMatchRecord], requested_window: int, cutoff_at) -> WindowAnalytics:
    """Calculate one Last-N view from records ordered newest-first."""
    if requested_window not in SUPPORTED_WINDOWS:
        raise ValueError(f"unsupported window: {requested_window}")
    ordered = list(records)
    if any(row.kickoff_at >= cutoff_at for row in ordered):
        raise AnalyticsInvariantError("analytics history includes target kickoff or a later fixture")
    ordered.sort(key=lambda row: (row.kickoff_at, row.fixture_id), reverse=True)
    recent = ordered[:requested_window]
    matches = len(recent)
    wins = sum(row.goals_for > row.goals_against for row in recent)
    draws = sum(row.goals_for == row.goals_against for row in recent)
    losses = matches - wins - draws
    goals_scored = sum(row.goals_for for row in recent)
    goals_conceded = sum(row.goals_against for row in recent)
    clean_sheets = sum(row.goals_against == 0 for row in recent)
    failed_to_score = sum(row.goals_for == 0 for row in recent)
    btts = sum(row.goals_for > 0 and row.goals_against > 0 for row in recent)
    total_goals: dict[str, GoalTotalsRate] = {}
    for threshold in TOTAL_GOAL_THRESHOLDS:
        over = sum(Decimal(row.goals_for + row.goals_against) > threshold for row in recent)
        total_goals[str(threshold)] = GoalTotalsRate(_rate(over, matches), _rate(matches - over, matches))
    return WindowAnalytics(
        requested_window=requested_window, matches=matches, wins=wins, draws=draws, losses=losses,
        points=wins * 3 + draws, points_per_game=_decimal(wins * 3 + draws, matches),
        goals_scored=goals_scored, goals_conceded=goals_conceded,
        average_goals_scored=_decimal(goals_scored, matches), average_goals_conceded=_decimal(goals_conceded, matches),
        average_xg=_average(row.expected_goals for row in recent),
        average_xga=_average(row.expected_goals_against for row in recent),
        average_shots=_average(row.total_shots for row in recent),
        average_shots_on_goal=_average(row.shots_on_goal for row in recent),
        average_possession_pct=_average(row.possession_pct for row in recent),
        average_corners=_average(row.corner_kicks for row in recent),
        average_yellow_cards=_average(row.yellow_cards for row in recent),
        average_red_cards=_average(row.red_cards for row in recent),
        clean_sheets=_rate(clean_sheets, matches), failed_to_score=_rate(failed_to_score, matches),
        btts=_rate(btts, matches), total_goals=total_goals, streaks=_streaks(recent),
    )


class AnalyticsEngine:
    """Coordinates repository reads and pure metric calculation; no API calls."""

    def __init__(self, repository: AnalyticsRepository) -> None:
        self._repository = repository

    def team_analytics(
        self, *, team_id: int, season_id: int, as_of_kickoff, scope: AnalyticsScope
    ) -> TeamAnalyticsBundle:
        history = self._repository.load_team_history(
            team_id=team_id, season_id=season_id, as_of_kickoff=as_of_kickoff, scope=scope,
        )
        return TeamAnalyticsBundle(
            team_id=team_id, season_id=season_id, as_of_kickoff=as_of_kickoff, scope=scope,
            windows={window: calculate_window(records=history, requested_window=window, cutoff_at=as_of_kickoff) for window in SUPPORTED_WINDOWS},
        )

    def fixture_analytics(self, *, fixture_id: int) -> FixtureAnalytics:
        fixture = self._repository.load_fixture_context(fixture_id=fixture_id)
        return FixtureAnalytics(
            fixture=fixture,
            home_overall=self.team_analytics(team_id=fixture.home_team_id, season_id=fixture.season_id, as_of_kickoff=fixture.kickoff_at, scope=AnalyticsScope.OVERALL),
            home_home=self.team_analytics(team_id=fixture.home_team_id, season_id=fixture.season_id, as_of_kickoff=fixture.kickoff_at, scope=AnalyticsScope.HOME),
            away_overall=self.team_analytics(team_id=fixture.away_team_id, season_id=fixture.season_id, as_of_kickoff=fixture.kickoff_at, scope=AnalyticsScope.OVERALL),
            away_away=self.team_analytics(team_id=fixture.away_team_id, season_id=fixture.season_id, as_of_kickoff=fixture.kickoff_at, scope=AnalyticsScope.AWAY),
        )
