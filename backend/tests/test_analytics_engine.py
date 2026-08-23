from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from app.analytics.engine import AnalyticsEngine, AnalyticsInvariantError, calculate_window
from app.analytics.models import AnalyticsScope, FixtureContext, TeamMatchRecord


def _record(
    fixture_id: int,
    days_before_cutoff: int,
    goals_for: int,
    goals_against: int,
    *,
    is_home: bool = True,
    xg: str | None = "1.000",
    xga: str | None = "1.000",
    red_cards: int | None = None,
) -> TeamMatchRecord:
    cutoff = datetime(2025, 1, 1, 12, tzinfo=UTC)
    return TeamMatchRecord(
        fixture_id=fixture_id,
        kickoff_at=cutoff - timedelta(days=days_before_cutoff),
        is_home=is_home,
        goals_for=goals_for,
        goals_against=goals_against,
        expected_goals=Decimal(xg) if xg is not None else None,
        expected_goals_against=Decimal(xga) if xga is not None else None,
        total_shots=10 + fixture_id,
        shots_on_goal=4 + fixture_id,
        possession_pct=Decimal("50.00"),
        corner_kicks=5,
        yellow_cards=2,
        red_cards=red_cards,
    )


CUTOFF = datetime(2025, 1, 1, 12, tzinfo=UTC)
HISTORY = [
    _record(5, 1, 2, 1, xg="1.500", xga="1.000"),
    _record(4, 2, 0, 0, xg="0.900", xga="0.700", red_cards=0),
    _record(3, 3, 1, 3, xg="0.800", xga="2.100"),
    _record(2, 4, 1, 0, is_home=False, xg="1.100", xga="0.600"),
    _record(1, 5, 3, 2, is_home=False, xg="2.000", xga="1.500"),
]


def test_last_n_metrics_keep_nullable_samples_and_calculate_rates() -> None:
    analytics = calculate_window(records=HISTORY, requested_window=5, cutoff_at=CUTOFF)

    assert (analytics.matches, analytics.wins, analytics.draws, analytics.losses) == (5, 3, 1, 1)
    assert analytics.points == 10
    assert analytics.points_per_game == Decimal("2.000")
    assert (analytics.goals_scored, analytics.goals_conceded) == (7, 6)
    assert analytics.average_goals_scored == Decimal("1.400")
    assert analytics.average_goals_conceded == Decimal("1.200")
    assert analytics.average_xg.value == Decimal("1.260")
    assert analytics.average_xga.value == Decimal("1.180")
    assert analytics.average_red_cards.value == Decimal("0.000")
    assert analytics.average_red_cards.sample_size == 1
    assert analytics.clean_sheets.count == 2
    assert analytics.failed_to_score.count == 1
    assert analytics.btts.count == 3
    assert analytics.total_goals["0.5"].over.rate == Decimal("0.800")
    assert analytics.total_goals["1.5"].over.count == 3
    assert analytics.total_goals["2.5"].over.count == 3
    assert analytics.total_goals["3.5"].over.count == 2
    assert analytics.streaks.wins == 1
    assert analytics.streaks.unbeaten == 2
    assert analytics.streaks.scored == 1


def test_window_uses_only_available_history_when_last_n_is_not_reached() -> None:
    analytics = calculate_window(records=HISTORY[:3], requested_window=20, cutoff_at=CUTOFF)

    assert analytics.matches == 3
    assert analytics.points_per_game == Decimal("1.333")
    assert analytics.total_goals["2.5"].under.count == 1


def test_target_fixture_and_later_matches_are_rejected() -> None:
    leaked = _record(999, 0, 9, 0)
    with pytest.raises(AnalyticsInvariantError, match="target kickoff"):
        calculate_window(records=[*HISTORY, leaked], requested_window=5, cutoff_at=CUTOFF)

    later = _record(998, -1, 9, 0)
    with pytest.raises(AnalyticsInvariantError, match="target kickoff"):
        calculate_window(records=[later], requested_window=5, cutoff_at=CUTOFF)


class FakeRepository:
    def __init__(self) -> None:
        self.calls: list[tuple[int, int, datetime, AnalyticsScope]] = []
        self.fixture = FixtureContext(77, 55, CUTOFF, 10, 20)

    def load_team_history(self, *, team_id, season_id, as_of_kickoff, scope):
        self.calls.append((team_id, season_id, as_of_kickoff, scope))
        if scope is AnalyticsScope.HOME:
            return [record for record in HISTORY if record.is_home]
        if scope is AnalyticsScope.AWAY:
            return [record for record in HISTORY if not record.is_home]
        return HISTORY

    def load_fixture_context(self, *, fixture_id):
        assert fixture_id == self.fixture.fixture_id
        return self.fixture


def test_fixture_comparison_is_season_scoped_and_uses_home_away_splits() -> None:
    repository = FakeRepository()
    result = AnalyticsEngine(repository).fixture_analytics(fixture_id=77)

    assert result.home_overall.season_id == 55
    assert result.home_overall.scope is AnalyticsScope.OVERALL
    assert result.home_home.scope is AnalyticsScope.HOME
    assert result.away_overall.scope is AnalyticsScope.OVERALL
    assert result.away_away.scope is AnalyticsScope.AWAY
    assert all(call[1] == 55 and call[2] == CUTOFF for call in repository.calls)
    assert [call[3] for call in repository.calls] == [
        AnalyticsScope.OVERALL,
        AnalyticsScope.HOME,
        AnalyticsScope.OVERALL,
        AnalyticsScope.AWAY,
    ]
