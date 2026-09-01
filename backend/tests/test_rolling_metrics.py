from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from app.analytics.models import AnalyticsScope, TeamMatchRecord
from app.analytics.rolling_metrics import calculate_rolling_metrics


NOW = datetime(2026, 9, 1, 12, tzinfo=UTC)


def _record(
    fixture_id: int,
    days_ago: int,
    goals_for: int,
    goals_against: int,
    *,
    is_home: bool = True,
    xg: str | None = "1.00",
    xga: str | None = "0.75",
) -> TeamMatchRecord:
    return TeamMatchRecord(
        fixture_id=fixture_id,
        kickoff_at=NOW - timedelta(days=days_ago),
        is_home=is_home,
        goals_for=goals_for,
        goals_against=goals_against,
        expected_goals=Decimal(xg) if xg is not None else None,
        expected_goals_against=Decimal(xga) if xga is not None else None,
        total_shots=10,
        shots_on_goal=4,
        possession_pct=Decimal("52.50"),
        corner_kicks=5,
        yellow_cards=None,
        red_cards=None,
    )


def _row(rows, scope: AnalyticsScope, window: int):
    return next(row for row in rows if row.scope is scope and row.window_size == window)


def test_uses_precomputed_opponent_xg_as_xga_and_handles_missing_xg() -> None:
    rows = calculate_rolling_metrics([
        _record(1, 2, 2, 1, xg="1.20", xga="2.40"),
        _record(2, 1, 1, 0, xg=None, xga=None),
    ])

    overall = _row(rows, AnalyticsScope.OVERALL, 0)
    assert overall.avg_xg == Decimal("1.2000")
    assert overall.avg_xga == Decimal("2.4000")
    assert overall.xg_sample_count == 1
    assert overall.xga_sample_count == 1


def test_scopes_filter_home_and_away_before_selecting_windows() -> None:
    rows = calculate_rolling_metrics([
        _record(1, 1, 2, 0, is_home=True),
        _record(2, 2, 0, 1, is_home=False),
        _record(3, 3, 1, 1, is_home=True),
    ])

    home = _row(rows, AnalyticsScope.HOME, 0)
    away = _row(rows, AnalyticsScope.AWAY, 0)
    assert (home.matches_count, home.avg_goals_for, home.conceded_rate) == (2, Decimal("1.5000"), Decimal("0.50000"))
    assert (away.matches_count, away.avg_goals_for, away.scored_rate) == (1, Decimal("0.0000"), Decimal("0.00000"))


def test_fewer_than_ten_and_goal_rates_are_calculated_from_actual_history() -> None:
    records = [
        _record(1, 1, 2, 1),  # BTTS, over 1.5 and 2.5
        _record(2, 2, 1, 0),
        _record(3, 3, 0, 2),  # over 1.5
        _record(4, 4, 3, 2),  # BTTS, all overs
    ]
    rows = calculate_rolling_metrics(records)

    last_ten = _row(rows, AnalyticsScope.OVERALL, 10)
    assert last_ten.matches_count == 4
    assert last_ten.btts_rate == Decimal("0.50000")
    assert last_ten.over_1_5_rate == Decimal("0.75000")
    assert last_ten.over_2_5_rate == Decimal("0.50000")
    assert last_ten.over_3_5_rate == Decimal("0.25000")
    assert last_ten.source_last_kickoff_at == NOW - timedelta(days=1)


def test_empty_scope_is_persistable_with_zero_rates_and_no_source_kickoff() -> None:
    rows = calculate_rolling_metrics([_record(1, 1, 1, 0, is_home=True)])

    away = _row(rows, AnalyticsScope.AWAY, 5)
    assert away.matches_count == 0
    assert away.avg_xg is None
    assert away.xg_sample_count == 0
    assert away.xga_sample_count == 0
    assert away.shots_sample_count == 0
    assert away.shots_on_goal_sample_count == 0
    assert away.corners_sample_count == 0
    assert away.possession_sample_count == 0
    assert away.avg_goals_for == Decimal("0.0000")
    assert away.scored_rate == Decimal("0.00000")
    assert away.source_last_kickoff_at is None
