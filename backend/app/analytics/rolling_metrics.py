"""Pure current-season rolling metrics for the scanner data layer.

The database writer supplies the team and season identifiers.  This module
only turns one team's completed-match records into metric values, which keeps
the aggregation independently testable and usable by both initial and
incremental backfills.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

from .models import AnalyticsScope, TeamMatchRecord


ROLLING_WINDOWS = (0, 5, 10)
ROLLING_SCOPES = (AnalyticsScope.OVERALL, AnalyticsScope.HOME, AnalyticsScope.AWAY)
_AVERAGE_SCALE = Decimal("0.0001")
_RATE_SCALE = Decimal("0.00001")


@dataclass(frozen=True)
class RollingMetricRow:
    """Metrics for one team's scope and window, ready for a persistence layer.

    ``window_size=0`` means all completed matches in the season.  Nullable
    averages have no source value when the provider omitted that statistic for
    every match in the selected window.
    """

    scope: AnalyticsScope
    window_size: int
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


def _divide(value: int | Decimal, denominator: int, *, scale: Decimal) -> Decimal:
    if denominator == 0:
        return Decimal("0").quantize(scale)
    return (Decimal(value) / Decimal(denominator)).quantize(scale, rounding=ROUND_HALF_UP)


def _nullable_average(values: Iterable[int | Decimal | None]) -> tuple[Decimal | None, int]:
    present = [Decimal(value) for value in values if value is not None]
    if not present:
        return None, 0
    return _divide(sum(present), len(present), scale=_AVERAGE_SCALE), len(present)


def _records_for_scope(records: list[TeamMatchRecord], scope: AnalyticsScope) -> list[TeamMatchRecord]:
    if scope is AnalyticsScope.HOME:
        return [record for record in records if record.is_home]
    if scope is AnalyticsScope.AWAY:
        return [record for record in records if not record.is_home]
    return records


def _window_records(records: list[TeamMatchRecord], window_size: int) -> list[TeamMatchRecord]:
    return records if window_size == 0 else records[:window_size]


def _build_row(records: list[TeamMatchRecord], *, scope: AnalyticsScope, window_size: int) -> RollingMetricRow:
    matches = len(records)
    goals_for = sum(record.goals_for for record in records)
    goals_against = sum(record.goals_against for record in records)
    rate = lambda count: _divide(count, matches, scale=_RATE_SCALE)
    total_goals = lambda record: record.goals_for + record.goals_against
    avg_xg, xg_sample_count = _nullable_average(record.expected_goals for record in records)
    avg_xga, xga_sample_count = _nullable_average(record.expected_goals_against for record in records)
    avg_shots, shots_sample_count = _nullable_average(record.total_shots for record in records)
    avg_shots_on_goal, shots_on_goal_sample_count = _nullable_average(record.shots_on_goal for record in records)
    avg_corners, corners_sample_count = _nullable_average(record.corner_kicks for record in records)
    avg_possession, possession_sample_count = _nullable_average(record.possession_pct for record in records)

    return RollingMetricRow(
        scope=scope,
        window_size=window_size,
        matches_count=matches,
        avg_xg=avg_xg,
        xg_sample_count=xg_sample_count,
        avg_xga=avg_xga,
        xga_sample_count=xga_sample_count,
        avg_goals_for=_divide(goals_for, matches, scale=_AVERAGE_SCALE),
        avg_goals_against=_divide(goals_against, matches, scale=_AVERAGE_SCALE),
        scored_rate=rate(sum(record.goals_for > 0 for record in records)),
        conceded_rate=rate(sum(record.goals_against > 0 for record in records)),
        btts_rate=rate(sum(record.goals_for > 0 and record.goals_against > 0 for record in records)),
        over_1_5_rate=rate(sum(total_goals(record) > Decimal("1.5") for record in records)),
        over_2_5_rate=rate(sum(total_goals(record) > Decimal("2.5") for record in records)),
        over_3_5_rate=rate(sum(total_goals(record) > Decimal("3.5") for record in records)),
        avg_shots=avg_shots,
        shots_sample_count=shots_sample_count,
        avg_shots_on_goal=avg_shots_on_goal,
        shots_on_goal_sample_count=shots_on_goal_sample_count,
        avg_corners=avg_corners,
        corners_sample_count=corners_sample_count,
        avg_possession=avg_possession,
        possession_sample_count=possession_sample_count,
        source_last_kickoff_at=records[0].kickoff_at if records else None,
    )


def calculate_rolling_metrics(records: Iterable[TeamMatchRecord]) -> tuple[RollingMetricRow, ...]:
    """Return scanner metrics for all scopes and 0/5/10 current-season windows.

    Input may be in any order.  A record's ``expected_goals_against`` is used
    directly: the normalizer is responsible for deriving it from the opposing
    team's xG, which avoids an accidental same-team xGA calculation here.
    """

    ordered = sorted(records, key=lambda record: (record.kickoff_at, record.fixture_id), reverse=True)
    rows: list[RollingMetricRow] = []
    for scope in ROLLING_SCOPES:
        scoped = _records_for_scope(ordered, scope)
        for window_size in ROLLING_WINDOWS:
            rows.append(_build_row(_window_records(scoped, window_size), scope=scope, window_size=window_size))
    return tuple(rows)
