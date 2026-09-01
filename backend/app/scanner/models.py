"""Small, transport-independent scanner query contract."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import Enum


class ScannerSide(str, Enum):
    HOME = "home"
    AWAY = "away"


class ScannerOperator(str, Enum):
    GREATER_THAN = ">"
    GREATER_THAN_OR_EQUAL = ">="
    LESS_THAN = "<"
    LESS_THAN_OR_EQUAL = "<="


class ScannerMetric(str, Enum):
    MATCHES_COUNT = "matches_count"
    AVG_XG = "avg_xg"
    AVG_XGA = "avg_xga"
    AVG_GOALS_FOR = "avg_goals_for"
    AVG_GOALS_AGAINST = "avg_goals_against"
    SCORED_RATE = "scored_rate"
    CONCEDED_RATE = "conceded_rate"
    BTTS_RATE = "btts_rate"
    OVER_1_5_RATE = "over_1_5_rate"
    OVER_2_5_RATE = "over_2_5_rate"
    OVER_3_5_RATE = "over_3_5_rate"
    AVG_SHOTS = "avg_shots"
    AVG_SHOTS_ON_GOAL = "avg_shots_on_goal"
    AVG_CORNERS = "avg_corners"
    AVG_POSSESSION = "avg_possession"


NULLABLE_AVERAGE_SAMPLE_COLUMNS: dict[ScannerMetric, str] = {
    ScannerMetric.AVG_XG: "xg_sample_count",
    ScannerMetric.AVG_XGA: "xga_sample_count",
    ScannerMetric.AVG_SHOTS: "shots_sample_count",
    ScannerMetric.AVG_SHOTS_ON_GOAL: "shots_on_goal_sample_count",
    ScannerMetric.AVG_CORNERS: "corners_sample_count",
    ScannerMetric.AVG_POSSESSION: "possession_sample_count",
}


@dataclass(frozen=True)
class ScannerFilter:
    side: ScannerSide
    metric: ScannerMetric
    operator: ScannerOperator
    value: Decimal
    min_samples: int | None = None


@dataclass(frozen=True)
class ScannerQuery:
    match_date: date
    timezone: str
    league_ids: tuple[int, ...]
    window_size: int
    min_matches: int
    filters: tuple[ScannerFilter, ...]
    limit: int
    offset: int
