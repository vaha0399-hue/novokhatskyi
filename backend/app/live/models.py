"""Stable live-domain values; no provider JSON or Redis representation leaks in."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class LiveFixtureStatus(StrEnum):
    FIRST_HALF = "first_half"
    HALF_TIME = "half_time"
    SECOND_HALF = "second_half"
    FINISHED = "finished"

    @property
    def is_terminal(self) -> bool:
        return self is LiveFixtureStatus.FINISHED


@dataclass(frozen=True)
class LiveScore:
    home: int
    away: int

    def __post_init__(self) -> None:
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value < 0
            for value in (self.home, self.away)
        ):
            raise ValueError("live score values must be non-negative integers")


@dataclass(frozen=True)
class ProviderLiveFixture:
    """One normalized API-Football fixture before canonical ID resolution."""

    external_fixture_id: int
    league_external_id: int
    season_start_year: int
    home_external_team_id: int
    away_external_team_id: int
    status: LiveFixtureStatus
    score: LiveScore
    elapsed_minute: int | None
    added_time: int | None


@dataclass(frozen=True)
class CanonicalFixtureReference:
    """Canonical identity and display data resolved from PostgreSQL."""

    fixture_id: int
    season_id: int
    league_id: int
    kickoff_at: datetime
    home_team_id: int
    home_team_name: str
    away_team_id: int
    away_team_name: str
