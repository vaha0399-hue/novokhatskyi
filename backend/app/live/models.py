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


@dataclass(frozen=True)
class LiveFixtureState:
    """Resolved current state persisted in Redis for one fixture."""

    fixture_id: int
    provider_fixture_id: int
    provider_league_id: int
    provider_season: int
    season_id: int
    league_id: int
    kickoff_at: datetime
    home_team_id: int
    home_team_name: str
    away_team_id: int
    away_team_name: str
    status: LiveFixtureStatus
    score: LiveScore
    elapsed_minute: int | None
    added_time: int | None
    observed_at: datetime

    def __post_init__(self) -> None:
        identifiers = (
            self.fixture_id,
            self.provider_fixture_id,
            self.provider_league_id,
            self.provider_season,
            self.season_id,
            self.league_id,
            self.home_team_id,
            self.away_team_id,
        )
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value <= 0
            for value in identifiers
        ):
            raise ValueError("live fixture identifiers must be positive integers")
        if self.home_team_id == self.away_team_id:
            raise ValueError("live fixture teams must be distinct")
        if any(
            not isinstance(value, str) or not value
            for value in (self.home_team_name, self.away_team_name)
        ):
            raise ValueError("live fixture team names must not be empty")
        if not isinstance(self.status, LiveFixtureStatus):
            raise ValueError("live fixture status must be normalized")
        if not isinstance(self.score, LiveScore):
            raise ValueError("live fixture score must be normalized")
        for value, field in (
            (self.elapsed_minute, "elapsed minute"),
            (self.added_time, "added time"),
        ):
            if value is not None and (
                not isinstance(value, int) or isinstance(value, bool) or value < 0
            ):
                raise ValueError(f"live fixture {field} must be a non-negative integer or null")
        if (
            not isinstance(self.kickoff_at, datetime)
            or not isinstance(self.observed_at, datetime)
            or self.kickoff_at.tzinfo is None
            or self.observed_at.tzinfo is None
        ):
            raise ValueError("live fixture timestamps must be timezone-aware")


def bind_live_fixture(
    provider: ProviderLiveFixture,
    canonical: CanonicalFixtureReference,
    *,
    observed_at: datetime,
) -> LiveFixtureState:
    """Combine normalized provider state with its strict canonical reference."""
    return LiveFixtureState(
        fixture_id=canonical.fixture_id,
        provider_fixture_id=provider.external_fixture_id,
        provider_league_id=provider.league_external_id,
        provider_season=provider.season_start_year,
        season_id=canonical.season_id,
        league_id=canonical.league_id,
        kickoff_at=canonical.kickoff_at,
        home_team_id=canonical.home_team_id,
        home_team_name=canonical.home_team_name,
        away_team_id=canonical.away_team_id,
        away_team_name=canonical.away_team_name,
        status=provider.status,
        score=provider.score,
        elapsed_minute=provider.elapsed_minute,
        added_time=provider.added_time,
        observed_at=observed_at,
    )
