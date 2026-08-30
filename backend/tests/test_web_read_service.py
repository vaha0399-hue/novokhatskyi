from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from app.analytics.models import FixtureContext
from app.web.repository import (
    FixtureRecord,
    LeagueRecord,
    MatchDateLeagueRecord,
    TeamRecord,
)
from app.web.service import WebNotFoundError, WebReadService, WebValidationError, _fixture


NOW = datetime(2024, 8, 16, 19, tzinfo=UTC)
HOME = TeamRecord(10, "Home")
AWAY = TeamRecord(20, "Away")


def _record(*, finalized_at: datetime | None) -> FixtureRecord:
    return FixtureRecord(
        context=FixtureContext(1, 3, NOW, HOME.id, AWAY.id),
        round_label="Regular Season - 1", lifecycle_state="completed",
        home_team=HOME, away_team=AWAY, home_goals=2, away_goals=1,
        result_finalized_at=finalized_at,
    )


def test_completed_but_unfinalized_fixture_never_exposes_final_score() -> None:
    assert _fixture(_record(finalized_at=None)).final_score is None
    assert _fixture(_record(finalized_at=NOW)).final_score is not None


class MissingSeasonRepository:
    def season_exists(self, *, season_id: int) -> bool:
        return False


def test_team_analytics_reports_unknown_season_before_team_membership_check() -> None:
    service = WebReadService(MissingSeasonRepository(), analytics=None)  # type: ignore[arg-type]
    with pytest.raises(WebNotFoundError) as error:
        service.team_analytics(team_id=10, season_id=999, scope=None, window=10)  # type: ignore[arg-type]
    assert error.value.code == "season_not_found"


class MatchDateRepository:
    def __init__(self) -> None:
        self.windows: list[tuple[datetime, datetime]] = []

    def list_match_date_leagues(
        self, *, start_at: datetime, end_at: datetime
    ) -> list[MatchDateLeagueRecord]:
        self.windows.append((start_at, end_at))
        return [
            MatchDateLeagueRecord(
                league=LeagueRecord(3, "Premier League", "England", None, "league"),
                fixture_count=8,
            ),
            MatchDateLeagueRecord(
                league=LeagueRecord(4, "La Liga", "Spain", None, "league"),
                fixture_count=6,
            ),
        ]

    def league(self, *, league_id: int) -> LeagueRecord | None:
        if league_id == 404:
            return None
        return LeagueRecord(league_id, "Premier League", "England", None, "league")

    def list_league_matches(
        self, *, league_id: int, start_at: datetime, end_at: datetime
    ) -> list[FixtureRecord]:
        self.windows.append((start_at, end_at))
        return []


def test_match_date_uses_browser_timezone_for_utc_boundaries() -> None:
    repository = MatchDateRepository()
    service = WebReadService(repository, analytics=None)  # type: ignore[arg-type]

    response = service.match_date_leagues(
        match_date=date(2026, 8, 31), timezone="Asia/Tokyo"
    )

    assert repository.windows == [
        (
            datetime(2026, 8, 30, 15, tzinfo=UTC),
            datetime(2026, 8, 31, 15, tzinfo=UTC),
        )
    ]
    late_utc_kickoff = datetime(2026, 8, 30, 23, 30, tzinfo=UTC)
    start_at, end_at = repository.windows[0]
    assert start_at <= late_utc_kickoff < end_at
    assert response.timezone == "Asia/Tokyo"
    assert [(item.league.id, item.fixture_count) for item in response.leagues] == [
        (3, 8),
        (4, 6),
    ]


def test_match_date_window_handles_dst_transition() -> None:
    repository = MatchDateRepository()
    service = WebReadService(repository, analytics=None)  # type: ignore[arg-type]

    service.league_matches(
        match_date=date(2026, 3, 29), league_id=3, timezone="Europe/London"
    )

    assert repository.windows == [
        (
            datetime(2026, 3, 29, 0, tzinfo=UTC),
            datetime(2026, 3, 29, 23, tzinfo=UTC),
        )
    ]


def test_match_date_rejects_unknown_timezone() -> None:
    service = WebReadService(MatchDateRepository(), analytics=None)  # type: ignore[arg-type]

    with pytest.raises(WebValidationError) as error:
        service.match_date_leagues(
            match_date=date(2026, 8, 30), timezone="Not/A_Timezone"
        )

    assert error.value.code == "invalid_timezone"


def test_match_date_rejects_date_without_a_representable_next_day() -> None:
    service = WebReadService(MatchDateRepository(), analytics=None)  # type: ignore[arg-type]

    with pytest.raises(WebValidationError) as error:
        service.match_date_leagues(match_date=date.max, timezone="UTC")

    assert error.value.code == "invalid_match_date"


class EmptyMatchDateRepository(MatchDateRepository):
    def list_match_date_leagues(
        self, *, start_at: datetime, end_at: datetime
    ) -> list[MatchDateLeagueRecord]:
        self.windows.append((start_at, end_at))
        return []


def test_match_date_without_fixtures_returns_empty_league_list() -> None:
    service = WebReadService(EmptyMatchDateRepository(), analytics=None)  # type: ignore[arg-type]

    response = service.match_date_leagues(
        match_date=date(2026, 8, 30), timezone="UTC"
    )

    assert response.leagues == []


def test_league_matches_distinguishes_unknown_league_from_empty_date() -> None:
    service = WebReadService(MatchDateRepository(), analytics=None)  # type: ignore[arg-type]

    empty = service.league_matches(
        match_date=date(2026, 8, 30), league_id=3, timezone="UTC"
    )
    assert empty.fixtures == []

    with pytest.raises(WebNotFoundError) as error:
        service.league_matches(
            match_date=date(2026, 8, 30), league_id=404, timezone="UTC"
        )
    assert error.value.code == "league_not_found"
