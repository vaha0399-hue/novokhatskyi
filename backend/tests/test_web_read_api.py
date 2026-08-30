from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime

import httpx
import pytest

from app.main import app
from app.web.dependencies import get_web_read_service
from app.web.dtos import (
    AverageMetricSummary, FixtureAnalyticsResponse, FixtureAnalyticsSide, FixtureScore,
    FixtureStatisticsResponse, FixtureStatisticsSide, FixtureSummary, FixtureTeamStatistics,
    GoalTotalsRateSummary, LeagueListResponse, LeagueMatchesResponse, LeagueReference,
    LeagueSeasonsResponse, MatchDateLeagueSummary, MatchDateLeaguesResponse, MetricSummary,
    PaginationMetadata, RateMetricSummary, SeasonFixturesResponse,
    SeasonReference, SeasonStandingRow, SeasonStandingsResponse, StandingsGroup,
    StreakSummary, TeamAnalyticsResponse, TeamReference,
)
from app.web.service import WebNotFoundError, WebValidationError


NOW = datetime(2024, 10, 1, 15, tzinfo=UTC)
HOME = TeamReference(id=10, name="Home")
AWAY = TeamReference(id=20, name="Away")
LEAGUE = LeagueReference(id=3, name="Premier League", country_name="England", logo_url=None, competition_type="league")
SEASON = SeasonReference(id=3, league=LEAGUE, start_year=2024, label="2024", starts_on=None, ends_on=None)


def _metrics() -> MetricSummary:
    average = AverageMetricSummary(value=1.0, sample_size=1)
    rate = RateMetricSummary(count=1, rate=1.0)
    return MetricSummary(
        matches=1, wins=1, draws=0, losses=0, points=3, points_per_game=3.0,
        goals_scored=2, goals_conceded=1, average_goals_scored=2.0, average_goals_conceded=1.0,
        average_xg=average, average_xga=average, average_shots=average,
        average_shots_on_goal=average, average_possession_pct=average, average_corners=average,
        average_yellow_cards=average, average_red_cards=AverageMetricSummary(value=None, sample_size=0),
        clean_sheets=rate, failed_to_score=RateMetricSummary(count=0, rate=0.0), btts=rate,
        total_goals={threshold: GoalTotalsRateSummary(over=rate, under=RateMetricSummary(count=0, rate=0.0)) for threshold in ("0.5", "1.5", "2.5", "3.5")},
        streaks=StreakSummary(wins=1, unbeaten=1, winless=0, losses=0, scored=1, clean_sheets=0, btts=1),
    )


def _fixture(fixture_id: int, *, kickoff_at: datetime, completed: bool = True) -> FixtureSummary:
    return FixtureSummary(
        id=fixture_id, season_id=3, kickoff_at=kickoff_at, round_label="Regular Season - 1",
        lifecycle_state="completed" if completed else "scheduled", home_team=HOME, away_team=AWAY,
        final_score=FixtureScore(home=2, away=1) if completed else None,
    )


class FakeService:
    def __init__(self) -> None:
        self.season_calls: list[tuple[int, int, int]] = []
        self.team_calls: list[tuple[int, int, object, int]] = []
        self.fixture_calls: list[tuple[int, int]] = []
        self.match_date_calls: list[tuple[date, str]] = []
        self.league_match_calls: list[tuple[date, int, str]] = []

    def leagues(self):
        return LeagueListResponse(leagues=[LEAGUE])

    def match_date_leagues(self, *, match_date: date, timezone: str):
        if timezone == "Not/A_Timezone":
            raise WebValidationError("invalid_timezone")
        self.match_date_calls.append((match_date, timezone))
        return MatchDateLeaguesResponse(
            date=match_date,
            timezone=timezone,
            leagues=[MatchDateLeagueSummary(league=LEAGUE, fixture_count=2)],
        )

    def league_matches(self, *, match_date: date, league_id: int, timezone: str):
        if league_id == 404:
            raise WebNotFoundError("league_not_found")
        self.league_match_calls.append((match_date, league_id, timezone))
        return LeagueMatchesResponse(
            date=match_date,
            timezone=timezone,
            league=LEAGUE,
            fixtures=[
                _fixture(1, kickoff_at=NOW),
                _fixture(2, kickoff_at=NOW.replace(hour=17), completed=False),
            ],
        )

    def league_seasons(self, *, league_id: int):
        if league_id == 404:
            raise WebNotFoundError("league_not_found")
        return LeagueSeasonsResponse(league=LEAGUE, seasons=[SEASON])

    def season_standings(self, *, season_id: int):
        if season_id == 404:
            raise WebNotFoundError("season_not_found")
        return SeasonStandingsResponse(
            season=SEASON, captured_at=NOW,
            groups=[StandingsGroup(name=None, rows=[SeasonStandingRow(
                rank=1, team=HOME, points=3, played=1, wins=1, draws=0, losses=0,
                goals_for=2, goals_against=1, goals_diff=1, form="W", status=None, description=None,
            )])],
        )

    def season_fixtures(self, *, season_id: int, limit: int, offset: int):
        if season_id == 404:
            raise WebNotFoundError("season_not_found")
        self.season_calls.append((season_id, limit, offset))
        fixtures = [_fixture(1, kickoff_at=NOW), _fixture(2, kickoff_at=NOW.replace(hour=17), completed=False)]
        return SeasonFixturesResponse(
            season_id=season_id, fixtures=fixtures,
            pagination=PaginationMetadata(total=2, limit=limit, offset=offset, next_offset=None),
        )

    def team_analytics(self, *, team_id: int, season_id: int, scope, window: int):
        if season_id == 404:
            raise WebNotFoundError("season_not_found")
        if team_id == 404:
            raise WebNotFoundError("team_not_found_in_season")
        self.team_calls.append((team_id, season_id, scope, window))
        return TeamAnalyticsResponse(team=HOME, season_id=season_id, scope=scope.value, window=window, as_of_kickoff=NOW, metrics=_metrics())

    def fixture_analytics(self, *, fixture_id: int, window: int):
        if fixture_id == 404:
            raise WebNotFoundError("fixture_not_found")
        self.fixture_calls.append((fixture_id, window))
        fixture = _fixture(fixture_id, kickoff_at=NOW)
        return FixtureAnalyticsResponse(
            fixture=fixture, window=window, historical_cutoff_at=fixture.kickoff_at,
            home=FixtureAnalyticsSide(team=HOME, overall=_metrics(), venue_split=_metrics()),
            away=FixtureAnalyticsSide(team=AWAY, overall=_metrics(), venue_split=_metrics()),
        )

    def fixture_statistics(self, *, fixture_id: int):
        if fixture_id == 404:
            raise WebNotFoundError("fixture_not_found")
        values = FixtureTeamStatistics(
            shots_on_goal=4, shots_off_goal=None, total_shots=9, blocked_shots=None,
            shots_inside_box=None, shots_outside_box=None, fouls=7, corner_kicks=3,
            offsides=1, yellow_cards=2, red_cards=0, goalkeeper_saves=3, total_passes=480,
            passes_accurate=400, possession_pct=55.0, pass_accuracy_pct=83.33,
            expected_goals=1.23, goals_prevented=None,
        )
        return FixtureStatisticsResponse(
            fixture=_fixture(fixture_id, kickoff_at=NOW),
            home=FixtureStatisticsSide(team=HOME, metrics=values),
            away=FixtureStatisticsSide(team=AWAY, metrics=None),
        )


class ASGIClient:
    """Small synchronous adapter that avoids TestClient version coupling."""

    def get(self, path: str) -> httpx.Response:
        async def request() -> httpx.Response:
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                return await client.get(path)

        return asyncio.run(request())


@pytest.fixture
def client_and_service():
    service = FakeService()
    app.dependency_overrides[get_web_read_service] = lambda: service
    try:
        yield ASGIClient(), service
    finally:
        app.dependency_overrides.clear()


def test_season_fixtures_contract_includes_deterministic_order_and_no_scheduled_score(client_and_service) -> None:
    client, service = client_and_service
    response = client.get("/web/v1/seasons/3/fixtures?limit=2&offset=0")

    assert response.status_code == 200
    body = response.json()
    assert [fixture["id"] for fixture in body["fixtures"]] == [1, 2]
    assert body["fixtures"][0]["final_score"] == {"home": 2, "away": 1}
    assert body["fixtures"][1]["final_score"] is None
    assert service.season_calls == [(3, 2, 0)]


def test_discovery_and_standings_contracts(client_and_service) -> None:
    client, _ = client_and_service

    leagues = client.get("/web/v1/leagues")
    seasons = client.get("/web/v1/leagues/3/seasons")
    standings = client.get("/web/v1/seasons/3/standings")

    assert leagues.status_code == seasons.status_code == standings.status_code == 200
    assert leagues.json() == {"leagues": [{"id": 3, "name": "Premier League", "country_name": "England", "logo_url": None, "competition_type": "league"}]}
    assert seasons.json()["seasons"][0]["id"] == 3
    assert standings.json()["groups"][0]["rows"][0]["team"] == {"id": 10, "name": "Home"}
    assert standings.json()["groups"][0]["rows"][0]["points"] == 3


def test_match_date_league_discovery_is_lightweight_and_timezone_explicit(client_and_service) -> None:
    client, service = client_and_service

    response = client.get(
        "/web/v1/matches/leagues?date=2026-08-30&timezone=Asia%2FTokyo"
    )

    assert response.status_code == 200
    assert response.json() == {
        "date": "2026-08-30",
        "timezone": "Asia/Tokyo",
        "leagues": [{"league": LEAGUE.model_dump(), "fixture_count": 2}],
    }
    assert service.match_date_calls == [(date(2026, 8, 30), "Asia/Tokyo")]


def test_selected_league_matches_contract_reuses_fixture_summary(client_and_service) -> None:
    client, service = client_and_service

    response = client.get(
        "/web/v1/matches?date=2026-08-30&league_id=3&timezone=UTC"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["league"]["id"] == 3
    assert [fixture["id"] for fixture in body["fixtures"]] == [1, 2]
    assert body["fixtures"][1]["final_score"] is None
    assert service.league_match_calls == [(date(2026, 8, 30), 3, "UTC")]


def test_fixture_statistics_contract_keeps_missing_metric_as_null(client_and_service) -> None:
    client, _ = client_and_service
    response = client.get("/web/v1/fixtures/1/statistics")

    assert response.status_code == 200
    body = response.json()
    assert body["home"]["metrics"]["shots_on_goal"] == 4
    assert body["home"]["metrics"]["shots_off_goal"] is None
    assert body["away"]["metrics"] is None


@pytest.mark.parametrize("path", [
    "/web/v1/teams/10/analytics?season_id=3&scope=invalid",
    "/web/v1/teams/10/analytics?season_id=3&window=7",
    "/web/v1/fixtures/1/analytics?window=7",
    "/web/v1/seasons/3/fixtures?limit=0",
    "/web/v1/matches/leagues?date=2026-08-30",
    "/web/v1/matches/leagues?date=not-a-date&timezone=UTC",
    "/web/v1/matches?date=2026-08-30&league_id=0&timezone=UTC",
])
def test_invalid_query_contract_is_422(client_and_service, path: str) -> None:
    client, _ = client_and_service
    assert client.get(path).status_code == 422


@pytest.mark.parametrize("path,code", [
    ("/web/v1/seasons/404/fixtures", "season_not_found"),
    ("/web/v1/leagues/404/seasons", "league_not_found"),
    ("/web/v1/seasons/404/standings", "season_not_found"),
    ("/web/v1/teams/10/analytics?season_id=404", "season_not_found"),
    ("/web/v1/teams/404/analytics?season_id=3", "team_not_found_in_season"),
    ("/web/v1/fixtures/404/analytics", "fixture_not_found"),
    ("/web/v1/fixtures/404/statistics", "fixture_not_found"),
    ("/web/v1/matches?date=2026-08-30&league_id=404&timezone=UTC", "league_not_found"),
])
def test_not_found_contract_is_stable(client_and_service, path: str, code: str) -> None:
    client, _ = client_and_service
    response = client.get(path)
    assert response.status_code == 404
    assert response.json() == {"detail": {"code": code}}


def test_unknown_timezone_contract_is_stable(client_and_service) -> None:
    client, _ = client_and_service
    response = client.get(
        "/web/v1/matches/leagues?date=2026-08-30&timezone=Not%2FA_Timezone"
    )
    assert response.status_code == 422
    assert response.json() == {"detail": {"code": "invalid_timezone"}}


def test_team_analytics_and_fixture_comparison_contract(client_and_service) -> None:
    client, service = client_and_service
    team_response = client.get("/web/v1/teams/10/analytics?season_id=3&scope=home&window=15")
    fixture_response = client.get("/web/v1/fixtures/1/analytics?window=5")

    assert team_response.status_code == 200, team_response.text
    assert fixture_response.status_code == 200, fixture_response.text
    assert team_response.json()["scope"] == "home"
    assert team_response.json()["window"] == 15
    assert fixture_response.json()["historical_cutoff_at"] == NOW.isoformat().replace("+00:00", "Z")
    assert fixture_response.json()["home"]["team"]["id"] == HOME.id
    assert fixture_response.json()["away"]["team"]["id"] == AWAY.id
    assert service.team_calls[0][1:] == (3, service.team_calls[0][2], 15)
    assert service.fixture_calls == [(1, 5)]
