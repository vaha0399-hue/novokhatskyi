"""Application service translating canonical reads into stable web DTOs."""

from __future__ import annotations

from collections import defaultdict
from decimal import Decimal

from app.analytics import AnalyticsEngine
from app.analytics.models import AnalyticsScope, AverageMetric, RateMetric, WindowAnalytics

from .dtos import (
    AverageMetricSummary, FixtureAnalyticsResponse, FixtureAnalyticsSide, FixtureScore,
    FixtureStatisticsResponse, FixtureStatisticsSide, FixtureSummary, FixtureTeamStatistics,
    GoalTotalsRateSummary, LeagueListResponse, LeagueReference, LeagueSeasonsResponse,
    MetricSummary, PaginationMetadata, RateMetricSummary, SeasonFixturesResponse,
    SeasonReference, SeasonStandingRow, SeasonStandingsResponse, StandingsGroup,
    StreakSummary, TeamAnalyticsResponse, TeamReference,
)
from .repository import (
    FixtureRecord, FixtureStatisticsRecord, LeagueRecord, SeasonRecord, TeamRecord,
    WebReadRepository,
)


class WebNotFoundError(LookupError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class WebValidationError(ValueError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _number(value: Decimal | None) -> float | None:
    return None if value is None else float(value)


def _average(metric: AverageMetric) -> AverageMetricSummary:
    return AverageMetricSummary(value=_number(metric.value), sample_size=metric.sample_size)


def _rate(metric: RateMetric) -> RateMetricSummary:
    return RateMetricSummary(count=metric.count, rate=_number(metric.rate))


def _metrics(value: WindowAnalytics) -> MetricSummary:
    return MetricSummary(
        matches=value.matches, wins=value.wins, draws=value.draws, losses=value.losses,
        points=value.points, points_per_game=_number(value.points_per_game),
        goals_scored=value.goals_scored, goals_conceded=value.goals_conceded,
        average_goals_scored=_number(value.average_goals_scored),
        average_goals_conceded=_number(value.average_goals_conceded),
        average_xg=_average(value.average_xg), average_xga=_average(value.average_xga),
        average_shots=_average(value.average_shots), average_shots_on_goal=_average(value.average_shots_on_goal),
        average_possession_pct=_average(value.average_possession_pct), average_corners=_average(value.average_corners),
        average_yellow_cards=_average(value.average_yellow_cards), average_red_cards=_average(value.average_red_cards),
        clean_sheets=_rate(value.clean_sheets), failed_to_score=_rate(value.failed_to_score), btts=_rate(value.btts),
        total_goals={key: GoalTotalsRateSummary(over=_rate(item.over), under=_rate(item.under)) for key, item in value.total_goals.items()},
        streaks=StreakSummary(**value.streaks.__dict__),
    )


def _team(value: TeamRecord) -> TeamReference:
    return TeamReference(id=value.id, name=value.name)


def _league(value: LeagueRecord) -> LeagueReference:
    return LeagueReference(
        id=value.id, name=value.name, country_name=value.country_name,
        logo_url=value.logo_url, competition_type=value.competition_type,
    )


def _season(value: SeasonRecord) -> SeasonReference:
    return SeasonReference(
        id=value.id, league=_league(value.league), start_year=value.start_year,
        label=value.label, starts_on=value.starts_on, ends_on=value.ends_on,
    )


def _fixture(value: FixtureRecord) -> FixtureSummary:
    score = None
    if value.lifecycle_state == "completed" and value.result_finalized_at is not None:
        if value.home_goals is None or value.away_goals is None:
            raise WebValidationError("completed_fixture_missing_score")
        score = FixtureScore(home=value.home_goals, away=value.away_goals)
    return FixtureSummary(
        id=value.context.fixture_id, season_id=value.context.season_id, kickoff_at=value.context.kickoff_at,
        round_label=value.round_label, lifecycle_state=value.lifecycle_state,
        home_team=_team(value.home_team), away_team=_team(value.away_team), final_score=score,
    )


def _fixture_statistics(value: FixtureStatisticsRecord) -> FixtureTeamStatistics:
    return FixtureTeamStatistics(
        shots_on_goal=value.shots_on_goal, shots_off_goal=value.shots_off_goal,
        total_shots=value.total_shots, blocked_shots=value.blocked_shots,
        shots_inside_box=value.shots_inside_box, shots_outside_box=value.shots_outside_box,
        fouls=value.fouls, corner_kicks=value.corner_kicks, offsides=value.offsides,
        yellow_cards=value.yellow_cards, red_cards=value.red_cards,
        goalkeeper_saves=value.goalkeeper_saves, total_passes=value.total_passes,
        passes_accurate=value.passes_accurate, possession_pct=_number(value.possession_pct),
        pass_accuracy_pct=_number(value.pass_accuracy_pct), expected_goals=_number(value.expected_goals),
        goals_prevented=_number(value.goals_prevented),
    )


class WebReadService:
    def __init__(self, repository: WebReadRepository, analytics: AnalyticsEngine) -> None:
        self._repository = repository
        self._analytics = analytics

    def leagues(self) -> LeagueListResponse:
        return LeagueListResponse(leagues=[_league(item) for item in self._repository.list_leagues()])

    def league_seasons(self, *, league_id: int) -> LeagueSeasonsResponse:
        league = self._repository.league(league_id=league_id)
        if league is None:
            raise WebNotFoundError("league_not_found")
        return LeagueSeasonsResponse(
            league=_league(league),
            seasons=[_season(item) for item in self._repository.list_league_seasons(league_id=league_id)],
        )

    def season_standings(self, *, season_id: int) -> SeasonStandingsResponse:
        season = self._repository.season(season_id=season_id)
        if season is None:
            raise WebNotFoundError("season_not_found")
        latest = self._repository.latest_season_standings(season_id=season_id)
        if latest is None:
            raise WebValidationError("season_standings_not_available")
        captured_at, rows = latest
        grouped: dict[int, list[SeasonStandingRow]] = defaultdict(list)
        names: dict[int, str | None] = {}
        for item in rows:
            grouped[item.group_index].append(
                SeasonStandingRow(
                    rank=item.rank, team=_team(item.team), points=item.points, played=item.played,
                    wins=item.wins, draws=item.draws, losses=item.losses, goals_for=item.goals_for,
                    goals_against=item.goals_against, goals_diff=item.goals_diff, form=item.form,
                    status=item.status, description=item.description,
                )
            )
            names[item.group_index] = item.group_name
        return SeasonStandingsResponse(
            season=_season(season), captured_at=captured_at,
            groups=[StandingsGroup(name=names[index], rows=grouped[index]) for index in sorted(grouped)],
        )

    def season_fixtures(self, *, season_id: int, limit: int, offset: int) -> SeasonFixturesResponse:
        if not self._repository.season_exists(season_id=season_id):
            raise WebNotFoundError("season_not_found")
        total, fixtures = self._repository.list_season_fixtures(season_id=season_id, limit=limit, offset=offset)
        next_offset = offset + limit if offset + limit < total else None
        return SeasonFixturesResponse(
            season_id=season_id, fixtures=[_fixture(item) for item in fixtures],
            pagination=PaginationMetadata(total=total, limit=limit, offset=offset, next_offset=next_offset),
        )

    def team_analytics(self, *, team_id: int, season_id: int, scope: AnalyticsScope, window: int) -> TeamAnalyticsResponse:
        if not self._repository.season_exists(season_id=season_id):
            raise WebNotFoundError("season_not_found")
        team = self._repository.team_in_season(team_id=team_id, season_id=season_id)
        if team is None:
            raise WebNotFoundError("team_not_found_in_season")
        cutoff = self._repository.latest_completed_team_cutoff(team_id=team_id, season_id=season_id)
        if cutoff is None:
            raise WebValidationError("team_has_no_completed_fixture_in_season")
        bundle = self._analytics.team_analytics(
            team_id=team_id, season_id=season_id, as_of_kickoff=cutoff, scope=scope,
        )
        return TeamAnalyticsResponse(
            team=_team(team), season_id=season_id, scope=scope.value, window=window,
            as_of_kickoff=cutoff, metrics=_metrics(bundle.windows[window]),
        )

    def fixture_analytics(self, *, fixture_id: int, window: int) -> FixtureAnalyticsResponse:
        fixture = self._repository.fixture(fixture_id=fixture_id)
        if fixture is None:
            raise WebNotFoundError("fixture_not_found")
        analytics = self._analytics.fixture_analytics(fixture_id=fixture_id)
        return FixtureAnalyticsResponse(
            fixture=_fixture(fixture), window=window, historical_cutoff_at=fixture.context.kickoff_at,
            home=FixtureAnalyticsSide(team=_team(fixture.home_team), overall=_metrics(analytics.home_overall.windows[window]), venue_split=_metrics(analytics.home_home.windows[window])),
            away=FixtureAnalyticsSide(team=_team(fixture.away_team), overall=_metrics(analytics.away_overall.windows[window]), venue_split=_metrics(analytics.away_away.windows[window])),
        )

    def fixture_statistics(self, *, fixture_id: int) -> FixtureStatisticsResponse:
        fixture = self._repository.fixture(fixture_id=fixture_id)
        if fixture is None:
            raise WebNotFoundError("fixture_not_found")
        statistics = {item.team_id: _fixture_statistics(item) for item in self._repository.fixture_statistics(fixture_id=fixture_id)}
        return FixtureStatisticsResponse(
            fixture=_fixture(fixture),
            home=FixtureStatisticsSide(
                team=_team(fixture.home_team), metrics=statistics.get(fixture.context.home_team_id),
            ),
            away=FixtureStatisticsSide(
                team=_team(fixture.away_team), metrics=statistics.get(fixture.context.away_team_id),
            ),
        )
