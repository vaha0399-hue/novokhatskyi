"""Web DTO composition for the read-only scheduled-fixture scanner."""

from __future__ import annotations

from decimal import Decimal

from app.scanner import ScannerFilter, ScannerQuery, ScannerService
from app.scanner.repository import ScannerFixtureRecord, ScannerMetricRecord, ScannerTeamRecord

from .dtos import (
    FixtureSummary, LeagueReference, PaginationMetadata, ScannerFixture,
    ScannerFixtureSide, ScannerMatchesRequest, ScannerMatchesResponse,
    ScannerMetricSnapshot, TeamReference,
)


def _number(value: Decimal | None) -> float | None:
    return None if value is None else float(value)


def _team(value: ScannerTeamRecord) -> TeamReference:
    return TeamReference(id=value.id, name=value.name)


def _metrics(value: ScannerMetricRecord | None) -> ScannerMetricSnapshot | None:
    if value is None:
        return None
    return ScannerMetricSnapshot(
        matches_count=value.matches_count,
        avg_xg=_number(value.avg_xg), xg_sample_count=value.xg_sample_count,
        avg_xga=_number(value.avg_xga), xga_sample_count=value.xga_sample_count,
        avg_goals_for=float(value.avg_goals_for), avg_goals_against=float(value.avg_goals_against),
        scored_rate=float(value.scored_rate), conceded_rate=float(value.conceded_rate),
        btts_rate=float(value.btts_rate), over_1_5_rate=float(value.over_1_5_rate),
        over_2_5_rate=float(value.over_2_5_rate), over_3_5_rate=float(value.over_3_5_rate),
        avg_shots=_number(value.avg_shots), shots_sample_count=value.shots_sample_count,
        avg_shots_on_goal=_number(value.avg_shots_on_goal),
        shots_on_goal_sample_count=value.shots_on_goal_sample_count,
        avg_corners=_number(value.avg_corners), corners_sample_count=value.corners_sample_count,
        avg_possession=_number(value.avg_possession), possession_sample_count=value.possession_sample_count,
        source_last_kickoff_at=value.source_last_kickoff_at, updated_at=value.updated_at,
    )


def _fixture(value: ScannerFixtureRecord) -> ScannerFixture:
    fixture = FixtureSummary(
        id=value.fixture_id, season_id=value.season_id, kickoff_at=value.kickoff_at,
        round_label=value.round_label, lifecycle_state="scheduled", home_team=_team(value.home_team),
        away_team=_team(value.away_team), final_score=None,
    )
    league = LeagueReference(
        id=value.league.id, name=value.league.name, country_name=value.league.country_name,
        logo_url=value.league.logo_url, competition_type=value.league.competition_type,
    )
    return ScannerFixture(
        fixture=fixture, league=league,
        home=ScannerFixtureSide(team=_team(value.home_team), overall=_metrics(value.home_overall), venue=_metrics(value.home_venue)),
        away=ScannerFixtureSide(team=_team(value.away_team), overall=_metrics(value.away_overall), venue=_metrics(value.away_venue)),
    )


class ScannerWebService:
    def __init__(self, scanner: ScannerService) -> None:
        self._scanner = scanner

    def matches(self, *, request: ScannerMatchesRequest) -> ScannerMatchesResponse:
        query = ScannerQuery(
            match_date=request.match_date, timezone=request.timezone,
            league_ids=tuple(request.league_ids), window_size=request.window,
            min_matches=request.min_matches,
            filters=tuple(
                ScannerFilter(
                    side=filter_.side, metric=filter_.field, operator=filter_.operator,
                    value=filter_.value, min_samples=filter_.min_samples,
                )
                for filter_ in request.filters
            ),
            limit=request.limit, offset=request.offset,
        )
        timezone, total, fixtures = self._scanner.scan(query=query)
        next_offset = request.offset + request.limit if request.offset + request.limit < total else None
        return ScannerMatchesResponse(
            date=request.match_date, timezone=timezone, window=request.window,
            min_matches=request.min_matches, fixtures=[_fixture(item) for item in fixtures],
            pagination=PaginationMetadata(total=total, limit=request.limit, offset=request.offset, next_offset=next_offset),
        )
