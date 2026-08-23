"""Versioned internal web read endpoints; no provider or write path."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from app.analytics.models import AnalyticsScope

from .dependencies import get_web_read_service
from .dtos import FixtureAnalyticsResponse, SeasonFixturesResponse, TeamAnalyticsResponse
from .service import WebNotFoundError, WebReadService, WebValidationError


router = APIRouter(prefix="/web/v1", tags=["web-read"])
Service = Annotated[WebReadService, Depends(get_web_read_service)]


def _validated_window(window: int) -> int:
    if window not in {5, 10, 15, 20}:
        raise HTTPException(status_code=422, detail={"code": "invalid_window"})
    return window


def _error(error: WebNotFoundError | WebValidationError) -> HTTPException:
    return HTTPException(status_code=404 if isinstance(error, WebNotFoundError) else 422, detail={"code": error.code})


@router.get("/seasons/{season_id}/fixtures", response_model=SeasonFixturesResponse)
def season_fixtures(
    season_id: int,
    service: Service,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> SeasonFixturesResponse:
    try:
        return service.season_fixtures(season_id=season_id, limit=limit, offset=offset)
    except (WebNotFoundError, WebValidationError) as error:
        raise _error(error) from error


@router.get("/teams/{team_id}/analytics", response_model=TeamAnalyticsResponse)
def team_analytics(
    team_id: int,
    service: Service,
    season_id: Annotated[int, Query(gt=0)],
    scope: AnalyticsScope = AnalyticsScope.OVERALL,
    window: Annotated[int, Query(ge=1, le=20)] = 10,
) -> TeamAnalyticsResponse:
    window = _validated_window(window)
    try:
        return service.team_analytics(team_id=team_id, season_id=season_id, scope=scope, window=window)
    except (WebNotFoundError, WebValidationError) as error:
        raise _error(error) from error


@router.get("/fixtures/{fixture_id}/analytics", response_model=FixtureAnalyticsResponse)
def fixture_analytics(
    fixture_id: int,
    service: Service,
    window: Annotated[int, Query(ge=1, le=20)] = 10,
) -> FixtureAnalyticsResponse:
    window = _validated_window(window)
    try:
        return service.fixture_analytics(fixture_id=fixture_id, window=window)
    except (WebNotFoundError, WebValidationError) as error:
        raise _error(error) from error
