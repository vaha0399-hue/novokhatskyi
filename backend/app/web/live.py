"""Redis-backed current live-state endpoint."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Response
from redis.exceptions import RedisError

from app.live import LiveFixtureState, LiveStateConsistencyError, RedisLiveStore

from .dependencies import get_live_store
from .dtos import FixtureScore, LiveFixtureDTO, LiveFixturesResponse, TeamReference


router = APIRouter(prefix="/web/v1", tags=["web-live"])
LiveStore = Annotated[RedisLiveStore, Depends(get_live_store)]


def _fixture(state: LiveFixtureState) -> LiveFixtureDTO:
    return LiveFixtureDTO(
        fixture_id=state.fixture_id,
        season_id=state.season_id,
        league_id=state.league_id,
        kickoff_at=state.kickoff_at,
        home_team=TeamReference(id=state.home_team_id, name=state.home_team_name),
        away_team=TeamReference(id=state.away_team_id, name=state.away_team_name),
        status=state.status.value,
        score=FixtureScore(home=state.score.home, away=state.score.away),
        elapsed_minute=state.elapsed_minute,
        added_time=state.added_time,
        observed_at=state.observed_at,
    )


@router.get("/live", response_model=LiveFixturesResponse)
async def live_fixtures(store: LiveStore, response: Response) -> LiveFixturesResponse:
    """Return the atomic Redis snapshot without contacting API-Football."""
    response.headers["Cache-Control"] = "no-store"
    try:
        states = await store.active()
    except (LiveStateConsistencyError, RedisError) as error:
        raise HTTPException(
            status_code=503,
            detail={"code": "live_state_unavailable"},
            headers={"Cache-Control": "no-store"},
        ) from error
    return LiveFixturesResponse(fixtures=[_fixture(state) for state in states])
