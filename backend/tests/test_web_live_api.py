from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import httpx
import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from app.live import LiveFixtureState, LiveFixtureStatus, LiveScore
from app.live.store import LiveStateConsistencyError
from app.main import app
from app.web.dependencies import get_live_store


KICKOFF = datetime(2026, 8, 30, 15, tzinfo=UTC)
OBSERVED = datetime(2026, 8, 30, 16, 7, tzinfo=UTC)


def _state(fixture_id: int = 101) -> LiveFixtureState:
    return LiveFixtureState(
        fixture_id=fixture_id,
        provider_fixture_id=1557383 + fixture_id,
        provider_league_id=39,
        provider_season=2026,
        season_id=14,
        league_id=7,
        kickoff_at=KICKOFF,
        home_team_id=10,
        home_team_name="Liverpool",
        away_team_id=20,
        away_team_name="Nottingham Forest",
        status=LiveFixtureStatus.SECOND_HALF,
        score=LiveScore(home=2, away=1),
        elapsed_minute=67,
        added_time=1,
        observed_at=OBSERVED,
    )


class FakeStore:
    def __init__(
        self,
        states: tuple[LiveFixtureState, ...] = (),
        error: Exception | None = None,
    ) -> None:
        self.states = states
        self.error = error
        self.read_count = 0

    async def active(self) -> tuple[LiveFixtureState, ...]:
        self.read_count += 1
        if self.error is not None:
            raise self.error
        return self.states


async def _get(store: FakeStore) -> httpx.Response:
    app.dependency_overrides[get_live_store] = lambda: store
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://testserver",
        ) as client:
            return await client.get("/web/v1/live")
    finally:
        app.dependency_overrides.clear()


def test_live_contract_exposes_only_normalized_internal_state() -> None:
    store = FakeStore((_state(),))

    response = asyncio.run(_get(store))

    assert response.status_code == 200
    assert response.json() == {
        "fixtures": [
            {
                "fixture_id": 101,
                "season_id": 14,
                "league_id": 7,
                "kickoff_at": "2026-08-30T15:00:00Z",
                "home_team": {"id": 10, "name": "Liverpool"},
                "away_team": {"id": 20, "name": "Nottingham Forest"},
                "status": "second_half",
                "score": {"home": 2, "away": 1},
                "elapsed_minute": 67,
                "added_time": 1,
                "observed_at": "2026-08-30T16:07:00Z",
            }
        ]
    }
    assert "provider" not in response.text
    assert response.headers["cache-control"] == "no-store"
    assert store.read_count == 1


def test_live_contract_returns_an_empty_collection_when_no_match_is_active() -> None:
    response = asyncio.run(_get(FakeStore()))

    assert response.status_code == 200
    assert response.json() == {"fixtures": []}
    assert response.headers["cache-control"] == "no-store"


@pytest.mark.parametrize(
    "error",
    [
        LiveStateConsistencyError("corrupt state"),
        RedisConnectionError("connection unavailable"),
        RedisTimeoutError("read timed out"),
    ],
)
def test_live_contract_fails_closed_when_current_state_is_unavailable(
    error: Exception,
) -> None:
    response = asyncio.run(_get(FakeStore(error=error)))

    assert response.status_code == 503
    assert response.json() == {"detail": {"code": "live_state_unavailable"}}
    assert response.headers["cache-control"] == "no-store"
