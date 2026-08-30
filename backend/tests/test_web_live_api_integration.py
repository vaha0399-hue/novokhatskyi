"""Real-Redis verification for the FastAPI live read path."""

from __future__ import annotations

import asyncio
import os
from dataclasses import replace
from datetime import UTC, datetime
from urllib.parse import urlsplit

import httpx
import pytest
from redis.asyncio import Redis

from app.live import (
    ACTIVE_FIXTURES_KEY,
    LiveFixtureState,
    LiveFixtureStatus,
    LiveScore,
    RedisLiveStore,
    fixture_key,
)
from app.live.store import encode_live_state
from app.main import app


TEST_REDIS_URL = os.environ.get("LIVE_REDIS_TEST_URL")
pytestmark = pytest.mark.skipif(
    not TEST_REDIS_URL,
    reason="LIVE_REDIS_TEST_URL is not configured",
)


def _state() -> LiveFixtureState:
    return LiveFixtureState(
        fixture_id=910000101,
        provider_fixture_id=1557383,
        provider_league_id=39,
        provider_season=2026,
        season_id=14,
        league_id=7,
        kickoff_at=datetime(2026, 8, 30, 15, tzinfo=UTC),
        home_team_id=10,
        home_team_name="Liverpool",
        away_team_id=20,
        away_team_name="Nottingham Forest",
        status=LiveFixtureStatus.SECOND_HALF,
        score=LiveScore(home=2, away=1),
        elapsed_minute=67,
        added_time=1,
        observed_at=datetime(2026, 8, 30, 16, 7, tzinfo=UTC),
    )


def test_app_lifespan_reads_real_redis_without_a_provider_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert TEST_REDIS_URL is not None
    parsed = urlsplit(TEST_REDIS_URL)
    assert (
        parsed.scheme == "unix" and parsed.path.startswith("/tmp/")
    ) or parsed.hostname in {"127.0.0.1", "localhost"}
    monkeypatch.setenv("REDIS_URL", TEST_REDIS_URL)

    async def exercise() -> None:
        fixture_id = _state().fixture_id
        admin = Redis.from_url(TEST_REDIS_URL, decode_responses=True)
        try:
            await admin.delete(fixture_key(fixture_id))
            await admin.srem(ACTIVE_FIXTURES_KEY, str(fixture_id))
            await RedisLiveStore(admin).apply_poll([_state()])

            async with app.router.lifespan_context(app):
                transport = httpx.ASGITransport(app=app)
                async with httpx.AsyncClient(
                    transport=transport,
                    base_url="http://testserver",
                ) as client:
                    response = await client.get("/web/v1/live")

                    terminal = replace(_state(), status=LiveFixtureStatus.FINISHED)
                    await admin.set(fixture_key(fixture_id), encode_live_state(terminal))
                    terminal_response = await client.get("/web/v1/live")

            assert response.status_code == 200
            assert response.json()["fixtures"][0]["fixture_id"] == fixture_id
            assert response.json()["fixtures"][0]["score"] == {"home": 2, "away": 1}
            assert terminal_response.status_code == 503
            assert terminal_response.json() == {
                "detail": {"code": "live_state_unavailable"}
            }
            assert not hasattr(app.state, "live_store")
        finally:
            await admin.delete(fixture_key(fixture_id))
            await admin.srem(ACTIVE_FIXTURES_KEY, str(fixture_id))
            await admin.aclose()

    asyncio.run(exercise())
