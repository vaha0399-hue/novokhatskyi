from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime
from urllib.parse import urlsplit

import pytest
from redis.asyncio import Redis

from app.live import (
    ACTIVE_FIXTURES_KEY,
    CanonicalFixtureReference,
    RedisLiveStore,
    bind_live_fixture,
    fixture_key,
    normalize_live_fixture,
)


TEST_REDIS_URL = os.environ.get("LIVE_REDIS_TEST_URL")
pytestmark = pytest.mark.skipif(
    not TEST_REDIS_URL, reason="LIVE_REDIS_TEST_URL is not configured"
)


def _state(fixture_id: int):
    provider = normalize_live_fixture(
        {
            "fixture": {
                "id": 1557383 + fixture_id,
                "status": {"short": "2H", "elapsed": 67, "extra": 1},
            },
            "league": {"id": 39, "season": 2026},
            "teams": {"home": {"id": 40}, "away": {"id": 65}},
            "goals": {"home": 2, "away": 1},
        }
    )
    canonical = CanonicalFixtureReference(
        fixture_id=fixture_id,
        season_id=14,
        league_id=7,
        kickoff_at=datetime(2026, 8, 30, 15, tzinfo=UTC),
        home_team_id=10,
        home_team_name="Liverpool",
        away_team_id=20,
        away_team_name="Nottingham Forest",
    )
    return bind_live_fixture(
        provider, canonical, observed_at=datetime(2026, 8, 30, 16, 7, tzinfo=UTC)
    )


def test_real_redis_current_state_round_trip_and_terminal_cleanup() -> None:
    assert TEST_REDIS_URL is not None
    parsed = urlsplit(TEST_REDIS_URL)
    assert (
        parsed.scheme == "unix" and parsed.path.startswith("/tmp/")
    ) or parsed.hostname in {"127.0.0.1", "localhost"}, "integration Redis must be local"

    async def exercise() -> None:
        client = Redis.from_url(TEST_REDIS_URL, decode_responses=True)
        store = RedisLiveStore(client)
        fixture_ids = (910000001, 910000002)
        try:
            await client.delete(*(fixture_key(fixture_id) for fixture_id in fixture_ids))
            await client.srem(ACTIVE_FIXTURES_KEY, *(str(fixture_id) for fixture_id in fixture_ids))

            await store.apply_poll([_state(fixture_id) for fixture_id in fixture_ids])
            active = {state.fixture_id: state for state in await store.active()}
            assert active[fixture_ids[0]] == _state(fixture_ids[0])
            assert active[fixture_ids[1]] == _state(fixture_ids[1])

            await store.apply_poll([], finished_fixture_ids=fixture_ids)
            assert await client.mget([fixture_key(fixture_id) for fixture_id in fixture_ids]) == [
                None,
                None,
            ]
            assert not {
                str(fixture_id) for fixture_id in fixture_ids
            } & await client.smembers(ACTIVE_FIXTURES_KEY)
        finally:
            await client.delete(*(fixture_key(fixture_id) for fixture_id in fixture_ids))
            await client.srem(ACTIVE_FIXTURES_KEY, *(str(fixture_id) for fixture_id in fixture_ids))
            await client.aclose()

    asyncio.run(exercise())
