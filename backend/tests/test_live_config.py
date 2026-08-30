from __future__ import annotations

import asyncio

import pytest

from app.live import (
    LiveConfigurationError,
    LiveSettings,
    create_redis_client,
    managed_redis_client,
)


def test_live_settings_use_canonical_initial_defaults() -> None:
    settings = LiveSettings.from_environment({"REDIS_URL": "redis://localhost:6379/0"})

    assert settings.poll_interval_seconds == 25
    assert settings.terminal_recheck_interval_seconds == 300
    assert settings.league_external_ids == (39,)
    assert settings.provider_live_parameter == "39"
    assert settings.redis_max_connections == 10


def test_live_settings_support_generic_multi_league_scope() -> None:
    settings = LiveSettings.from_environment(
        {
            "REDIS_URL": "rediss://redis.example.test:6380/1",
            "LIVE_POLL_INTERVAL_SECONDS": "30",
            "LIVE_TERMINAL_RECHECK_INTERVAL_SECONDS": "600",
            "LIVE_LEAGUE_EXTERNAL_IDS": "39,2-140",
            "LIVE_REDIS_MAX_CONNECTIONS": "6",
        }
    )

    assert settings.poll_interval_seconds == 30
    assert settings.terminal_recheck_interval_seconds == 600
    assert settings.league_external_ids == (39, 2, 140)
    assert settings.provider_live_parameter == "39-2-140"
    assert settings.redis_max_connections == 6


@pytest.mark.parametrize(
    "environment,error",
    [
        ({}, "REDIS_URL"),
        ({"REDIS_URL": "http://localhost"}, "REDIS_URL"),
        (
            {"REDIS_URL": "redis://localhost", "LIVE_POLL_INTERVAL_SECONDS": "0"},
            "LIVE_POLL_INTERVAL_SECONDS",
        ),
        (
            {
                "REDIS_URL": "redis://localhost",
                "LIVE_TERMINAL_RECHECK_INTERVAL_SECONDS": "0",
            },
            "LIVE_TERMINAL_RECHECK_INTERVAL_SECONDS",
        ),
        (
            {"REDIS_URL": "redis://localhost", "LIVE_LEAGUE_EXTERNAL_IDS": "39,39"},
            "duplicates",
        ),
        (
            {"REDIS_URL": "redis://localhost", "LIVE_LEAGUE_EXTERNAL_IDS": "39,"},
            "LIVE_LEAGUE_EXTERNAL_IDS",
        ),
    ],
)
def test_invalid_live_settings_fail_closed(environment: dict[str, str], error: str) -> None:
    with pytest.raises(LiveConfigurationError, match=error):
        LiveSettings.from_environment(environment)


def test_redis_client_uses_one_bounded_async_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, dict[str, object]]] = []
    sentinel = object()

    def from_url(url: str, **kwargs: object) -> object:
        calls.append((url, kwargs))
        return sentinel

    monkeypatch.setattr("app.live.config.Redis.from_url", from_url)
    settings = LiveSettings.from_environment(
        {"REDIS_URL": "redis://localhost:6379/0", "LIVE_REDIS_MAX_CONNECTIONS": "7"}
    )

    assert create_redis_client(settings) is sentinel
    assert calls == [
        (
            "redis://localhost:6379/0",
            {
                "decode_responses": True,
                "socket_connect_timeout": 1.0,
                "socket_timeout": 1.0,
                "health_check_interval": 30,
                "max_connections": 7,
            },
        )
    ]


def test_managed_redis_client_reuses_and_closes_one_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeRedisClient:
        def __init__(self) -> None:
            self.close_calls = 0
            self.ping_calls = 0

        async def ping(self) -> bool:
            self.ping_calls += 1
            return True

        async def aclose(self) -> None:
            self.close_calls += 1

    client = FakeRedisClient()
    created: list[LiveSettings] = []

    def create(settings: LiveSettings) -> FakeRedisClient:
        created.append(settings)
        return client

    monkeypatch.setattr("app.live.config.create_redis_client", create)
    settings = LiveSettings.from_environment({"REDIS_URL": "redis://localhost:6379/0"})

    async def exercise() -> None:
        async with managed_redis_client(settings) as first_reference:
            assert first_reference is client
            assert await first_reference.ping()
            assert await first_reference.ping()
            assert client.close_calls == 0

    asyncio.run(exercise())

    assert created == [settings]
    assert client.ping_calls == 2
    assert client.close_calls == 1
