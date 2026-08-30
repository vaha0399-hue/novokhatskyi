"""Environment-backed live worker and Redis configuration."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from urllib.parse import urlsplit

from redis.asyncio import Redis


DEFAULT_POLL_INTERVAL_SECONDS = 25
DEFAULT_TERMINAL_RECHECK_INTERVAL_SECONDS = 300
DEFAULT_LEAGUE_EXTERNAL_IDS = (39,)
DEFAULT_REDIS_MAX_CONNECTIONS = 10


class LiveConfigurationError(ValueError):
    """Live settings are missing or unsafe."""


def _positive_integer(raw: str, variable: str) -> int:
    try:
        value = int(raw)
    except ValueError as error:
        raise LiveConfigurationError(f"{variable} must be a positive integer") from error
    if value <= 0:
        raise LiveConfigurationError(f"{variable} must be a positive integer")
    return value


def _league_ids(raw: str) -> tuple[int, ...]:
    tokens = raw.replace("-", ",").split(",")
    if not tokens or any(not token.strip() for token in tokens):
        raise LiveConfigurationError("LIVE_LEAGUE_EXTERNAL_IDS is invalid")
    values = tuple(
        _positive_integer(token.strip(), "LIVE_LEAGUE_EXTERNAL_IDS") for token in tokens
    )
    if len(values) != len(set(values)):
        raise LiveConfigurationError("LIVE_LEAGUE_EXTERNAL_IDS contains duplicates")
    return values


@dataclass(frozen=True)
class LiveSettings:
    redis_url: str
    poll_interval_seconds: int = DEFAULT_POLL_INTERVAL_SECONDS
    league_external_ids: tuple[int, ...] = DEFAULT_LEAGUE_EXTERNAL_IDS
    redis_max_connections: int = DEFAULT_REDIS_MAX_CONNECTIONS
    terminal_recheck_interval_seconds: int = DEFAULT_TERMINAL_RECHECK_INTERVAL_SECONDS

    @property
    def provider_live_parameter(self) -> str:
        return "-".join(str(value) for value in self.league_external_ids)

    @classmethod
    def from_environment(
        cls, environ: Mapping[str, str] | None = None
    ) -> "LiveSettings":
        values = os.environ if environ is None else environ
        redis_url = values.get("REDIS_URL", "").strip()
        parsed_url = urlsplit(redis_url)
        if parsed_url.scheme not in {"redis", "rediss", "unix"}:
            raise LiveConfigurationError("REDIS_URL must use redis, rediss, or unix scheme")
        if parsed_url.scheme in {"redis", "rediss"} and not parsed_url.hostname:
            raise LiveConfigurationError("REDIS_URL must include a host")
        if parsed_url.scheme == "unix" and not parsed_url.path:
            raise LiveConfigurationError("REDIS_URL must include a socket path")
        return cls(
            redis_url=redis_url,
            poll_interval_seconds=_positive_integer(
                values.get(
                    "LIVE_POLL_INTERVAL_SECONDS", str(DEFAULT_POLL_INTERVAL_SECONDS)
                ),
                "LIVE_POLL_INTERVAL_SECONDS",
            ),
            terminal_recheck_interval_seconds=_positive_integer(
                values.get(
                    "LIVE_TERMINAL_RECHECK_INTERVAL_SECONDS",
                    str(DEFAULT_TERMINAL_RECHECK_INTERVAL_SECONDS),
                ),
                "LIVE_TERMINAL_RECHECK_INTERVAL_SECONDS",
            ),
            league_external_ids=_league_ids(
                values.get(
                    "LIVE_LEAGUE_EXTERNAL_IDS",
                    ",".join(str(value) for value in DEFAULT_LEAGUE_EXTERNAL_IDS),
                )
            ),
            redis_max_connections=_positive_integer(
                values.get(
                    "LIVE_REDIS_MAX_CONNECTIONS", str(DEFAULT_REDIS_MAX_CONNECTIONS)
                ),
                "LIVE_REDIS_MAX_CONNECTIONS",
            ),
        )


def create_redis_client(settings: LiveSettings) -> Redis:
    """Create one async client/pool for a FastAPI or worker process."""
    return Redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_connect_timeout=1.0,
        socket_timeout=1.0,
        health_check_interval=30,
        max_connections=settings.redis_max_connections,
    )


@asynccontextmanager
async def managed_redis_client(settings: LiveSettings) -> AsyncIterator[Redis]:
    """Own one reusable Redis pool and close it with the process lifecycle."""
    client = create_redis_client(settings)
    try:
        yield client
    finally:
        await client.aclose()
