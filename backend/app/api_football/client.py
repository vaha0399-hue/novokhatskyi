"""HTTP client for the official API-Football v3 endpoint."""

from __future__ import annotations

import asyncio
import os
import random
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from .errors import APIFootballAPIError, APIFootballConfigurationError, APIFootballHTTPError
from .budget import PostgresAPIFootballBudget, RequestBudget, UnavailableAPIFootballBudget

DEFAULT_BASE_URL = "https://v3.football.api-sports.io"
DEFAULT_TIMEOUT_SECONDS = 15.0
DEFAULT_MAX_5XX_RETRIES = 2
SAFE_RATE_LIMIT_HEADERS = frozenset(
    {
        "x-ratelimit-limit",
        "x-ratelimit-remaining",
        "x-ratelimit-requests-limit",
        "x-ratelimit-requests-remaining",
        "retry-after",
    }
)


def safe_rate_limit_headers(headers: Mapping[str, str]) -> dict[str, str]:
    """Return only non-sensitive rate-limit metadata from provider headers."""
    return {
        key.lower(): value
        for key, value in headers.items()
        if key.lower() in SAFE_RATE_LIMIT_HEADERS
    }


@dataclass(frozen=True)
class APIFootballResponse:
    """A successful API-Football response, retaining its unmodified body."""

    data: dict[str, Any]
    raw_body: bytes
    status_code: int
    headers: Mapping[str, str]


class APIFootballClient:
    """Reusable async client with one connection pool and no request logging."""

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float | httpx.Timeout = DEFAULT_TIMEOUT_SECONDS,
        transport: httpx.AsyncBaseTransport | None = None,
        budget: RequestBudget | None = None,
        budget_consumer: str = "legacy_manual",
        max_5xx_retries: int = DEFAULT_MAX_5XX_RETRIES,
    ) -> None:
        if not api_key:
            raise APIFootballConfigurationError("API_FOOTBALL_KEY is required.")
        if budget_consumer not in {"operations", "history", "legacy_manual"}:
            raise ValueError("budget_consumer must be operations, history, or legacy_manual")
        if max_5xx_retries < 0:
            raise ValueError("max_5xx_retries must be non-negative")

        self._api_key = api_key
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            headers={"x-apisports-key": api_key},
            timeout=timeout,
            transport=transport,
        )
        self._budget = budget or UnavailableAPIFootballBudget()
        self._budget_consumer = budget_consumer
        self._max_5xx_retries = max_5xx_retries

    @classmethod
    def from_environment(cls, **kwargs: Any) -> "APIFootballClient":
        """Create a client from the backend-only API_FOOTBALL_KEY variable."""
        api_key = os.environ.get("API_FOOTBALL_KEY")
        if not api_key:
            raise APIFootballConfigurationError("API_FOOTBALL_KEY is required.")
        return cls(api_key, budget=PostgresAPIFootballBudget.from_environment(), **kwargs)

    async def get(
        self, endpoint: str, *, params: Mapping[str, str | int] | None = None
    ) -> APIFootballResponse:
        """Make metered physical GETs; every internal retry reserves again."""
        normalized_endpoint = endpoint if endpoint.startswith("/") else f"/{endpoint}"
        for attempt in range(self._max_5xx_retries + 1):
            # This is intentionally immediately before the transport call.  It
            # commits before a timeout/unknown outcome and is never released.
            await self._budget.reserve(self._budget_consumer)
            try:
                response = await self._client.get(normalized_endpoint, params=params)
            except httpx.HTTPError as error:
                # An unknown outcome may have reached the provider; preserve
                # the reservation and retry only after a bounded jitter delay.
                if attempt < self._max_5xx_retries:
                    await asyncio.sleep((2**attempt) + random.uniform(0, 1))
                    continue
                raise APIFootballHTTPError(0) from error

            safe_headers = safe_rate_limit_headers(response.headers)
            # A 429 consumes its pre-reserved slot then creates one shared
            # cooldown before this client exposes the failure to its caller.
            await self._budget.observe(response.status_code, safe_headers)
            if response.status_code >= 500 and attempt < self._max_5xx_retries:
                await asyncio.sleep((2**attempt) + random.uniform(0, 1))
                continue
            break

        if response.is_error:
            raise APIFootballHTTPError(
                response.status_code,
                safe_headers=safe_headers,
            )

        try:
            payload = response.json()
        except ValueError as error:
            raise APIFootballAPIError(
                "invalid JSON response",
                raw_body=response.content,
                status_code=response.status_code,
                safe_headers=safe_headers,
            ) from error

        if not isinstance(payload, dict):
            raise APIFootballAPIError(
                "invalid top-level response",
                raw_body=response.content,
                status_code=response.status_code,
                safe_headers=safe_headers,
            )
        if payload.get("errors"):
            raise APIFootballAPIError(
                payload["errors"],
                raw_body=response.content,
                status_code=response.status_code,
                safe_headers=safe_rate_limit_headers(response.headers),
            )

        return APIFootballResponse(
            data=payload,
            raw_body=response.content,
            status_code=response.status_code,
            headers=dict(response.headers),
        )

    async def aclose(self) -> None:
        """Close the reusable connection pool owned by this client."""
        await self._client.aclose()

    async def __aenter__(self) -> "APIFootballClient":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    def response_contains_api_key(self, body: bytes) -> bool:
        """Check a response before persistence without exposing the configured key."""
        return self._api_key.encode() in body
