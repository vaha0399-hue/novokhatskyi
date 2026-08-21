"""HTTP client for the official API-Football v3 endpoint."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from .errors import APIFootballAPIError, APIFootballConfigurationError, APIFootballHTTPError

DEFAULT_BASE_URL = "https://v3.football.api-sports.io"
DEFAULT_TIMEOUT_SECONDS = 15.0


@dataclass(frozen=True)
class APIFootballResponse:
    """A successful API-Football response, retaining its unmodified body."""

    data: dict[str, Any]
    raw_body: bytes
    status_code: int
    headers: Mapping[str, str]


class APIFootballClient:
    """Minimal async client with no retries and no request logging."""

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float | httpx.Timeout = DEFAULT_TIMEOUT_SECONDS,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if not api_key:
            raise APIFootballConfigurationError("API_FOOTBALL_KEY is required.")

        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._transport = transport

    @classmethod
    def from_environment(cls, **kwargs: Any) -> "APIFootballClient":
        """Create a client from the backend-only API_FOOTBALL_KEY variable."""
        api_key = os.environ.get("API_FOOTBALL_KEY")
        if not api_key:
            raise APIFootballConfigurationError("API_FOOTBALL_KEY is required.")
        return cls(api_key, **kwargs)

    async def get(
        self, endpoint: str, *, params: Mapping[str, str | int] | None = None
    ) -> APIFootballResponse:
        """Make one GET request. This method deliberately does not retry requests."""
        normalized_endpoint = endpoint if endpoint.startswith("/") else f"/{endpoint}"
        async with httpx.AsyncClient(
            base_url=self._base_url,
            headers={"x-apisports-key": self._api_key},
            timeout=self._timeout,
            transport=self._transport,
        ) as client:
            try:
                response = await client.get(normalized_endpoint, params=params)
            except httpx.HTTPError as error:
                # Do not include the underlying message: it can contain the request URL.
                raise APIFootballHTTPError(0) from error

        if response.is_error:
            raise APIFootballHTTPError(response.status_code)

        try:
            payload = response.json()
        except ValueError as error:
            raise APIFootballAPIError("invalid JSON response") from error

        if not isinstance(payload, dict):
            raise APIFootballAPIError("invalid top-level response")
        if payload.get("errors"):
            raise APIFootballAPIError(payload["errors"])

        return APIFootballResponse(
            data=payload,
            raw_body=response.content,
            status_code=response.status_code,
            headers=dict(response.headers),
        )

    def response_contains_api_key(self, body: bytes) -> bool:
        """Check a response before persistence without exposing the configured key."""
        return self._api_key.encode() in body
