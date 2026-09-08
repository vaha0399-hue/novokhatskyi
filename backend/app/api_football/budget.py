"""Fail-closed, shared API-Football request budget adapters.

The PostgreSQL functions are the source of truth.  A reservation is committed
before the transport is called and is deliberately never released: after a
timeout the provider may already have received the request.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from psycopg import AsyncConnection
from psycopg.types.json import Jsonb


class APIFootballBudgetError(RuntimeError):
    """The shared budget could not safely authorize a physical request."""


class APIFootballBudgetDenied(APIFootballBudgetError):
    """A physical request is postponed by the shared budget."""

    def __init__(self, reason: str, retry_at: datetime | None = None) -> None:
        self.reason = reason
        self.retry_at = retry_at
        super().__init__(f"API-Football budget denied request: {reason}")


class RequestBudget(Protocol):
    async def reserve(self, consumer: str) -> None: ...

    async def observe(self, status_code: int, headers: Mapping[str, str]) -> None: ...


@dataclass(frozen=True)
class PostgresAPIFootballBudget:
    """One short, autocommit database call per budget transition.

    Keeping this connection separate from import/lease connections means HTTP
    is never performed while the row lock or a caller transaction is open.
    """

    database_url: str

    @classmethod
    def from_environment(cls) -> "PostgresAPIFootballBudget":
        database_url = os.environ.get("SUPABASE_DB_URL", "").strip()
        if not database_url:
            raise APIFootballBudgetError("SUPABASE_DB_URL is required for the shared API-Football budget")
        return cls(database_url)

    async def reserve(self, consumer: str) -> None:
        try:
            async with await AsyncConnection.connect(self.database_url, autocommit=True) as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute("SELECT allowed, reason, retry_at FROM ops.reserve_api_football_request(%s)", (consumer,))
                    row = await cursor.fetchone()
        except Exception as error:
            raise APIFootballBudgetError("shared API-Football budget is unavailable") from error
        if row is None:
            raise APIFootballBudgetError("shared API-Football budget returned no reservation")
        allowed, reason, retry_at = row
        if allowed is not True:
            raise APIFootballBudgetDenied(str(reason), retry_at)

    async def observe(self, status_code: int, headers: Mapping[str, str]) -> None:
        # Headers are evidence only.  The server-side function uses them only
        # to extend a 429 cooldown; it never credits capacity from a header.
        try:
            async with await AsyncConnection.connect(self.database_url, autocommit=True) as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(
                        "SELECT ops.observe_api_football_budget(%s,%s)",
                        (status_code, Jsonb(dict(headers))),
                    )
        except Exception as error:
            raise APIFootballBudgetError("shared API-Football budget observation failed") from error


class UnavailableAPIFootballBudget:
    """Default for a directly constructed client: fail closed, never unmetered."""

    async def reserve(self, consumer: str) -> None:
        raise APIFootballBudgetError("a shared API-Football budget adapter is required")

    async def observe(self, status_code: int, headers: Mapping[str, str]) -> None:
        raise APIFootballBudgetError("a shared API-Football budget adapter is required")
