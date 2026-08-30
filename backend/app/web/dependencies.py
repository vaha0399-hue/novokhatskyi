"""FastAPI dependencies for a backend-only, read-only database path."""

from __future__ import annotations

import os
from collections.abc import Generator
from typing import Any

import psycopg
from psycopg import Connection
from fastapi import Depends, Request

from app.analytics import AnalyticsEngine, PostgresAnalyticsRepository
from app.live import RedisLiveStore

from .repository import WebReadRepository
from .service import WebReadService


def get_read_connection() -> Generator[Connection[Any], None, None]:
    """Yield one PostgreSQL transaction explicitly prohibited from writing."""
    database_url = os.environ.get("SUPABASE_DB_URL")
    if not database_url:
        raise RuntimeError("SUPABASE_DB_URL is required")
    with psycopg.connect(database_url) as connection:
        connection.execute("SET TRANSACTION READ ONLY")
        try:
            yield connection
        finally:
            connection.rollback()


def get_web_read_service(
    connection: Connection[Any] = Depends(get_read_connection),
) -> WebReadService:
    """Compose repositories over the same request transaction; no N+1 DB setup."""
    return WebReadService(
        WebReadRepository(connection),
        AnalyticsEngine(PostgresAnalyticsRepository(connection)),
    )


def get_live_store(request: Request) -> RedisLiveStore:
    """Return the process-owned Redis live-state reader."""
    store = getattr(request.app.state, "live_store", None)
    if not isinstance(store, RedisLiveStore):
        raise RuntimeError("live Redis store is not initialized")
    return store
