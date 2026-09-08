from __future__ import annotations

import os
import threading
import asyncio
from datetime import UTC, datetime, timedelta

import psycopg
import pytest
import httpx

from app.api_football import APIFootballClient
from app.api_football.budget import APIFootballBudgetDenied, PostgresAPIFootballBudget
TEST_DB_URL = os.environ.get("API_FOOTBALL_BUDGET_TEST_DB_URL")
pytestmark = pytest.mark.skipif(not TEST_DB_URL, reason="API_FOOTBALL_BUDGET_TEST_DB_URL is not configured")


def _reset(connection: psycopg.Connection, *, daily: int = 4, minute: int = 2, operations: int = 2, history: int = 1, manual: int = 1, reserve: int = 0) -> None:
    connection.execute(
        "UPDATE ops.api_football_budget_config SET daily_limit=%s, minute_limit=%s, operations_limit=%s, history_limit=%s, legacy_manual_limit=%s, protected_reserve=%s WHERE singleton",
        (daily, minute, operations, history, manual, reserve),
    )
    connection.execute("DELETE FROM ops.api_football_budget_state")


def _reserve(connection: psycopg.Connection, consumer: str) -> tuple[bool, str, datetime | None]:
    row = connection.execute("SELECT allowed, reason, retry_at FROM ops.reserve_api_football_request(%s)", (consumer,)).fetchone()
    assert row is not None
    return bool(row[0]), str(row[1]), row[2]


def test_budget_is_atomic_across_real_connections_and_enforces_each_share() -> None:
    assert TEST_DB_URL is not None
    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        _reset(setup)
    barrier = threading.Barrier(3)
    results: list[tuple[bool, str, datetime | None]] = []

    def reserve() -> None:
        assert TEST_DB_URL is not None
        with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
            barrier.wait()
            results.append(_reserve(connection, "operations"))

    left, right, third = (threading.Thread(target=reserve) for _ in range(3))
    left.start(); right.start(); third.start()
    left.join(); right.join(); third.join()
    assert sorted(result[0] for result in results) == [False, True, True]
    assert [result[1] for result in results if not result[0]] == ["minute_limit"]
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        # New connections prove state survives process/connection restart.
        assert _reserve(connection, "history")[1] == "minute_limit"
        connection.execute("UPDATE ops.api_football_budget_state SET minute_window=date_trunc('minute', clock_timestamp()) - interval '1 minute'")
        assert _reserve(connection, "history")[:2] == (True, "reserved")
        assert _reserve(connection, "operations")[:2] == (False, "operations_limit")
        assert _reserve(connection, "legacy_manual")[:2] == (True, "reserved")
        assert _reserve(connection, "legacy_manual")[:2] == (False, "daily_limit")


def test_budget_reset_and_cooldown_are_shared_and_headers_never_credit_capacity() -> None:
    assert TEST_DB_URL is not None
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        _reset(connection, daily=2, minute=10, operations=2, history=0, manual=0, reserve=0)
        assert _reserve(connection, "operations")[:2] == (True, "reserved")
        # A stale or contradictory success header cannot make an extra slot.
        connection.execute("SELECT ops.observe_api_football_budget(%s,%s::jsonb)", (200, '{"x-ratelimit-requests-remaining":"999999"}'))
        assert _reserve(connection, "operations")[:2] == (True, "reserved")
        assert _reserve(connection, "operations")[:2] == (False, "daily_limit")
        # Reset is server-clock based and survives a new connection.
        connection.execute("UPDATE ops.api_football_budget_state SET daily_window=(clock_timestamp() AT TIME ZONE 'UTC')::date - 1, minute_window=date_trunc('minute', clock_timestamp()) - interval '1 minute'")
    with psycopg.connect(TEST_DB_URL, autocommit=True) as restarted:
        assert _reserve(restarted, "operations")[:2] == (True, "reserved")
        restarted.execute("SELECT ops.observe_api_football_budget(%s,%s::jsonb)", (429, '{"retry-after":"120"}'))
    with psycopg.connect(TEST_DB_URL, autocommit=True) as second_process:
        allowed, reason, retry_at = _reserve(second_process, "operations")
        assert (allowed, reason) == (False, "cooldown")
        assert retry_at is not None and retry_at >= datetime.now(UTC) + timedelta(seconds=100)


def test_live_and_sync_clients_share_budget_before_http_without_a_held_budget_lock() -> None:
    assert TEST_DB_URL is not None
    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        _reset(setup, daily=1, minute=10, operations=1, history=0, manual=0, reserve=0)
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        assert TEST_DB_URL is not None
        # The reservation's short autocommit transaction ended before HTTP.
        with psycopg.connect(TEST_DB_URL, autocommit=True) as observer:
            held = observer.execute(
                "SELECT count(*) FROM pg_locks l JOIN pg_class c ON c.oid=l.relation "
                "WHERE c.relnamespace='ops'::regnamespace AND c.relname='api_football_budget_state' "
                "AND l.mode='RowExclusiveLock'"
            ).fetchone()
            assert held == (0,)
        return httpx.Response(200, json={"errors": {}, "response": []})

    async def exercise() -> None:
        transport = httpx.MockTransport(handler)
        live = APIFootballClient("test-secret", transport=transport, budget=PostgresAPIFootballBudget(TEST_DB_URL), budget_consumer="operations", max_5xx_retries=0)
        sync = APIFootballClient("test-secret", transport=transport, budget=PostgresAPIFootballBudget(TEST_DB_URL), budget_consumer="operations", max_5xx_retries=0)
        await live.get("/fixtures", params={"live": "all"})
        with pytest.raises(APIFootballBudgetDenied):
            await sync.get("/fixtures", params={"league": 39})
        await live.aclose()
        await sync.aclose()

    asyncio.run(exercise())
    assert calls == 1
