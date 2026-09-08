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
from app.importer.season_sync import PostgresSeasonalSyncRepository, SeasonalLeaguePolicy
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


def test_season_budget_defer_resumes_the_same_due_legacy_item_without_duplication() -> None:
    assert TEST_DB_URL is not None
    policy = SeasonalLeaguePolicy("q04-resume", 987654, 2)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        provider = setup.execute("SELECT id FROM source.providers WHERE code='api-football'").fetchone()
        if provider is None:
            setup.execute("INSERT INTO source.providers(code,name) VALUES('api-football','API-Football')")
    with PostgresSeasonalSyncRepository(TEST_DB_URL, lease_owner="q04-first") as first:
        first_run = first.start_run([policy])
        item = first.claim_next(first_run, {policy.league_external_id: policy})
        assert item is not None
        first.defer(item, checkpoint={"outcome": "budget_pending"}, error="APIFootballBudgetDenied", delay_seconds=0)
        first.finish_run(first_run, status="failed", checkpoint={"outcome": "budget_pending"})
    with PostgresSeasonalSyncRepository(TEST_DB_URL, lease_owner="q04-second") as second:
        resumed_run = second.start_run([policy])
        resumed_item = second.claim_next(resumed_run, {policy.league_external_id: policy})
        assert resumed_run == first_run
        assert resumed_item is not None and resumed_item.id == item.id
        assert second.pending_delay_seconds(resumed_run) is None
    with psycopg.connect(TEST_DB_URL, autocommit=True) as verify:
        assert verify.execute("SELECT count(*) FROM ops.sync_runs WHERE operation=%s", ("seasonal_active_bootstrap",)).fetchone() == (1,)
        assert verify.execute("SELECT count(*) FROM ops.sync_work_items WHERE run_id=%s", (first_run,)).fetchone() == (1,)


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


def test_waiter_resets_at_the_shared_window_boundary_after_a_real_state_lock() -> None:
    """A waiter observes the committed window transition after row-lock release."""
    assert TEST_DB_URL is not None
    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        _reset(setup, daily=10, minute=1, operations=10, history=0, manual=0, reserve=0)
        setup.execute(
            "INSERT INTO ops.api_football_budget_state(singleton,daily_window,minute_window,daily_used,minute_used,operations_used) "
            "VALUES(true,(clock_timestamp() AT TIME ZONE 'UTC')::date,date_trunc('minute',clock_timestamp() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC',1,1,1)"
        )
    locked = threading.Event()
    release = threading.Event()
    result: list[tuple[bool, str, datetime | None]] = []

    def hold_state_lock() -> None:
        assert TEST_DB_URL is not None
        with psycopg.connect(TEST_DB_URL) as connection:
            connection.execute("SELECT * FROM ops.api_football_budget_state WHERE singleton FOR UPDATE")
            locked.set()
            assert release.wait(timeout=70)
            # Simulate the shared minute transition while the next reserver is
            # blocked on this real PostgreSQL row lock.
            connection.execute("UPDATE ops.api_football_budget_state SET minute_window=minute_window - interval '1 minute'")
            connection.commit()

    def reserve_after_wait() -> None:
        assert TEST_DB_URL is not None
        with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
            result.append(_reserve(connection, "operations"))

    holder = threading.Thread(target=hold_state_lock)
    holder.start(); assert locked.wait(timeout=5)
    waiter = threading.Thread(target=reserve_after_wait)
    waiter.start()
    release.set(); holder.join(timeout=5); waiter.join(timeout=5)
    assert result and result[0][:2] == (True, "reserved")
    with psycopg.connect(TEST_DB_URL, autocommit=True) as verify:
        row = verify.execute("SELECT minute_window,minute_used FROM ops.api_football_budget_state").fetchone()
        assert row is not None and row[0] == datetime.now(UTC).replace(second=0, microsecond=0) and row[1] == 1


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


def test_provider_headers_only_reduce_shared_capacity_and_reject_invalid_inputs() -> None:
    assert TEST_DB_URL is not None
    with psycopg.connect(TEST_DB_URL, autocommit=True) as first:
        _reset(first, daily=4, minute=4, operations=4, history=0, manual=0, reserve=0)
        assert _reserve(first, "operations")[:2] == (True, "reserved")
        # A contradictory header cannot make capacity appear or trigger an
        # arbitrary reset; normal reservations retain their local accounting.
        first.execute("SELECT ops.observe_api_football_budget(%s,%s::jsonb)", (200, '{"X-RateLimit-Limit":"2","X-RateLimit-Remaining":"3"}'))
        assert _reserve(first, "operations")[:2] == (True, "reserved")
        with pytest.raises(psycopg.errors.InvalidParameterValue):
            first.execute("SELECT ops.observe_api_football_budget(NULL, '{}'::jsonb)")
        with pytest.raises(psycopg.errors.InvalidParameterValue):
            first.execute("SELECT ops.observe_api_football_budget(200, NULL)")
        first.execute("SELECT ops.observe_api_football_budget(%s,%s::jsonb)", (200, '{"X-RateLimit-Requests-Limit":"7500","X-RateLimit-Requests-Remaining":"0"}'))
    # A second real connection sees provider exhaustion; no normal header or
    # UTC reset may credit it back without an independently trusted reset.
    with psycopg.connect(TEST_DB_URL, autocommit=True) as second:
        second.execute("UPDATE ops.api_football_budget_state SET daily_window=(clock_timestamp() AT TIME ZONE 'UTC')::date - 1")
        assert _reserve(second, "operations")[:2] == (False, "provider_daily_exhausted")


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


def test_every_internal_retry_is_debited_to_its_shared_consumer_share() -> None:
    assert TEST_DB_URL is not None
    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        _reset(setup, daily=4, minute=10, operations=2, history=2, manual=0, reserve=0)
    calls: dict[str, int] = {"operations": 0, "history": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        consumer = "history" if request.url.path == "/fixtures/statistics" else "operations"
        calls[consumer] += 1
        return httpx.Response(503 if calls[consumer] == 1 else 200, json={"errors": {}, "response": []})

    async def no_wait(_: float) -> None:
        return None

    async def exercise() -> None:
        transport = httpx.MockTransport(handler)
        operations = APIFootballClient("test-secret", transport=transport, budget=PostgresAPIFootballBudget(TEST_DB_URL), budget_consumer="operations", max_5xx_retries=1)
        history = APIFootballClient("test-secret", transport=transport, budget=PostgresAPIFootballBudget(TEST_DB_URL), budget_consumer="history", max_5xx_retries=1)
        await operations.get("/fixtures")
        await history.get("/fixtures/statistics")
        await operations.aclose(); await history.aclose()

    import app.api_football.client as client_module
    original_sleep = client_module.asyncio.sleep
    client_module.asyncio.sleep = no_wait
    try:
        asyncio.run(exercise())
    finally:
        client_module.asyncio.sleep = original_sleep
    assert calls == {"operations": 2, "history": 2}
    with psycopg.connect(TEST_DB_URL, autocommit=True) as verify:
        assert verify.execute("SELECT daily_used,operations_used,history_used FROM ops.api_football_budget_state").fetchone() == (4, 2, 2)
        assert _reserve(verify, "operations")[:2] == (False, "daily_limit")
