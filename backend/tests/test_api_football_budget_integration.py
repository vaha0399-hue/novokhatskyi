from __future__ import annotations

import os
import threading
import asyncio
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta

import psycopg
import pytest
import httpx

from app.api_football import APIFootballClient
from app.api_football.budget import APIFootballBudgetDenied, PostgresAPIFootballBudget
from app.importer import season_sync
from app.importer.season_sync import (
    PostgresSeasonalSyncRepository,
    SeasonalLeaguePolicy,
    SeasonalRunLeaseLost,
    SeasonalSyncWorker,
)
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
        first_acquisition = first.start_run([policy])
        first_run = first_acquisition.run_id
        item = first.claim_next(first_run, first_acquisition.run_token, {policy.league_external_id: policy})
        assert item is not None
        first.defer(
            item,
            first_acquisition.run_token,
            checkpoint={"outcome": "budget_pending"},
            error="APIFootballBudgetDenied",
            delay_seconds=0,
        )
        first.finish_run(first_run, first_acquisition.run_token, status="failed", checkpoint={"outcome": "budget_pending"})
    with PostgresSeasonalSyncRepository(TEST_DB_URL, lease_owner="q04-second") as second:
        second_acquisition = second.start_run([policy])
        resumed_run = second_acquisition.run_id
        resumed_item = second.claim_next(resumed_run, second_acquisition.run_token, {policy.league_external_id: policy})
        assert resumed_run == first_run
        assert resumed_item is not None and resumed_item.id == item.id
        assert second.pending_delay_seconds(resumed_run) is None
    with psycopg.connect(TEST_DB_URL, autocommit=True) as verify:
        assert verify.execute("SELECT count(*) FROM ops.sync_runs WHERE operation=%s", ("seasonal_active_bootstrap",)).fetchone() == (1,)
        assert verify.execute("SELECT count(*) FROM ops.sync_work_items WHERE run_id=%s", (first_run,)).fetchone() == (1,)


def test_overlapping_season_run_resume_and_start_share_one_real_postgres_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The second connection must wait through the first resume transaction."""
    assert TEST_DB_URL is not None
    monkeypatch.setattr(season_sync, "OPERATION", f"seasonal_active_bootstrap_overlap_{uuid.uuid4().hex}")
    policy = SeasonalLeaguePolicy(f"q04-overlap-{uuid.uuid4().hex}", 987655, 2)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        provider = setup.execute("SELECT id FROM source.providers WHERE code='api-football'").fetchone()
        if provider is None:
            setup.execute("INSERT INTO source.providers(code,name) VALUES('api-football','API-Football')")
    with PostgresSeasonalSyncRepository(TEST_DB_URL, lease_owner="q04-overlap-seed") as seed:
        failed_acquisition = seed.start_run([policy])
        failed_run = failed_acquisition.run_id
        item = seed.claim_next(failed_run, failed_acquisition.run_token, {policy.league_external_id: policy})
        assert item is not None
        seed.defer(
            item,
            failed_acquisition.run_token,
            checkpoint={"outcome": "budget_pending"},
            error="APIFootballBudgetDenied",
            delay_seconds=0,
        )
        seed.finish_run(
            failed_run,
            failed_acquisition.run_token,
            status="failed",
            checkpoint={"outcome": "budget_pending"},
        )

    resumed = threading.Event()
    release = threading.Event()
    second_finished = threading.Event()
    results: list[int] = []
    errors: list[BaseException] = []

    class BlockingConnection:
        def __init__(self, connection: psycopg.Connection) -> None:
            self._connection = connection

        def execute(self, statement: str, *args: object, **kwargs: object):
            result = self._connection.execute(statement, *args, **kwargs)
            if "UPDATE ops.sync_runs SET status='running'" in statement:
                resumed.set()
                assert release.wait(timeout=5)
            return result

        def __getattr__(self, name: str):
            return getattr(self._connection, name)

    def resume() -> None:
        try:
            with PostgresSeasonalSyncRepository(TEST_DB_URL, lease_owner="q04-overlap-resume") as repository:
                repository._connection = BlockingConnection(repository._conn)  # type: ignore[assignment]
                results.append(repository.start_run([policy]).run_id)
        except BaseException as error:  # pragma: no cover - reported below
            errors.append(error)

    def start() -> None:
        try:
            with PostgresSeasonalSyncRepository(TEST_DB_URL, lease_owner="q04-overlap-start") as repository:
                results.append(repository.start_run([policy]).run_id)
                second_finished.set()
        except BaseException as error:  # pragma: no cover - reported below
            errors.append(error)

    first = threading.Thread(target=resume)
    first.start()
    assert resumed.wait(timeout=5)
    second = threading.Thread(target=start)
    second.start()
    assert not second_finished.wait(timeout=0.2)
    release.set()
    first.join(timeout=5)
    second.join(timeout=5)
    assert not errors
    assert results == [failed_run, failed_run]
    with psycopg.connect(TEST_DB_URL, autocommit=True) as verify:
        assert verify.execute(
            "SELECT count(*) FROM ops.sync_runs run JOIN ops.sync_work_items item ON item.run_id=run.id "
            "WHERE run.id=%s AND item.scope->>'policy'=%s",
            (failed_run, policy.code),
        ).fetchone() == (1,)


def test_run_once_does_not_finish_another_workers_active_season_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A owns a leased job; B/C neither finish it nor create duplicate work."""
    assert TEST_DB_URL is not None
    operation = f"seasonal_active_bootstrap_owner_{uuid.uuid4().hex}"
    monkeypatch.setattr(season_sync, "OPERATION", operation)
    policy = SeasonalLeaguePolicy(f"q04-owner-{uuid.uuid4().hex}", 987656, 2)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        provider = setup.execute("SELECT id FROM source.providers WHERE code='api-football'").fetchone()
        if provider is None:
            setup.execute("INSERT INTO source.providers(code,name) VALUES('api-football','API-Football')")

    started = threading.Event()
    release = threading.Event()
    reports: list[object] = []
    errors: list[BaseException] = []

    class HoldingProvider:
        async def get(self, endpoint: str, *, params: dict[str, str | int] | None = None):
            assert endpoint == "/leagues" and params == {"id": policy.league_external_id}
            started.set()
            assert release.wait(timeout=5)
            payload = {
                "parameters": {"id": str(policy.league_external_id)},
                "response": [{"league": {"id": policy.league_external_id, "type": "League"}, "seasons": []}],
            }
            raw = str(payload).encode()
            return season_sync.APIFootballResponse(payload, raw, 200, {})

        def response_contains_api_key(self, _body: bytes) -> bool:
            return False

    def run_a() -> None:
        try:
            with PostgresSeasonalSyncRepository(TEST_DB_URL, lease_owner="q04-owner-a") as repository:
                reports.append(asyncio.run(SeasonalSyncWorker(provider=HoldingProvider(), repository=repository, policies=[policy]).run_once()))
        except BaseException as error:  # pragma: no cover - reported below
            errors.append(error)

    a = threading.Thread(target=run_a)
    a.start()
    assert started.wait(timeout=5)
    with PostgresSeasonalSyncRepository(TEST_DB_URL, lease_owner="q04-owner-b") as repository_b:
        b = asyncio.run(SeasonalSyncWorker(provider=HoldingProvider(), repository=repository_b, policies=[policy]).run_once())
    with PostgresSeasonalSyncRepository(TEST_DB_URL, lease_owner="q04-owner-c") as repository_c:
        c = asyncio.run(SeasonalSyncWorker(provider=HoldingProvider(), repository=repository_c, policies=[policy]).run_once())
    assert b.status == c.status == "running"
    assert b.provider_request_count == c.provider_request_count == 0
    with psycopg.connect(TEST_DB_URL, autocommit=True) as verify:
        run_id, status = verify.execute(
            "SELECT id,status FROM ops.sync_runs WHERE operation=%s", (operation,)
        ).fetchone()
        assert status == "running"
        assert verify.execute(
            "SELECT count(*) FROM ops.sync_runs WHERE operation=%s", (operation,)
        ).fetchone() == (1,)
        assert verify.execute(
            "SELECT count(*) FROM ops.sync_work_items "
            "WHERE run_id=%s AND status='running' AND lease_owner='q04-owner-a'", (run_id,)
        ).fetchone() == (1,)
        assert verify.execute(
            "SELECT count(*) FROM ops.sync_work_items WHERE run_id=%s", (run_id,)
        ).fetchone() == (1,)

    release.set()
    a.join(timeout=5)
    assert not a.is_alive()
    assert not errors
    assert len(reports) == 1 and reports[0].status == "succeeded"  # type: ignore[union-attr]
    with psycopg.connect(TEST_DB_URL, autocommit=True) as verify:
        assert verify.execute(
            "SELECT status FROM ops.sync_runs WHERE operation=%s", (operation,)
        ).fetchone() == ("succeeded",)
        assert verify.execute(
            "SELECT item.status FROM ops.sync_work_items item JOIN ops.sync_runs run ON run.id=item.run_id "
            "WHERE run.operation=%s", (operation,)
        ).fetchone() == ("succeeded",)


def test_run_once_recovers_same_run_when_lease_lost_before_first_claim() -> None:
    assert TEST_DB_URL is not None
    operation = f"seasonal_active_bootstrap_preclaim_lost_{uuid.uuid4().hex}"
    season_sync.OPERATION = operation
    policy = SeasonalLeaguePolicy(f"q04-preclaim-{uuid.uuid4().hex}", 987657, 2)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        provider = setup.execute("SELECT id FROM source.providers WHERE code='api-football'").fetchone()
        if provider is None:
            setup.execute("INSERT INTO source.providers(code,name) VALUES('api-football','API-Football')")

    ready = threading.Event()
    release = threading.Event()
    a_reports: list[object] = []

    class NeverCalledProvider:
        async def get(self, endpoint: str, *, params: dict[str, str | int] | None = None):
            raise AssertionError(f"provider should not be used when claim is blocked: {endpoint}")

        def response_contains_api_key(self, _body: bytes) -> bool:
            return False

    class ClaimBlockingRepository(PostgresSeasonalSyncRepository):
        def claim_next(
            self, run_id: int, run_token: int, policies: Mapping[int, SeasonalLeaguePolicy]
        ) -> season_sync.SeasonalWorkItem | None:
            ready.set()
            assert release.wait(timeout=10)
            return super().claim_next(run_id, run_token, policies)

    def run_a() -> None:
        try:
            with ClaimBlockingRepository(TEST_DB_URL, lease_owner="q04-preclaim-a") as repository:
                a_reports.append(asyncio.run(SeasonalSyncWorker(provider=NeverCalledProvider(), repository=repository, policies=[policy]).run_once()))
        except BaseException:
            a_reports.append(None)

    thread_a = threading.Thread(target=run_a)
    thread_a.start()
    assert ready.wait(timeout=5)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as verify:
        run_row = verify.execute("SELECT id FROM ops.sync_runs WHERE operation=%s", (operation,)).fetchone()
        assert run_row is not None
        run_id = run_row[0]
        verify.execute("UPDATE ops.sync_runs SET lease_expires_at=clock_timestamp()-interval '1 minute' WHERE id=%s", (run_id,))
    b_result = []
    with PostgresSeasonalSyncRepository(TEST_DB_URL, lease_owner="q04-preclaim-b") as repository_b:
        class FastProvider:
            async def get(self, endpoint: str, *, params: dict[str, str | int] | None = None):
                payload = {
                    "parameters": {"id": str(policy.league_external_id)},
                    "response": [{"league": {"id": policy.league_external_id, "type": "League"}, "seasons": []}],
                }
                raw = str(payload).encode()
                return season_sync.APIFootballResponse(payload, raw, 200, {})

            def response_contains_api_key(self, _body: bytes) -> bool:
                return False

        b_result.append(asyncio.run(SeasonalSyncWorker(provider=FastProvider(), repository=repository_b, policies=[policy]).run_once()))
    release.set()
    thread_a.join(timeout=5)
    assert len(a_reports) == 1
    assert a_reports[0].status == "running"  # type: ignore[union-attr]
    assert b_result[0].status == "succeeded"  # type: ignore[index]
    assert b_result[0].run_id == run_id
    with psycopg.connect(TEST_DB_URL, autocommit=True) as verify:
        assert verify.execute("SELECT count(*) FROM ops.sync_runs WHERE operation=%s", (operation,)).fetchone() == (1,)


def test_run_once_recovers_after_claim_when_lease_and_item_expire() -> None:
    assert TEST_DB_URL is not None
    operation = f"seasonal_active_bootstrap_claim_lost_{uuid.uuid4().hex}"
    season_sync.OPERATION = operation
    policy = SeasonalLeaguePolicy(f"q04-claimlost-{uuid.uuid4().hex}", 987658, 2)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        provider = setup.execute("SELECT id FROM source.providers WHERE code='api-football'").fetchone()
        if provider is None:
            setup.execute("INSERT INTO source.providers(code,name) VALUES('api-football','API-Football')")

    claim_started = threading.Event()
    continue_provider = threading.Event()
    a_reports: list[object] = []

    class SlowProvider:
        async def get(self, endpoint: str, *, params: dict[str, str | int] | None = None):
            claim_started.set()
            assert continue_provider.wait(timeout=10)
            payload = {
                "parameters": {"id": str(policy.league_external_id)},
                "response": [{"league": {"id": policy.league_external_id, "type": "League"}, "seasons": []}],
            }
            raw = str(payload).encode()
            return season_sync.APIFootballResponse(payload, raw, 200, {})

        def response_contains_api_key(self, _body: bytes) -> bool:
            return False

    def run_a() -> None:
        with PostgresSeasonalSyncRepository(TEST_DB_URL, lease_owner="q04-claimlost-a") as repository:
            a_reports.append(asyncio.run(SeasonalSyncWorker(provider=SlowProvider(), repository=repository, policies=[policy]).run_once()))

    thread_a = threading.Thread(target=run_a)
    thread_a.start()
    assert claim_started.wait(timeout=5)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as verify:
        run_row = verify.execute("SELECT id FROM ops.sync_runs WHERE operation=%s", (operation,)).fetchone()
        assert run_row is not None
        run_id = run_row[0]
        verify.execute(
            "UPDATE ops.sync_runs SET lease_expires_at=clock_timestamp()-interval '1 minute' WHERE id=%s",
            (run_id,),
        )
        verify.execute(
            "UPDATE ops.sync_work_items SET lease_expires_at=clock_timestamp()-interval '1 minute' "
            "WHERE run_id=%s AND status='running'",
            (run_id,),
        )
    b_reports: list[object] = []
    with PostgresSeasonalSyncRepository(TEST_DB_URL, lease_owner="q04-claimlost-b") as repository_b:
        class FastProvider:
            async def get(self, endpoint: str, *, params: dict[str, str | int] | None = None):
                payload = {
                    "parameters": {"id": str(policy.league_external_id)},
                    "response": [{"league": {"id": policy.league_external_id, "type": "League"}, "seasons": []}],
                }
                raw = str(payload).encode()
                return season_sync.APIFootballResponse(payload, raw, 200, {})

            def response_contains_api_key(self, _body: bytes) -> bool:
                return False

        continue_provider.set()
        b_reports.append(asyncio.run(SeasonalSyncWorker(provider=FastProvider(), repository=repository_b, policies=[policy]).run_once()))
    thread_a.join(timeout=10)
    assert len(a_reports) == 1 and len(b_reports) == 1
    assert b_reports[0].status == "succeeded"
    assert a_reports[0].status == "running"
    assert b_reports[0].run_id == a_reports[0].run_id


def test_finish_run_is_rejected_after_lease_takeover() -> None:
    assert TEST_DB_URL is not None
    operation = f"seasonal_active_bootstrap_finish_reject_{uuid.uuid4().hex}"
    season_sync.OPERATION = operation
    policy = SeasonalLeaguePolicy(f"q04-finishreject-{uuid.uuid4().hex}", 987659, 2)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        provider = setup.execute("SELECT id FROM source.providers WHERE code='api-football'").fetchone()
        if provider is None:
            setup.execute("INSERT INTO source.providers(code,name) VALUES('api-football','API-Football')")

    with PostgresSeasonalSyncRepository(TEST_DB_URL, lease_owner="q04-finish-a") as first:
        acquisition_a = first.start_run([policy])
        run_id = acquisition_a.run_id
        item = first.claim_next(run_id, acquisition_a.run_token, {policy.league_external_id: policy})
        assert item is not None
        with psycopg.connect(TEST_DB_URL, autocommit=True) as verify:
            verify.execute("UPDATE ops.sync_runs SET lease_expires_at=clock_timestamp()-interval '1 minute' WHERE id=%s", (run_id,))
            verify.execute(
                "UPDATE ops.sync_work_items SET lease_expires_at=clock_timestamp()-interval '1 minute' WHERE id=%s",
                (item.id,),
            )
    with PostgresSeasonalSyncRepository(TEST_DB_URL, lease_owner="q04-finish-b") as second:
        acquisition_b = second.start_run([policy])
        assert acquisition_b.run_id == run_id
        assert acquisition_b.run_token != acquisition_a.run_token
    with pytest.raises(season_sync.SeasonalSyncError, match="lease was lost"):
        with PostgresSeasonalSyncRepository(TEST_DB_URL, lease_owner="q04-finish-a") as first_after:
            first_after.finish_run(run_id, acquisition_a.run_token, status="failed", checkpoint={"outcome": "forced"})


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


def test_positive_provider_remaining_is_a_shared_decreasing_cap_and_stale_headers_do_not_credit() -> None:
    assert TEST_DB_URL is not None
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        _reset(connection, daily=20, minute=20, operations=20, history=0, manual=0, reserve=0)
        assert _reserve(connection, "operations")[:2] == (True, "reserved")
        connection.execute(
            "SELECT ops.observe_api_football_budget(%s,%s::jsonb)",
            (200, '{"x-ratelimit-requests-limit":"100","x-ratelimit-requests-remaining":"2","x-ratelimit-limit":"100","x-ratelimit-remaining":"2"}'),
        )
        assert connection.execute(
            "SELECT provider_daily_remaining,provider_minute_remaining FROM ops.api_football_budget_state"
        ).fetchone() == (2, 2)
        assert _reserve(connection, "operations")[:2] == (True, "reserved")
        # A stale response reports an older, larger remaining value.  It can
        # never replenish an already decremented provider cap.
        connection.execute(
            "SELECT ops.observe_api_football_budget(%s,%s::jsonb)",
            (200, '{"x-ratelimit-requests-limit":"100","x-ratelimit-requests-remaining":"99","x-ratelimit-limit":"100","x-ratelimit-remaining":"99"}'),
        )
        assert connection.execute(
            "SELECT provider_daily_remaining,provider_minute_remaining FROM ops.api_football_budget_state"
        ).fetchone() == (1, 1)
        assert _reserve(connection, "operations")[:2] == (True, "reserved")
        assert _reserve(connection, "operations")[:2] == (False, "provider_daily_exhausted")


def test_observe_binds_a_fresh_minute_cap_before_the_next_reservation() -> None:
    assert TEST_DB_URL is not None
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        _reset(connection, daily=10, minute=10, operations=10, history=0, manual=0, reserve=0)
        assert _reserve(connection, "operations")[:2] == (True, "reserved")
        # Model a request reserved before the boundary whose response returns
        # after it, reporting one remaining provider request in the new minute.
        connection.execute(
            "UPDATE ops.api_football_budget_state SET minute_window=minute_window - interval '1 minute'"
        )
        connection.execute(
            "SELECT ops.observe_api_football_budget(200,%s::jsonb)",
            ('{"x-ratelimit-limit":"100","x-ratelimit-remaining":"1"}',),
        )
        assert connection.execute(
            "SELECT minute_used,provider_minute_remaining FROM ops.api_football_budget_state"
        ).fetchone() == (0, 1)
        assert _reserve(connection, "operations")[:2] == (True, "reserved")
        assert _reserve(connection, "operations")[:2] == (False, "provider_minute_limit")


@pytest.mark.parametrize(
    "headers",
    [
        "{}",
        '{"x-ratelimit-limit":"2","x-ratelimit-remaining":"3"}',
        '{"x-ratelimit-limit":"not-a-number","x-ratelimit-remaining":"also-bad","retry-after":"invalid"}',
    ],
)
def test_429_always_sets_a_shared_base_cooldown_when_headers_are_unusable(headers: str) -> None:
    assert TEST_DB_URL is not None
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        _reset(connection, daily=10, minute=10, operations=10, history=0, manual=0, reserve=0)
        assert _reserve(connection, "operations")[:2] == (True, "reserved")
        connection.execute("SELECT ops.observe_api_football_budget(429,%s::jsonb)", (headers,))
        allowed, reason, retry_at = _reserve(connection, "operations")
        assert (allowed, reason) == (False, "cooldown")
        assert retry_at is not None and retry_at >= datetime.now(UTC).replace(second=0, microsecond=0) + timedelta(minutes=1)


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
