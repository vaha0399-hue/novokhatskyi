from __future__ import annotations

import os
import threading
import time
import uuid
from datetime import UTC, datetime, timedelta

import psycopg
import pytest
from psycopg.types.json import Jsonb

from app.importer.cup_bootstrap import CupCompetition
from app.importer.cup_queue import OPERATION as CUP_OPERATION, POLICY_VERSION as CUP_POLICY_VERSION
from app.importer.cup_queue_repository import PostgresCupQueueRepository
from app.importer.season_sync import PostgresSeasonalSyncRepository, SeasonalLeaguePolicy
from app.sync.policies import PostgresCompetitionSyncPolicyReader, SyncPolicyDenied, SyncPolicyGate
from app.sync.repository import PeriodicWork, PostgresSyncRepository, RecalculationWork
from app.sync.worker import RepeatableSyncWorker, WorkResult
from app.importer.cup_canonical import CupCanonicalSink

TEST_DB_URL = os.environ.get("REPEATABLE_SYNC_QUEUE_TEST_DB_URL")
pytestmark = pytest.mark.skipif(not TEST_DB_URL, reason="REPEATABLE_SYNC_QUEUE_TEST_DB_URL is not configured")


def _run(connection: psycopg.Connection, provider_id: int, operation: str) -> int:
    row = connection.execute("INSERT INTO ops.sync_runs(provider_id,operation) VALUES(%s,%s) RETURNING id", (provider_id, operation)).fetchone()
    assert row is not None
    return int(row[0])


def _enqueue(connection: psycopg.Connection, run_id: int, stable_key: str, *, priority: int = 0, available_at: datetime | None = None, execution_key: str | None = None) -> tuple[int, bool]:
    row = connection.execute("SELECT * FROM ops.enqueue_repeatable_sync_work_item(%s,%s,%s,%s,%s,%s,%s,%s,%s)", (run_id, stable_key, Jsonb({}), "q02-test", priority, available_at or datetime.now(UTC), stable_key, "entity:team:1", execution_key or "entity:provider:season:team:1")).fetchone()
    assert row is not None
    return int(row[0]), bool(row[1])


def test_repeatable_queue_uses_real_concurrent_connections_and_preserves_versions() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        provider = setup.execute("INSERT INTO source.providers(code,name) VALUES(%s,%s) RETURNING id", (f"q02-{suffix}", "Q02 test")).fetchone()
        assert provider is not None
        run_a, run_b = _run(setup, int(provider[0]), f"q02-a-{suffix}"), _run(setup, int(provider[0]), f"q02-b-{suffix}")
    stable = f"recalculation:metrics:provider:season:team:1:{suffix}:v1"
    barrier, results = threading.Barrier(2), []

    def enqueue(run_id: int) -> None:
        assert TEST_DB_URL is not None
        with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
            barrier.wait()
            results.append(_enqueue(connection, run_id, stable, priority=100000, execution_key=f"concurrent:{suffix}"))

    left, right = threading.Thread(target=enqueue, args=(run_a,)), threading.Thread(target=enqueue, args=(run_b,))
    left.start(); right.start(); left.join(); right.join()
    assert sorted(created for _id, created in results) == [False, True]
    assert len({item_id for item_id, _created in results}) == 1
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        assert connection.execute("SELECT count(*) FROM ops.sync_work_items WHERE stable_key=%s", (stable,)).fetchone()[0] == 1
        item_id = results[0][0]
        assert connection.execute("SELECT * FROM ops.claim_next_repeatable_sync_work_item_with_lease(%s,%s)", (f"q02-{suffix}", "1 minute")).fetchone()[0] == item_id
        token = connection.execute("SELECT lease_token FROM ops.sync_work_items WHERE id=%s", (item_id,)).fetchone()[0]
        assert connection.execute("SELECT ops.complete_repeatable_sync_work_item(%s,%s,%s,%s)", (item_id, f"q02-{suffix}", token, Jsonb({}))).fetchone()[0] is True
        owning_run = connection.execute("SELECT run_id FROM ops.sync_work_items WHERE id=%s", (item_id,)).fetchone()[0]
        with pytest.raises(psycopg.errors.RestrictViolation):
            connection.execute("DELETE FROM ops.sync_runs WHERE id=%s", (owning_run,))
        assert _enqueue(connection, run_b, stable) == (item_id, False)
        newer_id, created = _enqueue(connection, run_b, stable + ":v2")
        assert created and newer_id != item_id


def test_canonical_component_keys_persist_independently_and_each_deduplicates() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider = connection.execute("INSERT INTO source.providers(code,name) VALUES(%s,%s) RETURNING id", (f"q02-canonical-{suffix}", "Q02 canonical key test")).fetchone()
        assert provider is not None
        first_run = _run(connection, int(provider[0]), f"q02-canonical-first-{suffix}")
        second_run = _run(connection, int(provider[0]), f"q02-canonical-second-{suffix}")
        first = RecalculationWork(int(provider[0]), 1, "metrics", "team:9", "accepted:1", 0, {})
        second = RecalculationWork(int(provider[0]), 1, "metrics", "team:9:accepted", "1", 0, {})
        assert first.stable_key() != second.stable_key()
        first_id, first_created = _enqueue(connection, first_run, first.stable_key(), execution_key=f"canonical-first:{suffix}")
        second_id, second_created = _enqueue(connection, second_run, second.stable_key(), execution_key=f"canonical-second:{suffix}")
        assert first_created and second_created and first_id != second_id
        assert _enqueue(connection, second_run, first.stable_key(), execution_key=f"canonical-first:{suffix}") == (first_id, False)
        assert _enqueue(connection, first_run, second.stable_key(), execution_key=f"canonical-second:{suffix}") == (second_id, False)


def test_repeatable_claim_reserves_legacy_rows_for_old_workers() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider = connection.execute("INSERT INTO source.providers(code,name) VALUES(%s,%s) RETURNING id", (f"q02-legacy-claim-{suffix}", "Q02 legacy claim test")).fetchone()
        assert provider is not None
        legacy_run = _run(connection, int(provider[0]), f"q02-legacy-old-{suffix}")
        repeatable_run = _run(connection, int(provider[0]), f"q02-legacy-new-{suffix}")
        legacy_id = int(connection.execute(
            "INSERT INTO ops.sync_work_items(run_id,scope_key,scope,priority) VALUES(%s,%s,%s,%s) RETURNING id",
            (legacy_run, f"legacy-{suffix}", Jsonb({}), 1_000_000),
        ).fetchone()[0])
        repeatable_id, _ = _enqueue(connection, repeatable_run, f"repeatable-{suffix}", priority=2_000_000, execution_key=f"repeatable:{suffix}")
        claimed = connection.execute("SELECT id FROM ops.claim_next_repeatable_sync_work_item_with_lease(%s,%s)", (f"new-{suffix}", "1 minute")).fetchone()
        assert claimed is not None and int(claimed[0]) == repeatable_id
        old_claimed = connection.execute("SELECT id FROM ops.claim_next_sync_work_item(%s,%s,%s)", (legacy_run, f"old-{suffix}", "1 minute")).fetchone()
        assert old_claimed is not None and int(old_claimed[0]) == legacy_id
        with pytest.raises(psycopg.errors.InvalidParameterValue, match="reserved"):
            _enqueue(connection, repeatable_run, "legacy:forbidden")
        with pytest.raises(psycopg.errors.InvalidParameterValue, match="reserved"):
            _enqueue(connection, repeatable_run, "legacy")
        connection.execute("UPDATE ops.sync_work_items SET stable_key=%s WHERE id=%s", (f"legacy-rewritten-{suffix}", legacy_id))
        deleted = connection.execute(
            "DELETE FROM ops.sync_work_items WHERE id=%s RETURNING id", (legacy_id,)
        ).fetchone()
        assert deleted is not None and int(deleted[0]) == legacy_id
        assert connection.execute(
            "SELECT EXISTS(SELECT 1 FROM ops.sync_work_items WHERE id=%s)", (legacy_id,)
        ).fetchone()[0] is False


def test_repeatable_identity_cannot_be_changed_or_deleted_after_completion() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider = connection.execute("INSERT INTO source.providers(code,name) VALUES(%s,%s) RETURNING id", (f"q02-immutable-{suffix}", "Q02 immutable identity test")).fetchone()
        assert provider is not None
        run_id = _run(connection, int(provider[0]), f"q02-immutable-{suffix}")
        stable = f"immutable-{suffix}"
        item_id, _ = _enqueue(connection, run_id, stable, priority=3_000_000, execution_key=f"immutable:{suffix}")
        with pytest.raises(psycopg.errors.CheckViolation, match="immutable"):
            connection.execute("UPDATE ops.sync_work_items SET stable_key=%s WHERE id=%s", (f"changed-{suffix}", item_id))
        with pytest.raises(psycopg.errors.CheckViolation, match="durable history"):
            connection.execute("DELETE FROM ops.sync_work_items WHERE id=%s", (item_id,))
        claimed = connection.execute("SELECT id FROM ops.claim_next_repeatable_sync_work_item_with_lease(%s,%s)", (f"immutable-{suffix}", "1 minute")).fetchone()
        assert claimed is not None and int(claimed[0]) == item_id
        token = connection.execute("SELECT lease_token FROM ops.sync_work_items WHERE id=%s", (item_id,)).fetchone()[0]
        assert connection.execute("SELECT ops.complete_repeatable_sync_work_item(%s,%s,%s,%s)", (item_id, f"immutable-{suffix}", token, Jsonb({}))).fetchone()[0] is True
        assert _enqueue(connection, run_id, stable) == (item_id, False)


def test_repeatable_claim_respects_due_time_ages_priorities_and_retains_conflicts() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider = connection.execute("INSERT INTO source.providers(code,name) VALUES(%s,%s) RETURNING id", (f"q02-order-{suffix}", "Q02 order test")).fetchone()
        assert provider is not None
        run_id = _run(connection, int(provider[0]), f"q02-order-{suffix}")
        future_id, _ = _enqueue(connection, run_id, f"future:{suffix}", priority=10000, available_at=datetime.now(UTC) + timedelta(hours=1), execution_key=f"future:{suffix}")
        old_id, _ = _enqueue(connection, run_id, f"old:{suffix}", available_at=datetime.now(UTC) - timedelta(minutes=61), execution_key=f"old:{suffix}")
        high_id, _ = _enqueue(connection, run_id, f"high:{suffix}", priority=60, execution_key=f"high:{suffix}")
        claimed = connection.execute("SELECT id FROM ops.claim_next_repeatable_sync_work_item_with_lease(%s,%s)", (f"order-{suffix}", "1 minute")).fetchone()
        assert claimed is not None and int(claimed[0]) == old_id
        assert connection.execute("SELECT status FROM ops.sync_work_items WHERE id=%s", (future_id,)).fetchone()[0] == "pending"
        assert connection.execute("SELECT status FROM ops.sync_work_items WHERE id=%s", (high_id,)).fetchone()[0] == "pending"


def test_conflicting_claims_from_real_connections_leave_one_version_pending() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        provider = setup.execute("INSERT INTO source.providers(code,name) VALUES(%s,%s) RETURNING id", (f"q02-conflict-{suffix}", "Q02 conflict test")).fetchone()
        assert provider is not None
        run_id = _run(setup, int(provider[0]), f"q02-conflict-{suffix}")
        first, _ = _enqueue(setup, run_id, f"conflict-a:{suffix}", priority=300000, execution_key=f"conflict:{suffix}")
        second, _ = _enqueue(setup, run_id, f"conflict-b:{suffix}", priority=299999, execution_key=f"conflict:{suffix}")
    barrier, claimed = threading.Barrier(2), []

    def claim(owner: str) -> None:
        assert TEST_DB_URL is not None
        with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
            barrier.wait()
            row = connection.execute("SELECT id FROM ops.claim_next_repeatable_sync_work_item_with_lease(%s,%s)", (owner, "1 minute")).fetchone()
            claimed.append(None if row is None else int(row[0]))

    left, right = threading.Thread(target=claim, args=(f"conflict-a-{suffix}",)), threading.Thread(target=claim, args=(f"conflict-b-{suffix}",))
    left.start(); right.start(); left.join(); right.join()
    assert sum(value in {first, second} for value in claimed) == 1
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        statuses = dict(connection.execute("SELECT id,status FROM ops.sync_work_items WHERE id IN (%s,%s)", (first, second)).fetchall())
    assert list(statuses.values()).count("running") == 1 and list(statuses.values()).count("pending") == 1


def test_post_migration_cup_and_season_legacy_inserts_still_work() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider = connection.execute("INSERT INTO source.providers(code,name) VALUES(%s,%s) RETURNING id", (f"q02-legacy-{suffix}", "Q02 legacy path test")).fetchone()
        assert provider is not None
        provider_id = int(provider[0])
        cup = PostgresCupQueueRepository("unused")
        cup._conn_value, cup._provider_id = connection, provider_id
        cup_run = cup.create_run([CupCompetition(900001, "Q02 Cup", 2026, True)], operation=CUP_OPERATION, policy_version=CUP_POLICY_VERSION)
        season = PostgresSeasonalSyncRepository("unused")
        season._connection, season._provider_id = connection, provider_id
        season_run = season.start_run([SeasonalLeaguePolicy("q02-league", 900002, 2)])
        rows = connection.execute("SELECT run_id,stable_key,entity_key,execution_key FROM ops.sync_work_items WHERE run_id IN (%s,%s) ORDER BY run_id", (cup_run, season_run)).fetchall()
    assert len(rows) == 2
    assert all(all(str(value).startswith("legacy:") for value in row[1:]) for row in rows)


def test_q01_denial_prevents_postgres_repository_enqueue() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id = int(connection.execute("INSERT INTO source.providers(code,name) VALUES(%s,%s) RETURNING id", (f"q02-policy-{suffix}", "Q02 policy test")).fetchone()[0])
        country_id = int(connection.execute("INSERT INTO football.countries(name) VALUES(%s) RETURNING id", (f"Q02 country {suffix}",)).fetchone()[0])
        league_id = int(connection.execute("INSERT INTO football.leagues(name,country_id,competition_type) VALUES(%s,%s,'league') RETURNING id", (f"Q02 league {suffix}", country_id)).fetchone()[0])
        connection.execute("INSERT INTO source.league_provider_refs(provider_id,external_id,league_id) VALUES(%s,%s,%s)", (provider_id, f"q02-{suffix}", league_id))
        season_id = int(connection.execute("INSERT INTO football.seasons(league_id,start_year,label) VALUES(%s,2026,%s) RETURNING id", (league_id, f"Q02 {suffix}")).fetchone()[0])
        connection.execute("INSERT INTO source.season_provider_refs(provider_id,league_external_id,external_season,season_id) VALUES(%s,%s,2026,%s)", (provider_id, f"q02-{suffix}", season_id))
        connection.execute("INSERT INTO ops.competition_sync_policies(provider_id,season_id,enabled,allowed_work_types,coverage,refresh_intervals) VALUES(%s,%s,false,ARRAY['fixtures'],%s,%s)", (provider_id, season_id, Jsonb({"fixtures": {"state": "covered", "observed_on": "2026-09-08"}}), Jsonb({"fixtures": {"value": 1, "unit": "minute"}})))
        run_id = _run(connection, provider_id, f"q02-policy-run-{suffix}")
        work = PeriodicWork(provider_id, season_id, "fixtures", "fixture:42", datetime(2026, 9, 8, tzinfo=UTC), datetime(2026, 9, 8, 0, 5, tzinfo=UTC), 0, {})
        repository = PostgresSyncRepository(connection, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: datetime.now(UTC)))
        with pytest.raises(SyncPolicyDenied, match="disabled"):
            repository.enqueue_periodic(run_id, work, available_at=datetime.now(UTC))
        assert connection.execute("SELECT count(*) FROM ops.sync_work_items WHERE stable_key=%s", (work.stable_key(),)).fetchone()[0] == 0


def test_q03_fenced_lease_expiry_quarantine_and_conflicting_recovery() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id = int(connection.execute("INSERT INTO source.providers(code,name) VALUES(%s,%s) RETURNING id", (f"q03-{suffix}", "Q03 test")).fetchone()[0])
        run_id = _run(connection, provider_id, f"q03-{suffix}")
        first, _ = _enqueue(connection, run_id, f"q03-first:{suffix}", priority=9_000_000, execution_key=f"q03-group:{suffix}")
        second, _ = _enqueue(connection, run_id, f"q03-second:{suffix}", priority=8_999_999, execution_key=f"q03-group:{suffix}")
        claimed = connection.execute("SELECT * FROM ops.claim_next_repeatable_sync_work_item_with_lease(%s,%s,%s)", (f"one-{suffix}", "1 millisecond", 2)).fetchone()
        assert claimed is not None and int(claimed[0]) == first
        token = int(claimed[-1])
        # Reclaiming after expiry invalidates the old fence, and releases a Q02 conflict group.
        connection.execute("SELECT pg_sleep(0.01)")
        reclaimed = connection.execute("SELECT * FROM ops.claim_next_repeatable_sync_work_item_with_lease(%s,%s,%s)", (f"two-{suffix}", "1 minute", 2)).fetchone()
        assert reclaimed is not None and int(reclaimed[-1]) != token
        assert connection.execute("SELECT ops.checkpoint_repeatable_sync_work_item(%s,%s,%s,%s)", (first, f"one-{suffix}", token, Jsonb({"stale": True}))).fetchone()[0] is None
        new_token = int(reclaimed[-1])
        assert connection.execute("SELECT ops.requeue_repeatable_sync_work_item(%s,%s,%s,%s,%s,%s,%s)", (first, f"two-{suffix}", new_token, Jsonb({}), "contract", "0 seconds", True)).fetchone()[0] is True
        assert connection.execute("SELECT status FROM ops.sync_work_items WHERE id=%s", (first,)).fetchone()[0] == "quarantined"
        assert connection.execute("SELECT ops.retry_quarantined_repeatable_sync_work_item(%s)", (first,)).fetchone()[0] is True
        # A recovered version stays in history and remains executable; its sibling was never lost.
        assert connection.execute("SELECT count(*) FROM ops.sync_work_items WHERE id IN (%s,%s)", (first, second)).fetchone()[0] == 2


def test_q03_real_sessions_fence_old_owner_and_quarantine_attempt_limit() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        provider_id = int(setup.execute("INSERT INTO source.providers(code,name) VALUES(%s,%s) RETURNING id", (f"q03-fence-{suffix}", "Q03 fence test")).fetchone()[0])
        run_id = _run(setup, provider_id, f"q03-fence-{suffix}")
        item_id, _ = _enqueue(setup, run_id, f"q03-fence:{suffix}", priority=10_000_000, execution_key=f"q03-fence:{suffix}")
    with psycopg.connect(TEST_DB_URL, autocommit=True) as first, psycopg.connect(TEST_DB_URL, autocommit=True) as second:
        first_claim = first.execute("SELECT * FROM ops.claim_next_repeatable_sync_work_item_with_lease(%s,%s,%s)", (f"same-{suffix}", "1 minute", 2)).fetchone()
        assert first_claim is not None and int(first_claim[0]) == item_id
        old_token = int(first_claim[-1])
        # A live lease cannot be claimed by another real session.
        other = second.execute("SELECT * FROM ops.claim_next_repeatable_sync_work_item_with_lease(%s,%s,%s)", (f"other-{suffix}", "1 minute", 2)).fetchone()
        assert other is None or int(other[0]) != item_id
        assert first.execute("SELECT lease_owner,lease_token FROM ops.sync_work_items WHERE id=%s", (item_id,)).fetchone() == (f"same-{suffix}", old_token)
        second.execute("UPDATE ops.sync_work_items SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=%s", (item_id,))  # simulated stopped worker / expiry
        second_claim = second.execute("SELECT * FROM ops.claim_next_repeatable_sync_work_item_with_lease(%s,%s,%s)", (f"same-{suffix}", "1 minute", 2)).fetchone()
        assert second_claim is not None and int(second_claim[0]) == item_id
        new_token = int(second_claim[-1])
        assert new_token != old_token
        for function, args in (
            ("heartbeat_repeatable_sync_work_item", (item_id, f"same-{suffix}", old_token, "1 minute")),
            ("checkpoint_repeatable_sync_work_item", (item_id, f"same-{suffix}", old_token, Jsonb({"old": True}))),
            ("complete_repeatable_sync_work_item", (item_id, f"same-{suffix}", old_token, Jsonb({}))),
            ("requeue_repeatable_sync_work_item", (item_id, f"same-{suffix}", old_token, Jsonb({}), "old", "0 seconds", False)),
        ):
            placeholders = ",".join("%s" for _ in args)
            assert first.execute(f"SELECT ops.{function}({placeholders})", args).fetchone()[0] is None
        # The current short lease expires, reaches max attempts, and is preserved in quarantine.
        second.execute("UPDATE ops.sync_work_items SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=%s", (item_id,))
        second.execute("SELECT * FROM ops.claim_next_repeatable_sync_work_item_with_lease(%s,%s,%s)", (f"third-{suffix}", "1 minute", 2)).fetchone()
        row = second.execute("SELECT status,lease_token FROM ops.sync_work_items WHERE id=%s", (item_id,)).fetchone()
        assert row[0] == "quarantined"
        quarantined_token = int(row[1])
        assert second.execute("SELECT ops.retry_quarantined_repeatable_sync_work_item(%s)", (item_id,)).fetchone()[0] is True
        assert int(second.execute("SELECT lease_token FROM ops.sync_work_items WHERE id=%s", (item_id,)).fetchone()[0]) > quarantined_token


def test_q03_guarded_result_transaction_rolls_back_result_dependents_and_completion() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id = int(connection.execute("INSERT INTO source.providers(code,name) VALUES(%s,%s) RETURNING id", (f"q03-txn-{suffix}", "Q03 transaction test")).fetchone()[0])
        run_id = _run(connection, provider_id, f"q03-txn-{suffix}")
        item_id, _ = _enqueue(connection, run_id, f"q03-txn:{suffix}", priority=11_000_000, execution_key=f"q03-txn:{suffix}")
        claim = connection.execute("SELECT * FROM ops.claim_next_repeatable_sync_work_item_with_lease(%s,%s,%s)", (f"txn-{suffix}", "1 minute", 3)).fetchone()
        assert claim is not None and int(claim[0]) == item_id
        token = int(claim[-1])
        connection.execute("CREATE TEMP TABLE q03_atomic_marker(value text PRIMARY KEY)")
        with pytest.raises(RuntimeError):
            with connection.transaction():
                assert connection.execute("SELECT ops.guard_repeatable_sync_work_item_lease(%s,%s,%s)", (item_id, f"txn-{suffix}", token)).fetchone()[0] is True
                connection.execute("INSERT INTO q03_atomic_marker VALUES('result')")
                connection.execute("INSERT INTO q03_atomic_marker VALUES('dependent')")
                raise RuntimeError("completion failed")
        assert connection.execute("SELECT count(*) FROM q03_atomic_marker").fetchone()[0] == 0
        assert connection.execute("SELECT status FROM ops.sync_work_items WHERE id=%s", (item_id,)).fetchone()[0] == "running"
        with connection.transaction():
            assert connection.execute("SELECT ops.guard_repeatable_sync_work_item_lease(%s,%s,%s)", (item_id, f"txn-{suffix}", token)).fetchone()[0] is True
            connection.execute("INSERT INTO q03_atomic_marker VALUES('result')")
            connection.execute("INSERT INTO q03_atomic_marker VALUES('dependent')")
            assert connection.execute("SELECT ops.complete_repeatable_sync_work_item(%s,%s,%s,%s)", (item_id, f"txn-{suffix}", token, Jsonb({}))).fetchone()[0] is True
        assert connection.execute("SELECT count(*) FROM q03_atomic_marker").fetchone()[0] == 2


def test_q03_legacy_apis_cannot_claim_or_mutate_repeatable_work() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider = int(connection.execute("INSERT INTO source.providers(code,name) VALUES(%s,%s) RETURNING id", (f"q03-legacy-api-{suffix}", "Q03 legacy api")).fetchone()[0])
        run_id = _run(connection, provider, f"q03-legacy-api-{suffix}")
        item_id, _ = _enqueue(connection, run_id, f"q03-legacy-api:{suffix}", priority=12_000_000, execution_key=f"q03-legacy-api:{suffix}")
        assert connection.execute("SELECT * FROM ops.claim_next_sync_work_item(%s,%s,%s)", (run_id, "old", "1 minute")).fetchone() is None
        claimed = connection.execute("SELECT * FROM ops.claim_next_repeatable_sync_work_item_with_lease(%s,%s,%s)", ("new", "1 minute", 3)).fetchone()
        assert claimed is not None and int(claimed[0]) == item_id
        assert connection.execute("SELECT ops.complete_sync_work_item(%s,%s,%s)", (item_id, "new", Jsonb({}))).fetchone()[0] is None


def test_q03_runner_commits_before_fetch_and_heartbeats_on_separate_connection() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        provider = int(setup.execute("INSERT INTO source.providers(code,name) VALUES(%s,%s) RETURNING id", (f"q03-runner-{suffix}", "Q03 runner")).fetchone()[0])
        run_id = _run(setup, provider, f"q03-runner-{suffix}")
        item_id, _ = _enqueue(setup, run_id, f"q03-runner:{suffix}", priority=13_000_000, execution_key=f"q03-runner:{suffix}")
        setup.execute("UPDATE ops.sync_work_items SET scope=%s WHERE id=%s", (Jsonb({"_sync_policy": {"provider_id": provider, "season_id": 1, "work_type": "x", "instance_id": 1, "version": 1}}), item_id))
    class Gate:
        def before_enqueue(self, request): return type("A", (), {"coverage": None, "refresh_interval": None})()
        def before_execution(self, authorization): return authorization
    heartbeat_connections: list[object] = []
    def heartbeat_connection():
        heartbeat_connections.append(object())
        return psycopg.connect(TEST_DB_URL)
    with psycopg.connect(TEST_DB_URL) as connection:
        worker = RepeatableSyncWorker(connection, Gate(), f"runner-{suffix}", heartbeat_connection_factory=heartbeat_connection, heartbeat_interval=0.01)  # type: ignore[arg-type]
        def fetch(*_args):
            assert connection.info.transaction_status.name == "IDLE"
            time.sleep(0.05)
            assert heartbeat_connections
            return WorkResult({"done": True})
        worker.run_once(fetch, lambda writer, *_: writer.execute("CREATE TEMP TABLE q03_runner_persist(value text)"))
        assert connection.execute("SELECT status FROM ops.sync_work_items WHERE stable_key=%s", (f"q03-runner:{suffix}",)).fetchone()[0] == "succeeded"


def test_q03_runner_canonical_sink_nested_transaction_is_atomic() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    table = f"q03_canonical_{suffix}"
    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        setup.execute(f"CREATE TABLE ops.{table}(value text PRIMARY KEY)")
        provider = int(setup.execute("INSERT INTO source.providers(code,name) VALUES(%s,%s) RETURNING id", (f"q03-canon-{suffix}", "Q03 canonical")).fetchone()[0])
        run_id = _run(setup, provider, f"q03-canon-{suffix}")
        item_id, _ = _enqueue(setup, run_id, f"q03-canon:{suffix}", priority=14_000_000, execution_key=f"q03-canon:{suffix}")
        setup.execute("UPDATE ops.sync_work_items SET scope=%s WHERE id=%s", (Jsonb({"_sync_policy": {"provider_id": provider, "season_id": 1, "work_type": "x", "instance_id": 1, "version": 1}}), item_id))
    class Gate:
        def before_enqueue(self, request): return type("A", (), {"coverage": None, "refresh_interval": None})()
        def before_execution(self, authorization): return authorization
    def apply(writer, *_):
        sink = CupCanonicalSink(writer, write_validated_base=lambda conn, *_: _canonical(conn))  # type: ignore[arg-type]
        sink.write_cup_base(validated=None, collected=[])  # type: ignore[arg-type]
    def _canonical(conn):
        with conn.transaction(): conn.execute(f"INSERT INTO ops.{table} VALUES('canonical')")
    def dependent_then_fail(writer):
        writer.execute(f"INSERT INTO ops.{table} VALUES('dependent')")
        raise RuntimeError("dependent")
    with psycopg.connect(TEST_DB_URL) as connection:
        worker = RepeatableSyncWorker(connection, Gate(), f"canon-{suffix}")  # type: ignore[arg-type]
        with pytest.raises(RuntimeError):
            worker.run_once(lambda *_: WorkResult({}, (dependent_then_fail,)), apply)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as verify:
        assert verify.execute(f"SELECT count(*) FROM ops.{table}").fetchone()[0] == 0
        assert verify.execute("SELECT status FROM ops.sync_work_items WHERE id=%s", (item_id,)).fetchone()[0] == "running"
        token = verify.execute("SELECT lease_token FROM ops.sync_work_items WHERE id=%s", (item_id,)).fetchone()[0]
        verify.execute("SELECT ops.requeue_repeatable_sync_work_item(%s,%s,%s,%s,%s,%s,%s)", (item_id, f"canon-{suffix}", token, Jsonb({}), "retry", "0 seconds", False))
    with psycopg.connect(TEST_DB_URL) as connection:
        worker = RepeatableSyncWorker(connection, Gate(), f"canon-{suffix}")  # type: ignore[arg-type]
        worker.run_once(lambda *_: WorkResult({}, (lambda writer: writer.execute(f"INSERT INTO ops.{table} VALUES('dependent')"),)), apply)
        connection.commit()
        connection.execute("UPDATE ops.sync_work_items SET available_at=clock_timestamp()+interval '1 hour' WHERE status='pending' AND id<>%s", (item_id,))
        connection.commit()
        assert worker.run_once(lambda *_: pytest.fail("duplicate fetch"), lambda *_: pytest.fail("duplicate apply")) is False
    with psycopg.connect(TEST_DB_URL, autocommit=True) as verify:
        assert verify.execute(f"SELECT count(*) FROM ops.{table}").fetchone()[0] == 2
        assert verify.execute("SELECT status FROM ops.sync_work_items WHERE id=%s", (item_id,)).fetchone()[0] == "succeeded"
