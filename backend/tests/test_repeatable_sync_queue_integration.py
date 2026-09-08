from __future__ import annotations

import os
import threading
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
        assert connection.execute("SELECT * FROM ops.claim_next_repeatable_sync_work_item(%s,%s)", (f"q02-{suffix}", "1 minute")).fetchone()[0] == item_id
        assert connection.execute("SELECT ops.complete_sync_work_item(%s,%s,%s)", (item_id, f"q02-{suffix}", Jsonb({}))).fetchone()[0] is True
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
        claimed = connection.execute("SELECT id FROM ops.claim_next_repeatable_sync_work_item(%s,%s)", (f"new-{suffix}", "1 minute")).fetchone()
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
        claimed = connection.execute("SELECT id FROM ops.claim_next_repeatable_sync_work_item(%s,%s)", (f"immutable-{suffix}", "1 minute")).fetchone()
        assert claimed is not None and int(claimed[0]) == item_id
        assert connection.execute("SELECT ops.complete_sync_work_item(%s,%s,%s)", (item_id, f"immutable-{suffix}", Jsonb({}))).fetchone()[0] is True
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
        claimed = connection.execute("SELECT id FROM ops.claim_next_repeatable_sync_work_item(%s,%s)", (f"order-{suffix}", "1 minute")).fetchone()
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
            row = connection.execute("SELECT id FROM ops.claim_next_repeatable_sync_work_item(%s,%s)", (owner, "1 minute")).fetchone()
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
