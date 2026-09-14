from __future__ import annotations

import os
import threading
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
import pytest
from psycopg import sql
from psycopg.types.json import Jsonb

from app.api_football import APIFootballBudgetDenied, APIFootballBudgetError
from app.api_football.errors import APIFootballHTTPError
from app.importer.cup_bootstrap import CupCompetition
from app.importer.cup_queue import OPERATION as CUP_OPERATION, POLICY_VERSION as CUP_POLICY_VERSION
from app.importer.cup_queue_repository import PostgresCupQueueRepository
from app.importer.cup_queue import CupQueueError, CupWorkItem
from app.importer.season_sync import PostgresSeasonalSyncRepository, SeasonalLeaguePolicy, SeasonalSyncError, SeasonalWorkItem
from app.importer.catalogue_bootstrap import CatalogueBootstrapError, PostgresRepository as CatalogueRepository, WorkItem
from app.sync.policies import PostgresCompetitionSyncPolicyReader, SyncPolicyDenied, SyncPolicyGate
from app.sync.repository import LeasedWorkItem, PeriodicWork, PostgresSyncRepository, RecalculationWork
from app.sync.scheduler import AnalyticsInputSnapshot, PeriodicScheduleState, ScheduleDecisionReason, SyncScheduler
from app.sync.scheduler_process import Q05SchedulerProcess
from app.sync.scheduler_repository import PostgresSchedulerRepository
from app.sync.scheduler_repository import PostgresSchedulerSnapshotReader
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
        season_run = season.start_run([SeasonalLeaguePolicy("q02-league", 900002, 2)]).run_id
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


@pytest.mark.parametrize("change", ("disable", "version"))
def test_q05_policy_lock_rechecks_calculation_fingerprint_with_real_connections(change: str) -> None:
    """A policy update which wins the lock race invalidates the old candidate."""
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        provider_id = int(setup.execute(
            "INSERT INTO source.providers(code,name) VALUES(%s,%s) RETURNING id",
            (f"q05-policy-{suffix}", "Q05 policy lock test"),
        ).fetchone()[0])
        country_id = int(setup.execute(
            "INSERT INTO football.countries(name) VALUES(%s) RETURNING id", (f"Q05 country {suffix}",)
        ).fetchone()[0])
        league_id = int(setup.execute(
            "INSERT INTO football.leagues(name,country_id,competition_type) VALUES(%s,%s,'league') RETURNING id",
            (f"Q05 league {suffix}", country_id),
        ).fetchone()[0])
        setup.execute(
            "INSERT INTO source.league_provider_refs(provider_id,external_id,league_id) VALUES(%s,%s,%s)",
            (provider_id, f"q05-{suffix}", league_id),
        )
        season_id = int(setup.execute(
            "INSERT INTO football.seasons(league_id,start_year,label) VALUES(%s,2026,%s) RETURNING id",
            (league_id, f"Q05 {suffix}"),
        ).fetchone()[0])
        setup.execute(
            "INSERT INTO source.season_provider_refs(provider_id,league_external_id,external_season,season_id) VALUES(%s,%s,2026,%s)",
            (provider_id, f"q05-{suffix}", season_id),
        )
        policy = setup.execute(
            """INSERT INTO ops.competition_sync_policies(provider_id,season_id,enabled,allowed_work_types,coverage,refresh_intervals)
                 VALUES(%s,%s,true,ARRAY['calendar_refresh'],%s,%s)
                 RETURNING policy_instance_id,policy_version""",
            (provider_id, season_id,
             Jsonb({"calendar_refresh": {"state": "covered", "observed_on": "2026-09-08"}}),
             Jsonb({"calendar_refresh": {"value": 1, "unit": "hour"}})),
        ).fetchone()
        assert policy is not None
        run_id = _run(setup, provider_id, f"q05-policy-run-{suffix}")

    stable_key = f"q05-policy-lock:{change}:{suffix}"
    scope = Jsonb({"_sync_policy": {
        "provider_id": provider_id, "season_id": season_id, "work_type": "calendar_refresh",
        "instance_id": int(policy[0]), "version": int(policy[1]),
    }, "window_start": "2026-09-09T00:00:00+00:00", "window_end": "2026-09-09T01:00:00+00:00"})
    started = threading.Event()
    backend_pid: list[int] = []
    outcome: list[str] = []

    def enqueue_from_old_calculation() -> None:
        assert TEST_DB_URL is not None
        with psycopg.connect(TEST_DB_URL, autocommit=True) as contender:
            backend_pid.append(int(contender.execute("SELECT pg_backend_pid()").fetchone()[0]))
            started.set()
            try:
                contender.execute(
                    "SELECT * FROM ops.enqueue_repeatable_sync_work_and_checkpoint("
                    "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (run_id, stable_key, scope, "calendar_refresh", 0, datetime(2026, 9, 9, tzinfo=UTC),
                     stable_key, "season:" + str(season_id), "season:" + str(season_id), provider_id, season_id,
                     None, None, datetime(2026, 9, 9, 1, tzinfo=UTC), datetime(2026, 9, 9, 2, tzinfo=UTC)),
                ).fetchone()
                outcome.append("enqueued")
            except psycopg.Error as exc:
                outcome.append(exc.sqlstate or "unknown")

    with psycopg.connect(TEST_DB_URL) as policy_writer:
        policy_writer.execute(
            "SELECT 1 FROM ops.competition_sync_policies WHERE provider_id=%s AND season_id=%s FOR UPDATE",
            (provider_id, season_id),
        )
        contender = threading.Thread(target=enqueue_from_old_calculation)
        contender.start()
        assert started.wait(timeout=2)
        for _ in range(100):
            row = policy_writer.execute("SELECT wait_event_type FROM pg_stat_activity WHERE pid=%s", (backend_pid[0],)).fetchone()
            if row is not None and row[0] == "Lock":
                break
            time.sleep(0.01)
        else:
            pytest.fail("scheduler connection did not block on the policy row")
        if change == "disable":
            policy_writer.execute(
                "UPDATE ops.competition_sync_policies SET enabled=false WHERE provider_id=%s AND season_id=%s",
                (provider_id, season_id),
            )
        else:
            policy_writer.execute(
                "UPDATE ops.competition_sync_policies SET priority=priority+1 WHERE provider_id=%s AND season_id=%s",
                (provider_id, season_id),
            )
        policy_writer.commit()
        contender.join(timeout=5)
        assert not contender.is_alive()

    assert outcome == ["55000"]
    with psycopg.connect(TEST_DB_URL, autocommit=True) as check:
        assert check.execute("SELECT count(*) FROM ops.sync_work_items WHERE stable_key=%s", (stable_key,)).fetchone()[0] == 0
        assert check.execute(
            "SELECT count(*) FROM ops.sync_scheduler_checkpoints WHERE provider_id=%s AND season_id=%s AND work_type='calendar_refresh'",
            (provider_id, season_id),
        ).fetchone()[0] == 0


def _q05_scheduler_setup(connection: psycopg.Connection, suffix: str) -> tuple[int, int, int, int, int]:
    provider_id = int(connection.execute(
        "INSERT INTO source.providers(code,name) VALUES(%s,%s) RETURNING id",
        (f"q05-scheduler-{suffix}", "Q05 scheduler test"),
    ).fetchone()[0])
    country_id = int(connection.execute(
        "INSERT INTO football.countries(name) VALUES(%s) RETURNING id", (f"Q05 scheduler country {suffix}",)
    ).fetchone()[0])
    league_id = int(connection.execute(
        "INSERT INTO football.leagues(name,country_id,competition_type) VALUES(%s,%s,'league') RETURNING id",
        (f"Q05 scheduler league {suffix}", country_id),
    ).fetchone()[0])
    connection.execute(
        "INSERT INTO source.league_provider_refs(provider_id,external_id,league_id) VALUES(%s,%s,%s)",
        (provider_id, f"q05-scheduler-{suffix}", league_id),
    )
    season_id = int(connection.execute(
        "INSERT INTO football.seasons(league_id,start_year,label) VALUES(%s,2026,%s) RETURNING id",
        (league_id, f"Q05 scheduler {suffix}"),
    ).fetchone()[0])
    connection.execute(
        "INSERT INTO source.season_provider_refs(provider_id,league_external_id,external_season,season_id) VALUES(%s,%s,2026,%s)",
        (provider_id, f"q05-scheduler-{suffix}", season_id),
    )
    policy = connection.execute(
        """INSERT INTO ops.competition_sync_policies(provider_id,season_id,enabled,allowed_work_types,coverage,refresh_intervals)
             VALUES(%s,%s,true,ARRAY['calendar_refresh'],%s,%s)
             RETURNING policy_instance_id,policy_version""",
        (provider_id, season_id,
         Jsonb({"calendar_refresh": {"state": "covered", "observed_on": "2026-09-08"}}),
         Jsonb({"calendar_refresh": {"value": 1, "unit": "hour"}})),
    ).fetchone()
    assert policy is not None
    return provider_id, season_id, int(policy[0]), int(policy[1]), _run(connection, provider_id, f"q05-scheduler-run-{suffix}")


def _q05_fixture(connection: psycopg.Connection, *, provider_id: int, season_id: int, suffix: str,
                 kickoff_at: datetime | None, lifecycle_state: str = "scheduled", observed_at: datetime | None = None) -> int:
    home_id = int(connection.execute("INSERT INTO football.teams(name) VALUES(%s) RETURNING id", (f"Q05 home {suffix}",)).fetchone()[0])
    away_id = int(connection.execute("INSERT INTO football.teams(name) VALUES(%s) RETURNING id", (f"Q05 away {suffix}",)).fetchone()[0])
    connection.execute("INSERT INTO football.season_teams(season_id,team_id) VALUES(%s,%s),(%s,%s)", (season_id, home_id, season_id, away_id))
    inserted_state = "scheduled" if lifecycle_state == "completed" else lifecycle_state
    first_seen = observed_at or datetime.now(UTC)
    row = connection.execute("INSERT INTO football.fixtures(season_id,home_team_id,away_team_id,kickoff_at,lifecycle_state,first_seen_at,last_seen_at) VALUES(%s,%s,%s,%s,%s,%s,%s) RETURNING id", (season_id, home_id, away_id, kickoff_at, inserted_state, first_seen, first_seen)).fetchone()
    assert row is not None
    fixture_id = int(row[0])
    connection.execute("INSERT INTO source.fixture_provider_refs(provider_id,external_id,fixture_id) VALUES(%s,%s,%s)", (provider_id, f"q05-fixture-{suffix}", fixture_id))
    if lifecycle_state == "completed":
        assert kickoff_at is not None
        observed = observed_at or datetime.now(UTC)
        fetch_id = _q05_fetch(connection, provider_id=provider_id, at=observed, subject_fixture_id=fixture_id, purpose="postmatch_reconciliation")
        connection.execute("""UPDATE football.fixtures
                              SET lifecycle_state='completed',home_goals=1,away_goals=0,last_source_fetch_id=%s,
                                  terminal_status_observed_at=%s,result_available_at=%s
                            WHERE id=%s""", (fetch_id, observed, observed, fixture_id))
    return fixture_id


def _q05_fetch(connection: psycopg.Connection, *, provider_id: int, at: datetime,
               subject_fixture_id: int | None = None, purpose: str = "scheduled_refresh") -> int:
    row = connection.execute("""INSERT INTO source.provider_fetches(provider_id,endpoint,purpose,request_started_at,response_received_at,http_status,outcome,subject_fixture_id)
                                VALUES(%s,'/fixtures',%s,%s,%s,200,'success',%s) RETURNING id""", (provider_id, purpose, at, at, subject_fixture_id)).fetchone()
    assert row is not None
    return int(row[0])


def _q05_schedule_observation(
    connection: psycopg.Connection,
    *,
    provider_id: int,
    fixture_id: int,
    observed_kickoff_at: datetime | None,
    observed_at: datetime,
) -> int:
    fetch_id = _q05_fetch(
        connection,
        provider_id=provider_id,
        at=observed_at,
        subject_fixture_id=fixture_id,
    )
    connection.execute(
        "UPDATE source.provider_fetches SET normalized_at=%s WHERE id=%s",
        (observed_at, fetch_id),
    )
    connection.execute(
        """INSERT INTO source.fixture_schedule_observations(
               provider_id,fixture_id,source_fetch_id,observed_kickoff_at,observed_at
           ) VALUES(%s,%s,%s,%s,%s)""",
        (provider_id, fixture_id, fetch_id, observed_kickoff_at, observed_at),
    )
    return fetch_id


def test_fixture_schedule_observation_blocks_concurrent_fetch_time_rewrite() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    observed_at = datetime(2026, 9, 10, 10, 0, tzinfo=UTC)
    kickoff_at = datetime(2026, 9, 10, 14, 0, tzinfo=UTC)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        provider_id, season_id, *_ = _q05_scheduler_setup(setup, suffix)
        fixture_id = _q05_fixture(
            setup,
            provider_id=provider_id,
            season_id=season_id,
            suffix=suffix,
            kickoff_at=kickoff_at,
            observed_at=observed_at,
        )
        fetch_id = _q05_fetch(
            setup,
            provider_id=provider_id,
            at=observed_at,
            subject_fixture_id=fixture_id,
        )

    started = threading.Event()
    backend_pid: list[int] = []
    outcome: list[str] = []

    def rewrite_fetch_time() -> None:
        assert TEST_DB_URL is not None
        with psycopg.connect(TEST_DB_URL) as contender:
            backend_pid.append(int(contender.execute("SELECT pg_backend_pid()").fetchone()[0]))
            started.set()
            try:
                contender.execute(
                    "UPDATE source.provider_fetches SET response_received_at=%s WHERE id=%s",
                    (observed_at + timedelta(seconds=1), fetch_id),
                )
                contender.commit()
                outcome.append("updated")
            except psycopg.Error as error:
                contender.rollback()
                outcome.append(error.sqlstate or "unknown")

    with psycopg.connect(TEST_DB_URL) as observation_writer:
        observation_writer.execute(
            """INSERT INTO source.fixture_schedule_observations(
                   provider_id,fixture_id,source_fetch_id,observed_kickoff_at,observed_at
               ) VALUES(%s,%s,%s,%s,%s)""",
            (provider_id, fixture_id, fetch_id, kickoff_at, observed_at),
        )
        contender = threading.Thread(target=rewrite_fetch_time)
        contender.start()
        assert started.wait(timeout=2)
        with psycopg.connect(TEST_DB_URL, autocommit=True) as observer:
            for _ in range(100):
                wait = observer.execute(
                    "SELECT wait_event_type FROM pg_stat_activity WHERE pid=%s",
                    (backend_pid[0],),
                ).fetchone()
                if wait is not None and wait[0] == "Lock":
                    break
                time.sleep(0.01)
            else:
                pytest.fail("fetch timestamp rewrite did not wait for observation transaction")
        observation_writer.commit()
        contender.join(timeout=5)
        assert not contender.is_alive()

    assert outcome == ["23503"]
    with psycopg.connect(TEST_DB_URL, autocommit=True) as check:
        assert check.execute(
            "SELECT response_received_at FROM source.provider_fetches WHERE id=%s",
            (fetch_id,),
        ).fetchone()[0] == observed_at
        assert check.execute(
            """SELECT count(*) FROM source.fixture_schedule_observations
               WHERE provider_id=%s AND fixture_id=%s AND source_fetch_id=%s""",
            (provider_id, fixture_id, fetch_id),
        ).fetchone()[0] == 1


def _q05_policy(connection: psycopg.Connection, *, provider_id: int, season_id: int, work_types: tuple[str, ...]) -> None:
    connection.execute("UPDATE ops.competition_sync_policies SET enabled=true,allowed_work_types=%s,coverage=%s,refresh_intervals=%s WHERE provider_id=%s AND season_id=%s",
                       (list(work_types), Jsonb({name: {"state": "covered", "observed_on": "2026-09-09"} for name in work_types}), Jsonb({name: {"value": 1 if name != "schedule_near" else 3, "unit": "hour"} for name in work_types}), provider_id, season_id))


def test_q07_diagnostic_sql_runs_read_only_and_reports_partial_metric_pairs() -> None:
    """The runbook SQL must execute against the full current disposable schema."""
    assert TEST_DB_URL is not None
    suffix, now = uuid.uuid4().hex, datetime.now(UTC)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _instance, _version, _run_id = _q05_scheduler_setup(connection, suffix)
        fixture_id = _q05_fixture(
            connection, provider_id=provider_id, season_id=season_id, suffix=suffix,
            kickoff_at=now - timedelta(hours=8), lifecycle_state="completed",
        )
        team_id = int(connection.execute(
            "SELECT home_team_id FROM football.fixtures WHERE id=%s", (fixture_id,),
        ).fetchone()[0])
        connection.execute(
            """INSERT INTO football.fixture_team_statistics(
                   fixture_id,team_id,corner_kicks,yellow_cards,mapping_version,
                   observed_at,available_at,availability_basis
               ) VALUES(%s,%s,0,NULL,'q07-test',%s,%s,'reconstructed_conservative')""",
            (fixture_id, team_id, now - timedelta(hours=4), now - timedelta(hours=4)),
        )
        fetch_id = int(connection.execute(
            "SELECT last_source_fetch_id FROM football.fixtures WHERE id=%s", (fixture_id,),
        ).fetchone()[0])
        connection.execute(
            """INSERT INTO football.fixture_statistics_coverage(
                   fixture_id,coverage_state,team_count,last_source_fetch_id,observed_at,next_retry_at,attempts
               ) VALUES(%s,'partial',1,%s,%s,%s,1)""",
            (fixture_id, fetch_id, now - timedelta(hours=4), now + timedelta(hours=1)),
        )
        _q05_fixture(
            connection, provider_id=provider_id, season_id=season_id, suffix=f"future-{suffix}",
            kickoff_at=now + timedelta(days=2), lifecycle_state="scheduled",
        )
        _q05_fixture(
            connection, provider_id=provider_id, season_id=season_id, suffix=f"overdue-{suffix}",
            kickoff_at=now - timedelta(hours=1), lifecycle_state="scheduled",
        )
        connection.execute(
            """INSERT INTO ops.sync_work_items(
                   run_id,scope_key,scope,job_type,stable_key,entity_key,execution_key
               ) VALUES(%s,%s,%s,'calendar_refresh',%s,%s,%s)""",
            (
                _run_id, f"q07-invalid-{suffix}",
                Jsonb({"_sync_policy": {"provider_id": "not-a-number", "season_id": 999999999999999999999999}}),
                f"q07-invalid-{suffix}", f"q07-invalid-entity-{suffix}", f"q07-invalid-execution-{suffix}",
            ),
        )
        # These observations must be newer than the helper's source fetches,
        # proving provider freshness takes the latest record for *each*
        # endpoint rather than all-history success or another endpoint.
        for endpoint, outcome, provider_results, normalized_at, observed_at, subject_season_id in (
            ("/fixtures", "success", 1, now + timedelta(minutes=1), now + timedelta(minutes=1), None),
            ("/fixtures", "provider_error", None, None, now + timedelta(minutes=2), None),
            ("/fixtures/statistics", "success", 0, now + timedelta(minutes=3), now + timedelta(minutes=3), None),
            ("/standings", "success", 1, None, now + timedelta(minutes=4), season_id),
        ):
            connection.execute(
                """INSERT INTO source.provider_fetches(
                       provider_id,endpoint,purpose,request_started_at,response_received_at,
                       http_status,outcome,provider_results,subject_fixture_id,subject_season_id,normalized_at
                   ) VALUES(%s,%s,'scheduled_refresh',%s,%s,200,%s,%s,%s,%s,%s)""",
                (provider_id, endpoint, observed_at, observed_at, outcome, provider_results,
                 fixture_id if subject_season_id is None else None, subject_season_id, normalized_at),
            )

    report = (Path(__file__).parents[1] / "app" / "sync" / "diagnostics.sql").read_text(encoding="utf-8")
    with psycopg.connect(TEST_DB_URL) as connection:
        connection.execute("SET TRANSACTION READ ONLY")
        cursor = connection.execute(report)
        result_sets: list[list[tuple[object, ...]]] = []
        while True:
            result_sets.append(cursor.fetchall() if cursor.description is not None else [])
            if not cursor.nextset():
                break

    assert len(result_sets) == 6
    queue_columns = ("state", "work_items", "oldest_queue_age_seconds")
    queue_states = [dict(zip(queue_columns, row, strict=True)) for row in result_sets[0]]
    assert next(row for row in queue_states if row["state"] == "invalid_scope")["work_items"] >= 1
    lifecycle_columns = ("provider_id", "season_id", "lifecycle_state", "overdue_fixtures", "oldest_kickoff_at", "greatest_overdue_seconds")
    lifecycle = [dict(zip(lifecycle_columns, row, strict=True)) for row in result_sets[1]]
    assert next(row for row in lifecycle if row["provider_id"] == provider_id and row["lifecycle_state"] == "scheduled")["overdue_fixtures"] == 1
    metric_columns = ("provider_id", "season_id", "metric", "fixtures", "expected_team_pairs",
                      "observed_metric_team_pairs", "null_metric_team_pairs", "fixtures_without_statistics",
                      "fixtures_with_one_team_statistics", "complete_team_pair_missing_selected_metric",
                      "coverage_empty", "coverage_partial", "coverage_unknown_or_absent")
    metrics = [dict(zip(metric_columns, row, strict=True)) for row in result_sets[4]]
    corners = next(row for row in metrics if row["provider_id"] == provider_id and row["metric"] == "corner_kicks")
    yellows = next(row for row in metrics if row["provider_id"] == provider_id and row["metric"] == "yellow_cards")
    assert corners["fixtures_with_one_team_statistics"] == 1
    assert corners["observed_metric_team_pairs"] == 1  # zero is observed, not missing
    assert yellows["null_metric_team_pairs"] == 1
    assert yellows["coverage_partial"] == 1
    assert corners["fixtures"] == 1  # the future scheduled fixture is not eligible statistics coverage

    freshness_columns = (
        "provider_id", "season_id", "endpoint", "last_response_received_at", "last_normalized_at",
        "freshness_age_seconds", "lifetime_successes", "lifetime_failures", "latest_outcome",
        "latest_provider_results", "factual_status",
    )
    freshness = [dict(zip(freshness_columns, row, strict=True)) for row in result_sets[3]]
    fixture_report = next(row for row in freshness if row["provider_id"] == provider_id and row["endpoint"] == "/fixtures")
    statistics_report = next(row for row in freshness if row["provider_id"] == provider_id and row["endpoint"] == "/fixtures/statistics")
    standings_report = next(row for row in freshness if row["provider_id"] == provider_id and row["endpoint"] == "/standings")
    assert fixture_report["factual_status"] == "provider_failure_observed"
    assert fixture_report["latest_outcome"] == "provider_error"
    assert fixture_report["lifetime_successes"] >= 1 and fixture_report["lifetime_failures"] >= 1
    assert statistics_report["factual_status"] == "provider_empty_response_observed"
    assert standings_report["factual_status"] == "response_not_normalized"


def _q05_snapshot(now: datetime):
    assert TEST_DB_URL is not None
    with psycopg.connect(TEST_DB_URL) as reader:
        with reader.transaction():
            return PostgresSchedulerSnapshotReader(reader).read(now=now)


class _PrematchQueryRecordingConnection:
    def __init__(self, connection: psycopg.Connection) -> None:
        self._connection = connection
        self.query: str | None = None
        self.params: object | None = None

    def execute(self, query, params=None, **kwargs):
        if (
            isinstance(query, str)
            and "jsonb_to_recordset" in query
            and "source.fixture_schedule_observations" in query
        ):
            self.query = query
            self.params = params
        return self._connection.execute(query, params, **kwargs)


def _plan_nodes(plan: dict[str, object]):
    yield plan
    for child in plan.get("Plans", ()):
        yield from _plan_nodes(child)


def _q05_enqueue(process: Q05SchedulerProcess, *, run_id: int, now: datetime, snapshot, provider_id: int, season_id: int):
    matches = lambda item: (item.provider_id, item.season_id) == (provider_id, season_id)
    return process.enqueue_due(
        run_id=run_id, now=now,
        policies=tuple(item for item in snapshot.policies if matches(item)),
        schedule_state=tuple(item for item in snapshot.checkpoints if matches(item)),
        fixtures=tuple(item for item in snapshot.fixtures if matches(item)),
        seasons=tuple(item for item in snapshot.seasons if matches(item)),
        analytics_inputs=tuple(item for item in snapshot.analytics_inputs if matches(item)),
        prematch_observations=snapshot.prematch_observations,
        budget=snapshot.budget,
    )


def _q05_scheduler_transition(
    connection: psycopg.Connection,
    *, run_id: int, provider_id: int, season_id: int, policy_instance_id: int, policy_version: int,
    stable_key: str, window_start: datetime, window_end: datetime,
    expected_state: tuple[datetime, datetime] | None = None, checkpoint_end: datetime | None = None,
    next_deadline: datetime | None = None,
) -> tuple[int | None, bool, bool]:
    new_boundary = checkpoint_end or window_end
    scope = Jsonb({
        "_sync_policy": {
            "provider_id": provider_id, "season_id": season_id, "work_type": "calendar_refresh",
            "instance_id": policy_instance_id, "version": policy_version,
        },
        "provider_id": provider_id, "season_id": season_id, "work_type": "calendar_refresh",
        "window_start": window_start.isoformat(), "window_end": window_end.isoformat(),
    })
    row = connection.execute(
        "SELECT * FROM ops.enqueue_repeatable_sync_work_and_checkpoint("
        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (run_id, stable_key, scope, "calendar_refresh", 0, window_end, stable_key,
         f"season:{season_id}", f"season:{season_id}", provider_id, season_id,
         None if expected_state is None else expected_state[0],
         None if expected_state is None else expected_state[1],
         new_boundary, next_deadline or new_boundary + timedelta(hours=1)),
    ).fetchone()
    assert row is not None
    return None if row[0] is None else int(row[0]), bool(row[1]), bool(row[2])


class _Q05Dispatch:
    def fetch(self, item, authorization):
        raise AssertionError("scheduler must not fetch")

    def apply_result(self, writer, item, result) -> None:
        raise AssertionError("scheduler must not write results")


def _q05_process(
    connection: psycopg.Connection,
    *,
    now: datetime,
    work_types: tuple[str, ...] = ("prematch_check",),
) -> Q05SchedulerProcess:
    return Q05SchedulerProcess(
        connection,
        PostgresSchedulerRepository(
            connection,
            SyncPolicyGate(
                PostgresCompetitionSyncPolicyReader(connection),
                now=lambda: now,
            ),
        ),
        SyncScheduler(),
        {work_type: _Q05Dispatch() for work_type in work_types},
    )


def test_q05_reader_process_empty_registry_blocks_seasonal_override_writes() -> None:
    assert TEST_DB_URL is not None
    suffix, now = uuid.uuid4().hex, datetime(2026, 9, 9, 12, tzinfo=UTC)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _instance, _version, run_id = _q05_scheduler_setup(connection, suffix)
        _q05_policy(connection, provider_id=provider_id, season_id=season_id, work_types=("season_discovery", "standings_refresh"))
        connection.execute("UPDATE football.seasons SET starts_on=%s WHERE id=%s", ((now + timedelta(days=10)).date(), season_id))
        for work_type, age in (("season_discovery", timedelta(days=2)), ("standings_refresh", timedelta(hours=2))):
            connection.execute("INSERT INTO ops.sync_scheduler_checkpoints(provider_id,season_id,work_type,last_scheduled_window_end,next_deadline) VALUES(%s,%s,%s,%s,%s)", (provider_id, season_id, work_type, now - age, now - timedelta(hours=1)))
        snapshot = _q05_snapshot(now)
        process = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: now)), SyncScheduler(), {})
        result = _q05_enqueue(process, run_id=run_id, now=now, snapshot=snapshot, provider_id=provider_id, season_id=season_id)
        assert {item.reason for item in result.preview.decisions if item.work_type in {"season_discovery", "standings_refresh"}} == {"handler_unavailable"}
        assert result.enqueue_results == ()
        assert connection.execute("SELECT count(*) FROM ops.sync_work_items WHERE run_id=%s", (run_id,)).fetchone()[0] == 0
        assert connection.execute("SELECT count(*) FROM ops.sync_scheduler_checkpoints WHERE provider_id=%s AND season_id=%s", (provider_id, season_id)).fetchone()[0] == 2


def test_q05_reader_uses_statistics_coverage_not_result_reconciliation() -> None:
    assert TEST_DB_URL is not None
    suffix, now = uuid.uuid4().hex, datetime(2026, 9, 9, 12, tzinfo=UTC)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _instance, _version, run_id = _q05_scheduler_setup(connection, suffix)
        _q05_policy(connection, provider_id=provider_id, season_id=season_id, work_types=("statistics_retry",))
        fixture_id = _q05_fixture(connection, provider_id=provider_id, season_id=season_id, suffix=suffix, kickoff_at=now - timedelta(hours=8), lifecycle_state="completed")
        connection.execute("INSERT INTO ops.fixture_reconciliation_state(fixture_id,eligible_at,next_attempt_at) VALUES(%s,%s,%s)", (fixture_id, now - timedelta(hours=4), now - timedelta(hours=3)))
        source_fetch_id = int(connection.execute("SELECT last_source_fetch_id FROM football.fixtures WHERE id=%s", (fixture_id,)).fetchone()[0])
        retry_at = now - timedelta(minutes=5)
        connection.execute("""INSERT INTO football.fixture_statistics_coverage(fixture_id,coverage_state,team_count,last_source_fetch_id,observed_at,next_retry_at,attempts)
                              VALUES(%s,'unknown',0,%s,%s,%s,3)""", (fixture_id, source_fetch_id, now - timedelta(minutes=10), retry_at))
        snapshot = _q05_snapshot(now)
        fixture = next(item for item in snapshot.fixtures if item.fixture_id == fixture_id)
        assert fixture.statistics_eligible_at is None and fixture.statistics_retry_at == retry_at and fixture.statistics_attempts == 3 and not fixture.statistics_completed
        process = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: now)), SyncScheduler(), {"statistics_retry": _Q05Dispatch()})
        result = _q05_enqueue(process, run_id=run_id, now=now, snapshot=snapshot, provider_id=provider_id, season_id=season_id)
        assert next(item for item in result.preview.decisions if item.work_type == "statistics_retry").deadline == retry_at
        assert connection.execute("SELECT count(*) FROM ops.sync_work_items WHERE run_id=%s", (run_id,)).fetchone()[0] == 1


def test_q05_reader_schedules_initial_statistics_and_caps_persisted_retries() -> None:
    assert TEST_DB_URL is not None
    suffix, now = uuid.uuid4().hex, datetime(2026, 9, 9, 12, tzinfo=UTC)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _instance, _version, run_id = _q05_scheduler_setup(connection, suffix)
        _q05_policy(connection, provider_id=provider_id, season_id=season_id, work_types=("statistics_retry",))
        eligible_at = now - timedelta(hours=2)
        fixture_id = _q05_fixture(connection, provider_id=provider_id, season_id=season_id, suffix=suffix, kickoff_at=now - timedelta(hours=8), lifecycle_state="completed", observed_at=eligible_at)
        snapshot = _q05_snapshot(now)
        fixture = next(item for item in snapshot.fixtures if item.fixture_id == fixture_id)
        assert fixture.statistics_eligible_at == eligible_at and fixture.statistics_retry_at is None and not fixture.statistics_completed
        process = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: now)), SyncScheduler(), {"statistics_retry": _Q05Dispatch()})
        result = _q05_enqueue(process, run_id=run_id, now=now, snapshot=snapshot, provider_id=provider_id, season_id=season_id)
        assert len(result.enqueue_results) == 1 and result.enqueue_results[0].enqueued
        source_fetch_id = int(connection.execute("SELECT last_source_fetch_id FROM football.fixtures WHERE id=%s", (fixture_id,)).fetchone()[0])
        connection.execute(
            """INSERT INTO football.fixture_statistics_coverage(fixture_id,coverage_state,team_count,last_source_fetch_id,observed_at,next_retry_at,attempts)
                VALUES(%s,'empty',0,%s,%s,%s,5)""",
            (fixture_id, source_fetch_id, now - timedelta(minutes=10), now - timedelta(minutes=1)),
        )
        capped = _q05_snapshot(now)
        capped_result = _q05_enqueue(process, run_id=run_id, now=now, snapshot=capped, provider_id=provider_id, season_id=season_id)
        retry = next(item for item in capped_result.preview.decisions if item.work_type == "statistics_retry")
        assert retry.reason == "retry_exhausted" and capped_result.enqueue_results == ()


def test_q05_reader_handles_unknown_postponed_kickoff_and_saved_matchday() -> None:
    assert TEST_DB_URL is not None
    suffix, now = uuid.uuid4().hex, datetime(2026, 9, 9, 12, tzinfo=UTC)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _instance, _version, run_id = _q05_scheduler_setup(connection, suffix)
        _q05_policy(connection, provider_id=provider_id, season_id=season_id, work_types=("standings_refresh", "schedule_near", "prematch_check"))
        postponed_id = _q05_fixture(connection, provider_id=provider_id, season_id=season_id, suffix=f"postponed-{suffix}", kickoff_at=None, lifecycle_state="postponed")
        _q05_fixture(connection, provider_id=provider_id, season_id=season_id, suffix=f"today-{suffix}", kickoff_at=now + timedelta(hours=2))
        snapshot = _q05_snapshot(now)
        assert next(item for item in snapshot.fixtures if item.fixture_id == postponed_id).kickoff_at is None
        assert next(item for item in snapshot.seasons if item.season_id == season_id).matchday_today
        process = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: now)), SyncScheduler(), {})
        result = _q05_enqueue(process, run_id=run_id, now=now, snapshot=snapshot, provider_id=provider_id, season_id=season_id)
        assert not any(item.work is not None and item.work.scope.get("fixture_id") == postponed_id for item in result.preview.decisions)
        assert connection.execute("SELECT count(*) FROM ops.sync_work_items WHERE run_id=%s", (run_id,)).fetchone()[0] == 0


def test_q05_prematch_fresh_input_skips_queue_and_event_checkpoint() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    kickoff = datetime(2026, 9, 11, 15, 0, tzinfo=UTC)
    deadline = kickoff - timedelta(minutes=60)
    now = deadline + timedelta(minutes=5)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _instance, _version, run_id = _q05_scheduler_setup(connection, suffix)
        _q05_policy(connection, provider_id=provider_id, season_id=season_id, work_types=("prematch_check",))
        fixture_id = _q05_fixture(
            connection,
            provider_id=provider_id,
            season_id=season_id,
            suffix=suffix,
            kickoff_at=kickoff,
        )
        confirming_fetch_id = _q05_schedule_observation(
            connection,
            provider_id=provider_id,
            fixture_id=fixture_id,
            observed_kickoff_at=kickoff,
            observed_at=deadline - timedelta(minutes=30),
        )
        for index in range(12):
            _q05_schedule_observation(
                connection,
                provider_id=provider_id,
                fixture_id=fixture_id,
                observed_kickoff_at=kickoff,
                observed_at=(
                    deadline
                    - timedelta(hours=1)
                    - timedelta(minutes=index + 1)
                ),
            )
            _q05_schedule_observation(
                connection,
                provider_id=provider_id,
                fixture_id=fixture_id,
                observed_kickoff_at=kickoff + timedelta(hours=1),
                observed_at=deadline - timedelta(minutes=20, seconds=index),
            )
        _q05_schedule_observation(
            connection,
            provider_id=provider_id,
            fixture_id=fixture_id,
            observed_kickoff_at=kickoff,
            observed_at=now + timedelta(microseconds=1),
        )
        foreign_fixture_id = _q05_fixture(
            connection,
            provider_id=provider_id,
            season_id=season_id,
            suffix=f"foreign-{suffix}",
            kickoff_at=kickoff,
        )
        foreign_fetch_id = _q05_schedule_observation(
            connection,
            provider_id=provider_id,
            fixture_id=foreign_fixture_id,
            observed_kickoff_at=kickoff,
            observed_at=deadline - timedelta(minutes=25),
        )

        snapshot = _q05_snapshot(now)
        fixture_observations = [
            item
            for item in snapshot.prematch_observations
            if item.fixture_id == fixture_id
        ]
        assert [item.fetch_id for item in fixture_observations] == [
            confirming_fetch_id
        ]
        assert any(
            item.fetch_id == foreign_fetch_id
            for item in snapshot.prematch_observations
        )
        observation = fixture_observations[0]
        assert observation.fetch_successful and observation.fetch_normalized
        process = _q05_process(connection, now=now)
        result = _q05_enqueue(
            process,
            run_id=run_id,
            now=now,
            snapshot=snapshot,
            provider_id=provider_id,
            season_id=season_id,
        )
        decision = next(
            item
            for item in result.preview.decisions
            if item.work_type == "prematch_check" and item.deadline == deadline
        )
        assert decision.reason == ScheduleDecisionReason.FRESH_INPUT.value
        assert decision.scope["confirming_fetch_id"] == confirming_fetch_id
        assert result.enqueue_results == ()
        assert connection.execute(
            "SELECT count(*) FROM ops.sync_work_items WHERE run_id=%s",
            (run_id,),
        ).fetchone()[0] == 0
        assert connection.execute(
            """SELECT count(*) FROM ops.sync_scheduler_event_checkpoints
               WHERE provider_id=%s AND season_id=%s AND work_type='prematch_check'
                 AND entity_key=%s""",
            (provider_id, season_id, f"fixture:{fixture_id}"),
        ).fetchone()[0] == 0


def test_q05_prematch_materialized_snapshot_preserves_ordinary_enqueue_path() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    kickoff = datetime(2026, 9, 11, 15, 0, tzinfo=UTC)
    deadline = kickoff - timedelta(minutes=60)
    now = deadline + timedelta(minutes=5)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        provider_id, season_id, _instance, _version, run_id = _q05_scheduler_setup(setup, suffix)
        _q05_policy(setup, provider_id=provider_id, season_id=season_id, work_types=("prematch_check",))
        fixture_id = _q05_fixture(
            setup,
            provider_id=provider_id,
            season_id=season_id,
            suffix=suffix,
            kickoff_at=kickoff,
        )

    with (
        psycopg.connect(TEST_DB_URL) as reader_connection,
        psycopg.connect(TEST_DB_URL, autocommit=True) as writer,
    ):
        with reader_connection.transaction():
            snapshot = PostgresSchedulerSnapshotReader(reader_connection).read(now=now)
            assert not any(
                item.fixture_id == fixture_id
                for item in snapshot.prematch_observations
            )
            confirming_fetch_id = _q05_schedule_observation(
                writer,
                provider_id=provider_id,
                fixture_id=fixture_id,
                observed_kickoff_at=kickoff,
                observed_at=deadline - timedelta(minutes=30),
            )
            assert reader_connection.execute(
                """SELECT count(*) FROM source.fixture_schedule_observations
                   WHERE provider_id=%s AND fixture_id=%s""",
                (provider_id, fixture_id),
            ).fetchone()[0] == 0

    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        process = _q05_process(connection, now=now)
        result = _q05_enqueue(
            process,
            run_id=run_id,
            now=now,
            snapshot=snapshot,
            provider_id=provider_id,
            season_id=season_id,
        )
        decision = next(
            item
            for item in result.preview.decisions
            if item.work_type == "prematch_check" and item.deadline == deadline
        )
        assert decision.reason == ScheduleDecisionReason.DUE.value
        assert len(result.enqueue_results) == 1
        assert result.enqueue_results[0].enqueued
        assert result.enqueue_results[0].checkpoint_advanced

        restarted_snapshot = _q05_snapshot(now)
        assert any(
            item.fetch_id == confirming_fetch_id
            for item in restarted_snapshot.prematch_observations
        )
        restarted = _q05_enqueue(
            process,
            run_id=run_id,
            now=now,
            snapshot=restarted_snapshot,
            provider_id=provider_id,
            season_id=season_id,
        )
        restarted_decision = next(
            item
            for item in restarted.preview.decisions
            if item.work_type == "prematch_check" and item.deadline == deadline
        )
        assert restarted_decision.reason == ScheduleDecisionReason.FRESH_INPUT.value
        assert restarted_decision.scope["confirming_fetch_id"] == confirming_fetch_id
        assert restarted.enqueue_results == ()
        assert connection.execute(
            "SELECT count(*) FROM ops.sync_work_items WHERE run_id=%s",
            (run_id,),
        ).fetchone()[0] == 1
        assert connection.execute(
            """SELECT count(*) FROM ops.sync_scheduler_event_checkpoints
               WHERE provider_id=%s AND season_id=%s AND work_type='prematch_check'
                 AND entity_key=%s""",
            (provider_id, season_id, f"fixture:{fixture_id}"),
        ).fetchone()[0] == 1


def test_q05_prematch_reschedule_and_restart_do_not_false_skip_or_duplicate() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    old_kickoff = datetime(2026, 9, 11, 14, 0, tzinfo=UTC)
    new_kickoff = datetime(2026, 9, 11, 15, 0, tzinfo=UTC)
    new_t_minus_60 = new_kickoff - timedelta(minutes=60)
    first_run_at = new_t_minus_60 + timedelta(minutes=5)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        provider_id, season_id, _instance, _version, run_id = _q05_scheduler_setup(setup, suffix)
        _q05_policy(setup, provider_id=provider_id, season_id=season_id, work_types=("prematch_check",))
        fixture_id = _q05_fixture(
            setup,
            provider_id=provider_id,
            season_id=season_id,
            suffix=suffix,
            kickoff_at=old_kickoff,
        )
        _q05_schedule_observation(
            setup,
            provider_id=provider_id,
            fixture_id=fixture_id,
            observed_kickoff_at=old_kickoff,
            observed_at=new_t_minus_60 - timedelta(minutes=30),
        )
        setup.execute(
            "UPDATE football.fixtures SET kickoff_at=%s WHERE id=%s",
            (new_kickoff, fixture_id),
        )

    with psycopg.connect(TEST_DB_URL, autocommit=True) as first_connection:
        first_process = _q05_process(first_connection, now=first_run_at)
        first = _q05_enqueue(
            first_process,
            run_id=run_id,
            now=first_run_at,
            snapshot=_q05_snapshot(first_run_at),
            provider_id=provider_id,
            season_id=season_id,
        )
        first_decision = next(
            item
            for item in first.preview.decisions
            if item.work_type == "prematch_check"
            and item.deadline == new_t_minus_60
        )
        assert first_decision.reason == ScheduleDecisionReason.DUE.value
        assert len(first.enqueue_results) == 1 and first.enqueue_results[0].enqueued

    restarted_at = first_run_at + timedelta(seconds=1)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as restarted_connection:
        restarted_process = _q05_process(
            restarted_connection,
            now=restarted_at,
        )
        repeated = _q05_enqueue(
            restarted_process,
            run_id=run_id,
            now=restarted_at,
            snapshot=_q05_snapshot(restarted_at),
            provider_id=provider_id,
            season_id=season_id,
        )
        assert len(repeated.enqueue_results) == 1
        assert not repeated.enqueue_results[0].enqueued
        assert not repeated.enqueue_results[0].checkpoint_advanced

        confirming_fetch_id = _q05_schedule_observation(
            restarted_connection,
            provider_id=provider_id,
            fixture_id=fixture_id,
            observed_kickoff_at=new_kickoff,
            observed_at=new_t_minus_60 + timedelta(minutes=20),
        )

    t_minus_10 = new_kickoff - timedelta(minutes=10)
    final_run_at = t_minus_10 + timedelta(minutes=5)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as final_connection:
        final_process = _q05_process(final_connection, now=final_run_at)
        final = _q05_enqueue(
            final_process,
            run_id=run_id,
            now=final_run_at,
            snapshot=_q05_snapshot(final_run_at),
            provider_id=provider_id,
            season_id=season_id,
        )
        relevant = [
            item
            for item in final.preview.decisions
            if item.work_type == "prematch_check"
            and item.scope.get("fixture_id") == fixture_id
        ]
        assert {item.reason for item in relevant} == {
            ScheduleDecisionReason.FRESH_INPUT.value
        }
        assert {
            item.scope["confirming_fetch_id"] for item in relevant
        } == {confirming_fetch_id}
        assert final.enqueue_results == ()
        assert final_connection.execute(
            "SELECT count(*) FROM ops.sync_work_items WHERE run_id=%s",
            (run_id,),
        ).fetchone()[0] == 1
        assert final_connection.execute(
            """SELECT count(*) FROM ops.sync_scheduler_event_checkpoints
               WHERE provider_id=%s AND season_id=%s AND work_type='prematch_check'
                 AND entity_key=%s""",
            (provider_id, season_id, f"fixture:{fixture_id}"),
        ).fetchone()[0] == 1
        assert final_connection.execute(
            """SELECT count(*) FROM ops.sync_scheduler_event_checkpoints
               WHERE provider_id=%s AND season_id=%s AND work_type='prematch_check'
                 AND entity_key=%s AND scheduled_window_end=%s""",
            (provider_id, season_id, f"fixture:{fixture_id}", new_t_minus_60),
        ).fetchone()[0] == 1
        assert final_connection.execute(
            """SELECT count(*) FROM ops.sync_scheduler_event_checkpoints
               WHERE provider_id=%s AND season_id=%s AND work_type='prematch_check'
                 AND entity_key=%s AND scheduled_window_end=%s""",
            (provider_id, season_id, f"fixture:{fixture_id}", t_minus_10),
        ).fetchone()[0] == 0


def test_q05_prematch_reschedule_collision_creates_distinct_work_and_checkpoint() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    old_kickoff = datetime(2026, 9, 11, 13, 0, tzinfo=UTC)
    new_kickoff = old_kickoff + timedelta(minutes=50)
    colliding_deadline = old_kickoff - timedelta(minutes=10)
    scheduler_now = colliding_deadline + timedelta(minutes=5)

    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        provider_id, season_id, _instance, _version, run_id = _q05_scheduler_setup(
            setup,
            suffix,
        )
        _q05_policy(
            setup,
            provider_id=provider_id,
            season_id=season_id,
            work_types=("prematch_check",),
        )
        fixture_id = _q05_fixture(
            setup,
            provider_id=provider_id,
            season_id=season_id,
            suffix=suffix,
            kickoff_at=old_kickoff,
        )
        _q05_schedule_observation(
            setup,
            provider_id=provider_id,
            fixture_id=fixture_id,
            observed_kickoff_at=old_kickoff,
            observed_at=old_kickoff - timedelta(minutes=90),
        )

    with psycopg.connect(TEST_DB_URL, autocommit=True) as old_connection:
        old_result = _q05_enqueue(
            _q05_process(old_connection, now=scheduler_now),
            run_id=run_id,
            now=scheduler_now,
            snapshot=_q05_snapshot(scheduler_now),
            provider_id=provider_id,
            season_id=season_id,
        )
        old_t10 = next(
            item
            for item in old_result.preview.decisions
            if item.work_type == "prematch_check"
            and item.scope.get("fixture_id") == fixture_id
            and item.deadline == colliding_deadline
        )
        assert old_t10.reason == ScheduleDecisionReason.DUE.value
        assert len(old_result.enqueue_results) == 1
        assert old_result.enqueue_results[0].enqueued
        assert old_result.enqueue_results[0].checkpoint_advanced
        old_connection.execute(
            "UPDATE football.fixtures SET kickoff_at=%s WHERE id=%s",
            (new_kickoff, fixture_id),
        )

    with psycopg.connect(TEST_DB_URL, autocommit=True) as moved_connection:
        moved_result = _q05_enqueue(
            _q05_process(moved_connection, now=scheduler_now),
            run_id=run_id,
            now=scheduler_now,
            snapshot=_q05_snapshot(scheduler_now),
            provider_id=provider_id,
            season_id=season_id,
        )
        new_t60 = next(
            item
            for item in moved_result.preview.decisions
            if item.work_type == "prematch_check"
            and item.scope.get("fixture_id") == fixture_id
            and item.deadline == colliding_deadline
        )
        assert new_t60.reason == ScheduleDecisionReason.DUE.value
        assert new_t60.stable_key != old_t10.stable_key
        assert len(moved_result.enqueue_results) == 1
        assert moved_result.enqueue_results[0].enqueued
        assert moved_result.enqueue_results[0].checkpoint_advanced

        work_rows = moved_connection.execute(
            """SELECT stable_key,entity_key,execution_key,scope->>'kickoff_at'
                 FROM ops.sync_work_items
                WHERE run_id=%s AND job_type='prematch_check'
                ORDER BY id""",
            (run_id,),
        ).fetchall()
        assert len(work_rows) == 2
        assert work_rows[0][0] != work_rows[1][0]
        assert {row[1] for row in work_rows} == {f"fixture:{fixture_id}"}
        assert {row[2] for row in work_rows} == {
            f"entity:{provider_id}:{season_id}:fixture:{fixture_id}"
        }
        assert {row[3] for row in work_rows} == {
            old_kickoff.isoformat(),
            new_kickoff.isoformat(),
        }
        checkpoints = moved_connection.execute(
            """SELECT stable_key,scheduled_window_end
                 FROM ops.sync_scheduler_event_checkpoints
                WHERE provider_id=%s AND season_id=%s
                  AND work_type='prematch_check' AND entity_key=%s""",
            (provider_id, season_id, f"fixture:{fixture_id}"),
        ).fetchall()
        assert len(checkpoints) == 2
        assert {row[0] for row in checkpoints} == {
            old_t10.stable_key,
            new_t60.stable_key,
        }
        assert {row[1] for row in checkpoints} == {colliding_deadline}

    restarted_at = scheduler_now + timedelta(seconds=1)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as restarted_connection:
        restarted = _q05_enqueue(
            _q05_process(restarted_connection, now=restarted_at),
            run_id=run_id,
            now=restarted_at,
            snapshot=_q05_snapshot(restarted_at),
            provider_id=provider_id,
            season_id=season_id,
        )
        assert len(restarted.enqueue_results) == 1
        assert not restarted.enqueue_results[0].enqueued
        assert not restarted.enqueue_results[0].checkpoint_advanced
        assert restarted_connection.execute(
            """SELECT count(*) FROM ops.sync_work_items
               WHERE run_id=%s AND job_type='prematch_check'""",
            (run_id,),
        ).fetchone()[0] == 2
        assert restarted_connection.execute(
            """SELECT count(*) FROM ops.sync_scheduler_event_checkpoints
               WHERE provider_id=%s AND season_id=%s
                 AND work_type='prematch_check' AND entity_key=%s""",
            (provider_id, season_id, f"fixture:{fixture_id}"),
        ).fetchone()[0] == 2


def test_q05_prematch_without_evidence_still_obeys_policy_and_handler_gates() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    kickoff = datetime(2026, 9, 11, 15, 0, tzinfo=UTC)
    now = kickoff - timedelta(minutes=55)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _instance, _version, run_id = _q05_scheduler_setup(connection, suffix)
        _q05_policy(connection, provider_id=provider_id, season_id=season_id, work_types=("prematch_check",))
        fixture_id = _q05_fixture(
            connection,
            provider_id=provider_id,
            season_id=season_id,
            suffix=suffix,
            kickoff_at=kickoff,
        )
        connection.execute(
            """UPDATE ops.competition_sync_policies SET enabled=false
               WHERE provider_id=%s AND season_id=%s""",
            (provider_id, season_id),
        )
        disabled_snapshot = _q05_snapshot(now)
        assert not any(
            item.fixture_id == fixture_id
            for item in disabled_snapshot.prematch_observations
        )
        disabled_process = _q05_process(connection, now=now)
        disabled = _q05_enqueue(
            disabled_process,
            run_id=run_id,
            now=now,
            snapshot=disabled_snapshot,
            provider_id=provider_id,
            season_id=season_id,
        )
        assert any(
            item.work_type == "prematch_check" and item.reason == "disabled"
            for item in disabled.preview.decisions
        )
        assert disabled.enqueue_results == ()

        connection.execute(
            """UPDATE ops.competition_sync_policies SET enabled=true
               WHERE provider_id=%s AND season_id=%s""",
            (provider_id, season_id),
        )
        no_handler_snapshot = _q05_snapshot(now)
        no_handler_process = _q05_process(connection, now=now, work_types=())
        no_handler = _q05_enqueue(
            no_handler_process,
            run_id=run_id,
            now=now,
            snapshot=no_handler_snapshot,
            provider_id=provider_id,
            season_id=season_id,
        )
        assert any(
            item.work_type == "prematch_check"
            and item.reason == ScheduleDecisionReason.HANDLER_UNAVAILABLE.value
            for item in no_handler.preview.decisions
        )
        assert no_handler.enqueue_results == ()
        assert connection.execute(
            "SELECT count(*) FROM ops.sync_work_items WHERE run_id=%s",
            (run_id,),
        ).fetchone()[0] == 0
        assert connection.execute(
            """SELECT count(*) FROM ops.sync_scheduler_event_checkpoints
               WHERE provider_id=%s AND season_id=%s AND work_type='prematch_check'
                 AND entity_key=%s""",
            (provider_id, season_id, f"fixture:{fixture_id}"),
        ).fetchone()[0] == 0


@pytest.mark.parametrize("offset_minutes", (60, 10), ids=("t60", "t10"))
@pytest.mark.parametrize(
    ("boundary", "observed_delta", "is_fresh"),
    (
        ("lower", timedelta(), True),
        ("lower-outside", -timedelta(microseconds=1), False),
        ("upper", timedelta(), True),
        ("upper-outside", timedelta(microseconds=1), False),
    ),
)
def test_q05_prematch_db_boundaries_are_inclusive_for_t60_and_t10(
    offset_minutes: int,
    boundary: str,
    observed_delta: timedelta,
    is_fresh: bool,
) -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    now = datetime(2026, 9, 12, 12, 0, 0, 123456, tzinfo=UTC)
    target_deadline = now - timedelta(minutes=5)
    policy_interval = timedelta(hours=1)
    kickoff = target_deadline + timedelta(minutes=offset_minutes)
    observed_at = (
        target_deadline - policy_interval + observed_delta
        if boundary.startswith("lower")
        else now + observed_delta
    )

    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _instance, _version, run_id = _q05_scheduler_setup(
            connection,
            suffix,
        )
        _q05_policy(
            connection,
            provider_id=provider_id,
            season_id=season_id,
            work_types=("prematch_check",),
        )
        fixture_id = _q05_fixture(
            connection,
            provider_id=provider_id,
            season_id=season_id,
            suffix=f"{offset_minutes}-{boundary}-{suffix}",
            kickoff_at=kickoff,
        )
        fetch_id = _q05_schedule_observation(
            connection,
            provider_id=provider_id,
            fixture_id=fixture_id,
            observed_kickoff_at=kickoff,
            observed_at=observed_at,
        )

        snapshot = _q05_snapshot(now)
        result = _q05_enqueue(
            _q05_process(connection, now=now),
            run_id=run_id,
            now=now,
            snapshot=snapshot,
            provider_id=provider_id,
            season_id=season_id,
        )

        matching = [
            item
            for item in result.preview.decisions
            if item.work_type == "prematch_check"
            and item.scope.get("fixture_id") == fixture_id
            and item.deadline == target_deadline
        ]
        assert len(matching) == 1
        decision = matching[0]
        if is_fresh:
            assert decision.reason == ScheduleDecisionReason.FRESH_INPUT.value
            assert decision.scope["confirming_fetch_id"] == fetch_id
            assert connection.execute(
                "SELECT count(*) FROM ops.sync_work_items WHERE stable_key=%s",
                (decision.stable_key,),
            ).fetchone()[0] == 0
        else:
            assert decision.reason == ScheduleDecisionReason.DUE.value
            assert "confirming_fetch_id" not in decision.scope
            assert connection.execute(
                "SELECT count(*) FROM ops.sync_work_items WHERE stable_key=%s",
                (decision.stable_key,),
            ).fetchone()[0] == 1


def test_q05_prematch_fresh_input_survives_restart_after_one_day() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    kickoff = datetime(2026, 9, 12, 15, 0, tzinfo=UTC)
    observed_at = kickoff - timedelta(minutes=60)
    first_run_at = kickoff - timedelta(minutes=5)
    restarted_at = first_run_at + timedelta(days=1)

    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        provider_id, season_id, _instance, _version, run_id = _q05_scheduler_setup(
            setup,
            suffix,
        )
        _q05_policy(
            setup,
            provider_id=provider_id,
            season_id=season_id,
            work_types=("prematch_check",),
        )
        fixture_id = _q05_fixture(
            setup,
            provider_id=provider_id,
            season_id=season_id,
            suffix=suffix,
            kickoff_at=kickoff,
        )
        confirming_fetch_id = _q05_schedule_observation(
            setup,
            provider_id=provider_id,
            fixture_id=fixture_id,
            observed_kickoff_at=kickoff,
            observed_at=observed_at,
        )

    for scheduler_now in (first_run_at, restarted_at):
        with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
            result = _q05_enqueue(
                _q05_process(connection, now=scheduler_now),
                run_id=run_id,
                now=scheduler_now,
                snapshot=_q05_snapshot(scheduler_now),
                provider_id=provider_id,
                season_id=season_id,
            )
            prematch = [
                item
                for item in result.preview.decisions
                if item.work_type == "prematch_check"
                and item.scope.get("fixture_id") == fixture_id
            ]
            assert {item.deadline for item in prematch} == {
                kickoff - timedelta(minutes=60),
                kickoff - timedelta(minutes=10),
            }
            assert {item.reason for item in prematch} == {
                ScheduleDecisionReason.FRESH_INPUT.value
            }
            assert {
                item.scope["confirming_fetch_id"] for item in prematch
            } == {confirming_fetch_id}
            assert result.enqueue_results == ()
            assert connection.execute(
                "SELECT count(*) FROM ops.sync_work_items WHERE run_id=%s",
                (run_id,),
            ).fetchone()[0] == 0
            assert connection.execute(
                """SELECT count(*) FROM ops.sync_scheduler_event_checkpoints
                   WHERE provider_id=%s AND season_id=%s
                     AND work_type='prematch_check' AND entity_key=%s""",
                (provider_id, season_id, f"fixture:{fixture_id}"),
            ).fetchone()[0] == 0


def test_q05_prematch_reader_query_explain_analyzes_realistic_history() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    kickoff = datetime(2026, 9, 12, 15, 0, tzinfo=UTC)
    now = kickoff - timedelta(minutes=5)
    deadline = kickoff - timedelta(minutes=60)

    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        provider_id, season_id, _instance, _version, _run_id = _q05_scheduler_setup(
            setup,
            suffix,
        )
        _q05_policy(
            setup,
            provider_id=provider_id,
            season_id=season_id,
            work_types=("prematch_check",),
        )
        fixture_id = _q05_fixture(
            setup,
            provider_id=provider_id,
            season_id=season_id,
            suffix=suffix,
            kickoff_at=kickoff,
        )
        historical_fixture_ids = []
        for index in range(128):
            historical_kickoff = kickoff - timedelta(days=index + 2)
            historical_fixture_ids.append(
                _q05_fixture(
                    setup,
                    provider_id=provider_id,
                    season_id=season_id,
                    suffix=f"history-{index}-{suffix}",
                    kickoff_at=historical_kickoff,
                    lifecycle_state="completed",
                    observed_at=historical_kickoff + timedelta(hours=3),
                )
            )
        setup.execute(
            """WITH inserted_fetch AS (
                   INSERT INTO source.provider_fetches(
                       provider_id,endpoint,purpose,request_started_at,
                       response_received_at,http_status,outcome,normalized_at,
                       subject_fixture_id
                   )
                   SELECT %s,'/fixtures','scheduled_refresh',
                          %s + fixture.ordinality * interval '1 second'
                             + series * interval '1 microsecond',
                          %s + fixture.ordinality * interval '1 second'
                             + series * interval '1 microsecond',
                          200,'success',
                          %s + fixture.ordinality * interval '1 second'
                             + series * interval '1 microsecond',
                          fixture.fixture_id
                     FROM unnest(%s::bigint[]) WITH ORDINALITY
                          AS fixture(fixture_id, ordinality)
                    CROSS JOIN generate_series(1,125) AS series
                   RETURNING id,provider_id,subject_fixture_id,response_received_at
               )
               INSERT INTO source.fixture_schedule_observations(
                   provider_id,fixture_id,source_fetch_id,
                   observed_kickoff_at,observed_at
               )
               SELECT provider_id,subject_fixture_id,id,%s,response_received_at
                 FROM inserted_fetch""",
            (
                provider_id,
                deadline - timedelta(days=30),
                deadline - timedelta(days=30),
                deadline - timedelta(days=30),
                historical_fixture_ids,
                kickoff - timedelta(days=30),
            ),
        )
        history = (
            (32, deadline - timedelta(hours=2), kickoff),
            (31, deadline - timedelta(minutes=30), kickoff + timedelta(hours=1)),
            (1, deadline - timedelta(minutes=30), kickoff),
        )
        for row_count, first_observed_at, observed_kickoff_at in history:
            setup.execute(
                """WITH inserted_fetch AS (
                       INSERT INTO source.provider_fetches(
                           provider_id,endpoint,purpose,request_started_at,
                           response_received_at,http_status,outcome,normalized_at,
                           subject_fixture_id
                       )
                       SELECT %s,'/fixtures','scheduled_refresh',
                              %s + series * interval '1 microsecond',
                              %s + series * interval '1 microsecond',
                              200,'success',
                              %s + series * interval '1 microsecond',%s
                         FROM generate_series(1,%s) AS series
                       RETURNING id,provider_id,response_received_at
                   )
                   INSERT INTO source.fixture_schedule_observations(
                       provider_id,fixture_id,source_fetch_id,
                       observed_kickoff_at,observed_at
                   )
                   SELECT provider_id,%s,id,%s,response_received_at
                     FROM inserted_fetch""",
                (
                    provider_id,
                    first_observed_at,
                    first_observed_at,
                    first_observed_at,
                    fixture_id,
                    row_count,
                    fixture_id,
                    observed_kickoff_at,
                ),
            )
        setup.execute("ANALYZE source.fixture_schedule_observations")
        setup.execute("ANALYZE source.provider_fetches")
        assert setup.execute(
            """SELECT count(*) FROM source.fixture_schedule_observations
               WHERE provider_id=%s""",
            (provider_id,),
        ).fetchone()[0] == 16_064
        total_observation_count = int(setup.execute(
            "SELECT count(*) FROM source.fixture_schedule_observations"
        ).fetchone()[0])

    with psycopg.connect(TEST_DB_URL) as connection:
        with connection.transaction():
            recording = _PrematchQueryRecordingConnection(connection)
            snapshot = PostgresSchedulerSnapshotReader(recording).read(now=now)
            target_observations = [
                item
                for item in snapshot.prematch_observations
                if item.provider_id == provider_id and item.fixture_id == fixture_id
            ]
            assert len(target_observations) == 1
            assert recording.query is not None
            assert recording.params is not None
            assert connection.execute("SHOW enable_seqscan").fetchone()[0] == "on"
            explained = connection.execute(
                "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + recording.query,
                recording.params,
            ).fetchone()
            assert explained is not None
            document = explained[0][0]
            nodes = list(_plan_nodes(document["Plan"]))
            matching_index_nodes = [
                node
                for node in nodes
                if node.get("Index Name")
                == "fixture_schedule_observations_fixture_time_idx"
            ]
            observation_access_nodes = [
                {
                    key: node.get(key)
                    for key in (
                        "Node Type",
                        "Relation Name",
                        "Index Name",
                        "Plan Rows",
                        "Actual Rows",
                        "Actual Loops",
                        "Rows Removed by Filter",
                        "Shared Hit Blocks",
                        "Shared Read Blocks",
                    )
                }
                for node in nodes
                if node.get("Relation Name") == "fixture_schedule_observations"
            ]
            assert len(observation_access_nodes) == 1
            observation_access = observation_access_nodes[0]
            assert observation_access["Actual Loops"] == 1
            assert "Shared Hit Blocks" in observation_access
            assert "Shared Read Blocks" in observation_access
            if matching_index_nodes:
                assert len(matching_index_nodes) == 1
                assert observation_access["Index Name"] == (
                    "fixture_schedule_observations_fixture_time_idx"
                )
                assert observation_access["Actual Rows"] == 1
            else:
                assert observation_access["Node Type"] == "Seq Scan"
                assert observation_access["Index Name"] is None
                assert observation_access["Actual Rows"] == total_observation_count
                assert observation_access["Rows Removed by Filter"] == 0
            function_scan = next(
                node for node in nodes if node.get("Node Type") == "Function Scan"
            )
            prematch_range_count = len(recording.params[0].obj)
            assert function_scan["Actual Rows"] == prematch_range_count
            assert function_scan["Actual Loops"] == 1
            assert document["Plan"]["Actual Rows"] == len(
                snapshot.prematch_observations
            )
            assert document["Plan"]["Actual Loops"] == 1
            assert any(
                "Shared Hit Blocks" in node or "Shared Read Blocks" in node
                for node in nodes
            )
            assert document["Execution Time"] >= 0


def test_q05_colliding_analytics_identity_does_not_extend_the_source_deadline() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    original_at = datetime(2026, 9, 9, 12, 0, 10, tzinfo=UTC)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _instance, _version, run_id = _q05_scheduler_setup(connection, suffix)
        _q05_policy(connection, provider_id=provider_id, season_id=season_id, work_types=("analytics_recalculation",))
        fixture_id = _q05_fixture(connection, provider_id=provider_id, season_id=season_id, suffix=suffix, kickoff_at=original_at + timedelta(days=1), observed_at=original_at)
        original_fetch = _q05_fetch(connection, provider_id=provider_id, at=original_at)
        connection.execute("UPDATE football.fixtures SET last_source_fetch_id=%s,last_seen_at=%s WHERE id=%s", (original_fetch, original_at, fixture_id))
        first_due = datetime(2026, 9, 9, 12, 1, 0, 1, tzinfo=UTC)
        process = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: first_due)), SyncScheduler(), {"analytics_recalculation": _Q05Dispatch()})
        assert _q05_enqueue(process, run_id=run_id, now=first_due, snapshot=_q05_snapshot(first_due), provider_id=provider_id, season_id=season_id).enqueue_results[0].enqueued

        late_at = datetime(2026, 9, 9, 12, 0, 40, tzinfo=UTC)
        late_fetch = _q05_fetch(connection, provider_id=provider_id, at=late_at)
        connection.execute("UPDATE football.fixtures SET last_source_fetch_id=%s,last_seen_at=%s WHERE id=%s", (late_fetch, late_at, fixture_id))
        ordinary_at = datetime(2026, 9, 9, 12, 1, 10, tzinfo=UTC)
        ordinary_fetch = _q05_fetch(connection, provider_id=provider_id, at=ordinary_at)
        connection.execute("UPDATE football.fixtures SET last_source_fetch_id=%s,last_seen_at=%s WHERE id=%s", (ordinary_fetch, ordinary_at, fixture_id))

        rows = connection.execute(
            "SELECT window_end,deadline,latest_source_fetch_id,source_window_end FROM ops.fixture_analytics_recalculation_windows WHERE fixture_id=%s ORDER BY window_end",
            (fixture_id,),
        ).fetchall()
        assert rows == [
            (datetime(2026, 9, 9, 12, 1, tzinfo=UTC), datetime(2026, 9, 9, 12, 1, tzinfo=UTC), original_fetch, None),
            (datetime(2026, 9, 9, 12, 2, tzinfo=UTC), datetime(2026, 9, 9, 12, 1, tzinfo=UTC), late_fetch, datetime(2026, 9, 9, 12, 1, tzinfo=UTC)),
            (datetime(2026, 9, 9, 12, 3, tzinfo=UTC), datetime(2026, 9, 9, 12, 2, tzinfo=UTC), ordinary_fetch, datetime(2026, 9, 9, 12, 2, tzinfo=UTC)),
        ]
        snapshot = _q05_snapshot(datetime(2026, 9, 9, 12, 1, 20, tzinfo=UTC))
        pending = [item for item in snapshot.analytics_inputs if item.entity_key == f"fixture:{fixture_id}"]
        assert [(item.input_version, item.coalescing_deadline, item.window_identity) for item in pending] == [
            (late_fetch, datetime(2026, 9, 9, 12, 1, tzinfo=UTC), datetime(2026, 9, 9, 12, 2, tzinfo=UTC)),
            (ordinary_fetch, datetime(2026, 9, 9, 12, 2, tzinfo=UTC), datetime(2026, 9, 9, 12, 3, tzinfo=UTC)),
        ]


def test_q05_reader_process_coalesces_sequential_analytics_versions_until_close() -> None:
    assert TEST_DB_URL is not None
    suffix, opened = uuid.uuid4().hex, datetime(2026, 9, 9, 12, 0, 10, 123456, tzinfo=UTC)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _instance, _version, run_id = _q05_scheduler_setup(connection, suffix)
        _q05_policy(connection, provider_id=provider_id, season_id=season_id, work_types=("analytics_recalculation",))
        fixture_id = _q05_fixture(connection, provider_id=provider_id, season_id=season_id, suffix=suffix, kickoff_at=opened + timedelta(days=1), observed_at=opened)
        first_fetch = _q05_fetch(connection, provider_id=provider_id, at=opened)
        connection.execute("UPDATE football.fixtures SET last_source_fetch_id=%s,last_seen_at=%s WHERE id=%s", (first_fetch, opened, fixture_id))
        process = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: opened)), SyncScheduler(), {"analytics_recalculation": _Q05Dispatch()})
        first = _q05_snapshot(opened)
        assert _q05_enqueue(process, run_id=run_id, now=opened, snapshot=first, provider_id=provider_id, season_id=season_id).enqueue_results == ()
        later = opened + timedelta(seconds=30)
        second_fetch = _q05_fetch(connection, provider_id=provider_id, at=later)
        connection.execute("UPDATE football.fixtures SET last_source_fetch_id=%s,last_seen_at=%s WHERE id=%s", (second_fetch, later, fixture_id))
        second = _q05_snapshot(later)
        assert _q05_enqueue(process, run_id=run_id, now=later, snapshot=second, provider_id=provider_id, season_id=season_id).enqueue_results == ()
        closed = opened.replace(second=0) + timedelta(minutes=1)
        final = _q05_snapshot(closed)
        result = _q05_enqueue(process, run_id=run_id, now=closed, snapshot=final, provider_id=provider_id, season_id=season_id)
        assert len(result.enqueue_results) == 1 and result.enqueue_results[0].enqueued
        assert connection.execute("SELECT count(*) FROM ops.sync_work_items WHERE run_id=%s", (run_id,)).fetchone()[0] == 1
        assert connection.execute("SELECT input_version FROM ops.sync_scheduler_analytics_checkpoints WHERE provider_id=%s AND season_id=%s", (provider_id, season_id)).fetchone() == (str(second_fetch),)


def test_q05_analytics_windows_keep_their_first_deadline_across_delay_and_restart() -> None:
    assert TEST_DB_URL is not None
    suffix, opened = uuid.uuid4().hex, datetime(2026, 9, 9, 12, 0, 10, tzinfo=UTC)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        provider_id, season_id, _instance, _version, run_id = _q05_scheduler_setup(setup, suffix)
        _q05_policy(setup, provider_id=provider_id, season_id=season_id, work_types=("analytics_recalculation",))
        fixture_id = _q05_fixture(setup, provider_id=provider_id, season_id=season_id, suffix=suffix, kickoff_at=opened + timedelta(days=1), observed_at=opened)
        versions: list[int] = []
        events = (
            (datetime(2026, 9, 9, 12, 0, 10, tzinfo=UTC), datetime(2026, 9, 9, 12, 1, tzinfo=UTC)),
            (datetime(2026, 9, 9, 12, 0, 35, tzinfo=UTC), datetime(2026, 9, 9, 12, 1, tzinfo=UTC)),
            (datetime(2026, 9, 9, 12, 1, 0, tzinfo=UTC), datetime(2026, 9, 9, 12, 2, tzinfo=UTC)),
            (datetime(2026, 9, 9, 12, 1, 25, tzinfo=UTC), datetime(2026, 9, 9, 12, 2, tzinfo=UTC)),
            (datetime(2026, 9, 9, 12, 1, 50, tzinfo=UTC), datetime(2026, 9, 9, 12, 2, tzinfo=UTC)),
            (datetime(2026, 9, 9, 12, 2, 15, tzinfo=UTC), datetime(2026, 9, 9, 12, 3, tzinfo=UTC)),
            (datetime(2026, 9, 9, 12, 2, 40, tzinfo=UTC), datetime(2026, 9, 9, 12, 3, tzinfo=UTC)),
        )
        for observed, _window_end in events:
            fetch_id = _q05_fetch(setup, provider_id=provider_id, at=observed)
            versions.append(fetch_id)
            setup.execute("UPDATE football.fixtures SET last_source_fetch_id=%s,last_seen_at=%s WHERE id=%s", (fetch_id, observed, fixture_id))
    delayed = opened + timedelta(minutes=2, seconds=35, microseconds=321)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        snapshot = _q05_snapshot(delayed)
        inputs = [item for item in snapshot.analytics_inputs if item.entity_key == f"fixture:{fixture_id}"]
        expected_latest_by_window = (
            (versions[1], events[1][1]),
            (versions[4], events[4][1]),
            (versions[6], events[6][1]),
        )
        assert [(item.input_version, item.coalescing_deadline) for item in inputs] == [
            *expected_latest_by_window,
        ]
        process = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: delayed)), SyncScheduler(), {"analytics_recalculation": _Q05Dispatch()})
        result = _q05_enqueue(process, run_id=run_id, now=delayed, snapshot=snapshot, provider_id=provider_id, season_id=season_id)
        assert len(result.enqueue_results) == 2 and all(item.enqueued for item in result.enqueue_results)
    restarted = opened + timedelta(minutes=3, seconds=5, microseconds=777)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        snapshot = _q05_snapshot(restarted)
        process = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: restarted)), SyncScheduler(), {"analytics_recalculation": _Q05Dispatch()})
        result = _q05_enqueue(process, run_id=run_id, now=restarted, snapshot=snapshot, provider_id=provider_id, season_id=season_id)
        assert len(result.enqueue_results) == 1 and result.enqueue_results[0].enqueued
        accepted = connection.execute(
            "SELECT window_end,accepted_input_version FROM ops.fixture_analytics_recalculation_windows WHERE fixture_id=%s ORDER BY window_end",
            (fixture_id,),
        ).fetchall()
        assert [row[1] for row in accepted] == [str(versions[1]), str(versions[4]), str(versions[6])]
        assert connection.execute("SELECT count(*) FROM ops.sync_work_items WHERE run_id=%s", (run_id,)).fetchone()[0] == 3


def test_q05_analytics_event_on_a_window_boundary_waits_for_the_next_close() -> None:
    assert TEST_DB_URL is not None
    suffix, observed = uuid.uuid4().hex, datetime(2026, 9, 9, 12, 1, tzinfo=UTC)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _instance, _version, run_id = _q05_scheduler_setup(connection, suffix)
        _q05_policy(connection, provider_id=provider_id, season_id=season_id, work_types=("analytics_recalculation",))
        fixture_id = _q05_fixture(connection, provider_id=provider_id, season_id=season_id, suffix=suffix, kickoff_at=observed + timedelta(days=1), observed_at=observed)
        fetch_id = _q05_fetch(connection, provider_id=provider_id, at=observed)
        connection.execute("UPDATE football.fixtures SET last_source_fetch_id=%s,last_seen_at=%s WHERE id=%s", (fetch_id, observed, fixture_id))
        before_close = observed + timedelta(microseconds=1)
        process = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: before_close)), SyncScheduler(), {"analytics_recalculation": _Q05Dispatch()})
        assert _q05_enqueue(process, run_id=run_id, now=before_close, snapshot=_q05_snapshot(before_close), provider_id=provider_id, season_id=season_id).enqueue_results == ()
        after_close = observed + timedelta(minutes=1, microseconds=1)
        process = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: after_close)), SyncScheduler(), {"analytics_recalculation": _Q05Dispatch()})
        result = _q05_enqueue(process, run_id=run_id, now=after_close, snapshot=_q05_snapshot(after_close), provider_id=provider_id, season_id=season_id)
        assert len(result.enqueue_results) == 1 and result.enqueue_results[0].enqueued


def test_q05_late_analytics_version_gets_a_distinct_followup_window_without_duplicates() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    first_observed = datetime(2026, 9, 9, 12, 0, 10, tzinfo=UTC)
    first_due = datetime(2026, 9, 9, 12, 1, 0, 100, tzinfo=UTC)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _instance, _version, run_id = _q05_scheduler_setup(connection, suffix)
        _q05_policy(connection, provider_id=provider_id, season_id=season_id, work_types=("analytics_recalculation",))
        fixture_id = _q05_fixture(connection, provider_id=provider_id, season_id=season_id, suffix=suffix, kickoff_at=first_observed + timedelta(days=1), observed_at=first_observed)
        first_fetch = _q05_fetch(connection, provider_id=provider_id, at=first_observed)
        connection.execute("UPDATE football.fixtures SET last_source_fetch_id=%s,last_seen_at=%s WHERE id=%s", (first_fetch, first_observed, fixture_id))
        process = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: first_due)), SyncScheduler(), {"analytics_recalculation": _Q05Dispatch()})
        first = _q05_enqueue(process, run_id=run_id, now=first_due, snapshot=_q05_snapshot(first_due), provider_id=provider_id, season_id=season_id)
        assert len(first.enqueue_results) == 1 and first.enqueue_results[0].enqueued

        late_observed = datetime(2026, 9, 9, 12, 0, 40, tzinfo=UTC)
        late_fetch = _q05_fetch(connection, provider_id=provider_id, at=late_observed)
        connection.execute("UPDATE football.fixtures SET last_source_fetch_id=%s,last_seen_at=%s WHERE id=%s", (late_fetch, late_observed, fixture_id))
        windows = connection.execute(
            "SELECT window_end,latest_source_fetch_id,accepted_input_version,source_window_end FROM ops.fixture_analytics_recalculation_windows WHERE fixture_id=%s ORDER BY window_end",
            (fixture_id,),
        ).fetchall()
        assert windows == [
            (datetime(2026, 9, 9, 12, 1, tzinfo=UTC), first_fetch, str(first_fetch), None),
            (datetime(2026, 9, 9, 12, 2, tzinfo=UTC), late_fetch, None, datetime(2026, 9, 9, 12, 1, tzinfo=UTC)),
        ]
        late_due = datetime(2026, 9, 9, 12, 2, 0, 200, tzinfo=UTC)
        process = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: late_due)), SyncScheduler(), {"analytics_recalculation": _Q05Dispatch()})
        late = _q05_enqueue(process, run_id=run_id, now=late_due, snapshot=_q05_snapshot(late_due), provider_id=provider_id, season_id=season_id)
        assert len(late.enqueue_results) == 1 and late.enqueue_results[0].enqueued
        assert _q05_enqueue(process, run_id=run_id, now=late_due, snapshot=_q05_snapshot(late_due), provider_id=provider_id, season_id=season_id).enqueue_results == ()
        assert connection.execute("SELECT count(*) FROM ops.sync_work_items WHERE run_id=%s", (run_id,)).fetchone()[0] == 2


def test_q05_late_and_ordinary_windows_keep_distinct_versions_and_enqueue_once() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    original_observed = datetime(2026, 9, 9, 12, 0, 10, tzinfo=UTC)
    first_due = datetime(2026, 9, 9, 12, 1, 0, 101, tzinfo=UTC)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _instance, _version, run_id = _q05_scheduler_setup(connection, suffix)
        _q05_policy(connection, provider_id=provider_id, season_id=season_id, work_types=("analytics_recalculation",))
        fixture_id = _q05_fixture(connection, provider_id=provider_id, season_id=season_id, suffix=suffix, kickoff_at=original_observed + timedelta(days=1), observed_at=original_observed)
        original_fetch = _q05_fetch(connection, provider_id=provider_id, at=original_observed)
        connection.execute("UPDATE football.fixtures SET last_source_fetch_id=%s,last_seen_at=%s WHERE id=%s", (original_fetch, original_observed, fixture_id))
        process = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: first_due)), SyncScheduler(), {"analytics_recalculation": _Q05Dispatch()})
        assert _q05_enqueue(process, run_id=run_id, now=first_due, snapshot=_q05_snapshot(first_due), provider_id=provider_id, season_id=season_id).enqueue_results[0].enqueued

        late_observed = datetime(2026, 9, 9, 12, 0, 40, tzinfo=UTC)
        late_fetch = _q05_fetch(connection, provider_id=provider_id, at=late_observed)
        connection.execute("UPDATE football.fixtures SET last_source_fetch_id=%s,last_seen_at=%s WHERE id=%s", (late_fetch, late_observed, fixture_id))
        late_identity = datetime(2026, 9, 9, 12, 2, tzinfo=UTC)
        late_deadline = datetime(2026, 9, 9, 12, 1, tzinfo=UTC)
        assert connection.execute(
            "SELECT window_end,deadline,latest_source_fetch_id,source_window_end FROM ops.fixture_analytics_recalculation_windows WHERE fixture_id=%s AND accepted_at IS NULL",
            (fixture_id,),
        ).fetchone() == (late_identity, late_deadline, late_fetch, datetime(2026, 9, 9, 12, 1, tzinfo=UTC))

        # Re-capturing the same canonical version neither shifts its deadline nor adds a row.
        connection.execute("UPDATE football.fixtures SET last_source_fetch_id=%s,last_seen_at=%s WHERE id=%s", (late_fetch, late_observed, fixture_id))

    # A fresh connection proves the late identity/deadline survives restart.
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        late_run = late_deadline + timedelta(microseconds=201)
        process = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: late_run)), SyncScheduler(), {"analytics_recalculation": _Q05Dispatch()})
        late_result = _q05_enqueue(process, run_id=run_id, now=late_run, snapshot=_q05_snapshot(late_run), provider_id=provider_id, season_id=season_id)
        assert [item.enqueued for item in late_result.enqueue_results] == [True]

        ordinary_observed = datetime(2026, 9, 9, 12, 1, 20, tzinfo=UTC)
        ordinary_fetch = _q05_fetch(connection, provider_id=provider_id, at=ordinary_observed)
        connection.execute("UPDATE football.fixtures SET last_source_fetch_id=%s,last_seen_at=%s WHERE id=%s", (ordinary_fetch, ordinary_observed, fixture_id))
        ordinary_identity = datetime(2026, 9, 9, 12, 3, tzinfo=UTC)
        ordinary_deadline = datetime(2026, 9, 9, 12, 2, tzinfo=UTC)
        windows = connection.execute(
            "SELECT window_end,deadline,latest_source_fetch_id,source_window_end FROM ops.fixture_analytics_recalculation_windows WHERE fixture_id=%s ORDER BY window_end",
            (fixture_id,),
        ).fetchall()
        assert windows == [
            (datetime(2026, 9, 9, 12, 1, tzinfo=UTC), datetime(2026, 9, 9, 12, 1, tzinfo=UTC), original_fetch, None),
            (late_identity, late_deadline, late_fetch, datetime(2026, 9, 9, 12, 1, tzinfo=UTC)),
            (ordinary_identity, ordinary_deadline, ordinary_fetch, datetime(2026, 9, 9, 12, 2, tzinfo=UTC)),
        ]

    # The colliding ordinary window also survives restart and becomes due at
    # its source deadline, not at its later storage identity.
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        ordinary_run = ordinary_deadline + timedelta(microseconds=301)
        process = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: ordinary_run)), SyncScheduler(), {"analytics_recalculation": _Q05Dispatch()})
        ordinary_result = _q05_enqueue(process, run_id=run_id, now=ordinary_run, snapshot=_q05_snapshot(ordinary_run), provider_id=provider_id, season_id=season_id)
        assert [item.enqueued for item in ordinary_result.enqueue_results] == [True]
        assert _q05_enqueue(process, run_id=run_id, now=ordinary_run, snapshot=_q05_snapshot(ordinary_run), provider_id=provider_id, season_id=season_id).enqueue_results == ()
        accepted_versions = connection.execute(
            "SELECT window_end,accepted_input_version FROM ops.fixture_analytics_recalculation_windows WHERE fixture_id=%s ORDER BY window_end",
            (fixture_id,),
        ).fetchall()
        assert accepted_versions == [
            (datetime(2026, 9, 9, 12, 1, tzinfo=UTC), str(original_fetch)),
            (late_identity, str(late_fetch)),
            (ordinary_identity, str(ordinary_fetch)),
        ]
        queued_versions = connection.execute(
            "SELECT scope->>'input_version' FROM ops.sync_work_items WHERE run_id=%s AND job_type='analytics_recalculation' ORDER BY id",
            (run_id,),
        ).fetchall()
        assert queued_versions == [(str(original_fetch),), (str(late_fetch),), (str(ordinary_fetch),)]
        assert connection.execute("SELECT count(*) FROM ops.sync_work_items WHERE run_id=%s", (run_id,)).fetchone()[0] == 3


def test_q05_concurrent_late_captures_keep_the_latest_window_version() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    original_observed = datetime(2026, 9, 9, 12, 0, 10, tzinfo=UTC)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        provider_id, season_id, _instance, _version, run_id = _q05_scheduler_setup(setup, suffix)
        _q05_policy(setup, provider_id=provider_id, season_id=season_id, work_types=("analytics_recalculation",))
        fixture_id = _q05_fixture(setup, provider_id=provider_id, season_id=season_id, suffix=suffix, kickoff_at=original_observed + timedelta(days=1), observed_at=original_observed)
        original_fetch = _q05_fetch(setup, provider_id=provider_id, at=original_observed)
        setup.execute("UPDATE football.fixtures SET last_source_fetch_id=%s,last_seen_at=%s WHERE id=%s", (original_fetch, original_observed, fixture_id))
        first_due = datetime(2026, 9, 9, 12, 1, 0, 111, tzinfo=UTC)
        process = Q05SchedulerProcess(setup, PostgresSchedulerRepository(setup, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(setup), now=lambda: first_due)), SyncScheduler(), {"analytics_recalculation": _Q05Dispatch()})
        assert _q05_enqueue(process, run_id=run_id, now=first_due, snapshot=_q05_snapshot(first_due), provider_id=provider_id, season_id=season_id).enqueue_results[0].enqueued
        earlier_at = datetime(2026, 9, 9, 12, 0, 35, tzinfo=UTC)
        later_at = datetime(2026, 9, 9, 12, 0, 50, tzinfo=UTC)
        earlier_fetch = _q05_fetch(setup, provider_id=provider_id, at=earlier_at)
        later_fetch = _q05_fetch(setup, provider_id=provider_id, at=later_at)

    barrier = threading.Barrier(2)
    failures: list[BaseException] = []

    def capture(fetch_id: int, observed_at: datetime) -> None:
        assert TEST_DB_URL is not None
        try:
            with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
                barrier.wait()
                connection.execute(
                    "UPDATE football.fixtures SET last_source_fetch_id=%s,last_seen_at=%s WHERE id=%s",
                    (fetch_id, observed_at, fixture_id),
                )
        except BaseException as error:
            failures.append(error)

    left = threading.Thread(target=capture, args=(earlier_fetch, earlier_at))
    right = threading.Thread(target=capture, args=(later_fetch, later_at))
    left.start(); right.start(); left.join(); right.join()
    assert failures == []

    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        late_deadline = datetime(2026, 9, 9, 12, 2, tzinfo=UTC)
        late_rows = connection.execute(
            "SELECT window_end,latest_source_fetch_id,observed_at,source_window_end FROM ops.fixture_analytics_recalculation_windows WHERE fixture_id=%s AND accepted_at IS NULL",
            (fixture_id,),
        ).fetchall()
        assert late_rows == [(late_deadline, later_fetch, later_at, datetime(2026, 9, 9, 12, 1, tzinfo=UTC))]
        due = late_deadline + timedelta(microseconds=401)
        process = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: due)), SyncScheduler(), {"analytics_recalculation": _Q05Dispatch()})
        result = _q05_enqueue(process, run_id=run_id, now=due, snapshot=_q05_snapshot(due), provider_id=provider_id, season_id=season_id)
        assert [item.enqueued for item in result.enqueue_results] == [True]
        assert _q05_enqueue(process, run_id=run_id, now=due, snapshot=_q05_snapshot(due), provider_id=provider_id, season_id=season_id).enqueue_results == ()
        assert connection.execute(
            "SELECT accepted_input_version FROM ops.fixture_analytics_recalculation_windows WHERE fixture_id=%s AND window_end=%s",
            (fixture_id, late_deadline),
        ).fetchone() == (str(later_fetch),)
        assert connection.execute(
            "SELECT scope->>'input_version' FROM ops.sync_work_items WHERE run_id=%s AND job_type='analytics_recalculation' ORDER BY id",
            (run_id,),
        ).fetchall() == [(str(original_fetch),), (str(later_fetch),)]


def test_q05_late_analytics_window_survives_no_handler_policy_denial_and_rollback() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    first_observed = datetime(2026, 9, 9, 12, 0, 10, tzinfo=UTC)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _instance, _version, run_id = _q05_scheduler_setup(connection, suffix)
        _q05_policy(connection, provider_id=provider_id, season_id=season_id, work_types=("analytics_recalculation",))
        fixture_id = _q05_fixture(connection, provider_id=provider_id, season_id=season_id, suffix=suffix, kickoff_at=first_observed + timedelta(days=1), observed_at=first_observed)
        first_fetch = _q05_fetch(connection, provider_id=provider_id, at=first_observed)
        connection.execute("UPDATE football.fixtures SET last_source_fetch_id=%s,last_seen_at=%s WHERE id=%s", (first_fetch, first_observed, fixture_id))
        first_due = datetime(2026, 9, 9, 12, 1, 0, 100, tzinfo=UTC)
        first_process = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: first_due)), SyncScheduler(), {"analytics_recalculation": _Q05Dispatch()})
        assert _q05_enqueue(first_process, run_id=run_id, now=first_due, snapshot=_q05_snapshot(first_due), provider_id=provider_id, season_id=season_id).enqueue_results[0].enqueued
        late_observed = datetime(2026, 9, 9, 12, 0, 40, tzinfo=UTC)
        late_fetch = _q05_fetch(connection, provider_id=provider_id, at=late_observed)
        connection.execute("UPDATE football.fixtures SET last_source_fetch_id=%s,last_seen_at=%s WHERE id=%s", (late_fetch, late_observed, fixture_id))
        late_due = datetime(2026, 9, 9, 12, 2, 0, 300, tzinfo=UTC)

        snapshot = _q05_snapshot(late_due)
        no_handler = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: late_due)), SyncScheduler(), {})
        skipped = _q05_enqueue(no_handler, run_id=run_id, now=late_due, snapshot=snapshot, provider_id=provider_id, season_id=season_id)
        assert next(item for item in skipped.preview.decisions if item.work_type == "analytics_recalculation" and item.work is not None).reason == "handler_unavailable"
        assert skipped.enqueue_results == ()

        stale_policy = next(item for item in snapshot.policies if (item.provider_id, item.season_id) == (provider_id, season_id))
        connection.execute("UPDATE ops.competition_sync_policies SET priority=priority+1 WHERE provider_id=%s AND season_id=%s", (provider_id, season_id))
        stale_process = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: late_due)), SyncScheduler(), {"analytics_recalculation": _Q05Dispatch()})
        denied = stale_process.enqueue_due(run_id=run_id, now=late_due, policies=[stale_policy], schedule_state=[], analytics_inputs=snapshot.analytics_inputs)
        assert denied.policy_denials == ("version_changed",)

        fresh = _q05_snapshot(late_due)
        connection.execute("CREATE FUNCTION ops.q05_fail_late_window_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'q05 forced late window failure'; END; $$")
        connection.execute("CREATE TRIGGER q05_fail_late_window_checkpoint BEFORE INSERT ON ops.sync_scheduler_analytics_checkpoints FOR EACH ROW EXECUTE FUNCTION ops.q05_fail_late_window_checkpoint()")
        fresh_process = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: late_due)), SyncScheduler(), {"analytics_recalculation": _Q05Dispatch()})
        with pytest.raises(psycopg.errors.RaiseException, match="forced late window failure"):
            _q05_enqueue(fresh_process, run_id=run_id, now=late_due, snapshot=fresh, provider_id=provider_id, season_id=season_id)
        assert connection.execute("SELECT accepted_at FROM ops.fixture_analytics_recalculation_windows WHERE fixture_id=%s ORDER BY window_end DESC LIMIT 1", (fixture_id,)).fetchone() == (None,)
        assert connection.execute("SELECT count(*) FROM ops.sync_work_items WHERE run_id=%s", (run_id,)).fetchone()[0] == 1
        connection.execute("DROP TRIGGER q05_fail_late_window_checkpoint ON ops.sync_scheduler_analytics_checkpoints")
        connection.execute("DROP FUNCTION ops.q05_fail_late_window_checkpoint()")
        retry = _q05_enqueue(fresh_process, run_id=run_id, now=late_due, snapshot=_q05_snapshot(late_due), provider_id=provider_id, season_id=season_id)
        assert len(retry.enqueue_results) == 1 and retry.enqueue_results[0].enqueued


def test_q05_two_scheduler_connections_accept_one_analytics_window_once() -> None:
    assert TEST_DB_URL is not None
    suffix, observed = uuid.uuid4().hex, datetime(2026, 9, 9, 12, 0, 10, tzinfo=UTC)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        provider_id, season_id, _instance, _version, run_id = _q05_scheduler_setup(setup, suffix)
        _q05_policy(setup, provider_id=provider_id, season_id=season_id, work_types=("analytics_recalculation",))
        fixture_id = _q05_fixture(setup, provider_id=provider_id, season_id=season_id, suffix=suffix, kickoff_at=observed + timedelta(days=1), observed_at=observed)
        fetch_id = _q05_fetch(setup, provider_id=provider_id, at=observed)
        setup.execute("UPDATE football.fixtures SET last_source_fetch_id=%s,last_seen_at=%s WHERE id=%s", (fetch_id, observed, fixture_id))
    now, barrier, results, failures = observed.replace(second=0) + timedelta(minutes=1, seconds=1), threading.Barrier(2), [], []
    def enqueue() -> None:
        assert TEST_DB_URL is not None
        with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
            try:
                snapshot = _q05_snapshot(now)
                process = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: now)), SyncScheduler(), {"analytics_recalculation": _Q05Dispatch()})
                barrier.wait()
                results.append(_q05_enqueue(process, run_id=run_id, now=now, snapshot=snapshot, provider_id=provider_id, season_id=season_id))
            except BaseException as error:
                failures.append(error)
    left, right = threading.Thread(target=enqueue), threading.Thread(target=enqueue)
    left.start(); right.start(); left.join(); right.join()
    assert failures == []
    assert sum(result.enqueue_results[0].enqueued for result in results) == 1
    with psycopg.connect(TEST_DB_URL, autocommit=True) as check:
        assert check.execute("SELECT accepted_input_version FROM ops.fixture_analytics_recalculation_windows WHERE fixture_id=%s", (fixture_id,)).fetchone() == (str(fetch_id),)
        assert check.execute("SELECT count(*) FROM ops.sync_work_items WHERE run_id=%s", (run_id,)).fetchone()[0] == 1


def test_q05_analytics_window_rollback_preserves_the_due_window() -> None:
    assert TEST_DB_URL is not None
    suffix, observed = uuid.uuid4().hex, datetime(2026, 9, 9, 12, 0, 10, tzinfo=UTC)
    now = observed.replace(second=0) + timedelta(minutes=1, seconds=1)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _instance, _version, run_id = _q05_scheduler_setup(connection, suffix)
        _q05_policy(connection, provider_id=provider_id, season_id=season_id, work_types=("analytics_recalculation",))
        fixture_id = _q05_fixture(connection, provider_id=provider_id, season_id=season_id, suffix=suffix, kickoff_at=observed + timedelta(days=1), observed_at=observed)
        fetch_id = _q05_fetch(connection, provider_id=provider_id, at=observed)
        connection.execute("UPDATE football.fixtures SET last_source_fetch_id=%s,last_seen_at=%s WHERE id=%s", (fetch_id, observed, fixture_id))
        snapshot = _q05_snapshot(now)
        process = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: now)), SyncScheduler(), {"analytics_recalculation": _Q05Dispatch()})
        connection.execute("CREATE FUNCTION ops.q05_fail_window_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'q05 forced window checkpoint failure'; END; $$")
        connection.execute("CREATE TRIGGER q05_fail_window_checkpoint BEFORE INSERT ON ops.sync_scheduler_analytics_checkpoints FOR EACH ROW EXECUTE FUNCTION ops.q05_fail_window_checkpoint()")
        with pytest.raises(psycopg.errors.RaiseException, match="forced window checkpoint failure"):
            _q05_enqueue(process, run_id=run_id, now=now, snapshot=snapshot, provider_id=provider_id, season_id=season_id)
        assert connection.execute("SELECT accepted_at FROM ops.fixture_analytics_recalculation_windows WHERE fixture_id=%s", (fixture_id,)).fetchone() == (None,)
        assert connection.execute("SELECT count(*) FROM ops.sync_work_items WHERE run_id=%s", (run_id,)).fetchone()[0] == 0
        connection.execute("DROP TRIGGER q05_fail_window_checkpoint ON ops.sync_scheduler_analytics_checkpoints")
        connection.execute("DROP FUNCTION ops.q05_fail_window_checkpoint()")
        retry = _q05_enqueue(process, run_id=run_id, now=now, snapshot=_q05_snapshot(now), provider_id=provider_id, season_id=season_id)
        assert len(retry.enqueue_results) == 1 and retry.enqueue_results[0].enqueued


def test_q05_reader_ignores_expired_q04_counters_without_reserving_budget() -> None:
    assert TEST_DB_URL is not None
    suffix, now = uuid.uuid4().hex, datetime(2026, 9, 9, 12, 1, tzinfo=UTC)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _instance, _version, run_id = _q05_scheduler_setup(connection, suffix)
        _q05_policy(connection, provider_id=provider_id, season_id=season_id, work_types=("calendar_refresh", "analytics_recalculation"))
        connection.execute("INSERT INTO ops.sync_scheduler_checkpoints(provider_id,season_id,work_type,last_scheduled_window_end,next_deadline) VALUES(%s,%s,'calendar_refresh',%s,%s)", (provider_id, season_id, now - timedelta(hours=2), now - timedelta(hours=1)))
        observed = now - timedelta(seconds=1)
        fixture_id = _q05_fixture(connection, provider_id=provider_id, season_id=season_id, suffix=suffix, kickoff_at=now + timedelta(days=1), observed_at=observed)
        fetch_id = _q05_fetch(connection, provider_id=provider_id, at=observed)
        connection.execute("UPDATE football.fixtures SET last_source_fetch_id=%s,last_seen_at=%s WHERE id=%s", (fetch_id, observed, fixture_id))
        connection.execute("""INSERT INTO ops.api_football_budget_state(singleton,daily_window,minute_window,daily_used,minute_used)
                              VALUES(true,%s,%s,6000,300)
                              ON CONFLICT(singleton) DO UPDATE SET daily_window=excluded.daily_window,minute_window=excluded.minute_window,daily_used=excluded.daily_used,minute_used=excluded.minute_used""",
                           ((now - timedelta(days=1)).date(), now - timedelta(minutes=1)))
        snapshot = _q05_snapshot(now)
        assert snapshot.budget.daily_window != now.date() and snapshot.budget.minute_window != now.replace(second=0, microsecond=0)
        process = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: now)), SyncScheduler(), {"calendar_refresh": _Q05Dispatch(), "analytics_recalculation": _Q05Dispatch()})
        result = _q05_enqueue(process, run_id=run_id, now=now, snapshot=snapshot, provider_id=provider_id, season_id=season_id)
        assert len(result.enqueue_results) == 2 and all(item.enqueued for item in result.enqueue_results)
        assert connection.execute("SELECT daily_used,minute_used FROM ops.api_football_budget_state WHERE singleton=true").fetchone() == (6000, 300)


def test_q05_reader_process_excludes_old_completed_schedule_near_but_keeps_postponed() -> None:
    assert TEST_DB_URL is not None
    suffix, now = uuid.uuid4().hex, datetime(2026, 9, 9, 12, tzinfo=UTC)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _instance, _version, run_id = _q05_scheduler_setup(connection, suffix)
        _q05_policy(connection, provider_id=provider_id, season_id=season_id, work_types=("schedule_near",))
        connection.execute("UPDATE ops.api_football_budget_state SET daily_window=%s,minute_window=%s WHERE singleton=true", ((now - timedelta(days=1)).date(), now - timedelta(minutes=2)))
        old_id = _q05_fixture(connection, provider_id=provider_id, season_id=season_id, suffix=f"old-{suffix}", kickoff_at=now - timedelta(days=30), lifecycle_state="completed")
        postponed_id = _q05_fixture(connection, provider_id=provider_id, season_id=season_id, suffix=f"postponed-{suffix}", kickoff_at=now + timedelta(days=2), lifecycle_state="postponed")
        snapshot = _q05_snapshot(now)
        process = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: now)), SyncScheduler(), {"schedule_near": _Q05Dispatch()})
        result = _q05_enqueue(process, run_id=run_id, now=now, snapshot=snapshot, provider_id=provider_id, season_id=season_id)
        fixture_ids = {item.work.scope["fixture_id"] for item in result.preview.decisions if item.work_type == "schedule_near" and item.work is not None}
        assert fixture_ids == {postponed_id} and old_id not in fixture_ids
        assert connection.execute("SELECT count(*) FROM ops.sync_work_items WHERE run_id=%s", (run_id,)).fetchone()[0] == 1


def test_q05_process_enqueues_analytics_and_deduplicates_versions() -> None:
    assert TEST_DB_URL is not None
    suffix, now = uuid.uuid4().hex, datetime(2026, 9, 9, 1, tzinfo=UTC)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _instance, _version, run_id = _q05_scheduler_setup(connection, suffix)
        connection.execute("UPDATE ops.competition_sync_policies SET enabled=true,allowed_work_types=ARRAY['analytics_recalculation'],coverage=%s,refresh_intervals=%s WHERE provider_id=%s AND season_id=%s", (Jsonb({'analytics_recalculation': {'state':'covered','observed_on':'2026-09-09'}}), Jsonb({'analytics_recalculation': {'value':1,'unit':'minute'}}), provider_id, season_id))
        policy = PostgresCompetitionSyncPolicyReader(connection).get(provider_id=provider_id, season_id=season_id); assert policy is not None
        process = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: now)), SyncScheduler(), {'analytics_recalculation': _Q05Dispatch()})
        first_observed = now - timedelta(minutes=1)
        first = process.enqueue_due(run_id=run_id, now=now, policies=[policy], schedule_state=[], analytics_inputs=[AnalyticsInputSnapshot(provider_id, season_id, 'fixture:1', 1, first_observed)])
        assert len(first.enqueue_results) == 1 and first.enqueue_results[0].enqueued
        again = process.enqueue_due(run_id=run_id, now=now, policies=[policy], schedule_state=[], analytics_inputs=[AnalyticsInputSnapshot(provider_id, season_id, 'fixture:1', 1, first_observed)])
        assert len(again.enqueue_results) == 1 and not again.enqueue_results[0].enqueued
        newer = process.enqueue_due(run_id=run_id, now=now, policies=[policy], schedule_state=[], analytics_inputs=[AnalyticsInputSnapshot(provider_id, season_id, 'fixture:1', 2, now + timedelta(seconds=1))])
        assert newer.enqueue_results == ()
        newer = process.enqueue_due(run_id=run_id, now=now + timedelta(minutes=2), policies=[policy], schedule_state=[], analytics_inputs=[AnalyticsInputSnapshot(provider_id, season_id, 'fixture:1', 2, now + timedelta(seconds=1))])
        assert len(newer.enqueue_results) == 1 and newer.enqueue_results[0].enqueued
        assert connection.execute("SELECT count(*) FROM ops.sync_work_items WHERE run_id=%s", (run_id,)).fetchone()[0] == 2


def test_q05_two_scheduler_processes_do_not_duplicate_analytics_version() -> None:
    assert TEST_DB_URL is not None
    suffix, now = uuid.uuid4().hex, datetime(2026, 9, 9, 1, tzinfo=UTC)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        provider_id, season_id, _instance, _version, run_id = _q05_scheduler_setup(setup, suffix)
        setup.execute("UPDATE ops.competition_sync_policies SET enabled=true,allowed_work_types=ARRAY['analytics_recalculation'],coverage=%s,refresh_intervals=%s WHERE provider_id=%s AND season_id=%s", (Jsonb({'analytics_recalculation': {'state':'covered','observed_on':'2026-09-09'}}), Jsonb({'analytics_recalculation': {'value':1,'unit':'minute'}}), provider_id, season_id))
        policy = PostgresCompetitionSyncPolicyReader(setup).get(provider_id=provider_id, season_id=season_id); assert policy is not None
    barrier, results, failures = threading.Barrier(2), [], []
    def run() -> None:
        assert TEST_DB_URL is not None
        with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
            process = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: now)), SyncScheduler(), {'analytics_recalculation': _Q05Dispatch()})
            try:
                barrier.wait()
                results.append(process.enqueue_due(run_id=run_id, now=now, policies=[policy], schedule_state=[], analytics_inputs=[AnalyticsInputSnapshot(provider_id, season_id, 'fixture:1', 1, now - timedelta(minutes=1))]))
            except BaseException as error:
                failures.append(error)
    left, right = threading.Thread(target=run), threading.Thread(target=run); left.start(); right.start(); left.join(); right.join()
    assert failures == []
    assert sum(result.enqueue_results[0].enqueued for result in results) == 1
    with psycopg.connect(TEST_DB_URL, autocommit=True) as check:
        assert check.execute("SELECT count(*) FROM ops.sync_work_items WHERE run_id=%s", (run_id,)).fetchone()[0] == 1
        assert check.execute("SELECT count(*) FROM ops.sync_scheduler_analytics_checkpoints WHERE provider_id=%s AND season_id=%s AND entity_key='fixture:1' AND input_version='1'", (provider_id, season_id)).fetchone()[0] == 1


def test_q05_late_analytics_version_does_not_replace_newer_checkpoint() -> None:
    assert TEST_DB_URL is not None
    suffix, now = uuid.uuid4().hex, datetime(2026, 9, 9, 1, tzinfo=UTC)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _instance, _version, run_id = _q05_scheduler_setup(connection, suffix)
        connection.execute("UPDATE ops.competition_sync_policies SET enabled=true,allowed_work_types=ARRAY['analytics_recalculation'],coverage=%s,refresh_intervals=%s WHERE provider_id=%s AND season_id=%s", (Jsonb({'analytics_recalculation': {'state':'covered','observed_on':'2026-09-09'}}), Jsonb({'analytics_recalculation': {'value':1,'unit':'minute'}}), provider_id, season_id))
        policy = PostgresCompetitionSyncPolicyReader(connection).get(provider_id=provider_id, season_id=season_id); assert policy is not None
        process = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: now)), SyncScheduler(), {'analytics_recalculation': _Q05Dispatch()})
        newest = process.enqueue_due(run_id=run_id, now=now, policies=[policy], schedule_state=[], analytics_inputs=[AnalyticsInputSnapshot(provider_id, season_id, 'fixture:1', 2, now - timedelta(minutes=1))])
        late = process.enqueue_due(run_id=run_id, now=now + timedelta(seconds=1), policies=[policy], schedule_state=[], analytics_inputs=[AnalyticsInputSnapshot(provider_id, season_id, 'fixture:1', 1, now - timedelta(minutes=1, seconds=1))])
        assert newest.enqueue_results[0].enqueued and late.enqueue_results[0].enqueued
        versions = connection.execute("SELECT input_version FROM ops.sync_scheduler_analytics_checkpoints WHERE provider_id=%s AND season_id=%s AND entity_key='fixture:1' ORDER BY input_version", (provider_id, season_id)).fetchall()
        assert versions == [('1',), ('2',)]
        assert connection.execute("SELECT count(*) FROM ops.sync_work_items WHERE run_id=%s", (run_id,)).fetchone()[0] == 2


def test_q05_analytics_checkpoint_failure_rolls_back_enqueue_and_retry_is_safe() -> None:
    assert TEST_DB_URL is not None
    suffix, now = uuid.uuid4().hex, datetime(2026, 9, 9, 1, tzinfo=UTC)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _instance, _version, run_id = _q05_scheduler_setup(connection, suffix)
        connection.execute("UPDATE ops.competition_sync_policies SET enabled=true,allowed_work_types=ARRAY['analytics_recalculation'],coverage=%s,refresh_intervals=%s WHERE provider_id=%s AND season_id=%s", (Jsonb({'analytics_recalculation': {'state':'covered','observed_on':'2026-09-09'}}), Jsonb({'analytics_recalculation': {'value':1,'unit':'minute'}}), provider_id, season_id))
        policy = PostgresCompetitionSyncPolicyReader(connection).get(provider_id=provider_id, season_id=season_id); assert policy is not None
        process = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: now)), SyncScheduler(), {'analytics_recalculation': _Q05Dispatch()})
        connection.execute("CREATE FUNCTION ops.q05_fail_analytics_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'q05 forced analytics checkpoint failure'; END; $$")
        connection.execute("CREATE TRIGGER q05_fail_analytics_checkpoint BEFORE INSERT ON ops.sync_scheduler_analytics_checkpoints FOR EACH ROW EXECUTE FUNCTION ops.q05_fail_analytics_checkpoint()")
        with pytest.raises(psycopg.errors.RaiseException, match="forced analytics checkpoint failure"):
            process.enqueue_due(run_id=run_id, now=now, policies=[policy], schedule_state=[], analytics_inputs=[AnalyticsInputSnapshot(provider_id, season_id, 'fixture:1', 1, now - timedelta(minutes=1))])
        assert connection.execute("SELECT count(*) FROM ops.sync_work_items WHERE run_id=%s", (run_id,)).fetchone()[0] == 0
        assert connection.execute("SELECT count(*) FROM ops.sync_scheduler_analytics_checkpoints WHERE provider_id=%s AND season_id=%s", (provider_id, season_id)).fetchone()[0] == 0
        connection.execute("DROP TRIGGER q05_fail_analytics_checkpoint ON ops.sync_scheduler_analytics_checkpoints")
        connection.execute("DROP FUNCTION ops.q05_fail_analytics_checkpoint()")
        retry = process.enqueue_due(run_id=run_id, now=now, policies=[policy], schedule_state=[], analytics_inputs=[AnalyticsInputSnapshot(provider_id, season_id, 'fixture:1', 1, now - timedelta(minutes=1))])
        assert len(retry.enqueue_results) == 1 and retry.enqueue_results[0].enqueued and retry.enqueue_results[0].checkpoint_advanced


def test_q05_analytics_no_handler_and_stale_policy_write_nothing() -> None:
    assert TEST_DB_URL is not None
    suffix, now = uuid.uuid4().hex, datetime(2026, 9, 9, 1, tzinfo=UTC)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _instance, _version, run_id = _q05_scheduler_setup(connection, suffix)
        connection.execute("UPDATE ops.competition_sync_policies SET enabled=true,allowed_work_types=ARRAY['analytics_recalculation'],coverage=%s,refresh_intervals=%s WHERE provider_id=%s AND season_id=%s", (Jsonb({'analytics_recalculation': {'state':'covered','observed_on':'2026-09-09'}}), Jsonb({'analytics_recalculation': {'value':1,'unit':'minute'}}), provider_id, season_id))
        policy = PostgresCompetitionSyncPolicyReader(connection).get(provider_id=provider_id, season_id=season_id); assert policy is not None
        inp = [AnalyticsInputSnapshot(provider_id, season_id, 'fixture:1', 1, now - timedelta(minutes=1))]
        gate = SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: now)
        no_handler = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, gate), SyncScheduler(), {})
        no_handler_preview = no_handler.preview(now=now, policies=[policy], schedule_state=[], analytics_inputs=inp)
        assert next(item for item in no_handler_preview.decisions if item.work_type == 'analytics_recalculation' and item.work is not None).reason == 'handler_unavailable'
        assert no_handler.enqueue_due(run_id=run_id, now=now, policies=[policy], schedule_state=[], analytics_inputs=inp).enqueue_results == ()
        assert connection.execute("SELECT count(*) FROM ops.sync_work_items WHERE run_id=%s", (run_id,)).fetchone()[0] == 0
        assert connection.execute("SELECT count(*) FROM ops.sync_scheduler_analytics_checkpoints WHERE provider_id=%s AND season_id=%s", (provider_id, season_id)).fetchone()[0] == 0
        connection.execute("UPDATE ops.competition_sync_policies SET priority=priority+1 WHERE provider_id=%s AND season_id=%s", (provider_id, season_id))
        process = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, gate), SyncScheduler(), {'analytics_recalculation': _Q05Dispatch()})
        result = process.enqueue_due(run_id=run_id, now=now, policies=[policy], schedule_state=[], analytics_inputs=inp)
        assert result.policy_denials == ('version_changed',)
        assert connection.execute("SELECT count(*) FROM ops.sync_work_items WHERE run_id=%s", (run_id,)).fetchone()[0] == 0
        assert connection.execute("SELECT count(*) FROM ops.sync_scheduler_analytics_checkpoints WHERE provider_id=%s AND season_id=%s", (provider_id, season_id)).fetchone()[0] == 0


def test_q05_analytics_policy_rejection_rolls_back_and_same_connection_recovers() -> None:
    assert TEST_DB_URL is not None
    suffix, now = uuid.uuid4().hex, datetime(2026, 9, 9, 1, tzinfo=UTC)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _instance, _version, run_id = _q05_scheduler_setup(connection, suffix)
        connection.execute("UPDATE ops.competition_sync_policies SET enabled=true,allowed_work_types=ARRAY['analytics_recalculation'],coverage=%s,refresh_intervals=%s WHERE provider_id=%s AND season_id=%s", (Jsonb({'analytics_recalculation': {'state':'covered','observed_on':'2026-09-09'}}), Jsonb({'analytics_recalculation': {'value':1,'unit':'minute'}}), provider_id, season_id))
        reader = PostgresCompetitionSyncPolicyReader(connection); old = reader.get(provider_id=provider_id, season_id=season_id); assert old is not None
        connection.execute("UPDATE ops.competition_sync_policies SET priority=priority+1 WHERE provider_id=%s AND season_id=%s", (provider_id, season_id))
        process = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, SyncPolicyGate(reader, now=lambda: now)), SyncScheduler(), {'analytics_recalculation': _Q05Dispatch()})
        due_input = AnalyticsInputSnapshot(provider_id, season_id, 'fixture:1', 1, now - timedelta(minutes=1))
        stale = process.enqueue_due(run_id=run_id, now=now, policies=[old], schedule_state=[], analytics_inputs=[due_input])
        assert stale.policy_denials == ('version_changed',)
        assert connection.execute("SELECT count(*) FROM ops.sync_work_items WHERE run_id=%s", (run_id,)).fetchone()[0] == 0
        assert connection.execute("SELECT count(*) FROM ops.sync_scheduler_analytics_checkpoints WHERE provider_id=%s AND season_id=%s", (provider_id, season_id)).fetchone()[0] == 0
        fresh = reader.get(provider_id=provider_id, season_id=season_id); assert fresh is not None
        accepted = process.enqueue_due(run_id=run_id, now=now, policies=[fresh], schedule_state=[], analytics_inputs=[due_input])
        assert len(accepted.enqueue_results) == 1 and accepted.enqueue_results[0].enqueued


def test_q05_preview_fingerprint_survives_process_repository_to_locked_sql() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    now = datetime(2026, 9, 9, 1, tzinfo=UTC)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _instance_id, _version, run_id = _q05_scheduler_setup(connection, suffix)
        reader = PostgresCompetitionSyncPolicyReader(connection)
        policy = reader.get(provider_id=provider_id, season_id=season_id)
        assert policy is not None
        state = PeriodicScheduleState(provider_id, season_id, "calendar_refresh", now - timedelta(hours=1), now)
        gate = SyncPolicyGate(reader, now=lambda: now)
        process = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, gate), SyncScheduler(), {"calendar_refresh": _Q05Dispatch()})
        preview = process.preview(now=now, policies=[policy], schedule_state=[state])
        assert preview.planned_jobs and preview.planned_jobs[0].scope["_sync_policy"]["version"] == policy.policy_version  # type: ignore[index]
        connection.execute("UPDATE ops.competition_sync_policies SET priority=priority+1 WHERE provider_id=%s AND season_id=%s", (provider_id, season_id))
        result = process.enqueue_due(run_id=run_id, now=now, policies=[policy], schedule_state=[state])
        assert result.policy_denials == ("version_changed",) and result.enqueue_results == ()
        assert connection.execute("SELECT count(*) FROM ops.sync_work_items WHERE run_id=%s", (run_id,)).fetchone()[0] == 0
        assert connection.execute("SELECT count(*) FROM ops.sync_scheduler_checkpoints WHERE provider_id=%s AND season_id=%s", (provider_id, season_id)).fetchone()[0] == 0


def test_q05_read_only_snapshot_is_consistent_and_next_snapshot_sees_update() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        provider_id, season_id, _instance, version, _run_id = _q05_scheduler_setup(setup, suffix)
    with psycopg.connect(TEST_DB_URL) as reader_connection, psycopg.connect(TEST_DB_URL, autocommit=True) as writer:
        with reader_connection.transaction():
            snapshot = PostgresSchedulerSnapshotReader(reader_connection).read()
            assert next(item for item in snapshot.policies if (item.provider_id, item.season_id) == (provider_id, season_id)).policy_version == version
            writer.execute("UPDATE ops.competition_sync_policies SET priority=priority+1 WHERE provider_id=%s AND season_id=%s", (provider_id, season_id))
            # A later SELECT in this same transaction must retain the reader's
            # snapshot, even though another connection has committed v2.
            assert reader_connection.execute("SELECT policy_version FROM ops.competition_sync_policies WHERE provider_id=%s AND season_id=%s", (provider_id, season_id)).fetchone()[0] == version
    with psycopg.connect(TEST_DB_URL) as next_reader:
        with next_reader.transaction():
            next_snapshot = PostgresSchedulerSnapshotReader(next_reader).read()
            assert next(item for item in next_snapshot.policies if (item.provider_id, item.season_id) == (provider_id, season_id)).policy_version == version + 1


def test_q05_two_schedulers_keep_one_checkpoint_transition_and_stale_candidate_is_safe() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        provider_id, season_id, instance_id, version, run_id = _q05_scheduler_setup(setup, suffix)
    start, end = datetime(2026, 9, 9, tzinfo=UTC), datetime(2026, 9, 9, 1, tzinfo=UTC)
    barrier, outcomes = threading.Barrier(2), []

    def scheduler() -> None:
        assert TEST_DB_URL is not None
        with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
            barrier.wait()
            outcomes.append(_q05_scheduler_transition(
                connection, run_id=run_id, provider_id=provider_id, season_id=season_id,
                policy_instance_id=instance_id, policy_version=version,
                stable_key=f"q05-two-schedulers:{suffix}", window_start=start, window_end=end,
            ))

    first, second = threading.Thread(target=scheduler), threading.Thread(target=scheduler)
    first.start(); second.start(); first.join(); second.join()
    accepted = next(item for item in outcomes if item[1])
    assert accepted[0] is not None
    assert sorted((item[1], item[2]) for item in outcomes) == [(False, False), (True, True)]
    with psycopg.connect(TEST_DB_URL, autocommit=True) as check:
        assert check.execute("SELECT count(*) FROM ops.sync_work_items WHERE stable_key=%s", (f"q05-two-schedulers:{suffix}",)).fetchone()[0] == 1
        assert check.execute(
            "SELECT last_scheduled_window_end,next_deadline FROM ops.sync_scheduler_checkpoints WHERE provider_id=%s AND season_id=%s AND work_type='calendar_refresh'",
            (provider_id, season_id),
        ).fetchone() == (end, end + timedelta(hours=1))


def test_q05_rejects_inconsistent_window_without_moving_checkpoint() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, instance_id, version, run_id = _q05_scheduler_setup(connection, suffix)
        start, end = datetime(2026, 9, 9, tzinfo=UTC), datetime(2026, 9, 9, 1, tzinfo=UTC)
        first = _q05_scheduler_transition(connection, run_id=run_id, provider_id=provider_id, season_id=season_id,
                                          policy_instance_id=instance_id, policy_version=version,
                                          stable_key=f"q05-window-first:{suffix}", window_start=start, window_end=end)
        assert first[0] is not None and first[1:] == (True, True)
        before = connection.execute(
            "SELECT last_scheduled_window_end,next_deadline FROM ops.sync_scheduler_checkpoints WHERE provider_id=%s AND season_id=%s AND work_type='calendar_refresh'",
            (provider_id, season_id),
        ).fetchone()
        with pytest.raises(psycopg.errors.InvalidParameterValue, match="window must start"):
            _q05_scheduler_transition(connection, run_id=run_id, provider_id=provider_id, season_id=season_id,
                                      policy_instance_id=instance_id, policy_version=version,
                                      stable_key=f"q05-window-invalid:{suffix}",
                                      window_start=end + timedelta(minutes=1), window_end=end + timedelta(hours=1),
                                      expected_state=(end, end + timedelta(hours=1)), next_deadline=end + timedelta(hours=2))
        with pytest.raises(psycopg.errors.InvalidParameterValue, match="checkpoint end must equal"):
            _q05_scheduler_transition(connection, run_id=run_id, provider_id=provider_id, season_id=season_id,
                                      policy_instance_id=instance_id, policy_version=version,
                                      stable_key=f"q05-window-end-invalid:{suffix}",
                                      window_start=end, window_end=end + timedelta(hours=1),
                                      expected_state=(end, end + timedelta(hours=1)),
                                      checkpoint_end=end + timedelta(hours=2), next_deadline=end + timedelta(hours=3))
        assert connection.execute(
            "SELECT last_scheduled_window_end,next_deadline FROM ops.sync_scheduler_checkpoints WHERE provider_id=%s AND season_id=%s AND work_type='calendar_refresh'",
            (provider_id, season_id),
        ).fetchone() == before


def test_q05_checkpoint_failure_rolls_back_enqueue_and_retry_is_safe() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    stable_key = f"q05-rollback:{suffix}"
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, instance_id, version, run_id = _q05_scheduler_setup(connection, suffix)
        start, end = datetime(2026, 9, 9, tzinfo=UTC), datetime(2026, 9, 9, 1, tzinfo=UTC)
        connection.execute("CREATE FUNCTION ops.q05_reject_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'forced checkpoint failure'; END $$")
        connection.execute("CREATE TRIGGER q05_reject_checkpoint BEFORE INSERT ON ops.sync_scheduler_checkpoints FOR EACH ROW EXECUTE FUNCTION ops.q05_reject_checkpoint()")
        with pytest.raises(psycopg.errors.RaiseException, match="forced checkpoint failure"):
            _q05_scheduler_transition(connection, run_id=run_id, provider_id=provider_id, season_id=season_id,
                                      policy_instance_id=instance_id, policy_version=version, stable_key=stable_key,
                                      window_start=start, window_end=end)
        assert connection.execute("SELECT count(*) FROM ops.sync_work_items WHERE stable_key=%s", (stable_key,)).fetchone()[0] == 0
        assert connection.execute(
            "SELECT count(*) FROM ops.sync_scheduler_checkpoints WHERE provider_id=%s AND season_id=%s AND work_type='calendar_refresh'",
            (provider_id, season_id),
        ).fetchone()[0] == 0
        connection.execute("DROP TRIGGER q05_reject_checkpoint ON ops.sync_scheduler_checkpoints")
        connection.execute("DROP FUNCTION ops.q05_reject_checkpoint()")
        item_id, enqueued, advanced = _q05_scheduler_transition(
            connection, run_id=run_id, provider_id=provider_id, season_id=season_id,
            policy_instance_id=instance_id, policy_version=version, stable_key=stable_key,
            window_start=start, window_end=end,
        )
        assert item_id is not None and enqueued and advanced
        assert _q05_scheduler_transition(
            connection, run_id=run_id, provider_id=provider_id, season_id=season_id,
            policy_instance_id=instance_id, policy_version=version, stable_key=stable_key,
            window_start=start, window_end=end,
        ) == (None, False, False)


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
        retry_claim = second.execute("SELECT * FROM ops.claim_next_repeatable_sync_work_item_with_lease(%s,%s,%s)", (f"retry-{suffix}", "1 minute", 2)).fetchone()
        assert retry_claim is not None and int(retry_claim[0]) == item_id
        assert int(retry_claim[-1]) > quarantined_token


def test_q03_budget_deferrals_preserve_progress_and_retry_budget_until_success() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    owner = f"q03-budget-{suffix}"
    persisted_checkpoint = {"page": 7, "cursor": "continued"}
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id = int(connection.execute(
            "INSERT INTO source.providers(code,name) VALUES(%s,%s) RETURNING id",
            (f"q03-budget-{suffix}", "Q03 budget deferral"),
        ).fetchone()[0])
        run_id = _run(connection, provider_id, f"q03-budget-{suffix}")
        item_id, _ = _enqueue(
            connection, run_id, f"q03-budget:{suffix}", priority=10_500_000,
            execution_key=f"q03-budget:{suffix}",
        )
        connection.execute(
            "UPDATE ops.sync_work_items SET scope=%s,checkpoint=%s WHERE id=%s",
            (Jsonb({"_sync_policy": {"provider_id": provider_id, "season_id": 1, "work_type": "x", "instance_id": 1, "version": 1}}),
             Jsonb({"page": 1}), item_id),
        )
        class Gate:
            def before_enqueue(self, _request): return type("A", (), {"coverage": None, "refresh_interval": None})()
            def before_execution(self, authorization): return authorization

        worker = RepeatableSyncWorker(
            connection, Gate(), owner,
            heartbeat_connection_factory=lambda: psycopg.connect(TEST_DB_URL),
        )  # type: ignore[arg-type]
        repository = worker.repository

        # One legitimate failed execution remains counted against max_attempts.
        first = repository.claim_next(owner, max_attempts=2)
        assert first is not None and first.id == item_id
        assert repository.requeue(first, owner, {"page": 2}, "ordinary_retry")

        errors = (
            APIFootballBudgetDenied("daily", datetime.now(UTC) + timedelta(hours=1)),
            APIFootballBudgetError("budget unavailable"),
            APIFootballHTTPError(429),
            APIFootballBudgetError("budget unavailable"),
            APIFootballHTTPError(429),
        )
        for wait_number, error in enumerate(errors):
            def fetch(claimed, _authorization):
                assert connection.info.transaction_status.name == "IDLE"
                if wait_number == 0:
                    # Persisted progress may be newer than the claim snapshot.
                    assert claimed.checkpoint == {"page": 2}
                    with connection.transaction():
                        assert repository.checkpoint(claimed, owner, persisted_checkpoint)
                    assert connection.info.transaction_status.name == "IDLE"
                raise error

            assert worker.run_once(fetch, lambda *_args: pytest.fail("apply"), max_attempts=2) is True
            row = connection.execute(
                "SELECT status,checkpoint,attempts,attempts_in_budget,last_error,available_at > clock_timestamp() "
                "FROM ops.sync_work_items WHERE id=%s", (item_id,),
            ).fetchone()
            assert row == ("pending", persisted_checkpoint, wait_number + 2, 1, "budget_pending", True)

            # The deferred item is not claimable before its due time.
            not_due = repository.claim_next(f"early-{suffix}", max_attempts=2)
            assert not_due is None or not_due.id != item_id
            assert connection.execute(
                "SELECT status,attempts,attempts_in_budget FROM ops.sync_work_items WHERE id=%s", (item_id,),
            ).fetchone() == ("pending", wait_number + 2, 1)
            connection.execute(
                "UPDATE ops.sync_work_items SET available_at=clock_timestamp() WHERE id=%s", (item_id,),
            )

    # A new worker connection resumes the same durable item even though its
    # cumulative attempt count is already beyond the per-cycle maximum.
    with psycopg.connect(TEST_DB_URL) as connection:
        worker = RepeatableSyncWorker(
            connection, Gate(), owner,
            heartbeat_connection_factory=lambda: psycopg.connect(TEST_DB_URL),
        )  # type: ignore[arg-type]
        observed: list[object] = []
        assert worker.run_once(
            lambda item, _authorization: observed.append(item.checkpoint) or WorkResult({"done": True}),
            lambda *_args: None,
            max_attempts=2,
        ) is True
        connection.commit()
    assert observed == [persisted_checkpoint]
    with psycopg.connect(TEST_DB_URL, autocommit=True) as verify:
        assert verify.execute(
            "SELECT status,checkpoint,attempts,attempts_in_budget FROM ops.sync_work_items WHERE id=%s", (item_id,),
        ).fetchone() == ("succeeded", {"done": True}, 7, 2)


def test_q03_budget_defer_stale_or_expired_lease_cannot_mutate_item() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    owner = f"q03-budget-fence-{suffix}"
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id = int(connection.execute(
            "INSERT INTO source.providers(code,name) VALUES(%s,%s) RETURNING id",
            (f"q03-budget-fence-{suffix}", "Q03 budget fence"),
        ).fetchone()[0])
        run_id = _run(connection, provider_id, f"q03-budget-fence-{suffix}")
        item_id, _ = _enqueue(
            connection, run_id, f"q03-budget-fence:{suffix}", priority=10_400_000,
            execution_key=f"q03-budget-fence:{suffix}",
        )
        repository = PostgresSyncRepository(connection, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: datetime.now(UTC)))
        item = repository.claim_next(owner, max_attempts=3)
        assert item is not None and item.id == item_id
        stale = LeasedWorkItem(
            item.id, item.run_id, item.scope_key, item.scope, item.checkpoint, item.attempts,
            item.job_type, item.priority, item.stable_key, item.entity_key, item.execution_key,
            item.lease_token - 1,
        )
        before = connection.execute(
            "SELECT status,checkpoint,attempts,attempts_in_budget,last_error,lease_owner,lease_token "
            "FROM ops.sync_work_items WHERE id=%s", (item_id,),
        ).fetchone()
        assert repository.defer_for_budget(stale, owner, delay="1 hour") is False
        assert connection.execute(
            "SELECT status,checkpoint,attempts,attempts_in_budget,last_error,lease_owner,lease_token "
            "FROM ops.sync_work_items WHERE id=%s", (item_id,),
        ).fetchone() == before

        connection.execute(
            "UPDATE ops.sync_work_items SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=%s", (item_id,),
        )
        expired = connection.execute(
            "SELECT status,checkpoint,attempts,attempts_in_budget,last_error,lease_owner,lease_token "
            "FROM ops.sync_work_items WHERE id=%s", (item_id,),
        ).fetchone()
        assert repository.defer_for_budget(item, owner, delay="1 hour") is False
        assert connection.execute(
            "SELECT status,checkpoint,attempts,attempts_in_budget,last_error,lease_owner,lease_token "
            "FROM ops.sync_work_items WHERE id=%s", (item_id,),
        ).fetchone() == expired
        connection.execute(
            "UPDATE ops.sync_work_items SET status='succeeded',lease_owner=NULL,lease_expires_at=NULL WHERE id=%s",
            (item_id,),
        )


def test_q03_budget_defer_rolls_back_pending_transition_when_attempt_debit_fails() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    owner = f"q03-budget-rollback-{suffix}"
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id = int(connection.execute(
            "INSERT INTO source.providers(code,name) VALUES(%s,%s) RETURNING id",
            (f"q03-budget-rollback-{suffix}", "Q03 budget rollback"),
        ).fetchone()[0])
        run_id = _run(connection, provider_id, f"q03-budget-rollback-{suffix}")
        item_id, _ = _enqueue(
            connection, run_id, f"q03-budget-rollback:{suffix}", priority=10_300_000,
            execution_key=f"q03-budget-rollback:{suffix}",
        )
        repository = PostgresSyncRepository(connection, SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: datetime.now(UTC)))
        item = repository.claim_next(owner, max_attempts=3)
        assert item is not None and item.id == item_id
        original_requeue = repository.requeue

        def fail_after_requeue(*args, **kwargs):
            assert original_requeue(*args, **kwargs)
            raise RuntimeError("forced attempt debit failure")

        repository.requeue = fail_after_requeue  # type: ignore[method-assign]
        with pytest.raises(RuntimeError, match="forced attempt debit failure"):
            repository.defer_for_budget(item, owner, delay="1 hour")
        assert connection.execute(
            "SELECT status,checkpoint,attempts,attempts_in_budget,last_error,lease_owner,lease_token "
            "FROM ops.sync_work_items WHERE id=%s", (item_id,),
        ).fetchone() == ("running", {}, 1, 1, None, owner, item.lease_token)
        connection.execute(
            "UPDATE ops.sync_work_items SET status='succeeded',lease_owner=NULL,lease_expires_at=NULL WHERE id=%s",
            (item_id,),
        )


def test_q03_transient_http_retries_consume_attempt_budget_and_quarantine_at_limit() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    owner = f"q03-http-retry-{suffix}"
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id = int(connection.execute(
            "INSERT INTO source.providers(code,name) VALUES(%s,%s) RETURNING id",
            (f"q03-http-retry-{suffix}", "Q03 HTTP retry"),
        ).fetchone()[0])
        run_id = _run(connection, provider_id, f"q03-http-retry-{suffix}")
        item_id, _ = _enqueue(
            connection, run_id, f"q03-http-retry:{suffix}", priority=10_200_000,
            execution_key=f"q03-http-retry:{suffix}",
        )
        connection.execute(
            "UPDATE ops.sync_work_items SET scope=%s,checkpoint=%s WHERE id=%s",
            (Jsonb({"_sync_policy": {"provider_id": provider_id, "season_id": 1, "work_type": "x", "instance_id": 1, "version": 1}}),
             Jsonb({"page": 3}), item_id),
        )

        class Gate:
            def before_enqueue(self, _request): return type("A", (), {"coverage": None, "refresh_interval": None})()
            def before_execution(self, authorization): return authorization

        worker = RepeatableSyncWorker(
            connection, Gate(), owner,
            heartbeat_connection_factory=lambda: psycopg.connect(TEST_DB_URL),
        )  # type: ignore[arg-type]
        for attempt in (1, 2):
            assert worker.run_once(
                lambda *_args: (_ for _ in ()).throw(APIFootballHTTPError(503)),
                lambda *_args: pytest.fail("apply"),
                max_attempts=2,
            ) is True
            if attempt == 1:
                assert connection.execute(
                    "SELECT status,checkpoint,attempts,attempts_in_budget,last_error "
                    "FROM ops.sync_work_items WHERE id=%s", (item_id,),
                ).fetchone() == ("pending", {"page": 3}, attempt, attempt, "provider_http_503")
                connection.execute(
                    "UPDATE ops.sync_work_items SET available_at=clock_timestamp() WHERE id=%s", (item_id,),
                )
            else:
                assert connection.execute(
                    "SELECT status,checkpoint,attempts,attempts_in_budget,quarantine_reason "
                    "FROM ops.sync_work_items WHERE id=%s", (item_id,),
                ).fetchone() == ("quarantined", {"page": 3}, attempt, attempt, "provider_http_503_retry_exhausted")

        claimed = worker.repository.claim_next(f"exhaust-{suffix}", max_attempts=2)
        assert claimed is None or claimed.id != item_id
        assert connection.execute(
            "SELECT status,checkpoint,attempts,attempts_in_budget,quarantine_reason "
            "FROM ops.sync_work_items WHERE id=%s", (item_id,),
        ).fetchone() == ("quarantined", {"page": 3}, 2, 2, "provider_http_503_retry_exhausted")


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


@pytest.mark.parametrize(
    "attack",
    ("non_text", "compound", "comment_in_literal", "line_comment_lf", "line_comment_cr", "line_comment_crlf", "mutable_string_mode"),
)
def test_q03_runner_rejects_untrusted_sql_before_atomic_apply(attack: str) -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        provider = int(setup.execute("INSERT INTO source.providers(code,name) VALUES(%s,%s) RETURNING id", (f"q03-escape-{suffix}", "Q03 escape")).fetchone()[0])
        run_id = _run(setup, provider, f"q03-escape-{suffix}")
        item_id, _ = _enqueue(setup, run_id, f"q03-escape:{suffix}", priority=11_500_000, execution_key=f"q03-escape:{suffix}")
        setup.execute("UPDATE ops.sync_work_items SET scope=%s WHERE id=%s", (Jsonb({"_sync_policy": {"provider_id": provider, "season_id": 1, "work_type": "x", "instance_id": 1, "version": 1}}), item_id))
        setup.execute("CREATE TABLE ops.q03_escape_marker_" + suffix + "(value text PRIMARY KEY)")
    class Gate:
        def before_enqueue(self, request): return type("A", (), {"coverage": None, "refresh_interval": None})()
        def before_execution(self, authorization): return authorization
    table = "ops.q03_escape_marker_" + suffix
    with psycopg.connect(TEST_DB_URL) as connection:
        # A caller-owned INTRANS connection is refused without changing it.
        connection.execute("SELECT 1")
        assert connection.info.transaction_status.name == "INTRANS"
        worker = RepeatableSyncWorker(connection, Gate(), f"escape-{suffix}", heartbeat_connection_factory=lambda: psycopg.connect(TEST_DB_URL))  # type: ignore[arg-type]
        with pytest.raises(RuntimeError, match="IDLE"):
            worker.run_once(lambda *_: pytest.fail("fetch"), lambda *_: pytest.fail("apply"))
        assert connection.info.transaction_status.name == "INTRANS"
        connection.rollback()
        if attack == "mutable_string_mode":
            connection.execute("SET standard_conforming_strings=off")
            connection.commit()
        if attack == "non_text":
            query, error = sql.SQL(f"INSERT INTO {table} VALUES('result')"), TypeError
        elif attack == "compound":
            query, error = f"INSERT INTO {table} VALUES('result'); SELECT 1", RuntimeError
        elif attack == "mutable_string_mode":
            query, error = f"INSERT INTO {table} VALUES ('-- literal\\'); COMMIT; -- '", RuntimeError
        elif attack.startswith("line_comment_"):
            # PostgreSQL ends -- comments at LF, CR, and CRLF.  The statement
            # before each comment must remain inside the guarded transaction.
            ending = {
                "line_comment_lf": "\n",
                "line_comment_cr": "\r",
                "line_comment_crlf": "\r\n",
            }[attack]
            query, error = f"INSERT INTO {table} VALUES('result') -- comment{ending}; COMMIT", RuntimeError
        else:
            # The old regex treated this literal's ``--`` as a comment and
            # passed the following COMMIT through to PostgreSQL.
            query, error = f"INSERT INTO {table} VALUES('-- literal'); COMMIT", RuntimeError
        def apply(writer, *_args):
            if attack == "mutable_string_mode":
                writer.execute("SELECT set_config('standard_conforming_strings', 'on', false)")
            writer.execute(query)
        with pytest.raises(error):
            worker.run_once(
                lambda *_: WorkResult({"completed": attack}, (lambda writer: writer.execute(f"INSERT INTO {table} VALUES('dependent')"),)),
                apply,
            )
    with psycopg.connect(TEST_DB_URL, autocommit=True) as verify:
        assert verify.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0
        assert verify.execute("SELECT status,checkpoint FROM ops.sync_work_items WHERE id=%s", (item_id,)).fetchone() == ("running", {})


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


def test_q03_direct_legacy_requeues_cannot_mutate_repeatable_work() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider = int(connection.execute("INSERT INTO source.providers(code,name) VALUES(%s,%s) RETURNING id", (f"q03-direct-{suffix}", "Q03 direct legacy")).fetchone()[0])
        run_id = _run(connection, provider, f"q03-direct-{suffix}")
        repeatable_id, _ = _enqueue(connection, run_id, f"q03-direct:{suffix}", priority=12_500_000, execution_key=f"q03-direct:{suffix}")
        claim = connection.execute("SELECT * FROM ops.claim_next_repeatable_sync_work_item_with_lease(%s,%s)", (f"owner-{suffix}", "1 minute")).fetchone()
        assert claim is not None and int(claim[0]) == repeatable_id
        cup = PostgresCupQueueRepository("unused", lease_owner=f"owner-{suffix}"); cup._conn_value = connection
        season = PostgresSeasonalSyncRepository("unused", lease_owner=f"owner-{suffix}"); season._connection = connection
        catalogue = CatalogueRepository("unused", lease_owner=f"owner-{suffix}"); catalogue._conn_value = connection
        with pytest.raises(CupQueueError):
            cup.requeue(CupWorkItem(repeatable_id, None, 1, {}), checkpoint={}, error="old", delay_seconds=0)  # type: ignore[arg-type]
        with pytest.raises(SeasonalSyncError):
            season.fail(SeasonalWorkItem(repeatable_id, None), run_token=0, checkpoint={}, error="old")  # type: ignore[arg-type]
        with pytest.raises(CatalogueBootstrapError):
            catalogue.requeue(WorkItem(repeatable_id, None, 1, {}), checkpoint={}, error="old", delay_seconds=0)  # type: ignore[arg-type]
        assert connection.execute("SELECT status FROM ops.sync_work_items WHERE id=%s", (repeatable_id,)).fetchone()[0] == "running"
        legacy_id = int(connection.execute("INSERT INTO ops.sync_work_items(run_id,scope_key,scope) VALUES(%s,%s,%s) RETURNING id", (run_id, f"legacy-direct-{suffix}", Jsonb({}))).fetchone()[0])
        assert connection.execute("SELECT * FROM ops.claim_next_sync_work_item(%s,%s,%s)", (run_id, f"owner-{suffix}", "1 minute")).fetchone() is not None
        cup.requeue(CupWorkItem(legacy_id, None, 1, {}), checkpoint={}, error="legacy", delay_seconds=0)  # type: ignore[arg-type]
        assert connection.execute("SELECT status FROM ops.sync_work_items WHERE id=%s", (legacy_id,)).fetchone()[0] == "pending"
        connection.execute("UPDATE ops.sync_work_items SET status='succeeded' WHERE id=%s", (legacy_id,))
        connection.execute(
            "UPDATE ops.sync_runs SET status='running', lease_owner=%s, lease_token=0, lease_expires_at=clock_timestamp()+interval '1 minute' "
            "WHERE id=(SELECT run_id FROM ops.sync_work_items WHERE id=%s)",
            (f"owner-{suffix}", legacy_id),
        )
        season_legacy = int(connection.execute("INSERT INTO ops.sync_work_items(run_id,scope_key,scope) VALUES(%s,%s,%s) RETURNING id", (run_id, f"legacy-season-{suffix}", Jsonb({}))).fetchone()[0])
        assert connection.execute("SELECT * FROM ops.claim_next_sync_work_item(%s,%s,%s)", (run_id, f"owner-{suffix}", "1 minute")).fetchone() is not None
        season.fail(SeasonalWorkItem(season_legacy, None), run_token=0, checkpoint={}, error="legacy")  # type: ignore[arg-type]
        assert connection.execute("SELECT status FROM ops.sync_work_items WHERE id=%s", (season_legacy,)).fetchone()[0] == "failed"
        catalogue_legacy = int(connection.execute("INSERT INTO ops.sync_work_items(run_id,scope_key,scope) VALUES(%s,%s,%s) RETURNING id", (run_id, f"legacy-catalogue-{suffix}", Jsonb({}))).fetchone()[0])
        assert connection.execute("SELECT * FROM ops.claim_next_sync_work_item(%s,%s,%s)", (run_id, f"owner-{suffix}", "1 minute")).fetchone() is not None
        catalogue.requeue(WorkItem(catalogue_legacy, None, 1, {}), checkpoint={}, error="legacy", delay_seconds=0)  # type: ignore[arg-type]
        assert connection.execute("SELECT status FROM ops.sync_work_items WHERE id=%s", (catalogue_legacy,)).fetchone()[0] == "pending"


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
            with psycopg.connect(TEST_DB_URL, autocommit=True) as observer:
                first_expiry = observer.execute("SELECT lease_expires_at FROM ops.sync_work_items WHERE id=%s", (item_id,)).fetchone()[0]
            time.sleep(0.05)
            assert heartbeat_connections
            with psycopg.connect(TEST_DB_URL, autocommit=True) as observer:
                renewed_expiry = observer.execute("SELECT lease_expires_at FROM ops.sync_work_items WHERE id=%s", (item_id,)).fetchone()[0]
            assert renewed_expiry > first_expiry
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
        worker = RepeatableSyncWorker(connection, Gate(), f"canon-{suffix}", heartbeat_connection_factory=lambda: psycopg.connect(TEST_DB_URL))  # type: ignore[arg-type]
        with pytest.raises(RuntimeError):
            worker.run_once(lambda *_: WorkResult({}, (dependent_then_fail,)), apply)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as verify:
        assert verify.execute(f"SELECT count(*) FROM ops.{table}").fetchone()[0] == 0
        assert verify.execute("SELECT status FROM ops.sync_work_items WHERE id=%s", (item_id,)).fetchone()[0] == "running"
        token = verify.execute("SELECT lease_token FROM ops.sync_work_items WHERE id=%s", (item_id,)).fetchone()[0]
        verify.execute("SELECT ops.requeue_repeatable_sync_work_item(%s,%s,%s,%s,%s,%s,%s)", (item_id, f"canon-{suffix}", token, Jsonb({}), "retry", "0 seconds", False))
    with psycopg.connect(TEST_DB_URL) as connection:
        worker = RepeatableSyncWorker(connection, Gate(), f"canon-{suffix}", heartbeat_connection_factory=lambda: psycopg.connect(TEST_DB_URL))  # type: ignore[arg-type]
        worker.run_once(lambda *_: WorkResult({}, (lambda writer: writer.execute(f"INSERT INTO ops.{table} VALUES('dependent')"),)), apply)
        connection.commit()
        connection.execute("UPDATE ops.sync_work_items SET available_at=clock_timestamp()+interval '1 hour' WHERE status='pending' AND id<>%s", (item_id,))
        connection.commit()
        assert worker.run_once(lambda *_: pytest.fail("duplicate fetch"), lambda *_: pytest.fail("duplicate apply")) is False
    with psycopg.connect(TEST_DB_URL, autocommit=True) as verify:
        assert verify.execute(f"SELECT count(*) FROM ops.{table}").fetchone()[0] == 2
        assert verify.execute("SELECT status FROM ops.sync_work_items WHERE id=%s", (item_id,)).fetchone()[0] == "succeeded"
