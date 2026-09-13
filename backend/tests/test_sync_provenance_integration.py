from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import multiprocessing
import json
import os
import threading
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
import pytest
from psycopg.types.json import Jsonb

from app.api_football import APIFootballClient, APIFootballResponse
from app.importer.raw_spool import RawSpool, RawSpoolArtifact, RawSpoolCapacityError, RawSpoolError
from app.importer.season_bootstrap import BaseRequest
from app.sync.dispatch import Q03DispatchRegistry
from app.sync.policies import (
    AuthorizedSyncWork,
    PostgresCompetitionSyncPolicyReader,
    SyncPolicyGate,
    SyncWorkRequest,
)
from app.sync.provenance import ProviderProvenance, ProvenanceError, RawFetchCapture
from app.sync.repository import LeasedWorkItem, PeriodicWork, PostgresSyncRepository
from app.sync.worker import LeaseLost, RepeatableSyncWorker, WorkResult


TEST_DB_URL = os.environ.get("Q06_PROVENANCE_TEST_DB_URL")
pytestmark = pytest.mark.skipif(not TEST_DB_URL, reason="Q06_PROVENANCE_TEST_DB_URL is not configured")


def _response(payload: dict[str, object]) -> APIFootballResponse:
    body = json.dumps(payload, separators=(",", ":")).encode()
    return APIFootballResponse(payload, body, 200, {})


def _item(connection: psycopg.Connection, suffix: str) -> tuple[LeasedWorkItem, AuthorizedSyncWork]:
    provider_id = int(connection.execute(
        "INSERT INTO source.providers(code,name) VALUES(%s,%s) RETURNING id", (f"q06-{suffix}", "Q06 integration provider"),
    ).fetchone()[0])
    run_id = int(connection.execute(
        "INSERT INTO ops.sync_runs(provider_id,operation) VALUES(%s,%s) RETURNING id", (provider_id, "q06-integration"),
    ).fetchone()[0])
    work_item_id = int(connection.execute(
        """INSERT INTO ops.sync_work_items(run_id,scope_key,scope,job_type,stable_key,entity_key,execution_key)
           VALUES(%s,%s,%s,'q06',%s,%s,%s) RETURNING id""",
        (run_id, f"q06:{suffix}", Jsonb({"league": 39}), f"q06:stable:{suffix}", f"q06:entity:{suffix}", f"q06:execution:{suffix}"),
    ).fetchone()[0])
    return (
        LeasedWorkItem(work_item_id, run_id, f"q06:{suffix}", {"league": 39}, {}, 1, "q06", 0,
                       f"q06:stable:{suffix}", f"q06:entity:{suffix}", f"q06:execution:{suffix}", 1),
        AuthorizedSyncWork(SyncWorkRequest(provider_id, 1, "q06"), 1, 1, None, None),
    )


def _capture(
    *,
    endpoint: str = "/fixtures",
    params: dict[str, object] | None = None,
    payload: dict[str, object] | None = None,
    scope: dict[str, object] | None = None,
    purpose: str = "scheduled_refresh",
    retention_class: str = "standard",
) -> RawFetchCapture:
    now = datetime.now(UTC)
    return RawFetchCapture(
        endpoint, params or {"league": 39}, _response(payload or {
            "get": "fixtures", "parameters": {"league": "39"}, "errors": {}, "results": 0,
            "paging": {"current": 1, "total": 1}, "response": [],
        }), now, now, "fixtures-v1", purpose=purpose, retention_class=retention_class, scope=scope,
    )


def _enqueue_q06_runner_work(
    connection: psycopg.Connection, suffix: str,
) -> tuple[int, int, int, int, SyncPolicyGate]:
    provider_id = int(connection.execute(
        "INSERT INTO source.providers(code,name) VALUES(%s,%s) RETURNING id", (f"q06-runner-{suffix}", "Q06 runner provider"),
    ).fetchone()[0])
    country_id = int(connection.execute("INSERT INTO football.countries(name) VALUES(%s) RETURNING id", (f"Q06 {suffix}",)).fetchone()[0])
    league_id = int(connection.execute(
        "INSERT INTO football.leagues(name,country_id,competition_type) VALUES(%s,%s,'league') RETURNING id",
        (f"Q06 {suffix}", country_id),
    ).fetchone()[0])
    connection.execute("INSERT INTO source.league_provider_refs(provider_id,external_id,league_id) VALUES(%s,%s,%s)", (provider_id, f"q06-{suffix}", league_id))
    season_id = int(connection.execute(
        "INSERT INTO football.seasons(league_id,start_year,label) VALUES(%s,2026,%s) RETURNING id", (league_id, f"Q06 {suffix}"),
    ).fetchone()[0])
    connection.execute("INSERT INTO source.season_provider_refs(provider_id,league_external_id,external_season,season_id) VALUES(%s,%s,2026,%s)", (provider_id, f"q06-{suffix}", season_id))
    connection.execute(
        """INSERT INTO ops.competition_sync_policies(
               provider_id,season_id,enabled,allowed_work_types,coverage,refresh_intervals
           ) VALUES(%s,%s,true,ARRAY['fixtures'],%s,%s)""",
        (provider_id, season_id, Jsonb({"fixtures": {"state": "covered", "observed_on": "2026-09-13"}}), Jsonb({"fixtures": {"value": 1, "unit": "minute"}})),
    )
    run_id = int(connection.execute("INSERT INTO ops.sync_runs(provider_id,operation) VALUES(%s,%s) RETURNING id", (provider_id, f"q06-runner-{suffix}")).fetchone()[0])
    gate = SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: datetime.now(UTC))
    enqueued = PostgresSyncRepository(connection, gate).enqueue_periodic(
        run_id,
        PeriodicWork(provider_id, season_id, "fixtures", "fixture:42", datetime.now(UTC), datetime.now(UTC) + timedelta(minutes=1), 2_000_000_000, {"fixture": 42}),
        available_at=datetime.now(UTC) - timedelta(seconds=1),
    )
    return provider_id, season_id, run_id, enqueued.work_item_id, gate


def _requeue_for_replay(connection: psycopg.Connection, work_item_id: int, owner: str) -> None:
    lease_token = connection.execute("SELECT lease_token FROM ops.sync_work_items WHERE id=%s", (work_item_id,)).fetchone()[0]
    assert connection.execute(
        "SELECT ops.requeue_repeatable_sync_work_item(%s,%s,%s,%s,%s,%s,%s)",
        (work_item_id, owner, lease_token, Jsonb({}), "replay after domain failure", "0 seconds", False),
    ).fetchone()[0] is True


def _q06_subjects(
    connection: psycopg.Connection, provider_id: int, season_id: int, suffix: str,
) -> tuple[int, int]:
    home_id = int(connection.execute(
        "INSERT INTO football.teams(name) VALUES(%s) RETURNING id", (f"Q06 subject home {suffix}",),
    ).fetchone()[0])
    away_id = int(connection.execute(
        "INSERT INTO football.teams(name) VALUES(%s) RETURNING id", (f"Q06 subject away {suffix}",),
    ).fetchone()[0])
    connection.execute(
        "INSERT INTO football.season_teams(season_id,team_id) VALUES(%s,%s),(%s,%s)",
        (season_id, home_id, season_id, away_id),
    )
    fixture_id = int(connection.execute(
        """INSERT INTO football.fixtures(
               season_id,home_team_id,away_team_id,kickoff_at,lifecycle_state,first_seen_at,last_seen_at
           ) VALUES(%s,%s,%s,%s,'scheduled',%s,%s) RETURNING id""",
        (season_id, home_id, away_id, datetime.now(UTC) + timedelta(days=1), datetime.now(UTC), datetime.now(UTC)),
    ).fetchone()[0])
    connection.execute(
        "INSERT INTO source.fixture_provider_refs(provider_id,external_id,fixture_id) VALUES(%s,%s,%s)",
        (provider_id, f"q06-subject-fixture-{suffix}", fixture_id),
    )
    connection.execute(
        "INSERT INTO source.team_provider_refs(provider_id,external_id,team_id) VALUES(%s,%s,%s)",
        (provider_id, f"q06-subject-team-{suffix}", home_id),
    )
    return fixture_id, home_id


def _concurrent_db_cleanup(url: str, root: str, barrier: Any, results: Any) -> None:
    try:
        with psycopg.connect(url, autocommit=True) as connection:
            spool = RawSpool(Path(root), max_bytes=1)
            ProviderProvenance(connection, response_contains_api_key=lambda _body: False, spool=spool)
            barrier.wait(timeout=5)
            spool.enforce_limit()
            results.put("completed")
    except RawSpoolCapacityError:
        results.put("capacity")
    except BaseException as error:
        results.put(type(error).__name__)


def test_credential_bearing_raw_never_reaches_spool_or_database(tmp_path: Path) -> None:
    assert TEST_DB_URL is not None
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        item, authorization = _item(connection, uuid.uuid4().hex)
        client = APIFootballClient("q06-test-secret")
        spool = RawSpool(tmp_path / "spool")
        provenance = ProviderProvenance(connection, response_contains_api_key=client.response_contains_api_key, spool=spool)
        try:
            capture = _capture(payload={"response": ["q06-test-secret"], "results": 1, "paging": {"current": 1, "total": 1}})
            with pytest.raises(ProvenanceError, match="configured credential") as error:
                provenance.persist(item, authorization, (capture,))
        finally:
            asyncio.run(client.aclose())

        assert "q06-test-secret" not in str(error.value)
        assert not spool.root.exists()
        assert connection.execute(
            "SELECT count(*) FROM source.provider_fetches WHERE sync_work_item_id=%s", (item.id,),
        ).fetchone()[0] == 0


@pytest.mark.parametrize("location", ("parameters", "scope"), ids=("parameter-value", "nested-scope"))
def test_q06_credential_metadata_is_rejected_without_output(tmp_path: Path, location: str, caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture[str]) -> None:
    assert TEST_DB_URL is not None
    secret = "q06-metadata-secret"
    values = {"parameters": {"note": secret}, "scope": {"nested": {"token": secret}}}
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        item, authorization = _item(connection, uuid.uuid4().hex)
        spool = RawSpool(tmp_path / "spool")
        with pytest.raises(ProvenanceError) as error:
            ProviderProvenance(connection, response_contains_api_key=lambda body: secret.encode() in body, spool=spool).persist(
                item, authorization, (_capture(params=values[location] if location == "parameters" else None, scope=values[location] if location == "scope" else None),),
            )
        assert secret not in str(error.value)
        assert secret not in caplog.text
        assert secret not in capsys.readouterr().err
        assert not spool.root.exists()


def test_q06_detector_exception_does_not_expose_exception_chain(tmp_path: Path, caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture[str]) -> None:
    assert TEST_DB_URL is not None
    secret = "q06-detector-secret"
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        item, authorization = _item(connection, uuid.uuid4().hex)
        spool = RawSpool(tmp_path / "spool")
        def detector(_body: bytes) -> bool:
            raise RuntimeError(secret)
        with pytest.raises(ProvenanceError) as error:
            ProviderProvenance(connection, response_contains_api_key=detector, spool=spool).persist(item, authorization, (_capture(),))
        assert error.value.__cause__ is None
        assert secret not in repr(error.value)
        assert secret not in caplog.text
        assert secret not in capsys.readouterr().err
        assert not spool.root.exists()


def test_q03_runner_propagates_spool_capacity_without_contract_transition(tmp_path: Path) -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        _provider_id, _season_id, _run_id, work_item_id, gate = _enqueue_q06_runner_work(connection, suffix)
        spool = RawSpool(tmp_path / "spool", max_bytes=1)
        recorder = ProviderProvenance(connection, response_contains_api_key=lambda _body: False, spool=spool)
        worker = RepeatableSyncWorker(
            connection, gate, f"q06-capacity-{suffix}",
            heartbeat_connection_factory=lambda: psycopg.connect(TEST_DB_URL, autocommit=True),
            provenance=recorder,
        )
        with pytest.raises(RawSpoolCapacityError):
            worker.run_once(
                lambda *_args: WorkResult({}, raw_fetches=(_capture(),)),
                lambda *_args: (_ for _ in ()).throw(AssertionError("domain write must not run")),
            )
        state = connection.execute(
            "SELECT status,checkpoint FROM ops.sync_work_items WHERE id=%s", (work_item_id,),
        ).fetchone()
        assert state[0] == "running"
        assert state[1] == {}


def test_q03_runner_propagates_recovery_capacity_without_contract_transition(tmp_path: Path) -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _run_id, work_item_id, gate = _enqueue_q06_runner_work(connection, suffix)
        item = PostgresSyncRepository(connection, gate).claim_next(f"q06-recovery-capacity-{suffix}")
        assert item is not None and item.id == work_item_id
        authorization = gate.before_enqueue(SyncWorkRequest(provider_id, season_id, "fixtures"))
        spool = RawSpool(tmp_path / "spool")
        capture = _capture()
        directory = spool.work_item_request_directory(work_item_id=item.id, attempt=item.attempts, request_number=1)
        spool.stage(directory, RawSpoolArtifact(
            BaseRequest(capture.endpoint, dict(capture.params)), capture.response,
            capture.request_started_at, capture.response_received_at, dict(item.scope), item.id,
            item.attempts, capture.normalization_version, capture.purpose, capture.retention_class,
            f"work-item-{item.id}:attempt-{item.attempts}:request-000001",
        ))
        spool._max_bytes = 1  # type: ignore[attr-defined]
        worker = RepeatableSyncWorker(
            connection, gate, f"q06-recovery-capacity-{suffix}",
            heartbeat_connection_factory=lambda: psycopg.connect(TEST_DB_URL, autocommit=True),
            provenance=ProviderProvenance(connection, response_contains_api_key=lambda _body: False, spool=spool),
        )
        worker.repository.claim_next = lambda *_args, **_kwargs: item  # type: ignore[method-assign]

        with pytest.raises(RawSpoolCapacityError):
            worker.run_once(
                lambda *_args: (_ for _ in ()).throw(AssertionError("recovery capacity must stop before HTTP")),
                lambda *_args: (_ for _ in ()).throw(AssertionError("recovery capacity must stop before domain writes")),
            )

        assert connection.execute(
            "SELECT status,checkpoint FROM ops.sync_work_items WHERE id=%s", (item.id,),
        ).fetchone() == ("running", {})
        assert connection.execute(
            "SELECT count(*) FROM source.provider_fetches WHERE sync_work_item_id=%s", (item.id,),
        ).fetchone()[0] == 1
        assert not (directory / ".durable").exists()


def test_q06_capacity_eviction_uses_each_candidate_own_succeeded_work_item(tmp_path: Path) -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _run_id, first_work_item_id, gate = _enqueue_q06_runner_work(connection, suffix)
        first = PostgresSyncRepository(connection, gate).claim_next(f"q06-evict-first-{suffix}")
        assert first is not None and first.id == first_work_item_id
        authorization = gate.before_enqueue(SyncWorkRequest(provider_id, season_id, "fixtures"))
        spool = RawSpool(tmp_path / "spool")
        recorder = ProviderProvenance(connection, response_contains_api_key=lambda _body: False, spool=spool)
        recorder.persist(first, authorization, (_capture(),))
        first_directory = spool.work_item_request_directory(work_item_id=first.id, attempt=first.attempts, request_number=1)
        connection.execute(
            "UPDATE ops.sync_work_items SET status='succeeded',lease_owner=NULL,lease_expires_at=NULL,finished_at=clock_timestamp() WHERE id=%s",
            (first.id,),
        )
        second_provider_id, second_season_id, _run_id, second_work_item_id, second_gate = _enqueue_q06_runner_work(connection, f"second-{suffix}")
        second = PostgresSyncRepository(connection, second_gate).claim_next(f"q06-evict-second-{suffix}")
        assert second is not None and second.id == second_work_item_id
        spool._max_bytes = sum(  # type: ignore[attr-defined]
            path.stat().st_size for path in spool.root.rglob("*")
            if path.is_file() and path.name != ".spool.lock"
        ) + 1

        recorder.persist(second, second_gate.before_enqueue(SyncWorkRequest(second_provider_id, second_season_id, "fixtures")), (_capture(),))

        second_directory = spool.work_item_request_directory(work_item_id=second.id, attempt=second.attempts, request_number=1)
        assert not first_directory.exists()
        assert second_directory.exists()


def test_q06_fallback_item_scope_is_checked_for_configured_credential(tmp_path: Path) -> None:
    assert TEST_DB_URL is not None
    secret = "q06-fallback-scope-secret"
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        item, authorization = _item(connection, uuid.uuid4().hex)
        scoped_item = dataclasses.replace(item, scope={"league": secret})
        spool = RawSpool(tmp_path / "spool")
        with pytest.raises(ProvenanceError, match="configured credential") as error:
            ProviderProvenance(
                connection, response_contains_api_key=lambda body: secret.encode() in body, spool=spool,
            ).persist(scoped_item, authorization, (_capture(scope=None),))

        assert secret not in str(error.value)
        assert not spool.root.exists()
        assert connection.execute(
            "SELECT count(*) FROM source.provider_fetches WHERE sync_work_item_id=%s", (item.id,),
        ).fetchone()[0] == 0


def test_q06_succeeded_exact_db_copy_allows_local_purge_without_db_changes(tmp_path: Path) -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _run_id, work_item_id, gate = _enqueue_q06_runner_work(connection, suffix)
        item = PostgresSyncRepository(connection, gate).claim_next(f"q06-purge-{suffix}")
        assert item is not None and item.id == work_item_id
        authorization = gate.before_enqueue(SyncWorkRequest(provider_id, season_id, "fixtures"))
        spool = RawSpool(tmp_path / "spool")
        recorder = ProviderProvenance(connection, response_contains_api_key=lambda _body: False, spool=spool)
        capture = _capture()
        persisted = recorder.persist(item, authorization, (capture,))
        directory = spool.work_item_request_directory(work_item_id=item.id, attempt=item.attempts, request_number=1)
        before = connection.execute(
            "SELECT count(*),max(id) FROM source.provider_fetches WHERE sync_work_item_id=%s", (item.id,),
        ).fetchone()
        connection.execute(
            "UPDATE ops.sync_work_items SET status='succeeded', lease_owner=NULL, lease_expires_at=NULL, finished_at=clock_timestamp() WHERE id=%s",
            (item.id,),
        )
        spool._max_bytes = max(1, sum(path.stat().st_size for path in spool.root.rglob("*") if path.is_file()) - 1)  # type: ignore[attr-defined]
        spool.enforce_limit()
        assert not directory.exists()
        assert connection.execute(
            "SELECT count(*),max(id) FROM source.provider_fetches WHERE sync_work_item_id=%s", (item.id,),
        ).fetchone() == before
        assert connection.execute(
            "SELECT inline_body FROM source.provider_raw_payloads WHERE fetch_id=%s", (persisted[0].fetch_id,),
        ).fetchone()[0] == capture.response.raw_body


def test_q06_new_process_verifies_existing_durable_candidate_after_restart(tmp_path: Path) -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    spool = RawSpool(tmp_path / "spool")
    with psycopg.connect(TEST_DB_URL, autocommit=True) as first:
        provider_id, season_id, _run_id, work_item_id, gate = _enqueue_q06_runner_work(first, suffix)
        item = PostgresSyncRepository(first, gate).claim_next(f"q06-restart-purge-{suffix}")
        assert item is not None and item.id == work_item_id
        authorization = gate.before_enqueue(SyncWorkRequest(provider_id, season_id, "fixtures"))
        capture = _capture()
        persisted = ProviderProvenance(first, response_contains_api_key=lambda _body: False, spool=spool).persist(item, authorization, (capture,))
        directory = spool.work_item_request_directory(work_item_id=item.id, attempt=item.attempts, request_number=1)
        first.execute("UPDATE ops.sync_work_items SET status='succeeded', lease_owner=NULL, lease_expires_at=NULL, finished_at=clock_timestamp() WHERE id=%s", (item.id,))
        before = first.execute("SELECT count(*) FROM source.provider_fetches WHERE id=%s", (persisted[0].fetch_id,)).fetchone()[0]
    with psycopg.connect(TEST_DB_URL, autocommit=True) as second:
        restarted = RawSpool(spool.root)
        ProviderProvenance(second, response_contains_api_key=lambda _body: False, spool=restarted)
        restarted._max_bytes = 1  # type: ignore[attr-defined]
        restarted.enforce_limit()
        assert not directory.exists()
        assert second.execute("SELECT count(*) FROM source.provider_fetches WHERE id=%s", (persisted[0].fetch_id,)).fetchone()[0] == before


def test_q06_cleanup_journal_without_db_copy_never_calls_unlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _run_id, work_item_id, gate = _enqueue_q06_runner_work(connection, suffix)
        item = PostgresSyncRepository(connection, gate).claim_next(f"q06-arbitrary-journal-{suffix}")
        assert item is not None and item.id == work_item_id
        authorization = gate.before_enqueue(SyncWorkRequest(provider_id, season_id, "fixtures"))
        spool = RawSpool(tmp_path / "spool")
        ProviderProvenance(connection, response_contains_api_key=lambda _body: False, spool=spool)
        capture = _capture()
        directory = spool.work_item_request_directory(work_item_id=item.id, attempt=item.attempts, request_number=1)
        spool.stage(directory, RawSpoolArtifact(
            BaseRequest(capture.endpoint, dict(capture.params)), capture.response,
            capture.request_started_at, capture.response_received_at, dict(item.scope),
            item.id, item.attempts, capture.normalization_version, capture.purpose,
            capture.retention_class, f"work-item-{item.id}:attempt-{item.attempts}:request-000001",
        ))
        spool.mark_durable(directory)
        connection.execute(
            "UPDATE ops.sync_work_items SET status='succeeded', lease_owner=NULL, lease_expires_at=NULL, finished_at=clock_timestamp() WHERE id=%s",
            (item.id,),
        )
        quarantine = directory.with_name(".purging-a11ce")
        directory.rename(quarantine)
        request_path = quarantine / "fixtures.request.json"
        proof = quarantine / ".cleanup-proof.json"
        proof.write_bytes(request_path.read_bytes())
        (quarantine / ".durable").unlink()
        before = {path.name: path.read_bytes() for path in quarantine.iterdir()}
        unlinked: list[Path] = []
        original_unlink = Path.unlink

        def record_unlink(path: Path, *args: object, **kwargs: object) -> None:
            unlinked.append(path)
            original_unlink(path, *args, **kwargs)

        monkeypatch.setattr(Path, "unlink", record_unlink)
        spool._max_bytes = 1  # type: ignore[attr-defined]
        with pytest.raises(RawSpoolCapacityError):
            spool.enforce_limit()

        assert unlinked == []
        assert quarantine.is_dir()
        assert {path.name: path.read_bytes() for path in quarantine.iterdir()} == before


def test_q06_legacy_durable_artifact_without_cleanup_manifest_is_preserved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _run_id, work_item_id, gate = _enqueue_q06_runner_work(connection, suffix)
        item = PostgresSyncRepository(connection, gate).claim_next(f"q06-legacy-manifest-{suffix}")
        assert item is not None and item.id == work_item_id
        authorization = gate.before_enqueue(SyncWorkRequest(provider_id, season_id, "fixtures"))
        spool = RawSpool(tmp_path / "spool")
        capture = _capture()
        ProviderProvenance(connection, response_contains_api_key=lambda _body: False, spool=spool).persist(
            item, authorization, (capture,),
        )
        directory = spool.work_item_request_directory(work_item_id=item.id, attempt=item.attempts, request_number=1)
        request_path = directory / "fixtures.request.json"
        metadata = json.loads(request_path.read_text(encoding="utf-8"))
        metadata.pop("cleanup_manifest")
        request_path.write_text(json.dumps(metadata, sort_keys=True, separators=(",", ":")), encoding="utf-8")
        connection.execute(
            "UPDATE ops.sync_work_items SET status='succeeded', lease_owner=NULL, lease_expires_at=NULL, finished_at=clock_timestamp() WHERE id=%s",
            (item.id,),
        )
        before = {path.name: path.read_bytes() for path in directory.iterdir()}
        unlinked: list[Path] = []
        original_unlink = Path.unlink

        def record_unlink(path: Path, *args: object, **kwargs: object) -> None:
            unlinked.append(path)
            original_unlink(path, *args, **kwargs)

        monkeypatch.setattr(Path, "unlink", record_unlink)
        spool._max_bytes = 1  # type: ignore[attr-defined]
        with pytest.raises(RawSpoolError, match="cleanup proof"):
            spool.enforce_limit()

        assert unlinked == []
        assert directory.is_dir()
        assert {path.name: path.read_bytes() for path in directory.iterdir()} == before
        assert tuple(directory.parent.glob(".purging-*")) == ()


@pytest.mark.parametrize("state", ("pending", "running", "quarantine"), ids=("pending", "running", "quarantine"))
def test_q06_nonterminal_queue_states_keep_durable_spool(tmp_path: Path, state: str) -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    spool = RawSpool(tmp_path / "spool")
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _run_id, work_item_id, gate = _enqueue_q06_runner_work(connection, suffix)
        item = PostgresSyncRepository(connection, gate).claim_next(f"q06-state-{state}-{suffix}")
        assert item is not None and item.id == work_item_id
        authorization = gate.before_enqueue(SyncWorkRequest(provider_id, season_id, "fixtures"))
        recorder = ProviderProvenance(connection, response_contains_api_key=lambda _body: False, spool=spool)
        recorder.persist(item, authorization, (_capture(),))
        directory = spool.work_item_request_directory(work_item_id=item.id, attempt=item.attempts, request_number=1)
        if state == "pending":
            connection.execute("UPDATE ops.sync_work_items SET status='pending', lease_owner=NULL, lease_expires_at=NULL, available_at=clock_timestamp()+interval '1 hour' WHERE id=%s", (item.id,))
        elif state == "quarantine":
            connection.execute("UPDATE ops.sync_work_items SET status='quarantined', lease_owner=NULL, lease_expires_at=NULL, quarantined_at=clock_timestamp(), quarantine_reason='q06 test' WHERE id=%s", (item.id,))
        ProviderProvenance(connection, response_contains_api_key=lambda _body: False, spool=spool)
        spool._max_bytes = 1  # type: ignore[attr-defined]
        with pytest.raises(RawSpoolCapacityError):
            spool.enforce_limit()
        assert directory.exists()


def test_q06_concurrent_db_verified_cleanup_is_serial_and_idempotent(tmp_path: Path) -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    spool = RawSpool(tmp_path / "spool")
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _run_id, work_item_id, gate = _enqueue_q06_runner_work(connection, suffix)
        item = PostgresSyncRepository(connection, gate).claim_next(f"q06-concurrent-cleanup-{suffix}")
        assert item is not None and item.id == work_item_id
        authorization = gate.before_enqueue(SyncWorkRequest(provider_id, season_id, "fixtures"))
        capture = _capture()
        persisted = ProviderProvenance(connection, response_contains_api_key=lambda _body: False, spool=spool).persist(item, authorization, (capture,))
        directory = spool.work_item_request_directory(work_item_id=item.id, attempt=item.attempts, request_number=1)
        connection.execute("UPDATE ops.sync_work_items SET status='succeeded', lease_owner=NULL, lease_expires_at=NULL, finished_at=clock_timestamp() WHERE id=%s", (item.id,))
        before = connection.execute("SELECT count(*),max(id) FROM source.provider_fetches WHERE sync_work_item_id=%s", (item.id,)).fetchone()
    context = multiprocessing.get_context("spawn")
    barrier, results = context.Barrier(2), context.Queue()
    processes = [context.Process(target=_concurrent_db_cleanup, args=(TEST_DB_URL, str(spool.root), barrier, results)) for _ in range(2)]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=15)
        assert not process.is_alive()
        assert process.exitcode == 0
    assert sorted(results.get(timeout=2) for _ in processes) in (["completed", "completed"], ["capacity", "completed"])
    assert not directory.exists()
    with psycopg.connect(TEST_DB_URL, autocommit=True) as observer:
        assert observer.execute("SELECT count(*),max(id) FROM source.provider_fetches WHERE sync_work_item_id=%s", (item.id,)).fetchone() == before
        assert observer.execute("SELECT count(*),max(fetch_id) FROM source.provider_raw_payloads WHERE fetch_id=%s", (persisted[0].fetch_id,)).fetchone() == (1, persisted[0].fetch_id)


def test_q06_cleanup_holds_db_payload_lock_until_local_unlink_finishes(tmp_path: Path) -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    spool = RawSpool(tmp_path / "spool")
    entered, release = threading.Event(), threading.Event()
    failures: list[BaseException] = []
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _run_id, work_item_id, gate = _enqueue_q06_runner_work(connection, suffix)
        item = PostgresSyncRepository(connection, gate).claim_next(f"q06-payload-lock-{suffix}")
        assert item is not None and item.id == work_item_id
        authorization = gate.before_enqueue(SyncWorkRequest(provider_id, season_id, "fixtures"))
        capture = _capture()
        persisted = ProviderProvenance(connection, response_contains_api_key=lambda _body: False, spool=spool).persist(
            item, authorization, (capture,),
        )
        directory = spool.work_item_request_directory(work_item_id=item.id, attempt=item.attempts, request_number=1)
        connection.execute(
            "UPDATE ops.sync_work_items SET status='succeeded', lease_owner=NULL, lease_expires_at=NULL, finished_at=clock_timestamp() WHERE id=%s",
            (item.id,),
        )
        original_remove = spool._remove_quarantine_unlocked

        def pause_before_unlink(quarantine: Path) -> None:
            entered.set()
            assert release.wait(timeout=5)
            original_remove(quarantine)

        spool._remove_quarantine_unlocked = pause_before_unlink  # type: ignore[method-assign]
        spool._max_bytes = 1  # type: ignore[attr-defined]

        def clean() -> None:
            try:
                spool.enforce_limit()
            except BaseException as error:
                failures.append(error)

        cleaner = threading.Thread(target=clean)
        cleaner.start()
        assert entered.wait(timeout=5)
        with psycopg.connect(TEST_DB_URL, autocommit=True) as contender:
            contender.execute("SET lock_timeout = '100ms'")
            with pytest.raises(psycopg.errors.LockNotAvailable):
                contender.execute(
                    "UPDATE source.provider_raw_payloads SET inline_body=NULL,purged_at=clock_timestamp() WHERE fetch_id=%s",
                    (persisted[0].fetch_id,),
                )
        release.set()
        cleaner.join(timeout=5)
        assert not cleaner.is_alive()
        assert failures == []
        assert not directory.exists()
        assert connection.execute(
            "SELECT inline_body,purged_at FROM source.provider_raw_payloads WHERE fetch_id=%s", (persisted[0].fetch_id,),
        ).fetchone() == (capture.response.raw_body, None)


@pytest.mark.parametrize("mode", ("metadata", "purged"), ids=("metadata-mismatch", "payload-purged"))
def test_q06_purge_keeps_unproven_or_purged_db_copy(tmp_path: Path, mode: str) -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _run_id, work_item_id, gate = _enqueue_q06_runner_work(connection, suffix)
        item = PostgresSyncRepository(connection, gate).claim_next(f"q06-purge-negative-{mode}-{suffix}")
        assert item is not None and item.id == work_item_id
        authorization = gate.before_enqueue(SyncWorkRequest(provider_id, season_id, "fixtures"))
        capture = _capture()
        persisted = ProviderProvenance(connection, response_contains_api_key=lambda _body: False).persist(item, authorization, (capture,))
        candidate_capture = dataclasses.replace(capture, purpose="research") if mode == "metadata" else capture
        spool = RawSpool(tmp_path / "spool")
        directory = spool.work_item_request_directory(work_item_id=item.id, attempt=item.attempts, request_number=1)
        spool.stage(directory, RawSpoolArtifact(
            BaseRequest(candidate_capture.endpoint, dict(candidate_capture.params)), candidate_capture.response,
            candidate_capture.request_started_at, candidate_capture.response_received_at, dict(item.scope),
            item.id, item.attempts, candidate_capture.normalization_version, candidate_capture.purpose,
            candidate_capture.retention_class, f"work-item-{item.id}:attempt-{item.attempts}:request-000001",
        ))
        recorder = ProviderProvenance(connection, response_contains_api_key=lambda _body: False, spool=spool)
        if mode == "metadata":
            with pytest.raises(ProvenanceError, match="does not match"):
                recorder.recover_spooled_raw(item, authorization)
            spool.mark_durable(directory)
        else:
            connection.execute("SET session_replication_role = replica")
            connection.execute("UPDATE source.provider_raw_payloads SET inline_body=NULL, purged_at=clock_timestamp() WHERE fetch_id=%s", (persisted[0].fetch_id,))
            connection.execute("SET session_replication_role = DEFAULT")
            spool.mark_durable(directory)
        connection.execute("UPDATE ops.sync_work_items SET status='succeeded', lease_owner=NULL, lease_expires_at=NULL, finished_at=clock_timestamp() WHERE id=%s", (item.id,))
        spool._max_bytes = 1  # type: ignore[attr-defined]
        with pytest.raises(RawSpoolCapacityError):
            spool.enforce_limit()
        assert directory.exists()


def test_q06_purge_keeps_copy_when_db_is_unavailable(tmp_path: Path) -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    connection = psycopg.connect(TEST_DB_URL, autocommit=True)
    provider_id, season_id, _run_id, work_item_id, gate = _enqueue_q06_runner_work(connection, suffix)
    item = PostgresSyncRepository(connection, gate).claim_next(f"q06-purge-db-down-{suffix}")
    assert item is not None and item.id == work_item_id
    authorization = gate.before_enqueue(SyncWorkRequest(provider_id, season_id, "fixtures"))
    spool = RawSpool(tmp_path / "spool")
    recorder = ProviderProvenance(connection, response_contains_api_key=lambda _body: False, spool=spool)
    capture = _capture()
    recorder.persist(item, authorization, (capture,))
    directory = spool.work_item_request_directory(work_item_id=item.id, attempt=item.attempts, request_number=1)
    connection.execute("UPDATE ops.sync_work_items SET status='succeeded', lease_owner=NULL, lease_expires_at=NULL, finished_at=clock_timestamp() WHERE id=%s", (item.id,))
    connection.close()
    spool._max_bytes = 1  # type: ignore[attr-defined]
    with pytest.raises(RawSpoolCapacityError):
        spool.enforce_limit()
    assert directory.exists()


def test_each_physical_request_has_its_own_spool_capture_and_fetch_record(tmp_path: Path) -> None:
    assert TEST_DB_URL is not None
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        item, authorization = _item(connection, uuid.uuid4().hex)
        spool = RawSpool(tmp_path / "spool")
        provenance = ProviderProvenance(connection, response_contains_api_key=lambda _body: False, spool=spool)
        captures = (
            _capture(params={"id": 1}, payload={"response": ["first"], "results": 1, "paging": {"current": 1, "total": 1}}),
            _capture(params={"id": 2}, payload={"response": ["second"], "results": 1, "paging": {"current": 1, "total": 1}}),
            _capture(params={"id": 1}, payload={"response": ["third"], "results": 1, "paging": {"current": 1, "total": 1}}),
        )

        persisted = provenance.persist(item, authorization, captures)

        assert len({value.fetch_id for value in persisted}) == 3
        for request_number, capture in enumerate(captures, start=1):
            directory = spool.work_item_request_directory(
                work_item_id=item.id, attempt=item.attempts, request_number=request_number,
            )
            loaded = spool.load(directory, BaseRequest(capture.endpoint, dict(capture.params)))
            assert loaded is not None
            assert loaded.request.params == capture.params
            assert loaded.response.raw_body == capture.response.raw_body
            assert (loaded.work_item_id, loaded.work_item_attempt) == (item.id, item.attempts)
        rows = connection.execute(
            """SELECT request_params,raw.inline_body FROM source.provider_fetches provider_fetch
                 JOIN source.provider_raw_payloads raw ON raw.fetch_id=provider_fetch.id
                WHERE provider_fetch.sync_work_item_id=%s ORDER BY provider_fetch.id""",
            (item.id,),
        ).fetchall()
        assert [(row[0], row[1]) for row in rows] == [
            ({"id": 1}, captures[0].response.raw_body),
            ({"id": 2}, captures[1].response.raw_body),
            ({"id": 1}, captures[2].response.raw_body),
        ]


def test_replay_verifies_retained_raw_sha256_before_returning_it() -> None:
    assert TEST_DB_URL is not None
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        item, authorization = _item(connection, uuid.uuid4().hex)
        provenance = ProviderProvenance(connection, response_contains_api_key=lambda _body: False)
        capture = _capture()
        persisted = provenance.persist(item, authorization, (capture,))

        replay = provenance.latest_replay(item, endpoint="/fixtures", params={"league": 39})
        assert replay is not None and replay.fetch_id == persisted[0].fetch_id
        # Only a privileged storage-corruption simulation may bypass immutable
        # provenance. Normal application SQL is rejected by the Q06 trigger.
        with connection.transaction():
            connection.execute("SET LOCAL session_replication_role = replica")
            connection.execute(
                "UPDATE source.provider_fetches SET content_sha256=%s WHERE id=%s",
                (hashlib.sha256(b"wrong").digest(), persisted[0].fetch_id),
            )
        with pytest.raises(ProvenanceError, match="SHA-256 mismatch"):
            provenance.latest_replay(item, endpoint="/fixtures", params={"league": 39})


@pytest.mark.parametrize(
    ("payload", "expected"),
    (
        ({"results": -1, "paging": {"current": 1, "total": 1}, "response": []}, (None, 1, 1)),
        ({"results": True, "paging": {"current": True, "total": 2}, "response": []}, (None, None, 2)),
        ({"results": 0, "paging": {"current": 0, "total": 0}, "response": []}, (0, None, None)),
        ({"results": 0, "paging": {"current": 2, "total": 1}, "response": []}, (0, None, 1)),
        ({"results": 2_147_483_647, "paging": {"current": 1, "total": 1}, "response": []}, (2_147_483_647, 1, 1)),
        ({"results": 2_147_483_648, "paging": {"current": 1, "total": 1}, "response": []}, (None, 1, 1)),
        ({"results": 0, "paging": {"current": 2_147_483_647, "total": 2_147_483_647}, "response": []}, (0, 2_147_483_647, 2_147_483_647)),
        ({"results": 0, "paging": {"current": 2_147_483_648, "total": 2_147_483_647}, "response": []}, (0, None, 2_147_483_647)),
        ({"results": 0, "paging": {"current": 2_147_483_647, "total": 2_147_483_648}, "response": []}, (0, 2_147_483_647, None)),
    ),
)
def test_invalid_provider_summaries_become_null_while_raw_bytes_are_retained(payload, expected) -> None:
    assert TEST_DB_URL is not None
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        item, authorization = _item(connection, uuid.uuid4().hex)
        capture = _capture(payload=payload)
        provenance = ProviderProvenance(connection, response_contains_api_key=lambda _body: False)
        persisted = provenance.persist(item, authorization, (capture,))
        row = connection.execute(
            """SELECT provider_results,paging_current,paging_total,raw.inline_body
                 FROM source.provider_fetches provider_fetch
                 JOIN source.provider_raw_payloads raw ON raw.fetch_id=provider_fetch.id
                WHERE provider_fetch.id=%s""",
            (persisted[0].fetch_id,),
        ).fetchone()
        assert row == (*expected, capture.response.raw_body)


def test_recorder_persists_fixture_season_and_team_subjects() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, _run_id, work_item_id, gate = _enqueue_q06_runner_work(connection, suffix)
        item = PostgresSyncRepository(connection, gate).claim_next(f"q06-subjects-{suffix}")
        assert item is not None and item.id == work_item_id
        authorization = gate.before_enqueue(SyncWorkRequest(provider_id, season_id, "fixtures"))
        fixture_id, team_id = _q06_subjects(connection, provider_id, season_id, suffix)
        provenance = ProviderProvenance(connection, response_contains_api_key=lambda _body: False)

        persisted = provenance.persist(
            item,
            authorization,
            (
                _capture(endpoint="/standings", params={"league": 39, "season": 2026}, scope={"season_id": season_id}),
                _capture(endpoint="/fixtures/statistics", params={"fixture": 42}, scope={"fixture_id": fixture_id, "season_id": season_id}),
                _capture(endpoint="/fixtures/lineups", params={"fixture": 42}, scope={"fixture_id": fixture_id, "season_id": season_id}),
                _capture(endpoint="/teams/statistics", params={"team": 7, "season": 2026}, scope={"team_id": team_id, "season_id": season_id}),
            ),
        )

        assert connection.execute(
            """SELECT endpoint,subject_fixture_id,subject_season_id,subject_team_id
                 FROM source.provider_fetches WHERE id=ANY(%s) ORDER BY id""",
            ([value.fetch_id for value in persisted],),
        ).fetchall() == [
            ("/standings", None, season_id, None),
            ("/fixtures/statistics", fixture_id, season_id, None),
            ("/fixtures/lineups", fixture_id, season_id, None),
            ("/teams/statistics", None, season_id, team_id),
        ]


def test_registered_replay_recovers_precommit_spool_raw_without_http(tmp_path: Path) -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        _provider_id, _season_id, _run_id, work_item_id, gate = _enqueue_q06_runner_work(connection, suffix)
        owner = f"q06-spool-{suffix}"
        spool = RawSpool(tmp_path / "spool")
        first_recorder = ProviderProvenance(connection, response_contains_api_key=lambda _body: False, spool=spool)
        first_worker = RepeatableSyncWorker(
            connection, gate, owner,
            heartbeat_connection_factory=lambda: psycopg.connect(TEST_DB_URL, autocommit=True),
            provenance=first_recorder,
        )
        capture = _capture(purpose="research", retention_class="contract_sample")
        fail_function = f"ops.q06_spool_fail_{suffix}"
        fail_trigger = f"q06_spool_fail_{suffix}"
        connection.execute(
            f"""CREATE FUNCTION {fail_function}() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN RAISE EXCEPTION 'precommit raw persistence failure'; END $$""",
        )
        connection.execute(
            f"CREATE TRIGGER {fail_trigger} BEFORE INSERT ON source.provider_fetches "
            f"FOR EACH ROW EXECUTE FUNCTION {fail_function}()",
        )
        try:
            with pytest.raises(psycopg.errors.RaiseException, match="precommit raw persistence failure"):
                first_worker.run_once(
                    lambda *_args: WorkResult({}, raw_fetches=(capture, capture)),
                    lambda *_args: (_ for _ in ()).throw(AssertionError("domain write must not run")),
                )
        finally:
            connection.execute(f"DROP TRIGGER {fail_trigger} ON source.provider_fetches")
            connection.execute(f"DROP FUNCTION {fail_function}()")
        assert connection.execute(
            "SELECT count(*) FROM source.provider_fetches WHERE sync_work_item_id=%s", (work_item_id,),
        ).fetchone()[0] == 0
        _requeue_for_replay(connection, work_item_id, owner)

        class SpoolReplayDispatch:
            def __init__(self) -> None:
                self.http_calls = 0

            def replay(self, item, _authorization, recorder):
                with psycopg.connect(TEST_DB_URL, autocommit=True) as observer:
                    assert observer.execute(
                        "SELECT count(*) FROM source.provider_fetches WHERE sync_work_item_id=%s", (work_item_id,),
                    ).fetchone()[0] == 2
                assert all(
                    (spool.work_item_request_directory(
                        work_item_id=work_item_id, attempt=1, request_number=request_number,
                    ) / ".durable").is_file()
                    for request_number in (1, 2)
                )
                saved = recorder.latest_replay(item, endpoint="/fixtures", params={"league": 39})
                assert saved is not None
                return WorkResult({}, source_fetch_ids=(saved.fetch_id,), replay_normalization_version="fixtures-v2")

            def fetch(self, *_args):
                self.http_calls += 1
                raise AssertionError("spool replay must not call the provider")

            def apply_result(self, _writer, _item, _result):
                pass

        dispatch = SpoolReplayDispatch()
        with psycopg.connect(TEST_DB_URL) as recovery_connection:
            second_recorder = ProviderProvenance(
                recovery_connection, response_contains_api_key=lambda _body: False, spool=spool,
            )
            second_worker = RepeatableSyncWorker(
                recovery_connection, gate, owner,
                heartbeat_connection_factory=lambda: psycopg.connect(TEST_DB_URL, autocommit=True),
                provenance=second_recorder,
            )
            assert second_worker.run_registered_once(Q03DispatchRegistry({"fixtures": dispatch})) is True
        assert dispatch.http_calls == 0
        row = connection.execute(
            """SELECT provider_fetch.sync_work_item_attempt,provider_fetch.normalization_version,
                      provider_fetch.purpose,payload.retention_class,provider_fetch.request_started_at,
                      provider_fetch.response_received_at,provider_fetch.request_scope->>'physical_request_id',payload.inline_body
                 FROM source.provider_fetches provider_fetch
                 JOIN source.provider_raw_payloads payload ON payload.fetch_id=provider_fetch.id
                WHERE provider_fetch.sync_work_item_id=%s ORDER BY provider_fetch.id""",
            (work_item_id,),
        ).fetchall()
        assert row == [
            (
                1,
                capture.normalization_version,
                "research",
                "contract_sample",
                capture.request_started_at,
                capture.response_received_at,
                f"work-item-{work_item_id}:attempt-1:request-000001",
                capture.response.raw_body,
            ),
            (
                1,
                capture.normalization_version,
                "research",
                "contract_sample",
                capture.request_started_at,
                capture.response_received_at,
                f"work-item-{work_item_id}:attempt-1:request-000002",
                capture.response.raw_body,
            ),
        ]


def test_concurrent_spool_recovery_and_persist_share_one_physical_raw_fetch(tmp_path: Path) -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        provider_id, season_id, _run_id, work_item_id, gate = _enqueue_q06_runner_work(setup, suffix)
        item = PostgresSyncRepository(setup, gate).claim_next(f"q06-physical-{suffix}")
        assert item is not None and item.id == work_item_id
        authorization = gate.before_enqueue(SyncWorkRequest(provider_id, season_id, "fixtures"))
        capture = _capture()
        spool = RawSpool(tmp_path / "spool")
        request_directory = spool.work_item_request_directory(
            work_item_id=item.id, attempt=item.attempts, request_number=1,
        )
        physical_request_id = f"work-item-{item.id}:attempt-{item.attempts}:request-000001"
        spool.stage(
            request_directory,
            RawSpoolArtifact(
                BaseRequest(capture.endpoint, dict(capture.params)),
                capture.response,
                capture.request_started_at,
                capture.response_received_at,
                dict(item.scope),
                item.id,
                item.attempts,
                capture.normalization_version,
                capture.purpose,
                capture.retention_class,
                physical_request_id,
            ),
        )
        delay_function = f"ops.q06_physical_delay_{suffix}"
        delay_trigger = f"q06_physical_delay_{suffix}"
        setup.execute(
            f"""CREATE FUNCTION {delay_function}() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN PERFORM pg_sleep(0.5); RETURN NEW; END $$""",
        )
        setup.execute(
            f"CREATE TRIGGER {delay_trigger} BEFORE INSERT ON source.provider_fetches "
            f"FOR EACH ROW EXECUTE FUNCTION {delay_function}()",
        )
        barrier = threading.Barrier(2)
        recovered_ids: list[int] = []
        failures: list[BaseException] = []

        def recover() -> None:
            try:
                with psycopg.connect(TEST_DB_URL) as connection:
                    recorder = ProviderProvenance(
                        connection, response_contains_api_key=lambda _body: False, spool=spool,
                    )
                    barrier.wait(timeout=2)
                    recovered = recorder.recover_spooled_raw(item, authorization)
                    assert len(recovered) == 1
                    recovered_ids.append(recovered[0].fetch_id)
            except BaseException as error:
                failures.append(error)

        left = threading.Thread(target=recover)
        right = threading.Thread(target=recover)
        left.start(); right.start(); left.join(); right.join()
        setup.execute(f"DROP TRIGGER {delay_trigger} ON source.provider_fetches")
        setup.execute(f"DROP FUNCTION {delay_function}()")

        assert failures == []
        assert len(recovered_ids) == 2 and recovered_ids[0] == recovered_ids[1]
        fetch_id = recovered_ids[0]
        assert setup.execute(
            "SELECT count(*) FROM source.provider_fetches WHERE sync_work_item_id=%s", (item.id,),
        ).fetchone()[0] == 1
        assert setup.execute(
            "SELECT count(*) FROM source.provider_raw_payloads WHERE fetch_id=%s", (fetch_id,),
        ).fetchone()[0] == 1

        with psycopg.connect(TEST_DB_URL) as connection:
            recorder = ProviderProvenance(connection, response_contains_api_key=lambda _body: False, spool=spool)
            assert recorder.recover_spooled_raw(item, authorization) == ()
        with psycopg.connect(TEST_DB_URL) as connection:
            recorder = ProviderProvenance(connection, response_contains_api_key=lambda _body: False)
            persisted = recorder.persist(item, authorization, (capture,))
            assert tuple(value.fetch_id for value in persisted) == (fetch_id,)
        assert setup.execute(
            "SELECT count(*) FROM source.provider_fetches WHERE sync_work_item_id=%s", (item.id,),
        ).fetchone()[0] == 1
        assert setup.execute(
            "SELECT count(*) FROM source.provider_raw_payloads WHERE fetch_id=%s", (fetch_id,),
        ).fetchone()[0] == 1


def test_spool_recovery_rejects_mismatched_physical_raw_without_marking_durable(tmp_path: Path) -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        provider_id, season_id, _run_id, work_item_id, gate = _enqueue_q06_runner_work(setup, suffix)
        item = PostgresSyncRepository(setup, gate).claim_next(f"q06-mismatch-{suffix}")
        assert item is not None and item.id == work_item_id
        authorization = gate.before_enqueue(SyncWorkRequest(provider_id, season_id, "fixtures"))
        persisted_capture = _capture()
        persisted = ProviderProvenance(setup, response_contains_api_key=lambda _body: False).persist(
            item, authorization, (persisted_capture,),
        )
        spool_capture = _capture(payload={
            "get": "fixtures", "parameters": {"league": "39"}, "errors": {}, "results": 1,
            "paging": {"current": 1, "total": 1}, "response": ["different raw"],
        })
        spool = RawSpool(tmp_path / "spool")
        request_directory = spool.work_item_request_directory(
            work_item_id=item.id, attempt=item.attempts, request_number=1,
        )
        spool.stage(
            request_directory,
            RawSpoolArtifact(
                BaseRequest(spool_capture.endpoint, dict(spool_capture.params)),
                spool_capture.response,
                spool_capture.request_started_at,
                spool_capture.response_received_at,
                dict(item.scope),
                item.id,
                item.attempts,
                spool_capture.normalization_version,
                spool_capture.purpose,
                spool_capture.retention_class,
                f"work-item-{item.id}:attempt-{item.attempts}:request-000001",
            ),
        )

        with psycopg.connect(TEST_DB_URL) as connection:
            recorder = ProviderProvenance(connection, response_contains_api_key=lambda _body: False, spool=spool)
            with pytest.raises(ProvenanceError, match="does not match"):
                recorder.recover_spooled_raw(item, authorization)

        assert setup.execute(
            "SELECT count(*) FROM source.provider_fetches WHERE sync_work_item_id=%s", (item.id,),
        ).fetchone()[0] == 1
        assert setup.execute(
            "SELECT count(*) FROM source.provider_raw_payloads WHERE fetch_id=%s", (persisted[0].fetch_id,),
        ).fetchone()[0] == 1
        assert not (request_directory / ".durable").exists()


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("purpose", "research"),
        ("retention_class", "contract_sample"),
        ("normalization_version", "fixtures-v2"),
    ),
    ids=("purpose", "retention_class", "normalization_version"),
)
def test_spool_recovery_rejects_mismatched_metadata(tmp_path: Path, field: str, value: str) -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        provider_id, season_id, _run_id, work_item_id, gate = _enqueue_q06_runner_work(setup, suffix)
        item = PostgresSyncRepository(setup, gate).claim_next(f"q06-metadata-{field}-{suffix}")
        assert item is not None and item.id == work_item_id
        authorization = gate.before_enqueue(SyncWorkRequest(provider_id, season_id, "fixtures"))
        fixture_id, _team_id = _q06_subjects(setup, provider_id, season_id, suffix)
        capture = _capture(
            endpoint="/fixtures/statistics",
            params={"fixture": fixture_id},
            scope={"fixture_id": fixture_id, "season_id": season_id},
        )

        persisted = ProviderProvenance(setup, response_contains_api_key=lambda _body: False).persist(
            item,
            authorization,
            (capture,),
        )
        mismatch = dataclasses.replace(capture, **{field: value})
        assert field in {"purpose", "retention_class", "normalization_version"}
        if field == "purpose":
            assert mismatch.purpose == value
            assert mismatch.retention_class == capture.retention_class
            assert mismatch.normalization_version == capture.normalization_version
        elif field == "retention_class":
            assert mismatch.retention_class == value
            assert mismatch.purpose == capture.purpose
            assert mismatch.normalization_version == capture.normalization_version
        else:
            assert mismatch.normalization_version == value
            assert mismatch.purpose == capture.purpose
            assert mismatch.retention_class == capture.retention_class

        spool = RawSpool(tmp_path / "spool")
        request_directory = spool.work_item_request_directory(
            work_item_id=item.id, attempt=item.attempts, request_number=1,
        )
        physical_request_id = f"work-item-{item.id}:attempt-{item.attempts}:request-000001"
        spool.stage(
            request_directory,
            RawSpoolArtifact(
                BaseRequest(mismatch.endpoint, dict(mismatch.params)),
                mismatch.response,
                mismatch.request_started_at,
                mismatch.response_received_at,
                dict(capture.scope),
                item.id,
                item.attempts,
                mismatch.normalization_version,
                mismatch.purpose,
                mismatch.retention_class,
                physical_request_id,
            ),
        )

        with psycopg.connect(TEST_DB_URL) as recovery_connection:
            assert recovery_connection.info.transaction_status.name == "IDLE"
            recorder = ProviderProvenance(
                recovery_connection, response_contains_api_key=lambda _body: False, spool=spool,
            )
            with pytest.raises(ProvenanceError, match="does not match"):
                recorder.recover_spooled_raw(item, authorization)
            assert recovery_connection.info.transaction_status.name == "IDLE"

        assert setup.execute(
            "SELECT count(*) FROM source.provider_fetches WHERE sync_work_item_id=%s", (item.id,),
        ).fetchone()[0] == 1
        source_rows = setup.execute(
            """
            SELECT provider_fetch.id,provider_fetch.request_scope->>'physical_request_id',
                   provider_fetch.purpose,provider_fetch.request_scope->'scope',provider_fetch.subject_fixture_id,
                   provider_fetch.subject_season_id,provider_fetch.subject_team_id,
                   provider_fetch.normalization_version,provider_fetch.request_started_at,
                   provider_fetch.response_received_at,provider_raw_payloads.inline_body,provider_raw_payloads.retention_class
              FROM source.provider_fetches provider_fetch
              JOIN source.provider_raw_payloads
              ON source.provider_raw_payloads.fetch_id=provider_fetch.id
             WHERE provider_fetch.sync_work_item_id=%s
               AND provider_fetch.request_scope->>'physical_request_id'=%s
             LIMIT 2
            """,
            (item.id, physical_request_id),
        ).fetchone()
        assert source_rows is not None
        fetch_id = source_rows[0]
        assert source_rows[1] == physical_request_id
        assert source_rows[2] == capture.purpose
        assert source_rows[3] == capture.scope
        assert source_rows[4] == fixture_id
        assert source_rows[5] == season_id
        assert source_rows[6] is None
        assert source_rows[7] == capture.normalization_version
        assert source_rows[8] == capture.request_started_at
        assert source_rows[9] == capture.response_received_at
        assert source_rows[10] == capture.response.raw_body
        assert source_rows[11] == capture.retention_class

        assert setup.execute(
            "SELECT inline_body FROM source.provider_raw_payloads WHERE fetch_id=%s", (fetch_id,),
        ).fetchone()[0] == capture.response.raw_body
        assert not (request_directory / ".durable").exists()
        loaded = spool.load(request_directory, BaseRequest(capture.endpoint, dict(capture.params)))
        assert loaded is not None
        assert loaded.response.raw_body == capture.response.raw_body

        assert setup.execute(
            """
            SELECT count(*) FROM source.provider_raw_payloads payload
             JOIN source.provider_fetches provider_fetch ON provider_fetch.id=payload.fetch_id
             WHERE provider_fetch.id=%s
            """,
            (fetch_id,),
        ).fetchone()[0] == 1

        with psycopg.connect(TEST_DB_URL, autocommit=True) as normal_connection:
            normal_recorder = ProviderProvenance(normal_connection, response_contains_api_key=lambda _body: False)
            with pytest.raises(ProvenanceError, match="does not match"):
                normal_recorder.persist(item, authorization, (mismatch,))

        assert setup.execute(
            "SELECT sync_work_item_id, count(*) FROM source.provider_fetches WHERE sync_work_item_id=%s GROUP BY sync_work_item_id",
            (item.id,),
        ).fetchone() == (item.id, 1)
        assert setup.execute(
            "SELECT count(*) FROM source.provider_fetches WHERE sync_work_item_id=%s AND request_scope->>'physical_request_id'=%s",
            (item.id, physical_request_id),
        ).fetchone()[0] == 1
        assert setup.execute(
            "SELECT count(*) FROM source.provider_raw_payloads payload JOIN source.provider_fetches provider_fetch ON provider_fetch.id=payload.fetch_id WHERE provider_fetch.id=%s",
            (fetch_id,),
        ).fetchone()[0] == 1


def test_spool_recovery_with_matching_metadata_returns_existing_fetch_and_markers(tmp_path: Path) -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        provider_id, season_id, _run_id, work_item_id, gate = _enqueue_q06_runner_work(setup, suffix)
        item = PostgresSyncRepository(setup, gate).claim_next(f"q06-match-{suffix}")
        assert item is not None and item.id == work_item_id
        authorization = gate.before_enqueue(SyncWorkRequest(provider_id, season_id, "fixtures"))
        capture = _capture(purpose="research", retention_class="contract_sample")
        persisted = ProviderProvenance(setup, response_contains_api_key=lambda _body: False).persist(
            item,
            authorization,
            (capture,),
        )

        spool = RawSpool(tmp_path / "spool")
        request_directory = spool.work_item_request_directory(
            work_item_id=item.id, attempt=item.attempts, request_number=1,
        )
        spool.stage(
            request_directory,
            RawSpoolArtifact(
                BaseRequest(capture.endpoint, dict(capture.params)),
                capture.response,
                capture.request_started_at,
                capture.response_received_at,
                dict(capture.scope or item.scope),
                item.id,
                item.attempts,
                capture.normalization_version,
                capture.purpose,
                capture.retention_class,
                f"work-item-{item.id}:attempt-{item.attempts}:request-000001",
            ),
        )

        with psycopg.connect(TEST_DB_URL) as recovery_connection:
            assert recovery_connection.info.transaction_status.name == "IDLE"
            recorder = ProviderProvenance(recovery_connection, response_contains_api_key=lambda _body: False, spool=spool)
            recovered = recorder.recover_spooled_raw(item, authorization)
            assert tuple(value.fetch_id for value in recovered) == (persisted[0].fetch_id,)
            assert recovery_connection.info.transaction_status.name == "IDLE"
            assert (request_directory / ".durable").exists()

        row = setup.execute(
            "SELECT count(*) FROM source.provider_fetches WHERE sync_work_item_id=%s", (item.id,),
        ).fetchone()[0]
        assert row == 1
        assert setup.execute(
            "SELECT count(*) FROM source.provider_raw_payloads WHERE fetch_id=%s", (persisted[0].fetch_id,),
        ).fetchone()[0] == 1

        with psycopg.connect(TEST_DB_URL) as second_recovery_connection:
            recorder = ProviderProvenance(
                second_recovery_connection, response_contains_api_key=lambda _body: False, spool=spool,
            )
            assert recorder.recover_spooled_raw(item, authorization) == ()
            assert (request_directory / ".durable").exists()
        assert setup.execute(
            "SELECT count(*) FROM source.provider_fetches WHERE sync_work_item_id=%s", (item.id,),
        ).fetchone()[0] == 1
        assert setup.execute(
            "SELECT count(*) FROM source.provider_raw_payloads WHERE fetch_id=%s", (persisted[0].fetch_id,),
        ).fetchone()[0] == 1


def test_real_q03_runner_replays_raw_with_source_links_and_atomic_completion() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id, season_id, run_id, work_item_id, gate = _enqueue_q06_runner_work(connection, suffix)
        probe = f"ops.q06_replay_probe_{suffix}"
        connection.execute(f"CREATE TABLE {probe}(kind text PRIMARY KEY, source_fetch_id bigint NOT NULL REFERENCES source.provider_fetches(id))")
        owner = f"q06-runner-{suffix}"
        provenance = ProviderProvenance(connection, response_contains_api_key=lambda _body: False)
        worker = RepeatableSyncWorker(
            connection, gate, owner,
            heartbeat_connection_factory=lambda: psycopg.connect(TEST_DB_URL, autocommit=True),
            provenance=provenance,
        )
        capture = _capture()

        with pytest.raises(RuntimeError, match="domain rollback"):
            def fail_after_domain_write(writer, _item, result):
                writer.execute(
                    f"INSERT INTO {probe}(kind,source_fetch_id) VALUES('failed',%s)",
                    (result.source_fetch_ids[0],),
                )
                raise RuntimeError("domain rollback")

            worker.run_once(
                lambda *_args: WorkResult({}, raw_fetches=(capture,)),
                fail_after_domain_write,
            )

        assert connection.execute(f"SELECT count(*) FROM {probe}").fetchone()[0] == 0
        assert connection.execute(
            "SELECT status,finished_at FROM ops.sync_work_items WHERE id=%s", (work_item_id,),
        ).fetchone() == ("running", None)
        raw_row = connection.execute(
            """SELECT provider_fetch.id,provider_fetch.request_started_at,provider_fetch.response_received_at,
                      provider_fetch.normalization_version,provider_fetch.sync_work_item_attempt,raw.inline_body
                 FROM source.provider_fetches provider_fetch
                 JOIN source.provider_raw_payloads raw ON raw.fetch_id=provider_fetch.id
                WHERE provider_fetch.sync_work_item_id=%s""",
            (work_item_id,),
        ).fetchone()
        assert raw_row is not None
        fetch_id, original_started_at, original_received_at, version, original_attempt, raw_body = raw_row
        assert (version, original_attempt, raw_body) == ("fixtures-v1", 1, capture.response.raw_body)
        assert (original_started_at, original_received_at) == (capture.request_started_at, capture.response_received_at)

        _requeue_for_replay(connection, work_item_id, owner)
        dependent_stable_key = f"q06-dependent:{suffix}"

        class ReplayDispatch:
            def __init__(self) -> None:
                self.http_calls = 0
                self.capture: RawFetchCapture | None = None

            def replay(self, item, _authorization, recorder):
                saved = recorder.latest_replay(item, endpoint="/fixtures", params={"league": 39})
                assert saved is not None
                self.capture = saved.capture

                def enqueue_dependent(writer, source_fetch_id=saved.fetch_id):
                    writer.execute(
                        "SELECT ops.enqueue_repeatable_sync_work_item(%s,%s,%s,%s,%s,clock_timestamp(),%s,%s,%s)",
                        (
                            run_id, f"q06-dependent:{suffix}",
                            Jsonb({"source_fetch_ids": [source_fetch_id], "_sync_policy": {"provider_id": provider_id, "season_id": season_id, "work_type": "fixtures", "instance_id": 1, "version": 1}}),
                            "fixtures", 0, dependent_stable_key, f"q06-dependent:{suffix}", f"q06-dependent:{suffix}",
                        ),
                    )

                return WorkResult(
                    {"replayed_fetch_id": saved.fetch_id}, (enqueue_dependent,), source_fetch_ids=(saved.fetch_id,),
                    replay_normalization_version="fixtures-v2",
                )

            def fetch(self, *_args):
                self.http_calls += 1
                raise AssertionError("replay must not call the provider")

            def apply_result(self, writer, _item, result):
                writer.execute(f"INSERT INTO {probe}(kind,source_fetch_id) VALUES('result',%s)", (result.source_fetch_ids[0],))

        replay_dispatch = ReplayDispatch()
        registry = Q03DispatchRegistry({"fixtures": replay_dispatch})
        completion_fence_function = f"ops.q06_replay_completion_fence_{suffix}"
        completion_fence_trigger = f"q06_replay_completion_fence_{suffix}"
        connection.execute(
            f"""CREATE FUNCTION {completion_fence_function}() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                  UPDATE ops.sync_work_items SET lease_token=lease_token+1 WHERE id=NEW.sync_work_item_id;
                  RETURN NEW;
                END $$""",
        )
        connection.execute(
            f"CREATE TRIGGER {completion_fence_trigger} AFTER INSERT ON source.provider_fetch_replays "
            f"FOR EACH ROW EXECUTE FUNCTION {completion_fence_function}()",
        )
        with pytest.raises(LeaseLost, match="before completion"):
            worker.run_registered_once(registry)
        assert replay_dispatch.http_calls == 0
        assert connection.execute(f"SELECT count(*) FROM {probe}").fetchone()[0] == 0
        assert connection.execute("SELECT count(*) FROM ops.sync_work_items WHERE stable_key=%s", (dependent_stable_key,)).fetchone()[0] == 0
        assert connection.execute("SELECT count(*) FROM source.provider_fetch_replays WHERE source_fetch_id=%s", (fetch_id,)).fetchone()[0] == 0
        assert connection.execute("SELECT status,finished_at FROM ops.sync_work_items WHERE id=%s", (work_item_id,)).fetchone() == ("running", None)
        connection.execute(f"DROP TRIGGER {completion_fence_trigger} ON source.provider_fetch_replays")
        connection.execute(f"DROP FUNCTION {completion_fence_function}()")

        _requeue_for_replay(connection, work_item_id, owner)
        assert worker.run_registered_once(registry) is True
        assert replay_dispatch.http_calls == 0
        assert replay_dispatch.capture is not None
        assert (replay_dispatch.capture.request_started_at, replay_dispatch.capture.response_received_at) == (original_started_at, original_received_at)
        assert replay_dispatch.capture.normalization_version == "fixtures-v1"
        assert connection.execute(f"SELECT kind,source_fetch_id FROM {probe}").fetchall() == [("result", fetch_id)]
        assert connection.execute("SELECT scope->'source_fetch_ids' FROM ops.sync_work_items WHERE stable_key=%s", (dependent_stable_key,)).fetchone()[0] == [fetch_id]
        assert connection.execute(
            "SELECT source_fetch_id,sync_work_item_id,sync_work_item_attempt FROM source.provider_fetch_replays WHERE source_fetch_id=%s",
            (fetch_id,),
        ).fetchall() == [(fetch_id, work_item_id, 3)]
        assert connection.execute(
            "SELECT status,finished_at IS NOT NULL FROM ops.sync_work_items WHERE id=%s", (work_item_id,),
        ).fetchone() == ("succeeded", True)


def test_q06_registered_replay_rejects_a_stale_lease_token_before_domain_write() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        _provider_id, _season_id, _run_id, work_item_id, gate = _enqueue_q06_runner_work(connection, suffix)
        probe = f"ops.q06_stale_replay_{suffix}"
        connection.execute(f"CREATE TABLE {probe}(id integer PRIMARY KEY)")
        first_owner, current_owner = f"q06-stale-first-{suffix}", f"q06-stale-current-{suffix}"
        provenance = ProviderProvenance(connection, response_contains_api_key=lambda _body: False)
        first_worker = RepeatableSyncWorker(
            connection, gate, first_owner,
            heartbeat_connection_factory=lambda: psycopg.connect(TEST_DB_URL, autocommit=True),
            provenance=provenance,
        )
        with pytest.raises(RuntimeError, match="domain rollback"):
            first_worker.run_once(
                lambda *_args: WorkResult({}, raw_fetches=(_capture(),)),
                lambda _writer, _item, _result: (_ for _ in ()).throw(RuntimeError("domain rollback")),
            )
        _requeue_for_replay(connection, work_item_id, first_owner)
        current = PostgresSyncRepository(connection, gate).claim_next(current_owner)
        assert current is not None and current.id == work_item_id and current.lease_token > 1
        stale = LeasedWorkItem(
            current.id, current.run_id, current.scope_key, current.scope, current.checkpoint, current.attempts,
            current.job_type, current.priority, current.stable_key, current.entity_key, current.execution_key,
            current.lease_token - 1,
        )

        class ReplayDispatch:
            def __init__(self) -> None:
                self.http_calls = 0

            def replay(self, item, _authorization, recorder):
                saved = recorder.latest_replay(item, endpoint="/fixtures", params={"league": 39})
                assert saved is not None
                return WorkResult({}, source_fetch_ids=(saved.fetch_id,), replay_normalization_version="fixtures-v2")

            def fetch(self, *_args):
                self.http_calls += 1
                raise AssertionError("stale replay must not call the provider")

            def apply_result(self, writer, _item, _result):
                writer.execute(f"INSERT INTO {probe}(id) VALUES(1)")

        dispatch = ReplayDispatch()
        worker = RepeatableSyncWorker(
            connection, gate, current_owner,
            heartbeat_connection_factory=lambda: psycopg.connect(TEST_DB_URL, autocommit=True),
            provenance=provenance,
        )
        worker.repository.claim_next = lambda *_args, **_kwargs: stale  # type: ignore[method-assign]

        with pytest.raises(LeaseLost, match="before applying"):
            worker.run_registered_once(Q03DispatchRegistry({"fixtures": dispatch}))

        assert dispatch.http_calls == 0
        assert connection.execute(f"SELECT count(*) FROM {probe}").fetchone()[0] == 0
        assert connection.execute("SELECT count(*) FROM source.provider_fetch_replays WHERE sync_work_item_id=%s", (work_item_id,)).fetchone()[0] == 0
        assert connection.execute(
            "SELECT status,lease_owner,lease_token FROM ops.sync_work_items WHERE id=%s", (work_item_id,),
        ).fetchone() == ("running", current_owner, current.lease_token)


def test_q06_concurrent_registered_replays_apply_once_without_http() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as setup:
        _provider_id, _season_id, _run_id, work_item_id, gate = _enqueue_q06_runner_work(setup, suffix)
        probe = f"ops.q06_concurrent_replay_{suffix}"
        setup.execute(f"CREATE TABLE {probe}(id integer PRIMARY KEY, source_fetch_id bigint NOT NULL REFERENCES source.provider_fetches(id))")
        first_owner = f"q06-concurrent-first-{suffix}"
        first_provenance = ProviderProvenance(setup, response_contains_api_key=lambda _body: False)
        first_worker = RepeatableSyncWorker(
            setup, gate, first_owner,
            heartbeat_connection_factory=lambda: psycopg.connect(TEST_DB_URL, autocommit=True),
            provenance=first_provenance,
        )
        with pytest.raises(RuntimeError, match="domain rollback"):
            first_worker.run_once(
                lambda *_args: WorkResult({}, raw_fetches=(_capture(),)),
                lambda _writer, _item, _result: (_ for _ in ()).throw(RuntimeError("domain rollback")),
            )
        _requeue_for_replay(setup, work_item_id, first_owner)
        # Isolate the two workers to this high-priority replay item. Earlier
        # P02 cases intentionally leave unrelated history pending.
        setup.execute(
            "UPDATE ops.sync_work_items SET available_at=clock_timestamp()+interval '1 hour' WHERE status='pending' AND id<>%s",
            (work_item_id,),
        )

    barrier = threading.Barrier(2)
    outcomes: list[bool] = []
    failures: list[BaseException] = []
    http_calls: list[str] = []

    def run(owner: str) -> None:
        try:
            assert TEST_DB_URL is not None
            with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
                worker_gate = SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: datetime.now(UTC))
                provenance = ProviderProvenance(connection, response_contains_api_key=lambda _body: False)

                class ReplayDispatch:
                    def replay(self, item, _authorization, recorder):
                        saved = recorder.latest_replay(item, endpoint="/fixtures", params={"league": 39})
                        assert saved is not None
                        return WorkResult({}, source_fetch_ids=(saved.fetch_id,), replay_normalization_version="fixtures-v2")

                    def fetch(self, *_args):
                        http_calls.append(owner)
                        raise AssertionError("concurrent replay must not call the provider")

                    def apply_result(self, writer, _item, result):
                        writer.execute(
                            f"INSERT INTO {probe}(id,source_fetch_id) VALUES(1,%s)", (result.source_fetch_ids[0],),
                        )

                worker = RepeatableSyncWorker(
                    connection, worker_gate, owner,
                    heartbeat_connection_factory=lambda: psycopg.connect(TEST_DB_URL, autocommit=True),
                    provenance=provenance,
                )
                barrier.wait(timeout=2)
                outcomes.append(worker.run_registered_once(Q03DispatchRegistry({"fixtures": ReplayDispatch()})))
        except BaseException as error:
            failures.append(error)

    left = threading.Thread(target=run, args=(f"q06-concurrent-left-{suffix}",))
    right = threading.Thread(target=run, args=(f"q06-concurrent-right-{suffix}",))
    left.start(); right.start(); left.join(); right.join()

    assert failures == []
    assert sorted(outcomes) == [False, True]
    assert http_calls == []
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        assert connection.execute(f"SELECT count(*) FROM {probe}").fetchone()[0] == 1
        assert connection.execute("SELECT count(*) FROM source.provider_fetch_replays WHERE sync_work_item_id=%s", (work_item_id,)).fetchone()[0] == 1
        assert connection.execute("SELECT status FROM ops.sync_work_items WHERE id=%s", (work_item_id,)).fetchone()[0] == "succeeded"


def test_q06_hash_change_after_replay_load_blocks_fenced_domain_write() -> None:
    assert TEST_DB_URL is not None
    suffix = uuid.uuid4().hex
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        _provider_id, _season_id, _run_id, work_item_id, gate = _enqueue_q06_runner_work(connection, suffix)
        probe = f"ops.q06_replay_corrupt_{suffix}"
        connection.execute(f"CREATE TABLE {probe}(id integer PRIMARY KEY)")
        owner = f"q06-corrupt-{suffix}"
        provenance = ProviderProvenance(connection, response_contains_api_key=lambda _body: False)
        worker = RepeatableSyncWorker(
            connection, gate, owner,
            heartbeat_connection_factory=lambda: psycopg.connect(TEST_DB_URL, autocommit=True),
            provenance=provenance,
        )
        capture = _capture()
        with pytest.raises(RuntimeError, match="domain rollback"):
            worker.run_once(
                lambda *_args: WorkResult({}, raw_fetches=(capture,)),
                lambda _writer, _item, _result: (_ for _ in ()).throw(RuntimeError("domain rollback")),
            )
        fetch_id = connection.execute(
            "SELECT id FROM source.provider_fetches WHERE sync_work_item_id=%s", (work_item_id,),
        ).fetchone()[0]
        _requeue_for_replay(connection, work_item_id, owner)

        class CorruptAfterLoadReplay:
            def __init__(self) -> None:
                self.http_calls = 0

            def replay(self, item, _authorization, recorder):
                saved = recorder.latest_replay(item, endpoint="/fixtures", params={"league": 39})
                assert saved is not None
                with psycopg.connect(TEST_DB_URL, autocommit=True) as tamper:
                    with tamper.transaction():
                        tamper.execute("SET LOCAL session_replication_role = replica")
                        tamper.execute(
                            "UPDATE source.provider_fetches SET content_sha256=%s WHERE id=%s",
                            (hashlib.sha256(b"corrupt").digest(), saved.fetch_id),
                        )
                return WorkResult({}, source_fetch_ids=(saved.fetch_id,), replay_normalization_version="fixtures-v2")

            def __call__(self, *_args):
                self.http_calls += 1
                raise AssertionError("corrupt replay must not call the provider")

        corrupt_replay = CorruptAfterLoadReplay()
        with pytest.raises(ProvenanceError, match="SHA-256 mismatch"):
            worker.run_once(
                corrupt_replay,
                lambda writer, _item, _result: writer.execute(f"INSERT INTO {probe}(id) VALUES(1)"),
            )

        assert corrupt_replay.http_calls == 0
        assert connection.execute(f"SELECT count(*) FROM {probe}").fetchone()[0] == 0
        assert connection.execute(
            "SELECT status,finished_at FROM ops.sync_work_items WHERE id=%s", (work_item_id,),
        ).fetchone() == ("running", None)
        assert connection.execute(
            "SELECT count(*) FROM source.provider_fetch_replays WHERE source_fetch_id=%s", (fetch_id,),
        ).fetchone()[0] == 0
