from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pytest

from app.api_football import APIFootballResponse
from app.importer.raw_spool import RawSpool, RawSpoolArtifact, RawSpoolCapacityError, RawSpoolError
from app.importer.season_bootstrap import BaseRequest
from app.sync.provenance import ProvenanceError, RawFetchCapture
from app.sync.repository import LeasedWorkItem


def _response() -> APIFootballResponse:
    body = json.dumps({"get": "fixtures", "parameters": {"league": "39"}, "errors": {}, "results": 0, "paging": {"current": 1, "total": 1}, "response": []}).encode()
    return APIFootballResponse(json.loads(body), body, 200, {})


def test_q06_raw_spool_preserves_verified_work_provenance(tmp_path: Path) -> None:
    spool = RawSpool(tmp_path / "spool")
    now = datetime.now(UTC)
    artifact = RawSpoolArtifact(
        BaseRequest("/fixture/events", {"fixture": 42}), _response(), now, now,
        {"fixture": 42, "kind": "events"}, 7, 2, "fixture-events-v1",
    )
    directory = spool.work_item_directory(work_item_id=7, attempt=2)
    spool.stage(directory, artifact)

    loaded = spool.load(directory, artifact.request)

    assert loaded is not None
    assert loaded.scope == {"fixture": 42, "kind": "events"}
    assert (loaded.work_item_id, loaded.work_item_attempt) == (7, 2)
    assert loaded.normalization_version == "fixture-events-v1"


def test_q06_raw_capture_rejects_credential_bearing_metadata() -> None:
    now = datetime.now(UTC)
    with pytest.raises(ProvenanceError, match="credentials"):
        RawFetchCapture("/fixtures", {"api-key": "must-not-persist"}, _response(), now, now, "fixtures-v1")
    with pytest.raises(ProvenanceError, match="credentials"):
        RawFetchCapture("/fixtures", {"id": 7}, _response(), now, now, "fixtures-v1", scope={"headers": {"x": "no"}})


def test_q06_spool_refuses_over_limit_without_db_proof(tmp_path: Path) -> None:
    spool = RawSpool(tmp_path / "spool", max_bytes=10_000)
    now = datetime.now(UTC)
    artifact = RawSpoolArtifact(
        BaseRequest("/fixtures", {"league": 39}), _response(), now, now,
        {"league": 39}, 1, 1, "fixtures-v1", "scheduled_refresh", "standard",
        "work-item-1:attempt-1:request-000001",
    )
    durable = spool.work_item_request_directory(work_item_id=1, attempt=1, request_number=1)
    active = spool.work_item_request_directory(work_item_id=2, attempt=1, request_number=1)
    spool.stage(durable, artifact)
    spool.mark_durable(durable)
    spool.stage(active, RawSpoolArtifact(
        artifact.request, artifact.response, now, now, {"league": 39}, 2, 1,
        "fixtures-v1", "scheduled_refresh", "standard", "work-item-2:attempt-1:request-000001",
    ))

    # A durable marker is only a candidate.  Without a read-only DB verifier,
    # both captures remain available and the bounded store refuses growth.
    spool._max_bytes = sum(path.stat().st_size for path in active.iterdir())  # type: ignore[attr-defined]
    with pytest.raises(RawSpoolCapacityError):
        spool.enforce_limit()

    assert durable.exists()
    assert active.exists()


def test_q06_spool_purges_only_verified_durable_capture(tmp_path: Path) -> None:
    spool = RawSpool(tmp_path / "spool", max_bytes=10_000)
    now = datetime.now(UTC)
    artifact = RawSpoolArtifact(
        BaseRequest("/fixtures", {"league": 39}), _response(), now, now,
        {"league": 39}, 1, 1, "fixtures-v1", "scheduled_refresh", "standard",
        "work-item-1:attempt-1:request-000001",
    )
    durable = spool.work_item_request_directory(work_item_id=1, attempt=1, request_number=1)
    active = spool.work_item_request_directory(work_item_id=2, attempt=1, request_number=1)
    spool.stage(durable, artifact)
    spool.mark_durable(durable)
    spool.stage(active, RawSpoolArtifact(
        artifact.request, artifact.response, now, now, {"league": 39}, 2, 1,
        "fixtures-v1", "scheduled_refresh", "standard", "work-item-2:attempt-1:request-000001",
    ))
    spool._max_bytes = sum(path.stat().st_size for path in active.iterdir())  # type: ignore[attr-defined]

    with pytest.raises(RawSpoolCapacityError):
        spool.enforce_limit()
    assert durable.exists()

    spool.set_purge_verifier(lambda path: path.parent.parent.name == "work-item-1")
    spool.enforce_limit()
    assert not durable.exists()
    assert active.exists()


def test_q06_spool_rejects_oversized_artifact_before_writing(tmp_path: Path) -> None:
    spool = RawSpool(tmp_path / "spool", max_bytes=32)
    now = datetime.now(UTC)
    artifact = RawSpoolArtifact(BaseRequest("/fixtures", {"league": 39}), _response(), now, now)
    directory = spool.work_item_directory(work_item_id=1, attempt=1)

    with pytest.raises(RawSpoolCapacityError):
        spool.stage(directory, artifact)
    assert not directory.exists()


def test_q06_spool_rejects_symlinked_purge_path(tmp_path: Path) -> None:
    spool = RawSpool(tmp_path / "spool")
    target = tmp_path / "outside"
    target.mkdir()
    link = spool.root / "run-1"
    spool.root.mkdir(parents=True)
    link.symlink_to(target, target_is_directory=True)

    with pytest.raises(RawSpoolError):
        spool.purge_generation(link / "league-1-season-2026" / "generation-1")


def test_q06_public_purge_never_deletes_unproven_repeatable_capture(tmp_path: Path) -> None:
    spool = RawSpool(tmp_path / "spool", purge_verifier=lambda _directory: True)
    now = datetime.now(UTC)
    directory = spool.work_item_request_directory(work_item_id=1, attempt=1, request_number=1)
    spool.stage(directory, RawSpoolArtifact(
        BaseRequest("/fixtures", {"league": 39}), _response(), now, now,
        {"league": 39}, 1, 1, "fixtures-v1", "scheduled_refresh", "standard",
        "work-item-1:attempt-1:request-000001",
    ))

    with pytest.raises(RawSpoolError, match="not proven"):
        spool.purge_generation(directory)

    assert directory.exists()


def test_q06_spool_rejects_lexically_escaped_or_symlinked_root_and_lock(tmp_path: Path) -> None:
    root = tmp_path / "spool"
    outside = tmp_path / "outside"
    escaped = root / ".." / "outside" / "repeatable" / "work-item-1" / "attempt-1" / "request-000001"
    with pytest.raises(RawSpoolError):
        RawSpool(root).stage(escaped, RawSpoolArtifact(BaseRequest("/fixtures", {}), _response(), datetime.now(UTC), datetime.now(UTC)))

    target = tmp_path / "target"
    target.mkdir()
    root_link = tmp_path / "spool-link"
    root_link.symlink_to(target, target_is_directory=True)
    with pytest.raises(RawSpoolError):
        RawSpool(root_link).stage(
            root_link / "repeatable" / "work-item-1" / "attempt-1" / "request-000001",
            RawSpoolArtifact(BaseRequest("/fixtures", {}), _response(), datetime.now(UTC), datetime.now(UTC)),
        )

    root.mkdir()
    (root / ".spool.lock").symlink_to(outside)
    with pytest.raises(RawSpoolError):
        RawSpool(root).stage(
            root / "repeatable" / "work-item-1" / "attempt-1" / "request-000001",
            RawSpoolArtifact(BaseRequest("/fixtures", {}), _response(), datetime.now(UTC), datetime.now(UTC)),
        )


def test_q06_catalogue_marker_reserves_root_capacity(tmp_path: Path) -> None:
    spool = RawSpool(tmp_path / "spool", max_bytes=10_000)
    now = datetime.now(UTC)
    directory = spool.root / "catalogue" / "digest"
    spool.stage(directory, RawSpoolArtifact(BaseRequest("/leagues", {}), _response(), now, now))
    spool._max_bytes = sum(path.stat().st_size for path in spool.root.rglob("*") if path.is_file())  # type: ignore[attr-defined]

    with pytest.raises(RawSpoolCapacityError):
        spool.mark_catalogue_pending(directory)

    assert not (directory / ".queue-pending").exists()


@dataclass
class _Row:
    value: object
    def fetchone(self): return self.value


class _Connection:
    def __init__(self) -> None: self.active = False
    def transaction(self):
        connection = self
        class Tx:
            def __enter__(self): connection.active = True; return self
            def __exit__(self, *_): connection.active = False; return False
        return Tx()
    def execute(self, query, _params=None):
        if "guard_repeatable" in query or "complete_repeatable" in query:
            return _Row((True,))
        return _Row(None)


class _Gate:
    def before_enqueue(self, _request):
        return type("Auth", (), {"coverage": None, "refresh_interval": None})()
    def before_execution(self, authorization): return authorization


class _Provenance:
    def __init__(self) -> None:
        self.persisted: list[tuple[int, tuple[RawFetchCapture, ...]]] = []
        self.verified: list[tuple[tuple[int, ...], tuple[int, ...]]] = []
        self.reprocessed: list[tuple[int, tuple[int, ...]]] = []

    def persist(self, item, _authorization, captures):
        self.persisted.append((item.id, tuple(captures)))
        return tuple(type("Persisted", (), {"fetch_id": value})() for value in (41,))

    def verify_source_fetches(self, _writer, _item, source_fetch_ids, *, replayed_fetch_ids=()):
        self.verified.append((tuple(source_fetch_ids), tuple(replayed_fetch_ids)))

    def record_reprocessing(self, _writer, item, replayed_fetch_ids, normalization_version):
        self.reprocessed.append((item.id, tuple(replayed_fetch_ids), normalization_version))


def _item() -> LeasedWorkItem:
    return LeasedWorkItem(1, 1, "scope", {"_sync_policy": {"provider_id": 1, "season_id": 1, "work_type": "x", "instance_id": 1, "version": 1}}, {}, 1, "x", 0, "key", "entity", "exec", 9)


def test_q06_runner_persists_raw_before_a_domain_transaction_rolls_back() -> None:
    # Keep this contract test independent from a database: the runner must
    # complete durable provenance before it enters the fenced domain transaction.
    from app.sync.worker import RepeatableSyncWorker, WorkResult

    connection = _Connection()
    provenance = _Provenance()
    worker = RepeatableSyncWorker(connection, _Gate(), "owner", heartbeat_connection_factory=lambda: _Connection(), provenance=provenance)  # type: ignore[arg-type]
    worker.repository.claim_next = lambda *_args, **_kwargs: _item()  # type: ignore[method-assign]
    now = datetime.now(UTC)
    capture = RawFetchCapture("/fixtures", {"league": 39}, _response(), now, now, "fixtures-v1")

    with pytest.raises(RuntimeError, match="normalization failed"):
        worker.run_once(lambda *_: WorkResult({}, raw_fetches=(capture,)), lambda *_: (_ for _ in ()).throw(RuntimeError("normalization failed")))

    assert provenance.persisted == [(1, (capture,))]


def test_q06_runner_gives_canonical_writer_the_durable_fetch_id() -> None:
    from app.sync.worker import RepeatableSyncWorker, WorkResult

    connection = _Connection()
    provenance = _Provenance()
    worker = RepeatableSyncWorker(connection, _Gate(), "owner", heartbeat_connection_factory=lambda: _Connection(), provenance=provenance)  # type: ignore[arg-type]
    worker.repository.claim_next = lambda *_args, **_kwargs: _item()  # type: ignore[method-assign]
    now = datetime.now(UTC)
    capture = RawFetchCapture("/fixtures", {"league": 39}, _response(), now, now, "fixtures-v1")
    seen: list[tuple[int, ...]] = []

    assert worker.run_once(lambda *_: WorkResult({}, raw_fetches=(capture,)), lambda _writer, _item, result: seen.append(result.source_fetch_ids))
    assert seen == [(41,)]
    assert provenance.verified == [((41,), ())]


def test_q06_runner_rejects_fresh_raw_mixed_with_existing_source_ids() -> None:
    from app.sync.worker import RepeatableSyncWorker, WorkResult

    connection = _Connection()
    provenance = _Provenance()
    worker = RepeatableSyncWorker(connection, _Gate(), "owner", heartbeat_connection_factory=lambda: _Connection(), provenance=provenance)  # type: ignore[arg-type]
    worker.repository.claim_next = lambda *_args, **_kwargs: _item()  # type: ignore[method-assign]
    now = datetime.now(UTC)
    capture = RawFetchCapture("/fixtures", {"league": 39}, _response(), now, now, "fixtures-v1")

    assert worker.run_once(
        lambda *_: WorkResult({}, raw_fetches=(capture,), source_fetch_ids=(73,)),
        lambda *_: pytest.fail("mixed source result reached the domain writer"),
    ) is True
    assert provenance.persisted == []
    assert provenance.verified == []


def test_q06_runner_prefers_verified_replay_over_another_provider_call() -> None:
    from app.sync.dispatch import Q03DispatchRegistry
    from app.sync.worker import RepeatableSyncWorker, WorkResult

    class ReplayableDispatch:
        def __init__(self) -> None: self.http_calls = 0
        def replay(self, _item, _authorization, _provenance):
            return WorkResult({}, source_fetch_ids=(73,), replay_normalization_version="fixtures-v2")
        def fetch(self, *_args): self.http_calls += 1; raise AssertionError("replay must not call the provider")
        def apply_result(self, _writer, _item, result): seen.append(result.source_fetch_ids)

    connection = _Connection()
    provenance = _Provenance()
    worker = RepeatableSyncWorker(connection, _Gate(), "owner", heartbeat_connection_factory=lambda: _Connection(), provenance=provenance)  # type: ignore[arg-type]
    worker.repository.claim_next = lambda *_args, **_kwargs: _item()  # type: ignore[method-assign]
    seen: list[tuple[int, ...]] = []
    dispatch = ReplayableDispatch()
    registry = Q03DispatchRegistry({"x": dispatch})

    assert worker.run_registered_once(registry)
    assert dispatch.http_calls == 0
    assert seen == [(73,)]
    assert provenance.verified == [((73,), (73,))]
    assert provenance.reprocessed == [(1, (73,), "fixtures-v2")]
