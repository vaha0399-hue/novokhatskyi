from __future__ import annotations

import pytest

from app.sync.policies import PolicyDenialReason, SyncPolicyDenied
from app.sync.repository import LeasedWorkItem
from app.sync.worker import LeaseLost, RepeatableSyncWorker, WorkResult
from app.sync.worker import AtomicWorkTransaction
from app.importer.cup_canonical import CupCanonicalSink


class _Connection:
    def execute(self, query, params=None):
        return query, params

    def transaction(self):
        return _Transaction()


class _Transaction:
    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


class _Row:
    def __init__(self, value): self.value = value
    def fetchone(self): return self.value


class _RunnerConnection(_Connection):
    def __init__(self, guard=True): self.active = False; self.guard = guard
    def transaction(self):
        connection = self
        class Tx:
            def __enter__(self): connection.active = True; return self
            def __exit__(self, *_): connection.active = False; return False
        return Tx()
    def execute(self, query, params=None):
        if "guard_repeatable" in query: return _Row((self.guard,))
        if "complete_repeatable" in query: return _Row((True,))
        return _Row(None)


class _Gate:
    def __init__(self, denied=False): self.denied = denied
    def before_enqueue(self, request):
        if self.denied: raise SyncPolicyDenied(PolicyDenialReason.DISABLED)
        return type("Auth", (), {"coverage": None, "refresh_interval": None})()
    def before_execution(self, authorization):
        if self.denied: raise SyncPolicyDenied(PolicyDenialReason.DISABLED)
        return authorization


def _item():
    return LeasedWorkItem(1, 1, "scope", {"_sync_policy": {"provider_id": 1, "season_id": 1, "work_type": "x", "instance_id": 1, "version": 1}}, {}, 1, "x", 0, "key", "entity", "exec", 9)


def test_runner_fetch_is_outside_transaction_and_lost_guard_never_applies() -> None:
    connection = _RunnerConnection(guard=False)
    worker = RepeatableSyncWorker(connection, _Gate(), "owner")  # type: ignore[arg-type]
    worker.repository.claim_next = lambda *_args, **_kwargs: _item()  # type: ignore[method-assign]
    seen: list[bool] = []
    with pytest.raises(LeaseLost):
        worker.run_once(lambda *_: (seen.append(connection.active), WorkResult({}))[1], lambda *_: pytest.fail("apply"))
    assert seen == [False]


def test_runner_policy_denial_quarantines_without_fetch_or_apply() -> None:
    connection = _RunnerConnection()
    worker = RepeatableSyncWorker(connection, _Gate(denied=True), "owner")  # type: ignore[arg-type]
    worker.repository.claim_next = lambda *_args, **_kwargs: _item()  # type: ignore[method-assign]
    calls: list[object] = []
    worker.repository.requeue = lambda *args, **kwargs: calls.append(args) or True  # type: ignore[method-assign]
    assert worker.run_once(lambda *_: pytest.fail("fetch"), lambda *_: pytest.fail("apply")) is True
    assert calls and calls[0][-1] == "competition sync policy denied: disabled"


def test_runner_heartbeat_failure_prevents_apply_after_fetch() -> None:
    connection = _RunnerConnection()
    worker = RepeatableSyncWorker(connection, _Gate(), "owner", heartbeat=lambda _: False)  # type: ignore[arg-type]
    worker.repository.claim_next = lambda *_args, **_kwargs: _item()  # type: ignore[method-assign]
    with pytest.raises(LeaseLost, match="heartbeat"):
        worker.run_once(lambda *_: WorkResult({}), lambda *_: pytest.fail("apply"))


def test_cup_canonical_sink_accepts_atomic_nested_transaction_capability() -> None:
    connection = _Connection()
    writer = AtomicWorkTransaction(connection)  # type: ignore[arg-type]
    calls = []
    def canonical(conn, *_):
        with conn.transaction():
            calls.append(conn.execute("canonical"))
    sink = CupCanonicalSink(writer, write_validated_base=canonical)  # type: ignore[arg-type]
    sink.write_cup_base(validated=None, collected=[])  # type: ignore[arg-type]
    assert calls == [("canonical", None)]


def test_atomic_writer_capability_rejects_commit_rollback_close_and_connection_access() -> None:
    writer = AtomicWorkTransaction(_Connection())  # type: ignore[arg-type]
    assert writer.execute("SELECT 1") == ("SELECT 1", None)
    for forbidden in ("commit", "rollback", "close", "connection"):
        with pytest.raises(AttributeError):
            getattr(writer, forbidden)
    with writer.transaction():
        assert writer.execute("SELECT nested") == ("SELECT nested", None)
