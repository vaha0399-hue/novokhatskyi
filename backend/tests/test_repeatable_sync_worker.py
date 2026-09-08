from __future__ import annotations

import pytest
from time import sleep

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


class _HeartbeatConnection(_Connection):
    def __init__(self, value=True): self.value = value
    def __enter__(self): return self
    def __exit__(self, *_): return False
    def execute(self, *_args, **_kwargs): return _Row((self.value,))


def _heartbeat_factory(value=True):
    return lambda: _HeartbeatConnection(value)


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
    worker = RepeatableSyncWorker(connection, _Gate(), "owner", heartbeat_connection_factory=_heartbeat_factory())  # type: ignore[arg-type]
    worker.repository.claim_next = lambda *_args, **_kwargs: _item()  # type: ignore[method-assign]
    seen: list[bool] = []
    with pytest.raises(LeaseLost):
        worker.run_once(lambda *_: (seen.append(connection.active), WorkResult({}))[1], lambda *_: pytest.fail("apply"))
    assert seen == [False]


def test_runner_policy_denial_quarantines_without_fetch_or_apply() -> None:
    connection = _RunnerConnection()
    worker = RepeatableSyncWorker(connection, _Gate(denied=True), "owner", heartbeat_connection_factory=_heartbeat_factory())  # type: ignore[arg-type]
    worker.repository.claim_next = lambda *_args, **_kwargs: _item()  # type: ignore[method-assign]
    calls: list[object] = []
    worker.repository.requeue = lambda *args, **kwargs: calls.append(args) or True  # type: ignore[method-assign]
    assert worker.run_once(lambda *_: pytest.fail("fetch"), lambda *_: pytest.fail("apply")) is True
    assert calls and calls[0][-1] == "competition sync policy denied: disabled"


def test_runner_heartbeat_failure_prevents_apply_after_fetch() -> None:
    connection = _RunnerConnection()
    worker = RepeatableSyncWorker(connection, _Gate(), "owner", heartbeat_connection_factory=_heartbeat_factory(False), heartbeat_interval=0.001)  # type: ignore[arg-type]
    worker.repository.claim_next = lambda *_args, **_kwargs: _item()  # type: ignore[method-assign]
    with pytest.raises(LeaseLost, match="heartbeat"):
        worker.run_once(lambda *_: (sleep(0.01), WorkResult({}))[1], lambda *_: pytest.fail("apply"))


def test_cup_canonical_sink_accepts_atomic_nested_transaction_capability() -> None:
    connection = _Connection()
    writer = AtomicWorkTransaction(connection)  # type: ignore[arg-type]
    calls = []
    def canonical(conn, *_):
        with conn.transaction():
            calls.append(conn.execute("canonical"))
    sink = CupCanonicalSink(writer, write_validated_base=canonical)  # type: ignore[arg-type]
    sink.write_cup_base(validated=None, collected=[])  # type: ignore[arg-type]
    assert len(calls) == 1


def test_atomic_writer_capability_rejects_commit_rollback_close_and_connection_access() -> None:
    writer = AtomicWorkTransaction(_Connection())  # type: ignore[arg-type]
    with pytest.raises(AttributeError):
        writer.execute("SELECT 1").connection
    for statement in ("COMMIT", "/* bypass */ COMMIT"):
        with pytest.raises(RuntimeError, match="cannot control"):
            writer.execute(statement)
    for statement in ("SELECT 1; COMMIT", "SELECT 1; SELECT 2"):
        with pytest.raises(RuntimeError, match="exactly one"):
            writer.execute(statement)
    with pytest.raises(TypeError, match="text SQL"):
        writer.execute(b"SELECT 1")  # type: ignore[arg-type]
    for forbidden in ("commit", "rollback", "close", "connection"):
        with pytest.raises(AttributeError):
            getattr(writer, forbidden)
    with pytest.raises(AttributeError):
        writer.transaction().connection
    with writer.transaction():
        writer.execute("SELECT nested")


def test_atomic_writer_lexes_literals_and_comments_without_rewriting_sql() -> None:
    writer = AtomicWorkTransaction(_Connection())  # type: ignore[arg-type]
    for statement in (
        "SELECT E'-- literal; COMMIT'",
        "SELECT $tag$-- literal; COMMIT$tag$",
        "SELECT /* outer /* nested */ comment */ 1",
    ):
        writer.execute(statement)
    for statement in (
        "SELECT E'-- literal'; COMMIT",
        "SELECT $tag$-- literal; COMMIT$tag$; COMMIT",
        "SELECT /* outer /* nested */ comment */ 1; COMMIT",
        "SELECT 1 AS alias$tag$; COMMIT; $tag$",
        "SELECT 1 -- LF comment\n; COMMIT",
        "SELECT 1 -- CR comment\r; COMMIT",
        "SELECT 1 -- CRLF comment\r\n; COMMIT",
    ):
        with pytest.raises(RuntimeError, match="exactly one"):
            writer.execute(statement)
    for statement in ("SET LOCAL statement_timeout='1s'", "DO $$ BEGIN COMMIT; END $$", "CALL unsafe()"):
        with pytest.raises(RuntimeError, match="cannot control"):
            writer.execute(statement)
    writer.execute("SELECT set_config('standard_conforming_strings', 'on', false)")
    with pytest.raises(RuntimeError, match="requires E strings"):
        writer.execute(r"SELECT 'literal\'; COMMIT; -- '")


def test_runner_requires_heartbeat_connection_factory() -> None:
    with pytest.raises(ValueError, match="heartbeat connection"):
        RepeatableSyncWorker(_RunnerConnection(), _Gate(), "owner")  # type: ignore[arg-type]


def test_runner_heartbeat_factory_exception_prevents_apply() -> None:
    connection = _RunnerConnection()
    def broken_factory():
        raise OSError("heartbeat connection unavailable")
    worker = RepeatableSyncWorker(connection, _Gate(), "owner", heartbeat_connection_factory=broken_factory, heartbeat_interval=0.001)  # type: ignore[arg-type]
    worker.repository.claim_next = lambda *_args, **_kwargs: _item()  # type: ignore[method-assign]
    with pytest.raises(LeaseLost, match="heartbeat"):
        worker.run_once(lambda *_: (sleep(0.01), WorkResult({}))[1], lambda *_: pytest.fail("apply"))
