from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from time import sleep

from app.api_football import APIFootballBudgetDenied, APIFootballBudgetError, APIFootballClient
from app.api_football.errors import APIFootballHTTPError
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


class _CommitFailureConnection(_RunnerConnection):
    def execute(self, query, params=None):
        if "complete_repeatable" in query:
            raise RuntimeError("completion write failed")
        return super().execute(query, params)


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


def _item(*, checkpoint=None, attempts=1):
    return LeasedWorkItem(1, 1, "scope", {"_sync_policy": {"provider_id": 1, "season_id": 1, "work_type": "calendar_refresh", "instance_id": 1, "version": 1}}, checkpoint or {}, attempts, "calendar_refresh", 0, "key", "entity", "exec", 9)


def _events(caplog: pytest.LogCaptureFixture) -> list[dict[str, object]]:
    return [json.loads(record.message) for record in caplog.records if record.name == "app.sync.lifecycle"]


class _DenyBudget:
    def __init__(self, error): self.error = error
    async def reserve(self, _consumer): raise self.error
    async def observe(self, *_args): pytest.fail("denied request must not be observed")


@pytest.mark.parametrize(
    ("error_kind", "minimum_delay", "maximum_delay"),
    (
        ("denied", 115.0, 120.0),
        ("unavailable", 60.0, 60.0),
    ),
)
def test_runner_budget_error_defers_without_apply_or_contract_quarantine(
    error_kind: str, minimum_delay: float, maximum_delay: float,
) -> None:
    error = (
        APIFootballBudgetDenied("daily", datetime.now(UTC) + timedelta(seconds=120))
        if error_kind == "denied"
        else APIFootballBudgetError("budget unavailable")
    )
    connection = _RunnerConnection()
    worker = RepeatableSyncWorker(connection, _Gate(), "owner", heartbeat_connection_factory=_heartbeat_factory())  # type: ignore[arg-type]
    item = _item(checkpoint={"page": 4})
    worker.repository.claim_next = lambda *_args, **_kwargs: item  # type: ignore[method-assign]
    deferred: list[tuple[object, ...]] = []
    quarantined: list[tuple[object, ...]] = []
    worker.repository.defer_for_budget = lambda *args, **kwargs: deferred.append((*args, kwargs["delay"])) or True  # type: ignore[attr-defined,method-assign]
    worker.repository.requeue = lambda *args, **kwargs: quarantined.append((*args, kwargs)) or True  # type: ignore[method-assign]

    assert worker.run_once(lambda *_: (_ for _ in ()).throw(error), lambda *_: pytest.fail("apply")) is True

    assert len(deferred) == 1
    assert deferred[0][:2] == (item, "owner")
    delay = float(str(deferred[0][2]).removesuffix(" seconds"))
    assert minimum_delay <= delay <= maximum_delay
    assert quarantined == []


def test_runner_budget_denial_from_real_client_happens_before_http_and_defers() -> None:
    requests: list[httpx.Request] = []
    error = APIFootballBudgetDenied("daily", datetime.now(UTC) + timedelta(seconds=90))
    client = APIFootballClient(
        "test-secret",
        transport=httpx.MockTransport(lambda request: requests.append(request) or httpx.Response(200, json={})),
        budget=_DenyBudget(error),
        budget_consumer="operations",
    )
    connection = _RunnerConnection()
    worker = RepeatableSyncWorker(connection, _Gate(), "owner", heartbeat_connection_factory=_heartbeat_factory())  # type: ignore[arg-type]
    worker.repository.claim_next = lambda *_args, **_kwargs: _item(checkpoint={"page": 4})  # type: ignore[method-assign]
    deferred: list[object] = []
    worker.repository.defer_for_budget = lambda *args, **_kwargs: deferred.append(args) or True  # type: ignore[attr-defined,method-assign]
    try:
        assert worker.run_once(lambda *_: asyncio.run(client.get_once("fixtures")), lambda *_: pytest.fail("apply")) is True
    finally:
        asyncio.run(client.aclose())
    assert requests == []
    assert len(deferred) == 1


@pytest.mark.parametrize("status_code", (200, 429))
def test_runner_observation_failure_or_429_defers_without_refunding_consumed_reservation(status_code: int) -> None:
    class ConsumedThenUnavailableBudget:
        def __init__(self): self.reservations = 0
        async def reserve(self, _consumer): self.reservations += 1
        async def observe(self, observed_status, _headers):
            assert observed_status == status_code
            if status_code == 200:
                raise APIFootballBudgetError("observation unavailable")

    requests: list[httpx.Request] = []
    budget = ConsumedThenUnavailableBudget()
    client = APIFootballClient(
        "test-secret",
        transport=httpx.MockTransport(lambda request: requests.append(request) or httpx.Response(status_code, json={})),
        budget=budget,
        budget_consumer="operations",
    )
    connection = _RunnerConnection()
    worker = RepeatableSyncWorker(connection, _Gate(), "owner", heartbeat_connection_factory=_heartbeat_factory())  # type: ignore[arg-type]
    worker.repository.claim_next = lambda *_args, **_kwargs: _item()  # type: ignore[method-assign]
    deferred: list[object] = []
    worker.repository.defer_for_budget = lambda *args, **_kwargs: deferred.append(args) or True  # type: ignore[attr-defined,method-assign]
    try:
        assert worker.run_once(lambda *_: asyncio.run(client.get_once("fixtures")), lambda *_: pytest.fail("apply")) is True
    finally:
        asyncio.run(client.aclose())
    assert len(requests) == 1
    assert budget.reservations == 1
    assert len(deferred) == 1


def test_runner_budget_defer_fails_closed_after_heartbeat_or_lease_loss() -> None:
    connection = _RunnerConnection()
    worker = RepeatableSyncWorker(connection, _Gate(), "owner", heartbeat_connection_factory=_heartbeat_factory(False), heartbeat_interval=0.001)  # type: ignore[arg-type]
    worker.repository.claim_next = lambda *_args, **_kwargs: _item()  # type: ignore[method-assign]
    worker.repository.defer_for_budget = lambda *_args, **_kwargs: pytest.fail("failed heartbeat must not defer")  # type: ignore[attr-defined,method-assign]
    with pytest.raises(LeaseLost, match="heartbeat"):
        worker.run_once(
            lambda *_: (sleep(0.01), (_ for _ in ()).throw(APIFootballBudgetError("unavailable")))[1],
            lambda *_: pytest.fail("apply"),
        )

    healthy = RepeatableSyncWorker(connection, _Gate(), "owner", heartbeat_connection_factory=_heartbeat_factory())  # type: ignore[arg-type]
    healthy.repository.claim_next = lambda *_args, **_kwargs: _item()  # type: ignore[method-assign]
    healthy.repository.defer_for_budget = lambda *_args, **_kwargs: False  # type: ignore[attr-defined,method-assign]
    with pytest.raises(LeaseLost, match="lease"):
        healthy.run_once(
            lambda *_: (_ for _ in ()).throw(APIFootballBudgetError("unavailable")),
            lambda *_: pytest.fail("apply"),
        )


def test_runner_ordinary_fetch_error_remains_contract_quarantined(caplog: pytest.LogCaptureFixture) -> None:
    connection = _RunnerConnection()
    worker = RepeatableSyncWorker(connection, _Gate(), "owner", heartbeat_connection_factory=_heartbeat_factory())  # type: ignore[arg-type]
    worker.repository.claim_next = lambda *_args, **_kwargs: _item(checkpoint={"page": 4})  # type: ignore[method-assign]
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    worker.repository.requeue = lambda *args, **kwargs: calls.append((args, kwargs)) or True  # type: ignore[method-assign]
    with caplog.at_level(logging.INFO, logger="app.sync.lifecycle"):
        assert worker.run_once(lambda *_: (_ for _ in ()).throw(ValueError("bad payload")), lambda *_: pytest.fail("apply")) is True
    assert calls == [((_item(checkpoint={"page": 4}), "owner", {}, "bad payload"), {"contract_error": True})]
    assert _events(caplog)[-1]["event"] == "job_quarantined"


@pytest.mark.parametrize(("status_code", "transition"), ((429, "budget"), (503, "retry"), (0, "retry"), (400, "contract")))
def test_runner_classifies_provider_http_failures_without_apply(status_code: int, transition: str, caplog: pytest.LogCaptureFixture) -> None:
    connection = _RunnerConnection()
    worker = RepeatableSyncWorker(connection, _Gate(), "owner", heartbeat_connection_factory=_heartbeat_factory())  # type: ignore[arg-type]
    item = _item(checkpoint={"page": 4})
    worker.repository.claim_next = lambda *_args, **_kwargs: item  # type: ignore[method-assign]
    calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = []
    worker.repository.defer_for_budget = lambda *args, **kwargs: calls.append(("budget", args, kwargs)) or True  # type: ignore[method-assign]
    worker.repository.defer_for_retry = lambda *args, **kwargs: calls.append(("retry", args, kwargs)) or True  # type: ignore[attr-defined,method-assign]
    worker.repository.requeue = lambda *args, **kwargs: calls.append(("contract", args, kwargs)) or True  # type: ignore[method-assign]

    with caplog.at_level(logging.INFO, logger="app.sync.lifecycle"):
        assert worker.run_once(
            lambda *_: (_ for _ in ()).throw(APIFootballHTTPError(status_code)),
            lambda *_: pytest.fail("apply"),
        ) is True

    assert len(calls) == 1 and calls[0][0] == transition
    if transition == "budget":
        assert calls[0][1] == (item, "owner")
        assert float(str(calls[0][2]["delay"]).removesuffix(" seconds")) == 60
    elif transition == "retry":
        assert calls[0][1] == (item, "owner")
        assert calls[0][2]["error"] == f"provider_http_{status_code}"
        assert 2 <= float(str(calls[0][2]["delay"]).removesuffix(" seconds")) <= 3
    else:
        assert calls[0] == ("contract", (item, "owner", {}, f"API-Football returned HTTP {status_code}."), {"contract_error": True})
        assert _events(caplog)[-1]["event"] == "job_quarantined"
        assert _events(caplog)[-1]["reason"] == "provider_http_400"


def test_runner_quarantines_exhausted_5xx_instead_of_claiming_a_future_retry(caplog: pytest.LogCaptureFixture) -> None:
    connection = _RunnerConnection()
    worker = RepeatableSyncWorker(connection, _Gate(), "owner", heartbeat_connection_factory=_heartbeat_factory())  # type: ignore[arg-type]
    item = _item(attempts=5)
    worker.repository.claim_next = lambda *_args, **_kwargs: item  # type: ignore[method-assign]
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    worker.repository.requeue = lambda *args, **kwargs: calls.append((args, kwargs)) or True  # type: ignore[method-assign]
    worker.repository.defer_for_retry = lambda *_args, **_kwargs: pytest.fail("exhausted retry must not be deferred")  # type: ignore[attr-defined,method-assign]

    with caplog.at_level(logging.INFO, logger="app.sync.lifecycle"):
        assert worker.run_once(
            lambda *_: (_ for _ in ()).throw(APIFootballHTTPError(503)),
            lambda *_: pytest.fail("apply"), max_attempts=5,
        ) is True

    assert calls == [((item, "owner", {}, "provider_http_503_retry_exhausted"), {"contract_error": True})]
    assert _events(caplog)[-1]["event"] == "job_quarantined"
    assert _events(caplog)[-1]["reason"] == "provider_retry_exhausted"


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


def test_runner_claim_value_error_never_uses_an_unbound_item() -> None:
    worker = RepeatableSyncWorker(_RunnerConnection(), _Gate(), "owner", heartbeat_connection_factory=_heartbeat_factory())  # type: ignore[arg-type]
    worker.repository.claim_next = lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("bad claim"))  # type: ignore[method-assign]
    with pytest.raises(ValueError, match="bad claim"):
        worker.run_once(lambda *_: pytest.fail("fetch"), lambda *_: pytest.fail("apply"))


def test_runner_policy_requeue_false_raises_lease_lost_without_quarantine_event(caplog: pytest.LogCaptureFixture) -> None:
    worker = RepeatableSyncWorker(_RunnerConnection(), _Gate(denied=True), "owner", heartbeat_connection_factory=_heartbeat_factory())  # type: ignore[arg-type]
    worker.repository.claim_next = lambda *_args, **_kwargs: _item()  # type: ignore[method-assign]
    worker.repository.requeue = lambda *_args, **_kwargs: False  # type: ignore[method-assign]
    with caplog.at_level(logging.INFO, logger="app.sync.lifecycle"), pytest.raises(LeaseLost, match="policy failure"):
        worker.run_once(lambda *_: pytest.fail("fetch"), lambda *_: pytest.fail("apply"))
    assert [event["event"] for event in _events(caplog)] == ["job_lease_lost"]


def test_runner_generic_requeue_false_raises_lease_lost_without_quarantine_event(caplog: pytest.LogCaptureFixture) -> None:
    worker = RepeatableSyncWorker(_RunnerConnection(), _Gate(), "owner", heartbeat_connection_factory=_heartbeat_factory())  # type: ignore[arg-type]
    worker.repository.claim_next = lambda *_args, **_kwargs: _item()  # type: ignore[method-assign]
    worker.repository.requeue = lambda *_args, **_kwargs: False  # type: ignore[method-assign]
    with caplog.at_level(logging.INFO, logger="app.sync.lifecycle"), pytest.raises(LeaseLost, match="handler failure"):
        worker.run_once(lambda *_: (_ for _ in ()).throw(ValueError("unsafe secret")), lambda *_: pytest.fail("apply"))
    assert _events(caplog)[-1]["event"] == "job_lease_lost"


def test_runner_apply_failure_emits_terminal_failure_and_never_completed(caplog: pytest.LogCaptureFixture) -> None:
    worker = RepeatableSyncWorker(_RunnerConnection(), _Gate(), "owner", heartbeat_connection_factory=_heartbeat_factory())  # type: ignore[arg-type]
    worker.repository.claim_next = lambda *_args, **_kwargs: _item()  # type: ignore[method-assign]
    with caplog.at_level(logging.INFO, logger="app.sync.lifecycle"), pytest.raises(ValueError, match="apply failed"):
        worker.run_once(lambda *_: WorkResult({}), lambda *_: (_ for _ in ()).throw(ValueError("apply failed")))
    events = _events(caplog)
    assert events[-1]["event"] == "job_execution_failed"
    assert events[-1]["reason"] == "apply_or_commit_failure"
    assert all(event["event"] != "job_completed" for event in events)


def test_runner_provenance_failure_emits_terminal_failure_and_never_completed(caplog: pytest.LogCaptureFixture) -> None:
    class BrokenProvenance:
        def verify_source_fetches(self, *_args, **_kwargs) -> None:
            raise RuntimeError("source verification failed")

    class Replay:
        def replay(self, *_args) -> WorkResult:
            return WorkResult({}, source_fetch_ids=(17,), replay_normalization_version="test-v1")

    worker = RepeatableSyncWorker(
        _RunnerConnection(), _Gate(), "owner", provenance=BrokenProvenance(),
        heartbeat_connection_factory=_heartbeat_factory(),
    )  # type: ignore[arg-type]
    worker.repository.claim_next = lambda *_args, **_kwargs: _item()  # type: ignore[method-assign]
    with caplog.at_level(logging.INFO, logger="app.sync.lifecycle"), pytest.raises(RuntimeError, match="source verification"):
        worker.run_once(Replay(), lambda *_: pytest.fail("apply"))
    events = _events(caplog)
    assert events[-1]["event"] == "job_execution_failed"
    assert events[-1]["reason"] == "provenance_failure"
    assert all(event["event"] != "job_completed" for event in events)


def test_runner_completion_failure_emits_terminal_failure_and_never_completed(caplog: pytest.LogCaptureFixture) -> None:
    worker = RepeatableSyncWorker(
        _CommitFailureConnection(), _Gate(), "owner", heartbeat_connection_factory=_heartbeat_factory(),
    )  # type: ignore[arg-type]
    worker.repository.claim_next = lambda *_args, **_kwargs: _item()  # type: ignore[method-assign]
    with caplog.at_level(logging.INFO, logger="app.sync.lifecycle"), pytest.raises(RuntimeError, match="completion write failed"):
        worker.run_once(lambda *_: WorkResult({}), lambda *_: None)
    events = _events(caplog)
    assert events[-1]["event"] == "job_execution_failed"
    assert events[-1]["reason"] == "apply_or_commit_failure"
    assert all(event["event"] != "job_completed" for event in events)


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
