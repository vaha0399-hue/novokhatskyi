from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime

import pytest
from psycopg import OperationalError
from redis.exceptions import ConnectionError as RedisConnectionError

from app.api_football import APIFootballResponse
from app.api_football.errors import APIFootballHTTPError
from app.live import (
    CanonicalFixtureReference,
    LiveNormalizationError,
    LiveReconciliationError,
    LiveSettings,
    bind_live_fixture,
    normalize_live_fixture,
)
from app.live.repository import FixtureReconciliationTask
from app.live.worker import LiveWorker, run_supervised


NOW = datetime(2026, 8, 30, 16, 7, tzinfo=UTC)


def _entry(
    status: str = "2H",
    *,
    fixture_id: int = 1557383,
    league_id: int = 39,
    goals: tuple[int, int] = (2, 1),
) -> dict:
    return {
        "fixture": {
            "id": fixture_id,
            "status": {"short": status, "elapsed": 67, "extra": 1},
        },
        "league": {"id": league_id, "season": 2026},
        "teams": {"home": {"id": 40}, "away": {"id": 65}},
        "goals": {"home": goals[0], "away": goals[1]},
        # Deliberately wrong for live states: this field must not be read.
        "score": {"fulltime": {"home": 99, "away": 98}},
    }


def _response(parameters: dict[str, str], entries: list[dict]) -> APIFootballResponse:
    payload = {
        "get": "fixtures",
        "parameters": parameters,
        "errors": [],
        "results": len(entries),
        "paging": {"current": 1, "total": 1},
        "response": entries,
    }
    raw = json.dumps(payload, separators=(",", ":")).encode()
    return APIFootballResponse(payload, raw, 200, {})


def _terminal_entry(*, fixture_id: int = 1557383) -> dict:
    entry = _entry("FT", fixture_id=fixture_id)
    entry["score"] = {
        "halftime": {"home": 1, "away": 0},
        "fulltime": {"home": 2, "away": 1},
        "extratime": {"home": None, "away": None},
        "penalty": {"home": None, "away": None},
    }
    return entry


def _reference(fixture_id: int = 101) -> CanonicalFixtureReference:
    return CanonicalFixtureReference(
        fixture_id=fixture_id,
        season_id=14,
        league_id=7,
        kickoff_at=datetime(2026, 8, 30, 15, tzinfo=UTC),
        home_team_id=10,
        home_team_name="Liverpool",
        away_team_id=20,
        away_team_name="Nottingham Forest",
    )


def _stored_state(
    *, provider_fixture_id: int = 1557383, fixture_id: int = 101
):
    return bind_live_fixture(
        normalize_live_fixture(_entry("1H", fixture_id=provider_fixture_id)),
        _reference(fixture_id),
        observed_at=NOW,
    )


class FakeProvider:
    def __init__(self, outcomes: list[APIFootballResponse | Exception]) -> None:
        self.outcomes = outcomes
        self.calls: list[tuple[str, dict[str, str | int] | None]] = []

    async def get(
        self, endpoint: str, *, params: dict[str, str | int] | None = None
    ) -> APIFootballResponse:
        self.calls.append((endpoint, params))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    def response_contains_api_key(self, body: bytes) -> bool:
        assert body
        return False


class FakeRepository:
    def __init__(
        self,
        fixture_ids: dict[int, int] | None = None,
        *,
        due_task: FixtureReconciliationTask | None = None,
    ) -> None:
        self.fixture_ids = fixture_ids or {1557383: 101}
        self.resolved: list[int] = []
        self.handed_off: list[int] = []
        self.handoff_error: Exception | None = None
        self.due_task = due_task
        self.persisted: list[tuple] = []
        self.failures: list[dict] = []

    def _reference_for(self, provider_fixture_id: int) -> CanonicalFixtureReference:
        return _reference(self.fixture_ids[provider_fixture_id])

    async def resolve(self, fixture):
        self.resolved.append(fixture.external_fixture_id)
        return self._reference_for(fixture.external_fixture_id)

    async def ensure_terminal_reconciliation(self, fixture, *, observed_at):
        assert observed_at == NOW
        if self.handoff_error is not None:
            raise self.handoff_error
        self.handed_off.append(fixture.external_fixture_id)
        return self._reference_for(fixture.external_fixture_id)

    async def next_due_reconciliation(
        self, *, league_external_ids, as_of, exclude_fixture_ids=()
    ):
        if self.due_task and self.due_task.fixture_id not in exclude_fixture_ids:
            return self.due_task
        return None

    async def persist_reconciliation_response(self, *args, **kwargs):
        self.persisted.append((args, kwargs))

    async def record_reconciliation_failure(self, *args, **kwargs):
        self.failures.append(kwargs)


class FakeStore:
    def __init__(self, active=(), *, active_error: Exception | None = None) -> None:
        self.current = tuple(active)
        self.active_error = active_error
        self.applied: list[tuple[tuple, frozenset[int]]] = []

    async def active(self):
        if self.active_error is not None:
            raise self.active_error
        return self.current

    async def apply_poll(self, active_states, *, finished_fixture_ids=()):
        call = (tuple(active_states), frozenset(finished_fixture_ids))
        self.applied.append(call)
        current_by_id = {state.fixture_id: state for state in self.current}
        current_by_id.update({state.fixture_id: state for state in active_states})
        for fixture_id in finished_fixture_ids:
            current_by_id.pop(fixture_id, None)
        self.current = tuple(current_by_id.values())


def _settings(*league_ids: int) -> LiveSettings:
    return LiveSettings(
        redis_url="redis://localhost:6379/0",
        league_external_ids=league_ids or (39,),
    )


def test_poll_once_makes_one_scoped_request_and_publishes_current_score() -> None:
    provider = FakeProvider([_response({"live": "39"}, [_entry()])])
    repository = FakeRepository()
    store = FakeStore()
    worker = LiveWorker(
        provider=provider,
        repository=repository,
        store=store,
        settings=_settings(39),
        clock=lambda: NOW,
    )

    report = asyncio.run(worker.poll_once())

    assert report.provider_request_count == 1
    assert provider.calls == [("/fixtures", {"live": "39"})]
    assert repository.resolved == [1557383]
    state = store.applied[0][0][0]
    assert (state.score.home, state.score.away) == (2, 1)
    assert store.applied[0][1] == frozenset()


def test_empty_live_cycle_still_makes_exactly_one_provider_request() -> None:
    provider = FakeProvider([_response({"live": "39-2"}, [])])
    store = FakeStore()
    worker = LiveWorker(
        provider=provider,
        repository=FakeRepository(),
        store=store,
        settings=_settings(39, 2),
        clock=lambda: NOW,
    )

    report = asyncio.run(worker.poll_once())

    assert report.provider_request_count == 1
    assert provider.calls == [("/fixtures", {"live": "39-2"})]
    assert store.applied == [((), frozenset())]


def test_ft_in_live_feed_needs_no_additional_provider_request() -> None:
    provider = FakeProvider([_response({"live": "39"}, [_entry("FT")])])
    repository = FakeRepository()
    store = FakeStore([_stored_state()])
    worker = LiveWorker(
        provider=provider,
        repository=repository,
        store=store,
        settings=_settings(39),
        clock=lambda: NOW,
    )

    report = asyncio.run(worker.poll_once())

    assert report.provider_request_count == 1
    assert report.finished_count == 1
    assert repository.handed_off == [1557383]
    assert store.current == ()
    assert store.applied == [((), frozenset({101}))]


def test_due_postmatch_ft_uses_the_single_secondary_request_budget() -> None:
    task = FixtureReconciliationTask(
        fixture_id=101,
        provider_fixture_id=1557383,
        season_id=14,
        eligible_at=NOW,
        attempt_count=0,
        max_attempts=4,
    )
    provider = FakeProvider(
        [
            _response({"live": "39"}, []),
            _response({"id": "1557383"}, [_terminal_entry()]),
        ]
    )
    repository = FakeRepository(due_task=task)
    worker = LiveWorker(
        provider=provider,
        repository=repository,
        store=FakeStore([_stored_state()]),
        settings=_settings(39),
        clock=lambda: NOW,
    )

    report = asyncio.run(worker.poll_once())

    assert report.provider_request_count == 2
    assert repository.persisted
    assert repository.persisted[0][1]["result"].home_fulltime_goals == 2
    assert repository.failures == []


def test_ft_in_live_feed_excludes_just_finished_fixture_from_due_request() -> None:
    task = FixtureReconciliationTask(
        fixture_id=101,
        provider_fixture_id=1557383,
        season_id=14,
        eligible_at=NOW,
        attempt_count=0,
        max_attempts=4,
    )
    provider = FakeProvider([_response({"live": "39"}, [_terminal_entry()])])
    repository = FakeRepository(due_task=task)
    worker = LiveWorker(
        provider=provider,
        repository=repository,
        store=FakeStore([_stored_state()]),
        settings=_settings(39),
        clock=lambda: NOW,
    )

    report = asyncio.run(worker.poll_once())

    assert report.provider_request_count == 1
    assert repository.handed_off == [1557383]


def test_due_queue_and_disappearance_checks_alternate_fairly() -> None:
    disappeared_id = 1557383
    due_id = 1660000
    repository = FakeRepository(
        {disappeared_id: 101, due_id: 202},
        due_task=FixtureReconciliationTask(
            fixture_id=202,
            provider_fixture_id=due_id,
            season_id=14,
            eligible_at=NOW,
            attempt_count=0,
            max_attempts=4,
        ),
    )
    provider = FakeProvider(
        [
            _response({"live": "39"}, []),
            _response({"id": str(due_id)}, [_entry("2H", fixture_id=due_id)]),
            _response({"live": "39"}, []),
            _response({"id": str(disappeared_id)}, [_terminal_entry(fixture_id=disappeared_id)]),
        ]
    )
    worker = LiveWorker(
        provider=provider,
        repository=repository,
        store=FakeStore([_stored_state(provider_fixture_id=disappeared_id)]),
        settings=_settings(39),
        clock=lambda: NOW,
        monotonic_clock=lambda: 100.0,
    )

    first = asyncio.run(worker.poll_once())
    second = asyncio.run(worker.poll_once())

    assert first.provider_request_count == second.provider_request_count == 2
    assert provider.calls == [
        ("/fixtures", {"live": "39"}),
        ("/fixtures", {"id": due_id}),
        ("/fixtures", {"live": "39"}),
        ("/fixtures", {"id": disappeared_id}),
    ]
    assert repository.handed_off == [disappeared_id]


def test_due_postmatch_provider_failure_consumes_bounded_attempt() -> None:
    task = FixtureReconciliationTask(
        fixture_id=101,
        provider_fixture_id=1557383,
        season_id=14,
        eligible_at=NOW,
        attempt_count=0,
        max_attempts=4,
    )
    provider = FakeProvider(
        [_response({"live": "39"}, []), APIFootballHTTPError(503)]
    )
    repository = FakeRepository(due_task=task)
    worker = LiveWorker(
        provider=provider,
        repository=repository,
        store=FakeStore(),
        settings=_settings(39),
        clock=lambda: NOW,
    )

    report = asyncio.run(worker.poll_once())

    assert report.provider_request_count == 2
    assert repository.persisted == []
    assert repository.failures[0]["outcome"] == "http_error"


def test_disappeared_fixture_is_removed_only_after_fixture_bound_ft() -> None:
    provider = FakeProvider(
        [
            _response({"live": "39"}, []),
            _response({"id": "1557383"}, [_entry("FT")]),
        ]
    )
    repository = FakeRepository()
    store = FakeStore([_stored_state()])
    worker = LiveWorker(
        provider=provider,
        repository=repository,
        store=store,
        settings=_settings(39),
        clock=lambda: NOW,
    )

    report = asyncio.run(worker.poll_once())

    assert report.provider_request_count == 2
    assert report.finished_count == 1
    assert provider.calls[1] == ("/fixtures", {"id": 1557383})
    assert repository.handed_off == [1557383]
    assert store.current == ()


def test_disappeared_but_nonterminal_fixture_remains_live() -> None:
    provider = FakeProvider(
        [
            _response({"live": "39"}, []),
            _response({"id": "1557383"}, [_entry("2H", goals=(3, 1))]),
        ]
    )
    repository = FakeRepository()
    store = FakeStore([_stored_state()])
    worker = LiveWorker(
        provider=provider,
        repository=repository,
        store=store,
        settings=_settings(39),
        clock=lambda: NOW,
    )

    report = asyncio.run(worker.poll_once())

    assert report.finished_count == 0
    assert repository.handed_off == []
    assert store.current[0].score.home == 3
    assert store.applied[0][1] == frozenset()


def test_terminal_handoff_failure_leaves_redis_snapshot_untouched() -> None:
    provider = FakeProvider(
        [
            _response({"live": "39"}, []),
            _response({"id": "1557383"}, [_entry("FT")]),
        ]
    )
    repository = FakeRepository()
    repository.handoff_error = LiveReconciliationError("database rejected handoff")
    store = FakeStore([_stored_state()])
    worker = LiveWorker(
        provider=provider,
        repository=repository,
        store=store,
        settings=_settings(39),
        clock=lambda: NOW,
    )

    with pytest.raises(LiveReconciliationError, match="database rejected"):
        asyncio.run(worker.poll_once())

    assert store.applied == []
    assert store.current == (_stored_state(),)


def test_disappeared_fixture_rechecks_have_one_request_per_cycle_budget() -> None:
    fixture_ids = {1557383 + index: 101 + index for index in range(10)}
    stored = [
        _stored_state(provider_fixture_id=provider_id, fixture_id=fixture_id)
        for provider_id, fixture_id in fixture_ids.items()
    ]
    first_provider_id = min(fixture_ids)
    provider = FakeProvider(
        [
            _response({"live": "39"}, []),
            _response(
                {"id": str(first_provider_id)},
                [_entry("2H", fixture_id=first_provider_id)],
            ),
        ]
    )
    worker = LiveWorker(
        provider=provider,
        repository=FakeRepository(fixture_ids),
        store=FakeStore(stored),
        settings=_settings(39),
        clock=lambda: NOW,
        monotonic_clock=lambda: 100.0,
    )

    report = asyncio.run(worker.poll_once())

    assert report.provider_request_count == 2
    assert report.active_count == 10
    assert len(provider.calls) == 2
    assert provider.calls[1] == ("/fixtures", {"id": first_provider_id})


def test_nonterminal_disappearance_uses_five_minute_recheck_cooldown() -> None:
    monotonic_now = [100.0]
    provider = FakeProvider(
        [
            _response({"live": "39"}, []),
            _response({"id": "1557383"}, [_entry("2H")]),
            _response({"live": "39"}, []),
            _response({"live": "39"}, []),
            _response({"id": "1557383"}, [_entry("FT")]),
        ]
    )
    worker = LiveWorker(
        provider=provider,
        repository=FakeRepository(),
        store=FakeStore([_stored_state()]),
        settings=_settings(39),
        clock=lambda: NOW,
        monotonic_clock=lambda: monotonic_now[0],
    )

    first = asyncio.run(worker.poll_once())
    second = asyncio.run(worker.poll_once())
    monotonic_now[0] += 300
    third = asyncio.run(worker.poll_once())

    assert first.provider_request_count == 2
    assert second.provider_request_count == 1
    assert third.provider_request_count == 2
    assert [params for _, params in provider.calls] == [
        {"live": "39"},
        {"id": 1557383},
        {"live": "39"},
        {"live": "39"},
        {"id": 1557383},
    ]


def test_redis_read_failure_spends_no_provider_request() -> None:
    provider = FakeProvider([])
    worker = LiveWorker(
        provider=provider,
        repository=FakeRepository(),
        store=FakeStore(active_error=RedisConnectionError("unavailable")),
        settings=_settings(39),
    )

    with pytest.raises(RedisConnectionError):
        asyncio.run(worker.poll_once())

    assert provider.calls == []


def test_provider_failure_retries_after_configured_25_seconds() -> None:
    class StopLoop(Exception):
        pass

    delays: list[float] = []

    async def stop_after_delay(delay: float) -> None:
        delays.append(delay)
        raise StopLoop

    worker = LiveWorker(
        provider=FakeProvider([APIFootballHTTPError(503)]),
        repository=FakeRepository(),
        store=FakeStore(),
        settings=_settings(39),
        clock=lambda: NOW,
        sleep=stop_after_delay,
        monotonic_clock=lambda: 0.0,
    )

    with pytest.raises(StopLoop):
        asyncio.run(worker.run_forever())

    assert delays == [25]


def test_poll_cadence_subtracts_cycle_processing_time() -> None:
    class StopLoop(Exception):
        pass

    monotonic_values = iter((100.0, 105.0))
    delays: list[float] = []

    async def stop_after_delay(delay: float) -> None:
        delays.append(delay)
        raise StopLoop

    worker = LiveWorker(
        provider=FakeProvider([_response({"live": "39"}, [])]),
        repository=FakeRepository(),
        store=FakeStore(),
        settings=_settings(39),
        clock=lambda: NOW,
        sleep=stop_after_delay,
        monotonic_clock=lambda: next(monotonic_values),
    )

    with pytest.raises(StopLoop):
        asyncio.run(worker.run_forever())

    assert delays == [20]


def test_malformed_provider_payload_stays_fail_closed() -> None:
    response = _response({"live": "other"}, [])
    worker = LiveWorker(
        provider=FakeProvider([response]),
        repository=FakeRepository(),
        store=FakeStore(),
        settings=_settings(39),
    )

    with pytest.raises(LiveNormalizationError, match="parameters"):
        asyncio.run(worker.run_forever())


@pytest.mark.parametrize(
    "transient_error",
    [
        pytest.param(OperationalError("database secret"), id="postgres"),
        pytest.param(RedisConnectionError("redis secret"), id="redis"),
    ],
)
def test_supervisor_reconnects_only_transient_infrastructure_errors(
    transient_error: Exception,
    caplog: pytest.LogCaptureFixture,
) -> None:
    class StopLoop(Exception):
        pass

    attempts = 0
    delays: list[float] = []

    async def session_runner(provider, settings, database_url):
        nonlocal attempts
        attempts += 1
        assert database_url == "postgresql://server/database"
        if attempts == 1:
            raise transient_error
        raise StopLoop

    async def record_delay(delay: float) -> None:
        delays.append(delay)

    caplog.set_level(logging.WARNING)
    with pytest.raises(StopLoop):
        asyncio.run(
            run_supervised(
                FakeProvider([]),
                _settings(39),
                "postgresql://server/database",
                session_runner=session_runner,
                sleep=record_delay,
            )
        )

    assert attempts == 2
    assert delays == [25]
    assert "secret" not in caplog.text
