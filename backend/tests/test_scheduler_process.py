from __future__ import annotations

from contextlib import nullcontext
from datetime import UTC, date, datetime, timedelta

from app.sync.policies import CompetitionSyncPolicy, CoverageObservation, CoverageState, RefreshInterval
from app.sync.scheduler import FixtureScheduleSnapshot, PeriodicScheduleState, ScheduleDecisionReason, SeasonScheduleSnapshot, SyncScheduler
from app.sync.scheduler_process import Q05SchedulerProcess
from app.sync.scheduler_repository import SchedulerEnqueueResult


class _Connection:
    def __init__(self) -> None:
        self.transactions = 0

    def transaction(self):
        self.transactions += 1
        return nullcontext()


class _Repository:
    def __init__(self) -> None:
        self.calls = []

    def enqueue_and_advance(self, **kwargs):
        self.calls.append(kwargs)
        return SchedulerEnqueueResult(11, True, True)

    def enqueue_event(self, **kwargs):
        self.calls.append(kwargs)
        return SchedulerEnqueueResult(12, True, True)


class _Handler:
    def fetch(self, item, authorization):
        raise AssertionError("scheduler must not fetch")

    def apply_result(self, writer, item, result):
        raise AssertionError("scheduler must not import")


def _policy() -> CompetitionSyncPolicy:
    return CompetitionSyncPolicy(
        provider_id=7, season_id=101, policy_instance_id=1, enabled=True,
        allowed_work_types=frozenset({"calendar_refresh"}),
        coverage={"calendar_refresh": CoverageObservation(CoverageState.COVERED, date(2026, 9, 1))},
        refresh_intervals={"calendar_refresh": RefreshInterval(1, "hour")}, priority=3,
        history_depth_seasons=0, policy_version=1, paused_until=None,
    )


def _state() -> PeriodicScheduleState:
    start = datetime(2026, 9, 8, tzinfo=UTC)
    return PeriodicScheduleState(7, 101, "calendar_refresh", start, start + timedelta(hours=1))


def test_unavailable_handler_keeps_candidate_visible_but_never_starts_a_transaction() -> None:
    connection, repository = _Connection(), _Repository()
    process = Q05SchedulerProcess(connection, repository, SyncScheduler(), {})
    result = process.enqueue_due(run_id=4, now=datetime(2026, 9, 8, 1, tzinfo=UTC), policies=[_policy()], schedule_state=[_state()])
    assert result.preview.decisions[0].reason == ScheduleDecisionReason.HANDLER_UNAVAILABLE.value
    assert repository.calls == [] and connection.transactions == 0


def test_registered_q03_compatible_handler_allows_only_enqueue_checkpoint_transition() -> None:
    connection, repository = _Connection(), _Repository()
    process = Q05SchedulerProcess(connection, repository, SyncScheduler(), {"calendar_refresh": _Handler()})
    result = process.enqueue_due(run_id=4, now=datetime(2026, 9, 8, 1, tzinfo=UTC), policies=[_policy()], schedule_state=[_state()])
    assert len(result.enqueue_results) == len(repository.calls) == connection.transactions == 1
    assert repository.calls[0]["expected_state"] == _state()


def test_schedule_state_generator_is_materialized_before_preview_and_enqueue() -> None:
    connection, repository = _Connection(), _Repository()
    process = Q05SchedulerProcess(connection, repository, SyncScheduler(), {"calendar_refresh": _Handler()})
    result = process.enqueue_due(
        run_id=4, now=datetime(2026, 9, 8, 1, tzinfo=UTC), policies=[_policy()],
        schedule_state=(state for state in [_state()]),
    )
    assert len(result.enqueue_results) == 1
    assert repository.calls[0]["expected_state"] == _state()


def test_non_dispatch_object_does_not_make_a_handler_available() -> None:
    connection, repository = _Connection(), _Repository()
    process = Q05SchedulerProcess(connection, repository, SyncScheduler(), {"calendar_refresh": object()})  # type: ignore[dict-item]
    result = process.enqueue_due(run_id=4, now=datetime(2026, 9, 8, 1, tzinfo=UTC), policies=[_policy()], schedule_state=[_state()])
    assert result.preview.decisions[0].reason == ScheduleDecisionReason.HANDLER_UNAVAILABLE.value
    assert repository.calls == [] and connection.transactions == 0


def test_due_fixture_events_enqueue_independently_without_a_season_checkpoint() -> None:
    policy = CompetitionSyncPolicy(
        provider_id=7, season_id=101, policy_instance_id=1, enabled=True,
        allowed_work_types=frozenset({"schedule_near", "prematch_check"}),
        coverage={name: CoverageObservation(CoverageState.COVERED, date(2026, 9, 1)) for name in ("schedule_near", "prematch_check")},
        refresh_intervals={name: RefreshInterval(1, "hour") for name in ("schedule_near", "prematch_check")},
        priority=3, history_depth_seasons=0, policy_version=1, paused_until=None,
    )
    connection, repository = _Connection(), _Repository()
    process = Q05SchedulerProcess(connection, repository, SyncScheduler(), {"schedule_near": _Handler(), "prematch_check": _Handler()})
    result = process.enqueue_due(
        run_id=4, now=datetime(2026, 9, 8, 12, tzinfo=UTC), policies=[policy], schedule_state=[],
        fixtures=[FixtureScheduleSnapshot(9, 7, 101, datetime(2026, 9, 8, 12, 30, tzinfo=UTC), "scheduled")],
    )
    assert len(result.enqueue_results) == len(repository.calls) == connection.transactions == 2
    assert {call["work"].work_type for call in repository.calls} == {"schedule_near", "prematch_check"}


def test_empty_registry_blocks_seasonal_overrides_without_advancing_checkpoints() -> None:
    now = datetime(2026, 9, 8, 12, tzinfo=UTC)
    policy = CompetitionSyncPolicy(
        provider_id=7, season_id=101, policy_instance_id=1, enabled=True,
        allowed_work_types=frozenset({"season_discovery", "standings_refresh"}),
        coverage={name: CoverageObservation(CoverageState.COVERED, date(2026, 9, 1)) for name in ("season_discovery", "standings_refresh")},
        refresh_intervals={"season_discovery": RefreshInterval(1, "week"), "standings_refresh": RefreshInterval(1, "hour")},
        priority=3, history_depth_seasons=0, policy_version=1, paused_until=None,
    )
    states = [
        PeriodicScheduleState(7, 101, "season_discovery", now - timedelta(days=2), now - timedelta(days=1)),
        PeriodicScheduleState(7, 101, "standings_refresh", now - timedelta(hours=2), now - timedelta(hours=1)),
    ]
    connection, repository = _Connection(), _Repository()
    result = Q05SchedulerProcess(connection, repository, SyncScheduler(), {}).enqueue_due(
        run_id=4, now=now, policies=[policy], schedule_state=states,
        seasons=[SeasonScheduleSnapshot(7, 101, now + timedelta(days=10), True)],
    )
    relevant = [item for item in result.preview.decisions if item.work_type in {"season_discovery", "standings_refresh"}]
    assert {item.reason for item in relevant} == {ScheduleDecisionReason.HANDLER_UNAVAILABLE.value}
    assert all(item.next_state is None for item in relevant)
    assert repository.calls == [] and connection.transactions == 0
