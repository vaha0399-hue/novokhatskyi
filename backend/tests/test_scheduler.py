from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from app.sync.policies import CompetitionSyncPolicy, CoverageObservation, CoverageState, RefreshInterval
from app.sync.scheduler import AnalyticsInputSnapshot, ApiCost, FixtureScheduleSnapshot, PeriodicScheduleState, ScheduleDecisionReason, SeasonScheduleSnapshot, SyncScheduler


class _Budget:
    def __init__(self, *, cooldown_until=None, daily_limit=None, daily_used=None, minute_limit=None, minute_used=None) -> None:
        self.cooldown_until, self.daily_limit, self.daily_used = cooldown_until, daily_limit, daily_used
        self.minute_limit, self.minute_used = minute_limit, minute_used


def _policy(*, provider_id: int = 7, season_id: int = 101, **changes: object) -> CompetitionSyncPolicy:
    values: dict[str, object] = {
        "provider_id": provider_id, "season_id": season_id, "policy_instance_id": 1, "enabled": True,
        "allowed_work_types": frozenset({"calendar_refresh", "standings_refresh"}),
        "coverage": {
            "calendar_refresh": CoverageObservation(CoverageState.COVERED, date(2026, 9, 1)),
            "standings_refresh": CoverageObservation(CoverageState.COVERED, date(2026, 9, 1)),
        },
        "refresh_intervals": {
            "calendar_refresh": RefreshInterval(1, "hour"), "standings_refresh": RefreshInterval(1, "hour"),
        },
        "priority": 3, "history_depth_seasons": 2, "policy_version": 1, "paused_until": None,
    }
    values.update(changes)
    return CompetitionSyncPolicy(**values)  # type: ignore[arg-type]


def _state(work_type: str, end: datetime = datetime(2026, 9, 8, 0, tzinfo=UTC), *, season_id: int = 101, next_deadline: datetime | None = None) -> PeriodicScheduleState:
    return PeriodicScheduleState(7, season_id, work_type, end, next_deadline or end + timedelta(hours=1))


def _decision(preview, work_type: str):
    return next(item for item in preview.decisions if item.work_type == work_type)


def test_same_inputs_produce_identical_preview_and_keys_without_reading_a_clock() -> None:
    scheduler = SyncScheduler()
    inputs = dict(now=datetime(2026, 9, 8, 2, 31, tzinfo=UTC), policies=[_policy()], schedule_state=[_state("calendar_refresh"), _state("standings_refresh")])
    first = scheduler.preview(**inputs)
    second = scheduler.preview(**inputs)
    assert first == second
    assert [work.stable_key() for work in first.planned_jobs] == [work.stable_key() for work in second.planned_jobs]


def test_key_uses_closed_utc_window_not_launch_minute_and_crosses_utc_midnight() -> None:
    policy = _policy(allowed_work_types=frozenset({"calendar_refresh"}), refresh_intervals={"calendar_refresh": RefreshInterval(1, "hour")})
    state = [_state("calendar_refresh", datetime(2026, 9, 7, 23, tzinfo=UTC))]
    first = _decision(SyncScheduler().preview(now=datetime(2026, 9, 8, 0, 1, tzinfo=UTC), policies=[policy], schedule_state=state), "calendar_refresh")
    second = _decision(SyncScheduler().preview(now=datetime(2026, 9, 8, 0, 59, tzinfo=UTC), policies=[policy], schedule_state=state), "calendar_refresh")
    assert first.work is not None and first.work.window_end == datetime(2026, 9, 8, 0, tzinfo=UTC)
    assert first.stable_key == second.stable_key


def test_dst_input_is_normalized_to_utc_interval_identity() -> None:
    policy = _policy(allowed_work_types=frozenset({"calendar_refresh"}), refresh_intervals={"calendar_refresh": RefreshInterval(1, "hour")})
    result = _decision(SyncScheduler().preview(
        now=datetime(2026, 3, 8, 3, 30, tzinfo=ZoneInfo("America/New_York")), policies=[policy],
        schedule_state=[_state("calendar_refresh", datetime(2026, 3, 8, 6, tzinfo=UTC))],
    ), "calendar_refresh")
    assert result.work is not None
    assert (result.work.window_start, result.work.window_end) == (datetime(2026, 3, 8, 6, tzinfo=UTC), datetime(2026, 3, 8, 7, tzinfo=UTC))


def test_restart_sequence_coalesces_downtime_then_advances_without_overlap() -> None:
    scheduler = SyncScheduler()
    policy = _policy(allowed_work_types=frozenset({"calendar_refresh"}), refresh_intervals={"calendar_refresh": RefreshInterval(1, "hour")})
    first = _decision(scheduler.preview(now=datetime(2026, 9, 9, 0, 17, tzinfo=UTC), policies=[policy], schedule_state=[_state("calendar_refresh")]), "calendar_refresh")
    assert first.work is not None and first.next_state is not None
    assert first.work.window_start == datetime(2026, 9, 8, 0, tzinfo=UTC)
    assert first.work.window_end == datetime(2026, 9, 9, 0, tzinfo=UTC)
    assert first.deadline == datetime(2026, 9, 8, 1, tzinfo=UTC)
    # Simulate only the future atomic enqueue+state save; preview itself did not alter state.
    second = _decision(scheduler.preview(now=datetime(2026, 9, 9, 1, 5, tzinfo=UTC), policies=[policy], schedule_state=[first.next_state]), "calendar_refresh")
    assert second.work is not None
    assert second.work.window_start == first.work.window_end
    assert second.work.window_end == datetime(2026, 9, 9, 1, tzinfo=UTC)
    assert first.work.window_end <= second.work.window_start


@pytest.mark.parametrize(
    ("policy", "reason"),
    [
        (_policy(enabled=False), "disabled"),
        (_policy(paused_until=datetime(2026, 9, 8, 3, tzinfo=UTC)), "paused"),
        (_policy(allowed_work_types=frozenset({"calendar_refresh"}), refresh_intervals={"calendar_refresh": RefreshInterval(2, "hour")}), ScheduleDecisionReason.DUE.value),
    ],
)
def test_policy_changes_control_schedule_without_code_changes(policy: CompetitionSyncPolicy, reason: str) -> None:
    result = _decision(SyncScheduler().preview(now=datetime(2026, 9, 8, 1, tzinfo=UTC), policies=[policy], schedule_state=[_state("calendar_refresh")]), "calendar_refresh")
    assert result.reason == reason


def test_interval_change_preserves_old_first_deadline_and_schedules_uncovered_segment() -> None:
    old_state = _state("calendar_refresh")
    changed = _policy(allowed_work_types=frozenset({"calendar_refresh"}), refresh_intervals={"calendar_refresh": RefreshInterval(2, "hour")})
    result = _decision(SyncScheduler().preview(now=datetime(2026, 9, 8, 3, 5, tzinfo=UTC), policies=[changed], schedule_state=[old_state]), "calendar_refresh")
    assert result.work is not None
    assert result.deadline == datetime(2026, 9, 8, 1, tzinfo=UTC)
    assert (result.work.window_start, result.work.window_end) == (datetime(2026, 9, 8, 0, tzinfo=UTC), datetime(2026, 9, 8, 3, tzinfo=UTC))


def test_shorter_interval_does_not_wait_for_a_saved_future_deadline() -> None:
    state = _state("calendar_refresh", next_deadline=datetime(2026, 9, 8, 4, tzinfo=UTC))
    policy = _policy(allowed_work_types=frozenset({"calendar_refresh"}), refresh_intervals={"calendar_refresh": RefreshInterval(1, "hour")})
    result = _decision(SyncScheduler().preview(now=datetime(2026, 9, 8, 1, 5, tzinfo=UTC), policies=[policy], schedule_state=[state]), "calendar_refresh")
    assert result.reason == ScheduleDecisionReason.DUE.value
    assert result.deadline == datetime(2026, 9, 8, 1, tzinfo=UTC)


def test_shorter_interval_preserves_an_overdue_deadline_and_only_accelerates_future_work() -> None:
    state = _state("calendar_refresh", next_deadline=datetime(2026, 9, 8, 1, tzinfo=UTC))
    policy = _policy(allowed_work_types=frozenset({"calendar_refresh"}), refresh_intervals={"calendar_refresh": RefreshInterval(30, "minute")})
    result = _decision(SyncScheduler().preview(now=datetime(2026, 9, 8, 3, 5, tzinfo=UTC), policies=[policy], schedule_state=[state]), "calendar_refresh")
    assert result.work is not None and result.next_state is not None
    assert result.deadline == state.next_deadline
    assert result.next_state.next_deadline == result.work.window_end + timedelta(minutes=30)


def test_next_state_retains_not_due_and_denied_saved_scopes_unchanged() -> None:
    calendar = _state("calendar_refresh", next_deadline=datetime(2026, 9, 8, 3, tzinfo=UTC))
    standings = _state("standings_refresh", next_deadline=datetime(2026, 9, 8, 3, tzinfo=UTC))
    policy = _policy(enabled=False)
    preview = SyncScheduler().preview(now=datetime(2026, 9, 8, 1, tzinfo=UTC), policies=[policy], schedule_state=[calendar, standings])
    assert preview.next_state == (calendar, standings)


def test_policy_gate_keeps_coverage_and_unimplemented_types_explicit() -> None:
    policy = _policy(allowed_work_types=frozenset({"calendar_refresh", "fixtures_refresh"}), coverage={"calendar_refresh": CoverageObservation(CoverageState.UNKNOWN, date(2026, 9, 1))}, refresh_intervals={"calendar_refresh": RefreshInterval(1, "hour"), "fixtures_refresh": RefreshInterval(1, "hour")})
    preview = SyncScheduler().preview(now=datetime(2026, 9, 8, 1, tzinfo=UTC), policies=[policy], schedule_state=[_state("calendar_refresh")])
    assert _decision(preview, "calendar_refresh").reason == "coverage_not_confirmed"
    assert _decision(preview, "fixtures_refresh").reason == ScheduleDecisionReason.NOT_IMPLEMENTED.value


def test_section_7_type_without_its_saved_fixture_input_is_an_explicit_skip() -> None:
    policy = _policy(allowed_work_types=frozenset({"prematch_check"}), coverage={"prematch_check": CoverageObservation(CoverageState.COVERED, date(2026, 9, 1))}, refresh_intervals={"prematch_check": RefreshInterval(1, "hour")})
    preview = SyncScheduler().preview(now=datetime(2026, 9, 8, 1, tzinfo=UTC), policies=[policy], schedule_state=[])
    assert _decision(preview, "prematch_check").reason == ScheduleDecisionReason.INPUT_UNAVAILABLE.value


def test_two_seasons_have_distinct_q02_periodic_identities() -> None:
    preview = SyncScheduler().preview(now=datetime(2026, 9, 8, 1, tzinfo=UTC), policies=[_policy(season_id=101), _policy(season_id=102)], schedule_state=[_state("calendar_refresh", season_id=101), _state("calendar_refresh", season_id=102)])
    calendar_keys = [item.stable_key for item in preview.decisions if item.work_type == "calendar_refresh"]
    assert len(calendar_keys) == 2 and len(set(calendar_keys)) == 2


def test_preview_does_not_mutate_state_or_reserve_api_budget() -> None:
    state = [_state("calendar_refresh")]
    original = tuple(state)
    preview = SyncScheduler().preview(now=datetime(2026, 9, 8, 1, tzinfo=UTC), policies=[_policy()], schedule_state=state)
    assert tuple(state) == original
    assert preview.api_request_cost.value is ApiCost.UNKNOWN
    assert preview.api_request_cost.unknown_jobs == 2


def test_preview_keeps_due_candidate_visible_when_handler_is_unavailable() -> None:
    preview = SyncScheduler().preview(now=datetime(2026, 9, 8, 1, tzinfo=UTC), policies=[_policy()],
                                      schedule_state=[_state("calendar_refresh"), _state("standings_refresh")],
                                      executable_work_types=())
    calendar = _decision(preview, "calendar_refresh")
    assert calendar.work is not None and calendar.stable_key is not None
    assert calendar.reason == ScheduleDecisionReason.HANDLER_UNAVAILABLE.value
    assert calendar.next_state is None
    assert preview.planned_jobs == ()


def test_fixture_snapshot_calculates_section_7_deadlines_without_a_handler() -> None:
    now = datetime(2026, 9, 8, 12, tzinfo=UTC)
    policy = _policy(allowed_work_types=frozenset({"schedule_near", "prematch_check", "overdue_status_check"}),
                     coverage={name: CoverageObservation(CoverageState.COVERED, date(2026, 9, 1)) for name in ("schedule_near", "prematch_check", "overdue_status_check")},
                     refresh_intervals={name: RefreshInterval(1, "hour") for name in ("schedule_near", "prematch_check", "overdue_status_check")})
    fixture = FixtureScheduleSnapshot(9, 7, 101, now + timedelta(minutes=30), "scheduled")
    preview = SyncScheduler().preview(now=now, policies=[policy], schedule_state=[], fixtures=[fixture], executable_work_types=())
    checks = [item for item in preview.decisions if item.scope.get("fixture_id") == 9]
    assert {item.work_type for item in checks} == {"schedule_near", "prematch_check", "overdue_status_check"}
    due = [item for item in checks if item.work is not None]
    assert all(item.reason == ScheduleDecisionReason.HANDLER_UNAVAILABLE.value for item in due)
    assert all(item.stable_key is not None and item.deadline is not None for item in due)
    assert _decision(preview, "overdue_status_check").reason == ScheduleDecisionReason.INPUT_UNAVAILABLE.value


def test_fixture_periodic_keys_are_stable_inside_a_policy_interval() -> None:
    policy = _policy(allowed_work_types=frozenset({"schedule_near", "live_refresh"}),
                     coverage={name: CoverageObservation(CoverageState.COVERED, date(2026, 9, 1)) for name in ("schedule_near", "live_refresh")},
                     refresh_intervals={name: RefreshInterval(3, "hour") for name in ("schedule_near", "live_refresh")})
    fixture = FixtureScheduleSnapshot(9, 7, 101, datetime(2026, 9, 8, 14, tzinfo=UTC), "in_progress")
    first = SyncScheduler().preview(now=datetime(2026, 9, 8, 12, 0, tzinfo=UTC), policies=[policy], schedule_state=[], fixtures=[fixture], executable_work_types=())
    second = SyncScheduler().preview(now=datetime(2026, 9, 8, 12, 1, tzinfo=UTC), policies=[policy], schedule_state=[], fixtures=[fixture], executable_work_types=())
    assert [(item.work_type, item.stable_key) for item in first.decisions if item.work is not None] == [(item.work_type, item.stable_key) for item in second.decisions if item.work is not None]


def test_disabled_fixture_policy_returns_a_sortable_denial() -> None:
    policy = _policy(enabled=False, allowed_work_types=frozenset({"schedule_near"}),
                     coverage={"schedule_near": CoverageObservation(CoverageState.COVERED, date(2026, 9, 1))},
                     refresh_intervals={"schedule_near": RefreshInterval(1, "hour")})
    fixture = FixtureScheduleSnapshot(9, 7, 101, datetime(2026, 9, 8, 14, tzinfo=UTC), "scheduled")
    preview = SyncScheduler().preview(now=datetime(2026, 9, 8, 12, tzinfo=UTC), policies=[policy], schedule_state=[], fixtures=[fixture])
    decision = _decision(preview, "schedule_near")
    assert decision.reason == "disabled" and decision.scope["provider_id"] == 7


def test_budget_snapshot_defers_due_work_without_reserving_requests() -> None:
    now = datetime(2026, 9, 8, 1, tzinfo=UTC)
    preview = SyncScheduler().preview(now=now, policies=[_policy()], schedule_state=[_state("calendar_refresh"), _state("standings_refresh")],
                                      budget=_Budget(cooldown_until=now + timedelta(minutes=5)))
    assert {item.reason for item in preview.decisions} == {ScheduleDecisionReason.BUDGET_COOLDOWN.value}
    assert preview.planned_jobs == () and preview.api_request_cost.value is ApiCost.UNKNOWN


def test_discovery_quality_and_standings_modes_calculate_from_saved_season_inputs() -> None:
    policy = _policy(allowed_work_types=frozenset({"season_discovery", "quality_sweep", "standings_refresh"}),
                     coverage={name: CoverageObservation(CoverageState.COVERED, date(2026, 9, 1)) for name in ("season_discovery", "quality_sweep", "standings_refresh")},
                     refresh_intervals={"season_discovery": RefreshInterval(1, "week"), "quality_sweep": RefreshInterval(1, "day"), "standings_refresh": RefreshInterval(1, "hour")})
    now = datetime(2026, 9, 8, 12, tzinfo=UTC)
    states = [_state(name, now - timedelta(days=2), next_deadline=now - timedelta(days=1)) for name in ("season_discovery", "quality_sweep", "standings_refresh")]
    preview = SyncScheduler().preview(now=now, policies=[policy], schedule_state=states,
                                      seasons=[SeasonScheduleSnapshot(7, 101, now + timedelta(days=10), False)])
    assert {item.work_type for item in preview.planned_jobs} == {"season_discovery", "quality_sweep", "standings_refresh"}
    standings = _decision(preview, "standings_refresh")
    assert standings.work is not None and standings.work.window_end == now


def test_analytics_uses_latest_input_version_and_statistics_retries_are_bounded() -> None:
    policy = _policy(allowed_work_types=frozenset({"analytics_recalculation", "statistics_retry"}),
                     coverage={name: CoverageObservation(CoverageState.COVERED, date(2026, 9, 1)) for name in ("analytics_recalculation", "statistics_retry")},
                     refresh_intervals={"analytics_recalculation": RefreshInterval(1, "minute"), "statistics_retry": RefreshInterval(1, "hour")})
    now = datetime(2026, 9, 8, 12, tzinfo=UTC)
    fixture = FixtureScheduleSnapshot(9, 7, 101, now - timedelta(hours=4), "completed", statistics_eligible_at=now - timedelta(hours=2), statistics_attempts=2, statistics_max_attempts=5)
    preview = SyncScheduler().preview(now=now, policies=[policy], schedule_state=[], fixtures=[fixture],
                                      analytics_inputs=[AnalyticsInputSnapshot(7, 101, "team:9", 1, now - timedelta(seconds=30)), AnalyticsInputSnapshot(7, 101, "team:9", 2, now)])
    analytics = next(item for item in preview.decisions if item.work_type == "analytics_recalculation" and item.work is not None)
    assert analytics.work.scope["input_version"] == 2
    retry = _decision(preview, "statistics_retry")
    assert retry.deadline == fixture.statistics_eligible_at + timedelta(hours=1)
    exhausted = FixtureScheduleSnapshot(10, 7, 101, now - timedelta(hours=4), "completed", statistics_eligible_at=now, statistics_attempts=5, statistics_max_attempts=5)
    assert _decision(SyncScheduler().preview(now=now, policies=[policy], schedule_state=[], fixtures=[exhausted]), "statistics_retry").reason == ScheduleDecisionReason.RETRY_EXHAUSTED.value
