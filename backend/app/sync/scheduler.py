"""Deterministic, side-effect-free scheduling for the first Q05 slice.

This module calculates candidates only.  A later adapter will atomically save
``next_state`` with enqueueing the returned work; preview does neither and
never reserves API-Football capacity.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from app.sync.policies import (
    CompetitionSyncPolicy,
    CompetitionSyncPolicyReader,
    RefreshInterval,
    SyncPolicyDenied,
    SyncPolicyGate,
    SyncWorkRequest,
)
from app.sync.repository import PeriodicWork, RecalculationWork


CALENDAR_REFRESH = "calendar_refresh"
STANDINGS_REFRESH = "standings_refresh"
SUPPORTED_PERIODIC_WORK_TYPES = frozenset((CALENDAR_REFRESH, STANDINGS_REFRESH, "season_discovery", "quality_sweep"))
# These names make the whole Section 7 surface auditable.  Only the first two
# have a reviewed input adapter in this slice; the rest must remain explicit
# skips until their D/A readers and Q03 handlers are supplied.
SECTION_7_WORK_TYPES = frozenset((
    "season_discovery", "schedule_near", "schedule_far", "prematch_check", "live_refresh",
    "overdue_status_check", "result_finalization", "statistics_retry", "correction_check",
    "analytics_recalculation", "quality_sweep",
))
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class ScheduleDecisionReason(StrEnum):
    DUE = "due"
    NOT_DUE = "not_due"
    NOT_IMPLEMENTED = "not_implemented"
    INPUT_UNAVAILABLE = "input_unavailable"
    HANDLER_UNAVAILABLE = "handler_unavailable"
    BUDGET_COOLDOWN = "budget_cooldown"
    BUDGET_EXHAUSTED = "budget_exhausted"
    RETRY_EXHAUSTED = "retry_exhausted"


class ApiCost(StrEnum):
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class FixtureScheduleSnapshot:
    """Saved fixture facts required to calculate Section-7 deadlines.

    Values are supplied by a read adapter; the scheduler never fetches or
    normalizes football data itself.
    """
    fixture_id: int
    provider_id: int
    season_id: int
    kickoff_at: datetime | None
    lifecycle_state: str
    terminal_observed_at: datetime | None = None
    result_finalized_at: datetime | None = None
    first_terminal_observed_at: datetime | None = None
    statistics_eligible_at: datetime | None = None
    statistics_attempts: int = 0
    statistics_max_attempts: int = 5
    statistics_completed: bool = False
    statistics_retry_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.fixture_id <= 0 or self.provider_id <= 0 or self.season_id <= 0:
            raise ValueError("fixture, provider, and season ids must be positive")
        if self.kickoff_at is not None:
            _require_aware(self.kickoff_at, "fixture kickoff")
        for value in (self.terminal_observed_at, self.result_finalized_at, self.first_terminal_observed_at, self.statistics_eligible_at, self.statistics_retry_at):
            if value is not None:
                _require_aware(value, "fixture schedule timestamp")
        if self.statistics_attempts < 0 or self.statistics_max_attempts < 0:
            raise ValueError("statistics attempts must not be negative")


@dataclass(frozen=True)
class SeasonScheduleSnapshot:
    provider_id: int
    season_id: int
    expected_season_start: datetime | None = None
    matchday_today: bool = False


@dataclass(frozen=True)
class AnalyticsInputSnapshot:
    provider_id: int
    season_id: int
    entity_key: str
    input_version: str | int
    observed_at: datetime
    coalescing_deadline: datetime | None = None
    window_fixture_id: int | None = None
    window_identity: datetime | None = None

    def __post_init__(self) -> None:
        if self.provider_id <= 0 or self.season_id <= 0 or not self.entity_key.strip():
            raise ValueError("analytics input scope is malformed")
        _require_aware(self.observed_at, "analytics input observation")
        if self.coalescing_deadline is not None:
            _require_aware(self.coalescing_deadline, "analytics coalescing deadline")
        if self.window_fixture_id is not None and self.window_fixture_id <= 0:
            raise ValueError("analytics window fixture id must be positive")
        if self.window_identity is not None:
            _require_aware(self.window_identity, "analytics window identity")
        if (self.window_fixture_id is None) != (self.window_identity is None):
            raise ValueError("analytics window fixture id and identity must be supplied together")


@dataclass(frozen=True)
class PeriodicScheduleState:
    """Saved schedule progress, deliberately distinct from execution success.

    ``last_scheduled_window_end`` advances only when an enqueue adapter has
    durably accepted the candidate. ``next_deadline`` preserves the earliest
    unscheduled deadline if the policy interval changes later.
    """

    provider_id: int
    season_id: int
    work_type: str
    last_scheduled_window_end: datetime
    next_deadline: datetime

    def __post_init__(self) -> None:
        if self.provider_id <= 0 or self.season_id <= 0 or not self.work_type.strip():
            raise ValueError("provider, season, and nonblank work type are required")
        _require_aware(self.last_scheduled_window_end, "last scheduled window end")
        _require_aware(self.next_deadline, "next deadline")
        if self.next_deadline <= self.last_scheduled_window_end:
            raise ValueError("next deadline must follow last scheduled window end")

    def key(self) -> tuple[int, int, str]:
        return (self.provider_id, self.season_id, self.work_type)


@dataclass(frozen=True)
class SchedulerDecision:
    scope: Mapping[str, object]
    work_type: str
    stable_key: str | None
    deadline: datetime | None
    priority: int | None
    reason: str
    work: PeriodicWork | RecalculationWork | None = None
    api_cost: ApiCost | None = None
    next_state: PeriodicScheduleState | None = None


@dataclass(frozen=True)
class ApiCostEstimate:
    """Unknown work cost is never represented as a zero request estimate."""

    unknown_jobs: int

    @property
    def value(self) -> ApiCost | int:
        return ApiCost.UNKNOWN if self.unknown_jobs else 0


@dataclass(frozen=True)
class SchedulerPreview:
    decisions: tuple[SchedulerDecision, ...]
    next_state: tuple[PeriodicScheduleState, ...]
    api_request_cost: ApiCostEstimate

    @property
    def planned_jobs(self) -> tuple[PeriodicWork | RecalculationWork, ...]:
        return tuple(item.work for item in self.decisions if item.reason == ScheduleDecisionReason.DUE.value and item.work is not None)


class _PolicyReader(CompetitionSyncPolicyReader):
    def __init__(self, policies: Mapping[tuple[int, int], CompetitionSyncPolicy]) -> None:
        self._policies = policies

    def get(self, *, provider_id: int, season_id: int) -> CompetitionSyncPolicy | None:
        return self._policies.get((provider_id, season_id))


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")


def _interval_delta(interval: RefreshInterval) -> timedelta:
    units = {
        "second": timedelta(seconds=interval.value),
        "minute": timedelta(minutes=interval.value),
        "hour": timedelta(hours=interval.value),
        "day": timedelta(days=interval.value),
        "week": timedelta(weeks=interval.value),
    }
    try:
        return units[interval.unit]
    except KeyError as exc:
        raise ValueError("refresh interval unit is unsupported") from exc


def _closed_boundary(now: datetime, interval: timedelta) -> datetime:
    """A UTC boundary anchors keys independently from a launch minute or DST."""
    return _EPOCH + ((now - _EPOCH) // interval) * interval


def _next_closed_boundary(now: datetime, interval: timedelta) -> datetime:
    """Return the exclusive close of the window containing the event."""
    return _closed_boundary(now, interval) + interval


def _scope(policy: CompetitionSyncPolicy, work_type: str, start: datetime, end: datetime, windows: int) -> dict[str, object]:
    return {
        "provider_id": policy.provider_id,
        "season_id": policy.season_id,
        "work_type": work_type,
        "window_start": start.astimezone(UTC).isoformat(),
        "window_end": end.astimezone(UTC).isoformat(),
        "coalesced_windows": windows,
        "_sync_policy": _policy_fingerprint(policy, work_type),
    }


def _policy_fingerprint(policy: CompetitionSyncPolicy, work_type: str) -> dict[str, int | str]:
    """Authorization identity belongs to the calculation, never the writer."""
    return {
        "provider_id": policy.provider_id,
        "season_id": policy.season_id,
        "work_type": work_type,
        "instance_id": policy.policy_instance_id,
        "version": policy.policy_version,
    }


class SyncScheduler:
    """Calculates calendar and standings candidates from explicit input only."""

    def preview(
        self,
        *,
        now: datetime,
        policies: Iterable[CompetitionSyncPolicy],
        schedule_state: Iterable[PeriodicScheduleState],
        executable_work_types: Iterable[str] | None = None,
        fixtures: Iterable[FixtureScheduleSnapshot] = (),
        budget: object | None = None,
        seasons: Iterable[SeasonScheduleSnapshot] = (),
        analytics_inputs: Iterable[AnalyticsInputSnapshot] = (),
    ) -> SchedulerPreview:
        _require_aware(now, "now")
        current = now.astimezone(UTC)
        policy_by_scope = self._policies(policies)
        state_by_key = self._state(schedule_state)
        gate = SyncPolicyGate(_PolicyReader(policy_by_scope), now=lambda: current)
        executable = None if executable_work_types is None else frozenset(executable_work_types)
        decisions: list[SchedulerDecision] = []
        # Preview always returns a complete prospective snapshot.  A skipped or
        # not-due scope remains scheduled at its existing checkpoint; callers
        # can atomically replace only the due entries after queue insertion.

        for policy in sorted(policy_by_scope.values(), key=lambda item: (item.provider_id, item.season_id)):
            for work_type in sorted(SUPPORTED_PERIODIC_WORK_TYPES & set(policy.allowed_work_types)):
                decision = self._periodic_decision(policy, work_type, state_by_key.get((policy.provider_id, policy.season_id, work_type)), current, gate)
                decision = self._apply_handler_gate(decision, executable)
                decisions.append(decision)
            for work_type in sorted(set(policy.allowed_work_types) - SUPPORTED_PERIODIC_WORK_TYPES):
                if work_type in SECTION_7_WORK_TYPES:
                    continue
                decisions.append(SchedulerDecision(
                    scope={"provider_id": policy.provider_id, "season_id": policy.season_id, "work_type": work_type},
                    work_type=work_type, stable_key=None, deadline=None, priority=policy.priority,
                    reason=ScheduleDecisionReason.NOT_IMPLEMENTED.value,
                ))

        fixture_values = tuple(fixtures)
        season_values = {(item.provider_id, item.season_id): item for item in seasons}
        for policy in policy_by_scope.values():
            season = season_values.get((policy.provider_id, policy.season_id))
            if season is not None and season.expected_season_start is not None and "season_discovery" in policy.allowed_work_types and season.expected_season_start <= current + timedelta(days=30):
                # The daily preseason boundary is stable and never bypasses Q01.
                decision = self._periodic_decision(policy, "season_discovery", state_by_key.get((policy.provider_id, policy.season_id, "season_discovery")), current, gate, interval_override=timedelta(days=1))
                decisions = [item for item in decisions if not (item.scope.get("provider_id") == policy.provider_id and item.scope.get("season_id") == policy.season_id and item.work_type == "season_discovery")]
                decision = self._apply_handler_gate(decision, executable)
                decisions.append(decision)
            if season is not None and "standings_refresh" in policy.allowed_work_types:
                decision = self._periodic_decision(policy, "standings_refresh", state_by_key.get((policy.provider_id, policy.season_id, "standings_refresh")), current, gate, interval_override=None if season.matchday_today else timedelta(days=1))
                decisions = [item for item in decisions if not (item.scope.get("provider_id") == policy.provider_id and item.scope.get("season_id") == policy.season_id and item.work_type == "standings_refresh")]
                decision = self._apply_handler_gate(decision, executable)
                decisions.append(decision)
        for policy in policy_by_scope.values():
            if not any((item.provider_id, item.season_id) == (policy.provider_id, policy.season_id) for item in fixture_values):
                for work_type in sorted((set(policy.allowed_work_types) & SECTION_7_WORK_TYPES) - SUPPORTED_PERIODIC_WORK_TYPES - {"analytics_recalculation"}):
                    decisions.append(SchedulerDecision({"provider_id": policy.provider_id, "season_id": policy.season_id, "work_type": work_type}, work_type, None, None, policy.priority, ScheduleDecisionReason.INPUT_UNAVAILABLE.value))
        for fixture in sorted(fixture_values, key=lambda item: (item.provider_id, item.season_id, item.fixture_id)):
            policy = policy_by_scope.get((fixture.provider_id, fixture.season_id))
            if policy is None:
                continue
            for work_type, deadline in self._fixture_deadlines(fixture, policy, current):
                if work_type not in policy.allowed_work_types:
                    continue
                try:
                    gate.before_enqueue(SyncWorkRequest(fixture.provider_id, fixture.season_id, work_type))
                except SyncPolicyDenied as error:
                    decisions.append(SchedulerDecision({"provider_id": fixture.provider_id, "fixture_id": fixture.fixture_id, "season_id": fixture.season_id}, work_type, None, deadline, policy.priority, error.reason.value))
                    continue
                work = self._fixture_work(policy, fixture, work_type, deadline)
                reason = ScheduleDecisionReason.DUE.value if deadline <= current else ScheduleDecisionReason.NOT_DUE.value
                if executable is not None and work_type not in executable:
                    reason = ScheduleDecisionReason.HANDLER_UNAVAILABLE.value
                decisions.append(SchedulerDecision(work.scope, work_type, work.stable_key(), deadline, policy.priority, reason, work, ApiCost.UNKNOWN))
            scheduled_types = {work_type for work_type, _deadline in self._fixture_deadlines(fixture, policy, current)}
            for work_type in sorted((set(policy.allowed_work_types) & SECTION_7_WORK_TYPES) - scheduled_types):
                reason = ScheduleDecisionReason.RETRY_EXHAUSTED.value if (
                    work_type == "statistics_retry" and (fixture.statistics_eligible_at is not None or fixture.statistics_retry_at is not None)
                    and not fixture.statistics_completed and fixture.statistics_attempts >= min(fixture.statistics_max_attempts, 5)
                ) else ScheduleDecisionReason.INPUT_UNAVAILABLE.value
                decisions.append(SchedulerDecision(
                    {"provider_id": fixture.provider_id, "season_id": fixture.season_id, "fixture_id": fixture.fixture_id, "work_type": work_type},
                    work_type, None, None, policy.priority, reason,
                ))

        latest_inputs: dict[tuple[int, int, str, datetime | None, datetime | None], AnalyticsInputSnapshot] = {}
        for item in analytics_inputs:
            key = (item.provider_id, item.season_id, item.entity_key, item.coalescing_deadline, item.window_identity)
            if key not in latest_inputs or item.observed_at > latest_inputs[key].observed_at:
                latest_inputs[key] = item
        for item in latest_inputs.values():
            policy = policy_by_scope.get((item.provider_id, item.season_id))
            if policy is None or "analytics_recalculation" not in policy.allowed_work_types:
                continue
            try:
                gate.before_enqueue(SyncWorkRequest(item.provider_id, item.season_id, "analytics_recalculation"))
            except SyncPolicyDenied as error:
                decisions.append(SchedulerDecision({"provider_id": item.provider_id, "season_id": item.season_id, "entity_key": item.entity_key}, "analytics_recalculation", None, item.observed_at, policy.priority, error.reason.value))
                continue
            scope = {"provider_id": item.provider_id, "season_id": item.season_id, "entity_key": item.entity_key, "input_version": item.input_version, "_sync_policy": _policy_fingerprint(policy, "analytics_recalculation")}
            if item.window_fixture_id is not None:
                scope["_analytics_window_fixture_id"] = item.window_fixture_id
                assert item.window_identity is not None
                scope["_analytics_window_end"] = item.window_identity.astimezone(UTC).isoformat()
            work = RecalculationWork(item.provider_id, item.season_id, "analytics_recalculation", item.entity_key, item.input_version, policy.priority, scope)
            deadline = item.coalescing_deadline or _next_closed_boundary(item.observed_at.astimezone(UTC), timedelta(seconds=60))
            reason = ScheduleDecisionReason.DUE.value if deadline <= current else ScheduleDecisionReason.NOT_DUE.value
            decisions.append(self._apply_handler_gate(SchedulerDecision(work.scope, work.work_type, work.stable_key(), deadline, policy.priority, reason, work, ApiCost.UNKNOWN), executable))

        budget_reason = self._budget_reason(budget, current)
        if budget_reason is not None:
            decisions = [
                SchedulerDecision(item.scope, item.work_type, item.stable_key, item.deadline, item.priority,
                                  budget_reason, item.work, item.api_cost, item.next_state)
                if item.reason == ScheduleDecisionReason.DUE.value and item.work is not None and item.work_type != "analytics_recalculation" else item
                for item in decisions
            ]
        next_by_key = dict(state_by_key)
        for decision in decisions:
            if decision.reason == ScheduleDecisionReason.DUE.value and decision.next_state is not None:
                next_by_key[decision.next_state.key()] = decision.next_state
        decisions.sort(key=lambda item: (int(item.scope["provider_id"]), int(item.scope["season_id"]), item.work_type))
        unknown_jobs = sum(item.work is not None and item.api_cost is ApiCost.UNKNOWN for item in decisions)
        return SchedulerPreview(tuple(decisions), tuple(sorted(next_by_key.values(), key=lambda item: item.key())), ApiCostEstimate(int(unknown_jobs)))

    @staticmethod
    def _budget_reason(budget: object | None, now: datetime) -> str | None:
        if budget is None:
            return None
        cooldown = getattr(budget, "cooldown_until", None)
        if cooldown is not None and cooldown > now:
            return ScheduleDecisionReason.BUDGET_COOLDOWN.value
        windows = (
            ("daily_limit", "daily_used", "daily_window", now.date()),
            ("minute_limit", "minute_used", "minute_window", _closed_boundary(now, timedelta(minutes=1))),
        )
        for limit_name, used_name, window_name, current_window in windows:
            if getattr(budget, window_name, None) != current_window:
                continue
            limit, used = getattr(budget, limit_name, None), getattr(budget, used_name, None)
            if limit is not None and used is not None and used >= limit:
                return ScheduleDecisionReason.BUDGET_EXHAUSTED.value
        return None

    def _fixture_deadlines(self, fixture: FixtureScheduleSnapshot, policy: CompetitionSyncPolicy, now: datetime) -> tuple[tuple[str, datetime], ...]:
        if fixture.kickoff_at is None:
            # A postponed fixture may deliberately have no replacement time.
            # Do not invent a near window or any kickoff-relative event.
            return ()
        kickoff = fixture.kickoff_at.astimezone(UTC)
        def periodic_boundary(work_type: str) -> datetime:
            interval = policy.refresh_intervals.get(work_type)
            return now if interval is None else _closed_boundary(now, _interval_delta(interval))

        values: list[tuple[str, datetime]] = []
        if fixture.lifecycle_state in {"scheduled", "postponed"}:
            schedule_type = "schedule_near" if kickoff <= now + timedelta(days=7) else "schedule_far"
            values.append((schedule_type, periodic_boundary(schedule_type)))
        if fixture.lifecycle_state in {"scheduled", "postponed"}:
            values.extend((("prematch_check", kickoff - timedelta(minutes=60)), ("prematch_check", kickoff - timedelta(minutes=10))))
        if fixture.lifecycle_state in {"in_progress", "paused", "suspended", "interrupted"}:
            values.append(("live_refresh", periodic_boundary("live_refresh")))
        if fixture.lifecycle_state not in {"completed", "cancelled", "abandoned"} and now >= kickoff + timedelta(minutes=15):
            values.append(("overdue_status_check", kickoff + timedelta(minutes=15)))
        if fixture.terminal_observed_at is not None and fixture.result_finalized_at is None and now >= kickoff + timedelta(hours=3):
            values.append(("result_finalization", max(kickoff + timedelta(hours=3), fixture.terminal_observed_at.astimezone(UTC))))
        statistics_exhausted = fixture.statistics_attempts >= min(fixture.statistics_max_attempts, 5)
        if fixture.statistics_retry_at is not None and not fixture.statistics_completed and not statistics_exhausted:
            values.append(("statistics_retry", fixture.statistics_retry_at.astimezone(UTC)))
        elif fixture.statistics_eligible_at is not None and not fixture.statistics_completed and not statistics_exhausted:
            retry_offsets = (timedelta(), timedelta(minutes=15), timedelta(hours=1), timedelta(hours=6), timedelta(hours=24))
            if fixture.statistics_attempts < min(fixture.statistics_max_attempts, len(retry_offsets)):
                values.append(("statistics_retry", fixture.statistics_eligible_at.astimezone(UTC) + retry_offsets[fixture.statistics_attempts]))
        if fixture.first_terminal_observed_at is not None:
            first = fixture.first_terminal_observed_at.astimezone(UTC)
            values.extend((("correction_check", first + timedelta(hours=24)), ("correction_check", first + timedelta(hours=72))))
        return tuple(values)

    @staticmethod
    def _fixture_work(policy: CompetitionSyncPolicy, fixture: FixtureScheduleSnapshot, work_type: str, deadline: datetime) -> PeriodicWork:
        # A fixed one-second event window is a Q02 identity, never a launch-time key.
        start = deadline - timedelta(seconds=1)
        return PeriodicWork(policy.provider_id, policy.season_id, work_type, f"fixture:{fixture.fixture_id}", start, deadline, policy.priority,
                            {"provider_id": policy.provider_id, "season_id": policy.season_id, "fixture_id": fixture.fixture_id, "kickoff_at": fixture.kickoff_at.astimezone(UTC).isoformat() if fixture.kickoff_at is not None else None, "work_type": work_type, "window_start": start.isoformat(), "window_end": deadline.isoformat(), "_sync_policy": _policy_fingerprint(policy, work_type)})

    @staticmethod
    def _apply_handler_gate(decision: SchedulerDecision, executable: frozenset[str] | None) -> SchedulerDecision:
        if decision.work is None or executable is None or decision.work_type in executable:
            return decision
        return SchedulerDecision(
            decision.scope, decision.work_type, decision.stable_key, decision.deadline, decision.priority,
            ScheduleDecisionReason.HANDLER_UNAVAILABLE.value, decision.work, decision.api_cost,
        )

    @staticmethod
    def _policies(policies: Iterable[CompetitionSyncPolicy]) -> dict[tuple[int, int], CompetitionSyncPolicy]:
        result: dict[tuple[int, int], CompetitionSyncPolicy] = {}
        for policy in policies:
            key = (policy.provider_id, policy.season_id)
            if key in result:
                raise ValueError("duplicate competition sync policy scope")
            result[key] = policy
        return result

    @staticmethod
    def _state(schedule_state: Iterable[PeriodicScheduleState]) -> dict[tuple[int, int, str], PeriodicScheduleState]:
        result: dict[tuple[int, int, str], PeriodicScheduleState] = {}
        for state in schedule_state:
            if state.key() in result:
                raise ValueError("duplicate periodic schedule state")
            result[state.key()] = state
        return result

    def _periodic_decision(
        self,
        policy: CompetitionSyncPolicy,
        work_type: str,
        state: PeriodicScheduleState | None,
        now: datetime,
        gate: SyncPolicyGate,
        interval_override: timedelta | None = None,
    ) -> SchedulerDecision:
        try:
            authorization = gate.before_enqueue(SyncWorkRequest(policy.provider_id, policy.season_id, work_type))
        except SyncPolicyDenied as error:
            return SchedulerDecision(
                scope={"provider_id": policy.provider_id, "season_id": policy.season_id, "work_type": work_type},
                work_type=work_type, stable_key=None, deadline=None, priority=policy.priority, reason=error.reason.value,
            )

        interval = interval_override or _interval_delta(authorization.refresh_interval)
        if state is None:
            deadline = _closed_boundary(now, interval)
            start = deadline - interval
        else:
            start = state.last_scheduled_window_end.astimezone(UTC)
            saved_deadline = state.next_deadline.astimezone(UTC)
            # The first overdue deadline is already an accepted scheduling
            # obligation. Changing an interval can accelerate only the next
            # deadline after that window is enqueued; it cannot replace the
            # overdue one. A shorter interval still takes effect immediately
            # while the saved deadline is in the future.
            deadline = saved_deadline if saved_deadline <= now else min(saved_deadline, start + interval)
        if deadline > now:
            work = self._work(policy, work_type, start, deadline, 1)
            return SchedulerDecision(work.scope, work_type, work.stable_key(), deadline, policy.priority, ScheduleDecisionReason.NOT_DUE.value)

        # Coalesce all elapsed windows after the preserved first deadline.  The
        # resulting first deadline is never moved forward by a new interval.
        elapsed_after_deadline = (now - deadline) // interval
        end = deadline + elapsed_after_deadline * interval
        work = self._work(policy, work_type, start, end, int(elapsed_after_deadline) + 1)
        next_state = PeriodicScheduleState(policy.provider_id, policy.season_id, work_type, end, end + interval)
        return SchedulerDecision(work.scope, work_type, work.stable_key(), deadline, policy.priority,
                                 ScheduleDecisionReason.DUE.value, work, ApiCost.UNKNOWN, next_state)

    @staticmethod
    def _work(
        policy: CompetitionSyncPolicy,
        work_type: str,
        start: datetime,
        end: datetime,
        windows: int,
    ) -> PeriodicWork:
        return PeriodicWork(
            provider_id=policy.provider_id, season_id=policy.season_id, work_type=work_type,
            entity_key=f"season:{policy.season_id}", window_start=start, window_end=end,
            priority=policy.priority, scope=_scope(policy, work_type, start, end, windows),
        )
