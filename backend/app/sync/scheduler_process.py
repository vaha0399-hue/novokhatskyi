"""Opt-in Q05 boundary between deterministic planning and Q03 execution."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from psycopg import Connection

from app.sync.policies import CompetitionSyncPolicy, SyncPolicyDenied
from app.sync.dispatch import Q03Dispatch, Q03DispatchRegistry
from app.sync.repository import LeasedWorkItem
from app.sync.scheduler import AnalyticsInputSnapshot, FixtureScheduleSnapshot, PeriodicScheduleState, SeasonScheduleSnapshot, ScheduleDecisionReason, SchedulerPreview, SyncScheduler
from app.sync.scheduler_repository import PostgresSchedulerRepository, SchedulerEnqueueResult
from app.sync.policies import AuthorizedSyncWork
from app.sync.worker import AtomicWorkTransaction, WorkResult


Q05Handler = Q03Dispatch


@dataclass(frozen=True)
class SchedulerRunResult:
    preview: SchedulerPreview
    enqueue_results: tuple[SchedulerEnqueueResult, ...]
    policy_denials: tuple[str, ...]


class Q05SchedulerProcess:
    """Explicitly invoked producer; it neither fetches nor imports football data."""

    def __init__(self, connection: Connection[Any], repository: PostgresSchedulerRepository,
                 scheduler: SyncScheduler, handlers: Mapping[str, Q05Handler]) -> None:
        self._connection, self._repository, self._scheduler = connection, repository, scheduler
        self._registry = Q03DispatchRegistry(handlers)

    def preview(self, *, now: datetime, policies: Iterable[CompetitionSyncPolicy],
                schedule_state: Iterable[PeriodicScheduleState], fixtures: Iterable[FixtureScheduleSnapshot] = (), budget: object | None = None, seasons: Iterable[SeasonScheduleSnapshot] = (), analytics_inputs: Iterable[AnalyticsInputSnapshot] = ()) -> SchedulerPreview:
        return self._scheduler.preview(now=now, policies=policies, schedule_state=schedule_state,
                                       executable_work_types=self._registry.available_work_types(), fixtures=fixtures, budget=budget, seasons=seasons, analytics_inputs=analytics_inputs)

    def enqueue_due(self, *, run_id: int, now: datetime, policies: Iterable[CompetitionSyncPolicy],
                    schedule_state: Iterable[PeriodicScheduleState], fixtures: Iterable[FixtureScheduleSnapshot] = (), budget: object | None = None) -> SchedulerRunResult:
        # Preview and its transaction use precisely one materialized snapshot.
        # In particular, a generator must not lose the expected checkpoint on
        # its second traversal.
        policy_values, state_values, fixture_values = tuple(policies), tuple(schedule_state), tuple(fixtures)
        preview = self.preview(now=now, policies=policy_values, schedule_state=state_values, fixtures=fixture_values, budget=budget)
        state_by_key = {state.key(): state for state in state_values}
        results: list[SchedulerEnqueueResult] = []
        denials: list[str] = []
        for decision in preview.decisions:
            if decision.reason != ScheduleDecisionReason.DUE.value or decision.work is None:
                continue
            # Q01 authorization, Q02 enqueue, and checkpoint advancement share
            # this short transaction.  A denial rolls it back and preserves state.
            try:
                with self._connection.transaction():
                    if decision.next_state is None:
                        if "fixture_id" not in decision.work.scope:
                            continue
                        results.append(self._repository.enqueue_event(
                            run_id=run_id, work=decision.work, available_at=decision.deadline or now,
                        ))
                    else:
                        results.append(self._repository.enqueue_and_advance(
                            run_id=run_id, work=decision.work,
                            expected_state=state_by_key.get(decision.next_state.key()),
                            next_state=decision.next_state, available_at=decision.deadline or now,
                        ))
            except SyncPolicyDenied as error:
                denials.append(error.reason.value)
        return SchedulerRunResult(preview, tuple(results), tuple(denials))
