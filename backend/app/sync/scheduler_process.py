"""Opt-in Q05 boundary between deterministic planning and Q03 execution."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from psycopg import Connection

from app.sync.policies import CompetitionSyncPolicy, SyncPolicyDenied
from app.sync.repository import LeasedWorkItem
from app.sync.scheduler import FixtureScheduleSnapshot, PeriodicScheduleState, ScheduleDecisionReason, SchedulerPreview, SyncScheduler
from app.sync.scheduler_repository import PostgresSchedulerRepository, SchedulerEnqueueResult
from app.sync.policies import AuthorizedSyncWork
from app.sync.worker import AtomicWorkTransaction, WorkResult


class Q05Handler(Protocol):
    """The same callable pair required by ``RepeatableSyncWorker`` (Q03)."""

    def fetch(self, item: LeasedWorkItem, authorization: AuthorizedSyncWork) -> WorkResult: ...
    def apply_result(self, writer: AtomicWorkTransaction, item: LeasedWorkItem, result: WorkResult) -> None: ...


@dataclass(frozen=True)
class SchedulerRunResult:
    preview: SchedulerPreview
    enqueue_results: tuple[SchedulerEnqueueResult, ...]
    policy_denials: tuple[str, ...]


class Q05SchedulerProcess:
    """Explicitly invoked producer; it neither fetches nor imports football data."""

    def __init__(self, connection: Connection[Any], repository: PostgresSchedulerRepository,
                 scheduler: SyncScheduler, handlers: Mapping[str, Q05Handler]) -> None:
        self._connection, self._repository, self._scheduler, self._handlers = connection, repository, scheduler, dict(handlers)

    def preview(self, *, now: datetime, policies: Iterable[CompetitionSyncPolicy],
                schedule_state: Iterable[PeriodicScheduleState], fixtures: Iterable[FixtureScheduleSnapshot] = ()) -> SchedulerPreview:
        return self._scheduler.preview(now=now, policies=policies, schedule_state=schedule_state,
                                       executable_work_types=self._handlers, fixtures=fixtures)

    def enqueue_due(self, *, run_id: int, now: datetime, policies: Iterable[CompetitionSyncPolicy],
                    schedule_state: Iterable[PeriodicScheduleState], fixtures: Iterable[FixtureScheduleSnapshot] = ()) -> SchedulerRunResult:
        preview = self.preview(now=now, policies=policies, schedule_state=schedule_state, fixtures=fixtures)
        state_by_key = {state.key(): state for state in schedule_state}
        results: list[SchedulerEnqueueResult] = []
        denials: list[str] = []
        for decision in preview.decisions:
            if decision.reason != ScheduleDecisionReason.DUE.value or decision.work is None or decision.next_state is None:
                continue
            # Q01 authorization, Q02 enqueue, and checkpoint advancement share
            # this short transaction.  A denial rolls it back and preserves state.
            try:
                with self._connection.transaction():
                    results.append(self._repository.enqueue_and_advance(
                        run_id=run_id, work=decision.work,
                        expected_state=state_by_key.get(decision.next_state.key()),
                        next_state=decision.next_state, available_at=decision.deadline or now,
                    ))
            except SyncPolicyDenied as error:
                denials.append(error.reason.value)
        return SchedulerRunResult(preview, tuple(results), tuple(denials))
