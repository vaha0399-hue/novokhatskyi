"""Explicit Q05 scheduler entrypoint; never constructs an API-Football client."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import UTC, datetime

import psycopg

from app.sync.policies import PostgresCompetitionSyncPolicyReader, SyncPolicyGate
from app.sync.scheduler import SyncScheduler
from app.sync.scheduler_process import Q05SchedulerProcess
from app.sync.scheduler_repository import PostgresSchedulerRepository, PostgresSchedulerSnapshotReader


def _json_default(value: object) -> object:
    return value.isoformat() if isinstance(value, datetime) else str(value)


def main() -> None:
    parser = argparse.ArgumentParser(description="Opt-in deterministic Q05 scheduler")
    parser.add_argument("--database-url", required=True, help="explicit PostgreSQL URL; never read from API client configuration")
    parser.add_argument("--run-id", type=int, help="required only with --enqueue")
    parser.add_argument("--enqueue", action="store_true", help="enqueue only registered Q03-compatible handlers")
    parser.add_argument("--now", help="RFC3339 timestamp for deterministic operation")
    args = parser.parse_args()
    if args.enqueue and (args.run_id is None or args.run_id <= 0):
        parser.error("--enqueue requires a positive --run-id")
    now = datetime.fromisoformat(args.now) if args.now else datetime.now(UTC)
    if now.tzinfo is None:
        parser.error("--now must include a timezone")
    with psycopg.connect(args.database_url) as connection:
        with connection.transaction():
            snapshot = PostgresSchedulerSnapshotReader(connection).read(now=now)
        gate = SyncPolicyGate(PostgresCompetitionSyncPolicyReader(connection), now=lambda: now)
        # This executable deliberately registers no D/A handler. Deployment
        # code must inject reviewed Q03 handler pairs before using --enqueue.
        process = Q05SchedulerProcess(connection, PostgresSchedulerRepository(connection, gate), SyncScheduler(), {})
        result = (process.enqueue_due(run_id=args.run_id, now=now, policies=snapshot.policies, schedule_state=snapshot.checkpoints, fixtures=snapshot.fixtures, budget=snapshot.budget, seasons=snapshot.seasons, analytics_inputs=snapshot.analytics_inputs, prematch_observations=snapshot.prematch_observations)
                  if args.enqueue else process.preview(now=now, policies=snapshot.policies, schedule_state=snapshot.checkpoints, fixtures=snapshot.fixtures, budget=snapshot.budget, seasons=snapshot.seasons, analytics_inputs=snapshot.analytics_inputs, prematch_observations=snapshot.prematch_observations))
    print(json.dumps({"snapshot": asdict(snapshot), "result": asdict(result)}, default=_json_default, sort_keys=True))


if __name__ == "__main__":
    main()
