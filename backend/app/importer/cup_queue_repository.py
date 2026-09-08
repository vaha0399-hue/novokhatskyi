"""PostgreSQL control-plane adapter for the reviewed Cup queue.

This module intentionally owns only queue state and provider quota accounting.
The Cup canonical writer is injected into ``cup_queue.Worker`` separately, so
claiming a queue item cannot accidentally reuse a regular-league run.
"""

from __future__ import annotations

import os
import socket
import uuid
from collections.abc import Mapping, Sequence
from typing import Any

from psycopg import Connection
from psycopg.types.json import Jsonb

from app.importer.cup_bootstrap import CupCompetition
from app.importer.cup_queue import (
    OPERATION,
    POLICY_VERSION,
    CupQueueError,
    CupWorkItem,
)


PROVIDER_CODE = "api-football"
LEASE_SECONDS = 300


class PostgresCupQueueRepository:
    """Lease-safe Cup queue backed by ``ops.sync_runs`` work items."""

    def __init__(self, database_url: str, *, lease_owner: str | None = None) -> None:
        self._database_url = database_url
        self._owner = lease_owner or f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex}"
        self._conn_value: Connection[Any] | None = None
        self._provider_id: int | None = None

    def __enter__(self) -> "PostgresCupQueueRepository":
        self._conn_value = Connection.connect(self._database_url, autocommit=True)
        locked = self._conn.execute(
            "SELECT pg_try_advisory_lock(hashtextextended(%s,0))", (OPERATION,)
        ).fetchone()
        if locked is None or locked[0] is not True:
            self._conn_value.close()
            self._conn_value = None
            raise CupQueueError("another Cup queue worker is running")
        row = self._conn.execute(
            "SELECT id FROM source.providers WHERE code=%s", (PROVIDER_CODE,)
        ).fetchone()
        if row is None:
            self._conn.execute(
                "SELECT pg_advisory_unlock(hashtextextended(%s,0))", (OPERATION,)
            )
            self._conn_value.close()
            self._conn_value = None
            raise CupQueueError("API-Football provider is not configured")
        self._provider_id = int(row[0])
        return self

    def __exit__(self, *_: object) -> None:
        if self._conn_value is not None:
            self._conn_value.execute(
                "SELECT pg_advisory_unlock(hashtextextended(%s,0))", (OPERATION,)
            )
            self._conn_value.close()
        self._conn_value = None
        self._provider_id = None

    @property
    def _conn(self) -> Connection[Any]:
        if self._conn_value is None:
            raise CupQueueError("Cup queue repository is not connected")
        return self._conn_value

    @property
    def _provider(self) -> int:
        if self._provider_id is None:
            raise CupQueueError("Cup queue provider is not resolved")
        return self._provider_id

    @staticmethod
    def _scope_key(competition: CupCompetition) -> str:
        return f"cup-bootstrap:{competition.league_external_id}:{competition.season_start_year}"

    @staticmethod
    def _scope(competition: CupCompetition, *, policy_version: int) -> dict[str, Any]:
        if (
            not isinstance(competition.league_external_id, int)
            or competition.league_external_id <= 0
            or not isinstance(competition.name, str)
            or not competition.name.strip()
            or not isinstance(competition.season_start_year, int)
            or competition.season_start_year <= 0
            or not isinstance(competition.fixture_statistics_coverage, bool)
        ):
            raise CupQueueError("invalid Cup queue competition scope")
        return {
            "policy_version": policy_version,
            "league_external_id": competition.league_external_id,
            "name": competition.name.strip(),
            "provider_type": "Cup",
            "season_start_year": competition.season_start_year,
            "fixture_statistics_coverage": competition.fixture_statistics_coverage,
        }

    @classmethod
    def _competition_from_scope(cls, scope: Mapping[str, Any]) -> CupCompetition:
        policy_version = scope.get("policy_version")
        league_id = scope.get("league_external_id")
        name = scope.get("name")
        provider_type = scope.get("provider_type")
        season = scope.get("season_start_year")
        coverage = scope.get("fixture_statistics_coverage")
        if (
            policy_version != POLICY_VERSION
            or provider_type != "Cup"
            or not isinstance(league_id, int)
            or league_id <= 0
            or not isinstance(name, str)
            or not name.strip()
            or not isinstance(season, int)
            or season <= 0
            or not isinstance(coverage, bool)
        ):
            raise CupQueueError("claimed work item is not a valid Cup scope")
        return CupCompetition(league_id, name.strip(), season, coverage)

    def active_run(self) -> tuple[int, Mapping[str, Any]] | None:
        row = self._conn.execute(
            "SELECT id,checkpoint FROM ops.sync_runs "
            "WHERE provider_id=%s AND operation=%s AND status='running' ORDER BY id LIMIT 1",
            (self._provider, OPERATION),
        ).fetchone()
        if row is None:
            return None
        if not isinstance(row[1], Mapping):
            raise CupQueueError("Cup queue run checkpoint is malformed")
        return int(row[0]), dict(row[1])

    def create_run(
        self, items: Sequence[CupCompetition], *, operation: str, policy_version: int
    ) -> int:
        if operation != OPERATION or policy_version != POLICY_VERSION:
            raise CupQueueError("Cup queue operation or policy version is unsafe")
        scopes = [(self._scope_key(item), self._scope(item, policy_version=policy_version)) for item in items]
        if not scopes:
            raise CupQueueError("Cup queue cannot create an empty run")
        if len({key for key, _ in scopes}) != len(scopes):
            raise CupQueueError("Cup queue contains duplicate (league ID, season) scopes")
        with self._conn.transaction():
            row = self._conn.execute(
                "INSERT INTO ops.sync_runs(provider_id,operation,scope,status,started_at,checkpoint) "
                "VALUES(%s,%s,%s,'running',clock_timestamp(),%s) RETURNING id",
                (
                    self._provider,
                    OPERATION,
                    Jsonb({"policy_version": POLICY_VERSION, "provider_type": "Cup"}),
                    Jsonb({"provider_request_count": 0}),
                ),
            ).fetchone()
            assert row is not None
            run_id = int(row[0])
            for scope_key, scope in scopes:
                self._conn.execute(
                    "INSERT INTO ops.sync_work_items(run_id,scope_key,scope) VALUES(%s,%s,%s)",
                    (run_id, scope_key, Jsonb(scope)),
                )
        return run_id

    def claim_next(self, run_id: int) -> CupWorkItem | None:
        row = self._conn.execute(
            "SELECT * FROM ops.claim_next_sync_work_item(%s,%s,%s::interval)",
            (run_id, self._owner, f"{LEASE_SECONDS} seconds"),
        ).fetchone()
        if row is None:
            return None
        item_id, _scope_key, scope, checkpoint, attempts = row
        if not isinstance(scope, Mapping) or not isinstance(checkpoint, Mapping):
            raise CupQueueError("Cup queue work item is malformed")
        return CupWorkItem(
            int(item_id), self._competition_from_scope(scope), int(attempts), dict(checkpoint)
        )

    def unfinished_delay_seconds(self, run_id: int) -> float | None:
        row = self._conn.execute(
            "SELECT extract(epoch FROM min(available_at)-clock_timestamp()) "
            "FROM ops.sync_work_items WHERE run_id=%s AND status IN ('pending','running')",
            (run_id,),
        ).fetchone()
        return None if row is None or row[0] is None else max(0.0, float(row[0]))

    def reserve_request(self, daily_limit: int) -> bool:
        if not isinstance(daily_limit, int) or daily_limit < 1:
            raise CupQueueError("daily request limit must be positive")
        row = self._conn.execute(
            "SELECT ops.reserve_provider_daily_request(%s,%s)", (self._provider, daily_limit)
        ).fetchone()
        return row is not None and row[0] is True

    def renew(self, item: CupWorkItem) -> None:
        row = self._conn.execute(
            "SELECT ops.renew_sync_work_item(%s,%s,%s::interval)",
            (item.id, self._owner, f"{LEASE_SECONDS} seconds"),
        ).fetchone()
        if row is None or row[0] is not True:
            raise CupQueueError("lost Cup queue work-item lease")

    def complete(self, item: CupWorkItem, checkpoint: Mapping[str, Any]) -> None:
        row = self._conn.execute(
            "SELECT ops.complete_sync_work_item(%s,%s,%s)",
            (item.id, self._owner, Jsonb(dict(checkpoint))),
        ).fetchone()
        if row is None or row[0] is not True:
            raise CupQueueError("lost Cup queue work-item lease")

    def requeue(
        self, item: CupWorkItem, *, checkpoint: Mapping[str, Any], error: str, delay_seconds: float
    ) -> None:
        if delay_seconds < 0:
            raise CupQueueError("Cup queue retry delay must be non-negative")
        changed = self._conn.execute(
            "UPDATE ops.sync_work_items SET status='pending',checkpoint=%s,last_error=%s,"
            "available_at=clock_timestamp()+make_interval(secs=>%s),lease_owner=NULL,"
            "lease_expires_at=NULL WHERE id=%s AND status='running' AND lease_owner=%s "
            "AND lease_expires_at>=clock_timestamp() AND job_type='legacy'",
            (Jsonb(dict(checkpoint)), error[:500], delay_seconds, item.id, self._owner),
        ).rowcount
        if changed != 1:
            raise CupQueueError("lost Cup queue work-item lease")

    def checkpoint_run(self, run_id: int, checkpoint: Mapping[str, Any]) -> None:
        self._conn.execute(
            "UPDATE ops.sync_runs SET checkpoint=%s WHERE id=%s AND status='running'",
            (Jsonb(dict(checkpoint)), run_id),
        )

    def finish_run(self, run_id: int, *, checkpoint: Mapping[str, Any]) -> None:
        changed = self._conn.execute(
            "UPDATE ops.sync_runs SET status='succeeded',checkpoint=%s,finished_at=clock_timestamp() "
            "WHERE id=%s AND status='running' AND NOT EXISTS(SELECT 1 FROM ops.sync_work_items "
            "WHERE run_id=%s AND status IN ('pending','running'))",
            (Jsonb(dict(checkpoint)), run_id, run_id),
        ).rowcount
        if changed != 1:
            raise CupQueueError("Cup queue has unfinished work")
