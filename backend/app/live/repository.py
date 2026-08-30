"""Canonical identity resolution and post-match reconciliation handoff."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from psycopg import AsyncConnection, Connection
from psycopg.types.json import Jsonb

from app.api_football import APIFootballResponse

from .models import (
    CanonicalFixtureReference,
    LiveFixtureStatus,
    ProviderFinalResult,
    ProviderLiveFixture,
)
from .normalizer import normalize_final_result


PROVIDER_CODE = "api-football"
_RESOLUTION_SQL = """SELECT canonical.id,canonical.season_id,season.league_id,
          canonical.kickoff_at,home.id,home.name,away.id,away.name,
          canonical.lifecycle_state::text
   FROM source.providers provider
   JOIN source.fixture_provider_refs fixture_ref
     ON fixture_ref.provider_id=provider.id
   JOIN football.fixtures canonical ON canonical.id=fixture_ref.fixture_id
   JOIN football.seasons season ON season.id=canonical.season_id
   JOIN source.season_provider_refs season_ref
     ON season_ref.provider_id=provider.id
    AND season_ref.season_id=canonical.season_id
   JOIN source.team_provider_refs home_ref
     ON home_ref.provider_id=provider.id
    AND home_ref.team_id=canonical.home_team_id
   JOIN source.team_provider_refs away_ref
     ON away_ref.provider_id=provider.id
    AND away_ref.team_id=canonical.away_team_id
   JOIN football.teams home ON home.id=canonical.home_team_id
   JOIN football.teams away ON away.id=canonical.away_team_id
   WHERE provider.code=%s
     AND fixture_ref.external_id=%s
     AND season_ref.league_external_id=%s
     AND season_ref.external_season=%s
     AND home_ref.external_id=%s
     AND away_ref.external_id=%s"""


class LiveResolutionError(RuntimeError):
    """An expected provider fixture has no exact canonical identity."""


class LiveReconciliationError(RuntimeError):
    """A confirmed terminal fixture cannot enter canonical reconciliation."""


@dataclass(frozen=True)
class FixtureReconciliationTask:
    fixture_id: int
    provider_fixture_id: int
    season_id: int
    eligible_at: datetime
    attempt_count: int
    max_attempts: int


def _resolution_parameters(fixture: ProviderLiveFixture) -> tuple[object, ...]:
    return (
        PROVIDER_CODE,
        str(fixture.external_fixture_id),
        str(fixture.league_external_id),
        fixture.season_start_year,
        str(fixture.home_external_team_id),
        str(fixture.away_external_team_id),
    )


def _resolved_reference(
    fixture: ProviderLiveFixture, row: tuple[Any, ...] | None
) -> CanonicalFixtureReference:
    if row is None:
        raise LiveResolutionError(
            "canonical fixture mapping is missing or conflicts with provider identity"
        )
    lifecycle_state = str(row[8])
    eligible_states = (
        {"scheduled", "completed"} if fixture.status.is_terminal else {"scheduled"}
    )
    if lifecycle_state not in eligible_states:
        raise LiveResolutionError("canonical fixture is not eligible for this live state")
    return CanonicalFixtureReference(
        fixture_id=int(row[0]),
        season_id=int(row[1]),
        league_id=int(row[2]),
        kickoff_at=row[3],
        home_team_id=int(row[4]),
        home_team_name=str(row[5]),
        away_team_id=int(row[6]),
        away_team_name=str(row[7]),
    )


class PostgresLiveFixtureResolver:
    """Resolve provider identities without creating or updating mappings."""

    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection

    def resolve(self, fixture: ProviderLiveFixture) -> CanonicalFixtureReference:
        row = self._connection.execute(
            _RESOLUTION_SQL,
            _resolution_parameters(fixture),
        ).fetchone()
        return _resolved_reference(fixture, row)


class AsyncPostgresLiveRepository:
    """Resolve live identities and hand confirmed FT to the existing ops queue."""

    def __init__(self, connection: AsyncConnection[Any]) -> None:
        self._connection = connection

    async def resolve(
        self, fixture: ProviderLiveFixture
    ) -> CanonicalFixtureReference:
        cursor = await self._connection.execute(
            _RESOLUTION_SQL, _resolution_parameters(fixture)
        )
        return _resolved_reference(fixture, await cursor.fetchone())

    async def ensure_terminal_reconciliation(
        self,
        fixture: ProviderLiveFixture,
        *,
        observed_at: datetime,
    ) -> CanonicalFixtureReference:
        """Idempotently hand confirmed FT to the schema-owned +3h workflow.

        The early live response is never used as a final-result write. The
        same worker's post-match consumer owns the eligible provider fetch and
        invokes ``ops.finalize_fixture_result`` only after the schema's window.
        """
        if fixture.status is not LiveFixtureStatus.FINISHED:
            raise LiveReconciliationError("terminal handoff requires FT status")
        if observed_at.tzinfo is None:
            raise LiveReconciliationError("terminal observation must be timezone-aware")

        async with self._connection.transaction():
            cursor = await self._connection.execute(
                _RESOLUTION_SQL, _resolution_parameters(fixture)
            )
            row = await cursor.fetchone()
            reference = _resolved_reference(fixture, row)
            lifecycle_state = str(row[8])
            if observed_at < reference.kickoff_at:
                raise LiveReconciliationError("terminal status was observed before kickoff")

            eligible_at = reference.kickoff_at + timedelta(hours=3)
            if lifecycle_state == "scheduled":
                await self._connection.execute(
                    """INSERT INTO ops.fixture_reconciliation_state(
                               fixture_id,eligible_at,next_attempt_at
                           )
                           SELECT id,%s,%s FROM football.fixtures
                           WHERE id=%s AND lifecycle_state='scheduled'
                           ON CONFLICT(fixture_id) DO NOTHING""",
                    (eligible_at, eligible_at, reference.fixture_id),
                )

            state_cursor = await self._connection.execute(
                """SELECT fixture.lifecycle_state::text,
                          fixture.result_finalized_at,
                          reconciliation.state::text,
                          reconciliation.eligible_at,
                          reconciliation.next_attempt_at,
                          reconciliation.attempt_count,
                          reconciliation.max_attempts
                   FROM football.fixtures fixture
                   LEFT JOIN ops.fixture_reconciliation_state reconciliation
                     ON reconciliation.fixture_id=fixture.id
                   WHERE fixture.id=%s""",
                (reference.fixture_id,),
            )
            state = await state_cursor.fetchone()
            if state is None:
                raise LiveReconciliationError("canonical fixture disappeared during handoff")
            if state[0] == "completed" and state[1] is not None:
                return reference
            if state[0] != "scheduled":
                raise LiveReconciliationError(
                    "canonical fixture is not eligible for terminal handoff"
                )
            if (
                state[2] not in {"waiting", "pending"}
                or state[3] < eligible_at
                or state[4] < state[3]
                or state[5] >= state[6]
            ):
                raise LiveReconciliationError(
                    "canonical reconciliation state cannot accept terminal handoff"
                )
            return reference

    async def next_due_reconciliation(
        self,
        *,
        league_external_ids: tuple[int, ...],
        as_of: datetime,
        exclude_fixture_ids: Collection[int] = (),
    ) -> FixtureReconciliationTask | None:
        if as_of.tzinfo is None:
            raise LiveReconciliationError("reconciliation clock must be timezone-aware")
        query = """SELECT reconciliation.fixture_id,fixture_ref.external_id,
                      fixture.season_id,reconciliation.eligible_at,
                      reconciliation.attempt_count,reconciliation.max_attempts
               FROM ops.fixture_reconciliation_state reconciliation
               JOIN football.fixtures fixture ON fixture.id=reconciliation.fixture_id
               JOIN source.fixture_provider_refs fixture_ref
                 ON fixture_ref.fixture_id=fixture.id
               JOIN source.providers provider
                 ON provider.id=fixture_ref.provider_id
               JOIN source.season_provider_refs season_ref
                 ON season_ref.season_id=fixture.season_id
                AND season_ref.provider_id=provider.id
               WHERE provider.code=%s
                 AND season_ref.league_external_id=ANY(%s)
                 AND reconciliation.state IN ('waiting','pending')
                 AND reconciliation.attempt_count < reconciliation.max_attempts
                 AND reconciliation.next_attempt_at <= %s
                 AND fixture.lifecycle_state='scheduled'
               """
        params: list[object] = [
            PROVIDER_CODE,
            [str(value) for value in league_external_ids],
            as_of,
        ]
        if exclude_fixture_ids:
            query += " AND NOT (reconciliation.fixture_id = ANY(%s::bigint[]))"
            params.append([int(value) for value in exclude_fixture_ids])
        query += " ORDER BY reconciliation.next_attempt_at,reconciliation.fixture_id LIMIT 1"
        cursor = await self._connection.execute(query, params)
        row = await cursor.fetchone()
        if row is None:
            return None
        try:
            provider_fixture_id = int(row[1])
        except (TypeError, ValueError) as error:
            raise LiveReconciliationError("reconciliation provider fixture ID is invalid") from error
        if provider_fixture_id <= 0:
            raise LiveReconciliationError("reconciliation provider fixture ID is invalid")
        return FixtureReconciliationTask(
            fixture_id=int(row[0]),
            provider_fixture_id=provider_fixture_id,
            season_id=int(row[2]),
            eligible_at=row[3],
            attempt_count=int(row[4]),
            max_attempts=int(row[5]),
        )

    async def _lock_reconciliation_task(
        self, task: FixtureReconciliationTask
    ) -> tuple[int, int, str, datetime, int, int]:
        cursor = await self._connection.execute(
            """SELECT provider.id,fixture.season_id,reconciliation.state::text,
                      reconciliation.eligible_at,reconciliation.attempt_count,
                      reconciliation.max_attempts
               FROM ops.fixture_reconciliation_state reconciliation
               JOIN football.fixtures fixture ON fixture.id=reconciliation.fixture_id
               JOIN source.fixture_provider_refs fixture_ref
                 ON fixture_ref.fixture_id=fixture.id
               JOIN source.providers provider
                 ON provider.id=fixture_ref.provider_id
                AND provider.code=%s
               WHERE reconciliation.fixture_id=%s
                 AND fixture_ref.external_id=%s
               FOR UPDATE OF reconciliation""",
            (PROVIDER_CODE, task.fixture_id, str(task.provider_fixture_id)),
        )
        row = await cursor.fetchone()
        if row is None:
            raise LiveReconciliationError("reconciliation task identity no longer matches")
        state = str(row[2])
        if state not in {"waiting", "pending"} or int(row[4]) >= int(row[5]):
            raise LiveReconciliationError("reconciliation task is no longer mutable")
        if int(row[1]) != task.season_id:
            raise LiveReconciliationError("reconciliation task season identity changed")
        return int(row[0]), int(row[1]), state, row[3], int(row[4]), int(row[5])

    @staticmethod
    def _validate_reconciliation_response(
        task: FixtureReconciliationTask,
        fixture: ProviderLiveFixture,
        response: APIFootballResponse,
        *,
        request_started_at: datetime,
        response_received_at: datetime,
    ) -> None:
        if fixture.external_fixture_id != task.provider_fixture_id:
            raise LiveReconciliationError("reconciliation response fixture ID is unexpected")
        if request_started_at.tzinfo is None or response_received_at.tzinfo is None:
            raise LiveReconciliationError("reconciliation response timestamps are invalid")
        if response_received_at < request_started_at:
            raise LiveReconciliationError("reconciliation response timestamps are invalid")
        payload = response.data
        entries = payload.get("response")
        if (
            response.status_code != 200
            or payload.get("get") != "fixtures"
            or payload.get("parameters") != {"id": str(task.provider_fixture_id)}
            or payload.get("errors") not in ({}, [], None)
            or payload.get("paging") != {"current": 1, "total": 1}
            or payload.get("results") != 1
            or not isinstance(entries, list)
            or len(entries) != 1
        ):
            raise LiveReconciliationError("reconciliation response is not the exact requested fixture")
        try:
            raw_payload = json.loads(response.raw_body)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise LiveReconciliationError("reconciliation raw response is invalid JSON") from error
        if raw_payload != payload:
            raise LiveReconciliationError("reconciliation parsed response does not match raw bytes")

    async def persist_reconciliation_response(
        self,
        task: FixtureReconciliationTask,
        fixture: ProviderLiveFixture,
        response: APIFootballResponse,
        *,
        request_started_at: datetime,
        response_received_at: datetime,
        result: ProviderFinalResult | None,
        next_attempt_at: datetime,
    ) -> None:
        self._validate_reconciliation_response(
            task,
            fixture,
            response,
            request_started_at=request_started_at,
            response_received_at=response_received_at,
        )
        if result is not None and result.fixture != fixture:
            raise LiveReconciliationError("reconciliation final result does not match fixture")
        if fixture.status.is_terminal != (result is not None):
            raise LiveReconciliationError("reconciliation terminal state is inconsistent")
        if next_attempt_at <= response_received_at:
            raise LiveReconciliationError("next reconciliation attempt must be in the future")
        async with self._connection.transaction():
            provider_id, season_id, _, eligible_at, _, _ = await self._lock_reconciliation_task(task)
            if request_started_at < eligible_at or response_received_at < eligible_at:
                raise LiveReconciliationError("reconciliation response is before eligibility")
            reference = await self.resolve(fixture)
            if reference.fixture_id != task.fixture_id or reference.season_id != season_id:
                raise LiveReconciliationError("reconciliation response mapping changed")
            params = {"id": str(task.provider_fixture_id)}
            params_bytes = json.dumps(params, sort_keys=True, separators=(",", ":")).encode()
            paging = response.data["paging"]
            fetch_cursor = await self._connection.execute(
                """INSERT INTO source.provider_fetches(
                           provider_id,endpoint,request_params,request_params_sha256,purpose,
                           request_started_at,response_received_at,http_status,outcome,
                           provider_results,paging_current,paging_total,content_sha256,
                           subject_fixture_id,subject_season_id
                       ) VALUES(%s,'/fixtures',%s,%s,'postmatch_reconciliation',%s,%s,%s,
                                'success',%s,%s,%s,%s,%s,%s)
                       RETURNING id""",
                (
                    provider_id,
                    Jsonb(params),
                    hashlib.sha256(params_bytes).digest(),
                    request_started_at,
                    response_received_at,
                    response.status_code,
                    response.data["results"],
                    paging["current"],
                    paging["total"],
                    hashlib.sha256(response.raw_body).digest(),
                    task.fixture_id,
                    season_id,
                ),
            )
            fetch_row = await fetch_cursor.fetchone()
            assert fetch_row is not None
            fetch_id = int(fetch_row[0])
            await self._connection.execute(
                """INSERT INTO source.provider_raw_payloads(
                           fetch_id,inline_body,content_type,byte_count,retention_class,expires_at
                       ) VALUES(%s,%s,'application/json',%s,'standard',%s)""",
                (
                    fetch_id,
                    response.raw_body,
                    len(response.raw_body),
                    response_received_at + timedelta(days=30),
                ),
            )
            await self._connection.execute(
                "UPDATE source.provider_fetches SET normalized_at=%s WHERE id=%s",
                (response_received_at, fetch_id),
            )
            if result is None:
                await self._connection.execute(
                    "SELECT * FROM ops.record_fixture_reconciliation_attempt(%s,%s,%s)",
                    (task.fixture_id, fetch_id, next_attempt_at),
                )
                return
            await self._connection.execute(
                """SELECT * FROM ops.finalize_fixture_result(
                           %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                       )""",
                (
                    task.fixture_id,
                    fetch_id,
                    fixture.score.home,
                    fixture.score.away,
                    result.home_halftime_goals,
                    result.away_halftime_goals,
                    result.home_fulltime_goals,
                    result.away_fulltime_goals,
                    result.home_extratime_goals,
                    result.away_extratime_goals,
                    result.home_penalty_goals,
                    result.away_penalty_goals,
                ),
            )
            await self._connection.execute(
                """INSERT INTO source.fixture_provider_status(
                           provider_id,fixture_id,status_code,observed_at,source_fetch_id
                       ) VALUES(%s,%s,'FT',%s,%s)
                       ON CONFLICT(provider_id,fixture_id) DO UPDATE SET
                           status_code=excluded.status_code,observed_at=excluded.observed_at,
                           source_fetch_id=excluded.source_fetch_id
                       WHERE source.fixture_provider_status.observed_at < excluded.observed_at""",
                (provider_id, task.fixture_id, response_received_at, fetch_id),
            )
            await self._connection.execute(
                """UPDATE source.fixture_provider_refs
                   SET last_seen_at=greatest(last_seen_at,%s)
                   WHERE provider_id=%s AND fixture_id=%s""",
                (response_received_at, provider_id, task.fixture_id),
            )

    async def record_reconciliation_failure(
        self,
        task: FixtureReconciliationTask,
        *,
        request_started_at: datetime,
        response_received_at: datetime,
        next_attempt_at: datetime,
        http_status: int | None,
        outcome: str,
        error_class: str,
    ) -> None:
        if next_attempt_at <= response_received_at:
            raise LiveReconciliationError("next reconciliation attempt must be in the future")
        if outcome not in {"provider_error", "http_error", "transport_error"}:
            raise LiveReconciliationError("reconciliation failure outcome is invalid")
        async with self._connection.transaction():
            provider_id, season_id, _, eligible_at, _, _ = await self._lock_reconciliation_task(task)
            if request_started_at < eligible_at or response_received_at < eligible_at:
                raise LiveReconciliationError("reconciliation failure is before eligibility")
            params = {"id": str(task.provider_fixture_id)}
            params_bytes = json.dumps(params, sort_keys=True, separators=(",", ":")).encode()
            fetch_cursor = await self._connection.execute(
                """INSERT INTO source.provider_fetches(
                           provider_id,endpoint,request_params,request_params_sha256,purpose,
                           request_started_at,response_received_at,http_status,outcome,
                           sanitized_error_class,sanitized_error_text,subject_fixture_id,
                           subject_season_id
                       ) VALUES(%s,'/fixtures',%s,%s,'postmatch_reconciliation',%s,%s,%s,
                                %s,%s,'provider request failed',%s,%s)
                       RETURNING id""",
                (
                    provider_id,
                    Jsonb(params),
                    hashlib.sha256(params_bytes).digest(),
                    request_started_at,
                    response_received_at,
                    http_status,
                    outcome,
                    error_class,
                    task.fixture_id,
                    season_id,
                ),
            )
            fetch_row = await fetch_cursor.fetchone()
            assert fetch_row is not None
            await self._connection.execute(
                "SELECT * FROM ops.record_fixture_reconciliation_attempt(%s,%s,%s)",
                (task.fixture_id, int(fetch_row[0]), next_attempt_at),
            )
