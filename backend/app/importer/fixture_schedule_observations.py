"""Append-only fixture schedule observations from retained provider responses."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from psycopg import AsyncConnection, Connection
from psycopg.types.json import Jsonb


class FixtureScheduleObservationConflict(RuntimeError):
    """One provider fetch was replayed with contradictory fixture data."""


_INSERT_SQL = """WITH input AS (
        SELECT *
        FROM jsonb_to_recordset(%s::jsonb) AS item(
            fixture_id bigint,
            observed_kickoff_at timestamptz
        )
    )
    INSERT INTO source.fixture_schedule_observations(
        provider_id,
        fixture_id,
        source_fetch_id,
        observed_kickoff_at,
        observed_at
    )
    SELECT %s, fixture_id, %s, observed_kickoff_at, %s
    FROM input
    ON CONFLICT(provider_id, fixture_id, source_fetch_id) DO NOTHING"""

_SELECT_SQL = """SELECT fixture_id, observed_kickoff_at, observed_at
    FROM source.fixture_schedule_observations
    WHERE provider_id=%s
      AND source_fetch_id=%s
      AND fixture_id=ANY(%s)"""


def _input_rows(
    expected: Mapping[int, datetime | None],
) -> list[dict[str, int | str | None]]:
    return [
        {
            "fixture_id": fixture_id,
            "observed_kickoff_at": (
                None if kickoff_at is None else kickoff_at.isoformat()
            ),
        }
        for fixture_id, kickoff_at in expected.items()
    ]


def _verify_persisted(
    expected: Mapping[int, datetime | None],
    *,
    observed_at: datetime,
    source_fetch_id: int,
    persisted: list[tuple[Any, ...]],
) -> None:
    actual = {
        int(fixture_id): (observed_kickoff_at, persisted_observed_at)
        for fixture_id, observed_kickoff_at, persisted_observed_at in persisted
    }
    if len(actual) != len(expected) or any(
        actual.get(fixture_id) != (kickoff_at, observed_at)
        for fixture_id, kickoff_at in expected.items()
    ):
        raise FixtureScheduleObservationConflict(
            "conflicting fixture schedule observation for provider fetch "
            f"{source_fetch_id}"
        )


def record_fixture_schedule_observations(
    conn: Connection[Any],
    *,
    provider_id: int,
    source_fetch_id: int,
    observed_at: datetime,
    kickoff_by_fixture_id: Mapping[int, datetime | None],
) -> None:
    """Record and verify fixture facts inside the caller's transaction.

    Exact replay is idempotent.  A replay that assigns a different kickoff or
    receipt time to the same provider/fixture/fetch identity fails explicitly.
    """
    expected = dict(kickoff_by_fixture_id)
    if not expected:
        return

    conn.execute(
        _INSERT_SQL,
        (Jsonb(_input_rows(expected)), provider_id, source_fetch_id, observed_at),
    )
    persisted = conn.execute(
        _SELECT_SQL,
        (provider_id, source_fetch_id, list(expected)),
    ).fetchall()
    _verify_persisted(
        expected,
        observed_at=observed_at,
        source_fetch_id=source_fetch_id,
        persisted=persisted,
    )


async def record_fixture_schedule_observations_async(
    conn: AsyncConnection[Any],
    *,
    provider_id: int,
    source_fetch_id: int,
    observed_at: datetime,
    kickoff_by_fixture_id: Mapping[int, datetime | None],
) -> None:
    """Async adapter for the same append-only observation writer."""
    expected = dict(kickoff_by_fixture_id)
    if not expected:
        return

    await conn.execute(
        _INSERT_SQL,
        (Jsonb(_input_rows(expected)), provider_id, source_fetch_id, observed_at),
    )
    cursor = await conn.execute(
        _SELECT_SQL,
        (provider_id, source_fetch_id, list(expected)),
    )
    _verify_persisted(
        expected,
        observed_at=observed_at,
        source_fetch_id=source_fetch_id,
        persisted=await cursor.fetchall(),
    )
