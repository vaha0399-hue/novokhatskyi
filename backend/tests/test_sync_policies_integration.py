from __future__ import annotations

import os
from datetime import UTC, datetime

import psycopg
import pytest
from psycopg.errors import CheckViolation
from psycopg.types.json import Jsonb

from app.sync.policies import (
    PolicyCheckedEnqueuer,
    PolicyCheckedExecutor,
    PostgresCompetitionSyncPolicyReader,
    SyncPolicyDenied,
    SyncPolicyGate,
    SyncWorkRequest,
)


TEST_DB_URL = os.environ.get("COMPETITION_SYNC_POLICIES_TEST_DB_URL")
pytestmark = pytest.mark.skipif(
    not TEST_DB_URL, reason="COMPETITION_SYNC_POLICIES_TEST_DB_URL is not configured"
)


def test_db_policy_recreation_rejects_stale_authorization_before_callback() -> None:
    assert TEST_DB_URL is not None
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        provider_id = connection.execute(
            "INSERT INTO source.providers(code,name) VALUES('policy-adapter', 'Policy adapter') RETURNING id"
        ).fetchone()[0]
        country_id = connection.execute(
            "INSERT INTO football.countries(name) VALUES('Policy adapter country') RETURNING id"
        ).fetchone()[0]
        league_id = connection.execute(
            """INSERT INTO football.leagues(name,country_id,competition_type)
                 VALUES('Policy adapter competition',%s,'league') RETURNING id""",
            (country_id,),
        ).fetchone()[0]
        connection.execute(
            "INSERT INTO source.league_provider_refs(provider_id,external_id,league_id) VALUES(%s,'adapter-league',%s)",
            (provider_id, league_id),
        )
        season_id = connection.execute(
            "INSERT INTO football.seasons(league_id,start_year,label) VALUES(%s,2026,'2026/27') RETURNING id",
            (league_id,),
        ).fetchone()[0]
        connection.execute(
            "INSERT INTO source.season_provider_refs(provider_id,league_external_id,external_season,season_id) VALUES(%s,'adapter-league',2026,%s)",
            (provider_id, season_id),
        )
        old_instance_id = connection.execute(
            """INSERT INTO ops.competition_sync_policies(
                   provider_id,season_id,enabled,allowed_work_types,coverage,refresh_intervals
                 ) VALUES(%s,%s,true,ARRAY['coverage_refresh'],%s,%s) RETURNING policy_instance_id""",
            (provider_id, season_id, Jsonb({}), Jsonb({"coverage_refresh": {"value": 1, "unit": "hour"}})),
        ).fetchone()[0]
        gate = SyncPolicyGate(
            PostgresCompetitionSyncPolicyReader(connection),
            now=lambda: datetime.now(UTC),
        )
        request = SyncWorkRequest(int(provider_id), int(season_id), "coverage_refresh")
        enqueued: list[object] = []
        authorization = PolicyCheckedEnqueuer(gate).enqueue(request, lambda value: (enqueued.append(value), value)[1])
        with pytest.raises(CheckViolation, match="instance is immutable"):
            connection.execute(
                "UPDATE ops.competition_sync_policies SET policy_instance_id=DEFAULT WHERE provider_id=%s AND season_id=%s",
                (provider_id, season_id),
            )
        connection.execute(
            "DELETE FROM ops.competition_sync_policies WHERE provider_id=%s AND season_id=%s",
            (provider_id, season_id),
        )
        recreated_instance_id = connection.execute(
            """INSERT INTO ops.competition_sync_policies(
                   provider_id,season_id,enabled,allowed_work_types,coverage,refresh_intervals,policy_instance_id,policy_version
                 ) OVERRIDING SYSTEM VALUE VALUES(%s,%s,true,ARRAY['coverage_refresh'],%s,%s,%s,1)
                 RETURNING policy_instance_id""",
            (provider_id, season_id, Jsonb({}), Jsonb({"coverage_refresh": {"value": 25, "unit": "second"}}), old_instance_id),
        ).fetchone()[0]
        assert recreated_instance_id != old_instance_id
        executed: list[object] = []

        with pytest.raises(SyncPolicyDenied, match="instance_changed"):
            PolicyCheckedExecutor(gate).execute(authorization, lambda value: executed.append(value))
        new_authorization = PolicyCheckedEnqueuer(gate).enqueue(request, lambda value: value)
        assert PolicyCheckedExecutor(gate).execute(new_authorization, lambda value: value.refresh_interval.unit) == "second"

    assert len(enqueued) == 1
    assert executed == []
