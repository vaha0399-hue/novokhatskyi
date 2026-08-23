"""Read-only SQL regression checks against an explicitly configured database."""

from __future__ import annotations

import os

import psycopg
import pytest

from app.analytics.models import AnalyticsScope
from app.analytics.repository import PostgresAnalyticsRepository, TEAM_HISTORY_SQL


TEST_DB_URL = os.environ.get("ANALYTICS_TEST_DB_URL")
pytestmark = pytest.mark.skipif(not TEST_DB_URL, reason="ANALYTICS_TEST_DB_URL is not configured")


def test_epl_2024_history_query_is_strictly_pre_kickoff_and_complete() -> None:
    assert TEST_DB_URL is not None
    with psycopg.connect(TEST_DB_URL, autocommit=True) as connection:
        target = connection.execute(
            """SELECT id, season_id, kickoff_at, home_team_id FROM football.fixtures
               WHERE lifecycle_state='completed' ORDER BY kickoff_at, id OFFSET 20 LIMIT 1"""
        ).fetchone()
        assert target is not None
        fixture_id, season_id, cutoff, team_id = target
        repository = PostgresAnalyticsRepository(connection)
        records = repository.load_team_history(
            team_id=team_id,
            season_id=season_id,
            as_of_kickoff=cutoff,
            scope=AnalyticsScope.OVERALL,
        )

        assert records
        assert all(record.kickoff_at < cutoff for record in records)
        assert len({record.fixture_id for record in records}) == len(records)
        assert all(record.expected_goals is not None for record in records)
        assert all(record.total_shots is not None for record in records)
        assert "f.kickoff_at < %(as_of_kickoff)s" in TEAM_HISTORY_SQL
        assert "f.season_id = %(season_id)s" in TEAM_HISTORY_SQL
