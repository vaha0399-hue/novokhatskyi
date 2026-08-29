from __future__ import annotations

import json
import os
from dataclasses import replace
from pathlib import Path

import psycopg
import pytest

from app.live import LiveResolutionError, PostgresLiveFixtureResolver, normalize_live_fixture


TEST_DB_URL = os.environ.get("LIVE_DOMAIN_TEST_DB_URL")
pytestmark = pytest.mark.skipif(
    not TEST_DB_URL, reason="LIVE_DOMAIN_TEST_DB_URL is not configured"
)
FIXTURES = (
    Path(__file__).parents[2]
    / "samples"
    / "api-football"
    / "pro-canary-2026-08-29"
    / "03-fixtures-epl-2026.raw.json"
)


def test_real_epl_2026_provider_fixture_resolves_to_canonical_id() -> None:
    assert TEST_DB_URL is not None
    payload = json.loads(FIXTURES.read_bytes())
    finished = next(
        item for item in payload["response"] if item["fixture"]["status"]["short"] == "FT"
    )
    provider_fixture = normalize_live_fixture(finished)

    with psycopg.connect(TEST_DB_URL) as connection:
        connection.execute("SET TRANSACTION READ ONLY")
        resolved = PostgresLiveFixtureResolver(connection).resolve(provider_fixture)
        connection.rollback()

    assert resolved is not None
    assert resolved.fixture_id > 0
    assert resolved.season_id > 0
    assert resolved.league_id > 0
    assert resolved.home_team_id != resolved.away_team_id


@pytest.mark.parametrize(
    "field",
    [
        "external_fixture_id",
        "season_start_year",
        "home_external_team_id",
        "away_external_team_id",
    ],
)
def test_real_resolver_rejects_each_mismatched_provider_identity(field: str) -> None:
    assert TEST_DB_URL is not None
    payload = json.loads(FIXTURES.read_bytes())
    finished = next(
        item for item in payload["response"] if item["fixture"]["status"]["short"] == "FT"
    )
    provider_fixture = normalize_live_fixture(finished)
    mismatched = replace(provider_fixture, **{field: getattr(provider_fixture, field) + 1})

    with psycopg.connect(TEST_DB_URL) as connection:
        connection.execute("SET TRANSACTION READ ONLY")
        with pytest.raises(LiveResolutionError, match="missing or conflicts"):
            PostgresLiveFixtureResolver(connection).resolve(mismatched)
        connection.rollback()
