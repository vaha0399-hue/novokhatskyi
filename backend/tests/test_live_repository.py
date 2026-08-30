from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.live import LiveResolutionError, PostgresLiveFixtureResolver, normalize_live_fixture


def _fixture():
    return normalize_live_fixture(
        {
            "fixture": {"id": 1557383, "status": {"short": "1H", "elapsed": 10, "extra": None}},
            "league": {"id": 39, "season": 2026},
            "teams": {"home": {"id": 40}, "away": {"id": 65}},
            "goals": {"home": 0, "away": 0},
        }
    )


class FakeCursor:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class FakeConnection:
    def __init__(self, row):
        self.row = row
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, query: str, params: tuple[object, ...]):
        self.calls.append((query, params))
        return FakeCursor(self.row)


def test_resolver_returns_canonical_internal_identity() -> None:
    kickoff = datetime(2026, 8, 16, 15, tzinfo=UTC)
    connection = FakeConnection(
        (101, 14, 7, kickoff, 10, "Liverpool", 20, "Forest", "scheduled")
    )

    resolved = PostgresLiveFixtureResolver(connection).resolve(_fixture())  # type: ignore[arg-type]

    assert resolved is not None
    assert resolved.fixture_id == 101
    assert resolved.season_id == 14
    assert resolved.league_id == 7
    assert resolved.home_team_id == 10
    assert resolved.away_team_id == 20
    assert connection.calls[0][1] == ("api-football", "1557383", "39", 2026, "40", "65")
    query = connection.calls[0][0]
    assert "fixture_ref.external_id=%s" in query
    assert "season_ref.league_external_id=%s" in query
    assert "home_ref.external_id=%s" in query
    assert "away_ref.external_id=%s" in query


def test_resolver_rejects_completed_fixture_as_current_live_state() -> None:
    kickoff = datetime(2026, 8, 16, 15, tzinfo=UTC)
    connection = FakeConnection(
        (101, 14, 7, kickoff, 10, "Liverpool", 20, "Forest", "completed")
    )

    with pytest.raises(LiveResolutionError, match="not eligible"):
        PostgresLiveFixtureResolver(connection).resolve(_fixture())  # type: ignore[arg-type]


def test_resolver_fails_closed_for_unknown_or_mismatched_fixture() -> None:
    connection = FakeConnection(None)

    with pytest.raises(LiveResolutionError, match="missing or conflicts"):
        PostgresLiveFixtureResolver(connection).resolve(_fixture())  # type: ignore[arg-type]
