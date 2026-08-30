from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app.web.repository import WebReadRepository


class QueryResult:
    def fetchall(self) -> list[tuple[Any, ...]]:
        return [(3, "Premier League", "England", None, "league", 2)]


class RecordingConnection:
    def __init__(self) -> None:
        self.query = ""
        self.parameters: tuple[Any, ...] = ()

    def execute(self, query: str, parameters: tuple[Any, ...]) -> QueryResult:
        self.query = query
        self.parameters = parameters
        return QueryResult()


def test_match_date_discovery_keeps_historical_retired_leagues() -> None:
    connection = RecordingConnection()
    repository = WebReadRepository(connection)  # type: ignore[arg-type]
    start_at = datetime(2026, 8, 30, tzinfo=UTC)
    end_at = datetime(2026, 8, 31, tzinfo=UTC)

    leagues = repository.list_match_date_leagues(start_at=start_at, end_at=end_at)

    assert [(item.league.id, item.fixture_count) for item in leagues] == [(3, 2)]
    assert "retired_at" not in connection.query
    assert connection.parameters == (start_at, end_at)
