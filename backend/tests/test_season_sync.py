from __future__ import annotations

import json
import asyncio
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

import pytest

from app.api_football import APIFootballBudgetDenied, APIFootballResponse
from app.api_football.errors import APIFootballHTTPError
from app.importer.active_season import ActiveSeasonScope
from app.importer.season_bootstrap import CollectedBaseResponse
from app.importer.season_sync import (
    APPROVED_LEAGUE_POLICIES,
    SeasonalLeaguePolicy,
    SeasonalRunAcquisition,
    SeasonalSyncWorker,
    SeasonalWorkItem,
    discover_current_season,
)


def _response(payload: dict[str, Any]) -> APIFootballResponse:
    raw = json.dumps(payload, separators=(",", ":")).encode()
    return APIFootballResponse(payload, raw, 200, {"x-ratelimit-remaining": "100"})


def _league_response(league_id: int, *, season: int | None) -> APIFootballResponse:
    return _response(
        {
            "parameters": {"id": str(league_id)},
            "response": [
                {
                    "league": {"id": league_id, "type": "League"},
                    "seasons": [] if season is None else [{"year": season, "current": True}],
                }
            ],
        }
    )


@dataclass
class FakeProvider:
    responses: list[APIFootballResponse | Exception]
    calls: list[tuple[str, Mapping[str, str | int] | None]] = field(default_factory=list)

    async def get(self, endpoint: str, *, params: Mapping[str, str | int] | None = None) -> APIFootballResponse:
        self.calls.append((endpoint, params))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def response_contains_api_key(self, body: bytes) -> bool:
        return False


@dataclass
class FakeRepository:
    policies: Sequence[SeasonalLeaguePolicy]
    existing: set[tuple[int, int]] = field(default_factory=set)
    due: bool = True
    imported: list[ActiveSeasonScope] = field(default_factory=list)
    complete_checkpoints: list[dict[str, Any]] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    deferred: list[dict[str, Any]] = field(default_factory=list)
    finished: tuple[str, dict[str, Any]] | None = None
    rate_headers: list[dict[str, str]] = field(default_factory=list)
    _next: int = 0

    def start_run(self, policies: Sequence[SeasonalLeaguePolicy]) -> SeasonalRunAcquisition:
        assert tuple(policies) == tuple(self.policies)
        return SeasonalRunAcquisition(55, run_token=1234, acquired=True)

    def renew_run_lease(self, run_id: int, run_token: int) -> bool:
        return True

    def claim_next(
        self, run_id: int, run_token: int, policies: Mapping[int, SeasonalLeaguePolicy]
    ) -> SeasonalWorkItem | None:
        if self._next >= len(self.policies):
            return None
        policy = self.policies[self._next]
        self._next += 1
        return SeasonalWorkItem(self._next, policy)

    def season_exists(self, *, league_external_id: int, season_start_year: int) -> bool:
        return (league_external_id, season_start_year) in self.existing

    def discovery_due(self, *, league_external_id: int) -> bool:
        return self.due

    def observe_rate_limit(self, *, endpoint: str, headers: Mapping[str, str]) -> None:
        self.rate_headers.append(dict(headers))

    def import_and_verify(self, *, scope: ActiveSeasonScope, collected: Sequence[CollectedBaseResponse]) -> None:
        self.imported.append(scope)

    def complete(self, item: SeasonalWorkItem, run_token: int, checkpoint: Mapping[str, Any]) -> None:
        self.complete_checkpoints.append(dict(checkpoint))

    def fail(self, item: SeasonalWorkItem, run_token: int, *, checkpoint: Mapping[str, Any], error: str) -> None:
        self.failed.append(error)

    def defer(
        self,
        item: SeasonalWorkItem,
        run_token: int,
        *,
        checkpoint: Mapping[str, Any],
        error: str,
        delay_seconds: float,
    ) -> None:
        self.deferred.append({"id": item.id, "checkpoint": dict(checkpoint), "error": error, "delay_seconds": delay_seconds})

    def pending_delay_seconds(self, run_id: int) -> float | None:
        return None

    def finish_run(self, run_id: int, run_token: int, *, status: str, checkpoint: Mapping[str, Any]) -> None:
        self.finished = (status, dict(checkpoint))


def test_approved_policies_are_bounded_to_reviewed_leagues() -> None:
    assert [(policy.code, policy.league_external_id, policy.expected_fixture_count) for policy in APPROVED_LEAGUE_POLICIES] == [
        ("premier-league", 39, 380),
        ("la-liga", 140, 380),
        ("serie-a", 135, 380),
        ("bundesliga", 78, 306),
        ("ligue-1", 61, 306),
    ]


def test_discovery_rejects_multiple_current_seasons() -> None:
    policy = SeasonalLeaguePolicy("test", 39, 20)
    response = _response(
        {"parameters": {"id": "39"}, "response": [{"league": {"id": 39, "type": "League"}, "seasons": [{"year": 2026, "current": True}, {"year": 2027, "current": True}]}]}
    )

    with pytest.raises(Exception, match="multiple"):
        discover_current_season(response, policy)


def test_worker_skips_current_season_already_canonical_without_base_requests() -> None:
    policy = SeasonalLeaguePolicy("test", 39, 20)
    provider = FakeProvider([_league_response(39, season=2026)])
    repository = FakeRepository([policy], existing={(39, 2026)})

    report = asyncio.run(SeasonalSyncWorker(provider=provider, repository=repository, policies=[policy]).run_once())

    assert report.status == "succeeded"
    assert [item.outcome for item in report.leagues] == ["already_imported"]
    assert provider.calls == [("/leagues", {"id": 39})]
    assert not repository.imported


def test_worker_does_not_call_provider_while_canonical_season_is_in_progress() -> None:
    policy = SeasonalLeaguePolicy("test", 39, 20)
    provider = FakeProvider([])
    repository = FakeRepository([policy], due=False)

    report = asyncio.run(SeasonalSyncWorker(provider=provider, repository=repository, policies=[policy]).run_once())

    assert report.status == "succeeded"
    assert report.leagues[0].outcome == "season_in_progress"
    assert provider.calls == []


def test_worker_marks_partial_new_season_not_ready_without_import() -> None:
    policy = SeasonalLeaguePolicy("test", 39, 20)
    provider = FakeProvider(
        [
            _league_response(39, season=2027),
            _response({"parameters": {"id": "39", "season": "2027"}, "response": []}),
            _response({"parameters": {"league": "39", "season": "2027"}, "results": 0, "response": []}),
            _response({"parameters": {"league": "39", "season": "2027"}, "results": 0, "response": []}),
            _response({"parameters": {"league": "39", "season": "2027"}, "results": 0, "response": []}),
        ]
    )
    repository = FakeRepository([policy])

    report = asyncio.run(SeasonalSyncWorker(provider=provider, repository=repository, policies=[policy]).run_once())

    assert report.status == "succeeded"
    assert report.leagues[0].outcome == "not_ready"
    assert not repository.imported
    assert repository.complete_checkpoints[-1]["reason"] == "base_responses_partial"


def test_rate_limit_stops_remaining_policies() -> None:
    first = SeasonalLeaguePolicy("first", 39, 20)
    second = SeasonalLeaguePolicy("second", 140, 20)
    provider = FakeProvider([APIFootballHTTPError(429, safe_headers={"retry-after": "60"})])
    repository = FakeRepository([first, second])

    report = asyncio.run(SeasonalSyncWorker(provider=provider, repository=repository, policies=[first, second]).run_once())

    assert report.status == "failed"
    assert len(provider.calls) == 1
    assert repository.failed == ["ProviderQuotaExhausted"]
    assert repository.finished is not None and repository.finished[0] == "failed"


def test_budget_denial_defers_durable_work_without_marking_it_failed_or_succeeded() -> None:
    policy = SeasonalLeaguePolicy("first", 39, 20)
    provider = FakeProvider([APIFootballBudgetDenied("cooldown")])
    repository = FakeRepository([policy])

    report = asyncio.run(SeasonalSyncWorker(provider=provider, repository=repository, policies=[policy]).run_once())

    assert report.status == "failed"
    assert report.leagues[0].outcome == "budget_pending"
    assert repository.failed == []
    assert repository.complete_checkpoints == []
    assert repository.deferred and repository.deferred[0]["checkpoint"]["outcome"] == "budget_pending"
    assert repository.finished is not None and repository.finished[0] == "failed"
