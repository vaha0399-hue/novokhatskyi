from __future__ import annotations

import asyncio
import copy
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from app.api_football import APIFootballBudgetDenied, APIFootballResponse
from app.importer.cup_bootstrap import CupCompetition
from app.importer.cup_queue import OPERATION, CupQueueSettings, CupWorkItem, Worker
from app.importer.raw_spool import RawSpool


def _response(payload: dict[str, Any]) -> APIFootballResponse:
    return APIFootballResponse(payload, json.dumps(payload, sort_keys=True).encode(), 200, {})


def _base_payloads() -> dict[str, dict[str, Any]]:
    coverage = {"standings": True, "injuries": False, "fixtures": {"statistics_fixtures": True, "lineups": False}}
    return {
        "/leagues": {"get": "leagues", "parameters": {"id": "2", "season": "2026"}, "errors": {}, "results": 1, "paging": {"current": 1, "total": 1}, "response": [{"league": {"id": 2, "name": "Cup One", "type": "Cup", "logo": None}, "country": {"name": "World", "code": "WO", "flag": None}, "seasons": [{"year": 2026, "current": True, "start": "2026-01-01", "end": "2026-12-31", "coverage": coverage}]}]},
        "/teams": {"get": "teams", "parameters": {"league": "2", "season": "2026"}, "errors": {}, "results": 2, "paging": {"current": 1, "total": 1}, "response": [{"team": {"id": 10, "name": "A", "country": "World", "founded": None, "national": False, "code": None, "logo": None}, "venue": {}}, {"team": {"id": 11, "name": "B", "country": "World", "founded": None, "national": False, "code": None, "logo": None}, "venue": {}}]},
        "/standings": {"get": "standings", "parameters": {"league": "2", "season": "2026"}, "errors": {}, "results": 0, "paging": {"current": 1, "total": 1}, "response": []},
        "/fixtures": {"get": "fixtures", "parameters": {"league": "2", "season": "2026"}, "errors": {}, "results": 1, "paging": {"current": 1, "total": 1}, "response": [{"fixture": {"id": 100, "date": "2026-09-01T12:00:00+00:00", "timezone": "UTC", "referee": None, "venue": {"id": None, "name": None, "city": None}, "status": {"short": "NS"}}, "league": {"id": 2, "season": 2026, "round": "Final"}, "teams": {"home": {"id": 10}, "away": {"id": 11}}, "goals": {"home": None, "away": None}, "score": {key: {"home": None, "away": None} for key in ("halftime", "fulltime", "extratime", "penalty")}}]},
    }


@dataclass
class _Provider:
    payloads: dict[str, dict[str, Any]]
    calls: list[str] = field(default_factory=list)
    async def get(self, endpoint: str, *, params=None):
        self.calls.append(endpoint)
        return _response(copy.deepcopy(self.payloads[endpoint]))


@dataclass
class _Sink:
    writes: int = 0
    def write_cup_base(self, *, validated, collected) -> None:
        self.writes += 1


@dataclass
class _Repository:
    item: CupWorkItem | None = None
    created: list[CupCompetition] = field(default_factory=list)
    complete_calls: list[Mapping[str, Any]] = field(default_factory=list)
    requeues: list[Mapping[str, Any]] = field(default_factory=list)
    reserve_calls: int = 0
    checkpoints: list[Mapping[str, Any]] = field(default_factory=list)
    claimed: bool = False
    def active_run(self): return None
    def create_run(self, items, *, operation, policy_version):
        assert operation == OPERATION and policy_version == 1
        self.created = list(items); self.item = CupWorkItem(1, self.created[0], 1, {}); return 7
    def claim_next(self, run_id):
        if self.claimed: return None
        self.claimed = True; return self.item
    def unfinished_delay_seconds(self, run_id): return None
    def reserve_request(self, daily_limit): self.reserve_calls += 1; return True
    def renew(self, item): pass
    def complete(self, item, checkpoint): self.complete_calls.append(checkpoint)
    def requeue(self, item, *, checkpoint, error, delay_seconds): self.requeues.append(checkpoint)
    def checkpoint_run(self, run_id, checkpoint): self.checkpoints.append(checkpoint)
    def finish_run(self, run_id, *, checkpoint): pass


def _settings(tmp_path: Path) -> CupQueueSettings:
    allow = tmp_path / "cups.txt"; allow.write_text("2\tCup One\n", encoding="utf-8")
    with_stats = tmp_path / "with.json"; with_stats.write_text(json.dumps([{ "league_id": 2, "name": "Cup One", "season": 2026, "type": "Cup", "coverage": {"fixtures": {"statistics_fixtures": True}} }]), encoding="utf-8")
    without_stats = tmp_path / "without.json"; without_stats.write_text("[]", encoding="utf-8")
    return CupQueueSettings(tmp_path / "spool", allow, with_stats, without_stats)


def test_queue_uses_classified_scope_stages_four_raw_responses_and_runs_statistics(tmp_path: Path) -> None:
    provider, repository, sink = _Provider(_base_payloads()), _Repository(), _Sink()
    observed = []
    async def statistics(scope):
        observed.append(scope)
        return type("Report", (), {"stopped_reason": None, "errors": (), "fixtures_normalized": 1, "statistics_rows_written": 2, "api_requests": 1})()
    report = asyncio.run(Worker(provider=provider, repository=repository, spool=RawSpool(tmp_path / "spool"), settings=_settings(tmp_path), canonical_sink=sink, statistics_backfill=statistics).run_once())
    assert report.status == "succeeded"
    assert provider.calls == ["/leagues", "/teams", "/standings", "/fixtures"]
    assert repository.reserve_calls == 4
    assert sink.writes == 1
    assert [(scope.league_external_id, scope.season_start_year, scope.select_all_completed) for scope in observed] == [(2, 2026, True)]
    assert repository.complete_calls[0]["fixture_statistics_coverage"] is True


def test_queue_never_calls_statistics_for_no_coverage(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    settings.with_fixture_stats_path.write_text("[]", encoding="utf-8")
    settings.without_fixture_stats_path.write_text(json.dumps([{ "league_id": 2, "name": "Cup One", "season": 2026, "type": "Cup", "coverage": {"fixtures": {"statistics_fixtures": False}} }]), encoding="utf-8")
    called = False
    async def statistics(scope):
        nonlocal called; called = True
        raise AssertionError("must not run")
    repository = _Repository()
    asyncio.run(Worker(provider=_Provider(_base_payloads()), repository=repository, spool=RawSpool(tmp_path / "spool"), settings=settings, canonical_sink=_Sink(), statistics_backfill=statistics).run_once())
    assert called is False
    assert repository.complete_calls[0]["statistics"]["outcome"] == "unavailable_by_catalogue"


def test_queue_pauses_before_request_when_run_quota_is_exhausted(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    settings = CupQueueSettings(settings.spool_dir, settings.allow_list_path, settings.with_fixture_stats_path, settings.without_fixture_stats_path, run_request_cap=1)
    repository = _Repository()
    report = asyncio.run(Worker(provider=_Provider(_base_payloads()), repository=repository, spool=RawSpool(tmp_path / "spool"), settings=settings, canonical_sink=_Sink()).run_once())
    assert report.status == "paused_quota"
    assert len(repository.requeues) == 1
    assert repository.requeues[0]["outcome"] == "retry_pending"
    assert repository.checkpoints[-1]["stopped_reason"] == "CupQueueQuotaExhausted"


def test_queue_defers_budget_denial_without_completing_the_durable_item(tmp_path: Path) -> None:
    class DeniedProvider(_Provider):
        async def get(self, endpoint: str, *, params=None):
            self.calls.append(endpoint)
            raise APIFootballBudgetDenied("cooldown")

    repository = _Repository()
    report = asyncio.run(Worker(provider=DeniedProvider(_base_payloads()), repository=repository, spool=RawSpool(tmp_path / "spool"), settings=_settings(tmp_path), canonical_sink=_Sink()).run_once())

    assert report.status == "paused_budget"
    assert repository.complete_calls == []
    assert repository.requeues and repository.requeues[0]["outcome"] == "budget_pending"
    assert repository.checkpoints[-1]["stopped_reason"] == "APIFootballBudgetDenied"
