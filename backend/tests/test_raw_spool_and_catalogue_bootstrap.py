from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from app.api_football import APIFootballResponse
from app.importer.catalogue_bootstrap import CatalogueCompetition, Report, Settings, Worker, parse_catalogue
from app.importer.raw_spool import RawSpool, RawSpoolArtifact
from app.importer.season_bootstrap import BaseRequest


def _response(payload: dict[str, Any]) -> APIFootballResponse:
    return APIFootballResponse(payload, json.dumps(payload, sort_keys=True).encode(), 200, {})


def test_raw_spool_round_trip_is_hash_verified(tmp_path: Path) -> None:
    spool = RawSpool(tmp_path / "spool")
    request = BaseRequest("/teams", {"league": 39, "season": 2026})
    response = _response({"get": "teams", "parameters": {"league": "39", "season": "2026"}, "errors": {}, "results": 0, "paging": {"current": 1, "total": 1}, "response": []})
    now = datetime.now(UTC)
    directory = spool.capture_directory(run_id=1, league_external_id=39, season_start_year=2026, generation=1)

    spool.stage(directory, RawSpoolArtifact(request, response, now, now))
    loaded = spool.load(directory, request)

    assert loaded is not None
    assert loaded.response.raw_body == response.raw_body
    assert (directory / "teams.raw.json").stat().st_mode & 0o777 == 0o600


def test_raw_spool_recovers_only_an_interrupted_endpoint_write(tmp_path: Path) -> None:
    spool = RawSpool(tmp_path / "spool")
    request = BaseRequest("/fixtures", {"league": 39, "season": 2026})
    directory = spool.capture_directory(run_id=1, league_external_id=39, season_start_year=2026, generation=1)
    directory.mkdir(parents=True)
    (directory / "fixtures.raw.json").write_bytes(b"{}")

    assert spool.discard_partial(directory, request) is True
    assert spool.load(directory, request) is None


def test_catalogue_replay_is_consumed_after_queue_creation(tmp_path: Path) -> None:
    spool = RawSpool(tmp_path / "spool")
    request = BaseRequest("/leagues", {})
    response = _response({"get": "leagues", "parameters": {}, "errors": {}, "results": 0, "paging": {"current": 1, "total": 1}, "response": []})
    directory = spool.root / "catalogue" / "digest"
    now = datetime.now(UTC)
    spool.stage(directory, RawSpoolArtifact(request, response, now, now))
    spool.mark_catalogue_pending(directory)

    assert spool.latest_catalogue() is not None
    spool.consume_pending_catalogues()
    assert spool.latest_catalogue() is None


def test_catalogue_classifies_only_current_regular_leagues() -> None:
    response = _response(
        {
            "get": "leagues", "parameters": {}, "errors": {}, "results": 3, "paging": {"current": 1, "total": 1},
            "response": [
                {"league": {"id": 39, "name": "Premier League", "type": "League"}, "seasons": [{"year": 2026, "current": True, "coverage": {"standings": True}}]},
                {"league": {"id": 2, "name": "Champions League", "type": "Cup"}, "seasons": [{"year": 2026, "current": True, "coverage": {"standings": True}}]},
                {"league": {"id": 9, "name": "Unpublished", "type": "League"}, "seasons": []},
            ],
        }
    )

    items = parse_catalogue(response)

    assert [(item.league_external_id, item.initial_outcome) for item in items] == [
        (2, "deferred_unsupported_type"), (9, "deferred_no_current_season"), (39, None)
    ]


@dataclass
class _Provider:
    response: APIFootballResponse
    calls: list[tuple[str, Mapping[str, str | int] | None]] = field(default_factory=list)

    async def get(self, endpoint: str, *, params: Mapping[str, str | int] | None = None) -> APIFootballResponse:
        self.calls.append((endpoint, params))
        return self.response

    def response_contains_api_key(self, body: bytes) -> bool:
        return False


@dataclass
class _Repository:
    items: list[CatalogueCompetition] = field(default_factory=list)
    completed: list[str] = field(default_factory=list)
    _claimed: bool = False

    def active_run(self): return None
    def create_run(self, items, *, catalogue_sha256, request_count): self.items = list(items); return 1
    def claim_next(self, run_id):
        if self._claimed: return None
        self._claimed = True
        from app.importer.catalogue_bootstrap import WorkItem
        return WorkItem(1, self.items[0], 1, {})
    def unfinished_delay_seconds(self, run_id): return None
    def reserve_request(self, daily_limit): return True
    def renew(self, item): pass
    def observe_rate_limit(self, *, endpoint, headers): pass
    def canonical_scope_is_complete(self, competition): return False
    def import_and_verify(self, *, scope, collected): raise AssertionError("cup must not import")
    def complete(self, item, checkpoint): self.completed.append(checkpoint["outcome"])
    def requeue(self, *args, **kwargs): raise AssertionError("cup must not requeue")
    def checkpoint_run(self, *args, **kwargs): raise AssertionError("cup must not pause")
    def finish_run(self, *args, **kwargs): pass


def test_worker_stages_catalogue_then_defers_cup_without_scope_calls(tmp_path: Path) -> None:
    catalogue = _response({"get": "leagues", "parameters": {}, "errors": {}, "results": 1, "paging": {"current": 1, "total": 1}, "response": [{"league": {"id": 2, "name": "Champions League", "type": "Cup"}, "seasons": [{"year": 2026, "current": True, "coverage": {"standings": True}}]}]})
    provider, repository = _Provider(catalogue), _Repository()
    settings = Settings("postgresql://unused", tmp_path / "spool", pacing_seconds=0)

    report: Report = asyncio.run(Worker(provider=provider, repository=repository, spool=RawSpool(settings.spool_dir), settings=settings).run_once())

    assert report.status == "succeeded"
    assert repository.completed == ["deferred_unsupported_type"]
    assert provider.calls == [("/leagues", {})]

    # A completed queue must not replay the catalogue capture on its next run.
    next_provider, next_repository = _Provider(catalogue), _Repository()
    asyncio.run(Worker(provider=next_provider, repository=next_repository, spool=RawSpool(settings.spool_dir), settings=settings).run_once())
    assert next_provider.calls == [("/leagues", {})]


def test_worker_replays_pending_catalogue_after_pre_queue_crash(tmp_path: Path) -> None:
    catalogue = _response({"get": "leagues", "parameters": {}, "errors": {}, "results": 1, "paging": {"current": 1, "total": 1}, "response": [{"league": {"id": 2, "name": "Cup", "type": "Cup"}, "seasons": []}]})
    spool = RawSpool(tmp_path / "spool")
    directory = spool.root / "catalogue" / "interrupted"
    now = datetime.now(UTC)
    spool.stage(directory, RawSpoolArtifact(BaseRequest("/leagues", {}), catalogue, now, now))
    spool.mark_catalogue_pending(directory)
    provider, repository = _Provider(catalogue), _Repository()
    settings = Settings("postgresql://unused", spool.root, pacing_seconds=0)

    asyncio.run(Worker(provider=provider, repository=repository, spool=spool, settings=settings).run_once())

    assert provider.calls == []


def test_worker_captures_real_epl_raw_then_calls_canonical_import(tmp_path: Path) -> None:
    sample = Path(__file__).parents[2] / "samples/api-football/epl-2026-refresh-2026-08-31T1225Z"
    scope_payloads = [json.loads((sample / name).read_text()) for name in (
        "01-leagues-epl-2026.raw.json", "02-teams-epl-2026.raw.json",
        "03-standings-epl-2026.raw.json", "04-fixtures-epl-2026.raw.json",
    )]
    catalogue = {"get": "leagues", "parameters": {}, "errors": {}, "results": 1, "paging": {"current": 1, "total": 1}, "response": [scope_payloads[0]["response"][0]]}

    @dataclass
    class Provider:
        responses: list[APIFootballResponse] = field(default_factory=lambda: [_response(catalogue), *[_response(value) for value in scope_payloads]])
        async def get(self, endpoint, *, params=None): return self.responses.pop(0)
        def response_contains_api_key(self, body): return False

    @dataclass
    class Repository:
        created: list[CatalogueCompetition] = field(default_factory=list); imported: int = 0; claimed: bool = False
        def active_run(self): return None
        def create_run(self, items, **kwargs): self.created = list(items); return 9
        def claim_next(self, run_id):
            if self.claimed: return None
            self.claimed = True
            from app.importer.catalogue_bootstrap import WorkItem
            return WorkItem(3, self.created[0], 1, {})
        def unfinished_delay_seconds(self, run_id): return None
        def reserve_request(self, daily_limit): return True
        def renew(self, item): pass
        def observe_rate_limit(self, **kwargs): pass
        def canonical_scope_is_complete(self, competition): return False
        def import_and_verify(self, *, scope, collected): self.imported = len(collected)
        def complete(self, item, checkpoint): pass
        def requeue(self, *args, **kwargs): raise AssertionError("real complete sample must not requeue")
        def checkpoint_run(self, *args, **kwargs): raise AssertionError("real complete sample must not pause")
        def finish_run(self, *args, **kwargs): pass

    repository = Repository(); settings = Settings("postgresql://unused", tmp_path / "spool", pacing_seconds=0)
    async def statistics_backfill(league_id: int, season_year: int) -> Any:
        assert (league_id, season_year) == (39, 2026)
        return type("StatisticsReport", (), {"stopped_reason": None, "errors": (), "fixtures_normalized": 1, "statistics_rows_written": 2, "api_requests": 1})()

    report = asyncio.run(Worker(provider=Provider(), repository=repository, spool=RawSpool(settings.spool_dir), settings=settings, statistics_backfill=statistics_backfill).run_once())

    assert report.status == "succeeded"
    assert repository.imported == 4
    assert not (settings.spool_dir / "run-9/league-39-season-2026/generation-1").exists()
