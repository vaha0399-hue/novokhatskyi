from __future__ import annotations

import asyncio
import copy
import json
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from app.api_football import APIFootballResponse
from app.importer.active_season import CupSeasonScope
from app.importer.cup_bootstrap import (
    CupBootstrapError,
    CupCompetition,
    CupRawFirstWorker,
    capture_allow_list,
    load_cup_allow_list,
    load_classified_cup_scopes,
)
from app.importer.raw_spool import RawSpool


def _response(payload: dict) -> APIFootballResponse:
    raw = json.dumps(payload, separators=(",", ":")).encode()
    return APIFootballResponse(payload, raw, 200, {})


def _classification(*records: dict) -> str:
    return json.dumps(list(records), separators=(",", ":"))


def _classified_record(league_id: int, season: int, *, statistics: bool, provider_type: str = "Cup") -> dict:
    return {"league_id": league_id, "name": "Cup One", "season": season, "type": provider_type, "coverage": {"fixtures": {"statistics_fixtures": statistics}}}


def test_allow_list_is_strict_id_tab_name(tmp_path: Path) -> None:
    path = tmp_path / "cups.txt"
    path.write_text("# reviewed selection\n  # source note\n2\tCup One\n9\tCup Nine\n", encoding="utf-8")

    assert [entry.league_external_id for entry in load_cup_allow_list(path)] == [2, 9]
    path.write_text("2 Cup One\n", encoding="utf-8")
    with pytest.raises(CupBootstrapError, match="ID<TAB>name"):
        load_cup_allow_list(path)


def test_classification_selects_exact_allow_listed_cup_scopes_and_coverage(tmp_path: Path) -> None:
    path = tmp_path / "cups.txt"; path.write_text("2\tReviewed name\n", encoding="utf-8")
    with_stats, without_stats = tmp_path / "with.json", tmp_path / "without.json"
    with_stats.write_text(_classification(_classified_record(2, 2024, statistics=True)), encoding="utf-8")
    without_stats.write_text(_classification(_classified_record(2, 2026, statistics=False)), encoding="utf-8")
    selected = load_classified_cup_scopes(allow_list_path=path, with_fixture_stats_path=with_stats, without_fixture_stats_path=without_stats)

    assert selected == (CupCompetition(2, "Cup One", 2024, True), CupCompetition(2, "Cup One", 2026, False))


def test_classification_refuses_selected_id_that_is_not_a_cup(tmp_path: Path) -> None:
    path = tmp_path / "cups.txt"; path.write_text("2\tCup One\n", encoding="utf-8")
    with_stats, without_stats = tmp_path / "with.json", tmp_path / "without.json"
    with_stats.write_text(_classification(_classified_record(2, 2026, statistics=True, provider_type="League")), encoding="utf-8")
    without_stats.write_text("[]", encoding="utf-8")
    with pytest.raises(CupBootstrapError, match="not a classified Cup"):
        load_classified_cup_scopes(allow_list_path=path, with_fixture_stats_path=with_stats, without_fixture_stats_path=without_stats)


def _base_payloads() -> dict[str, dict]:
    coverage = {"standings": True, "fixtures": {"statistics_fixtures": False, "lineups": False}, "injuries": False}
    league = {"get": "leagues", "parameters": {"id": "2", "season": "2026"}, "errors": {}, "results": 1, "paging": {"current": 1, "total": 1}, "response": [{"league": {"id": 2, "name": "Cup One", "type": "Cup", "logo": None}, "country": {"name": "World", "code": "WO", "flag": None}, "seasons": [{"year": 2026, "current": True, "start": "2026-08-01", "end": "2027-05-31", "coverage": coverage}]}]}
    team = lambda external_id, name: {"team": {"id": external_id, "name": name, "country": "World", "founded": None, "national": False, "code": None, "logo": None}, "venue": {"id": None, "name": None, "address": None, "city": None, "capacity": None, "surface": None, "image": None}}
    teams = {"get": "teams", "parameters": {"league": "2", "season": "2026"}, "errors": {}, "results": 2, "paging": {"current": 1, "total": 1}, "response": [team(10, "A"), team(11, "B")]}
    standings = {"get": "standings", "parameters": {"league": "2", "season": "2026"}, "errors": {}, "results": 0, "paging": {"current": 1, "total": 1}, "response": []}
    fixture = {"fixture": {"id": 100, "date": "2026-09-01T12:00:00+00:00", "timezone": "UTC", "referee": None, "venue": {"id": None, "name": None, "city": None}, "status": {"short": "NS"}}, "league": {"id": 2, "season": 2026, "round": "Final"}, "teams": {"home": {"id": 10}, "away": {"id": 11}}, "goals": {"home": None, "away": None}, "score": {period: {"home": None, "away": None} for period in ("halftime", "fulltime", "extratime", "penalty")}}
    fixtures = {"get": "fixtures", "parameters": {"league": "2", "season": "2026"}, "errors": {}, "results": 1, "paging": {"current": 1, "total": 1}, "response": [fixture]}
    return {"/leagues": league, "/teams": teams, "/standings": standings, "/fixtures": fixtures}


@dataclass
class _Provider:
    payloads: dict[str, dict]
    calls: list[tuple[str, dict | None]] = field(default_factory=list)

    async def get(self, endpoint: str, *, params=None) -> APIFootballResponse:
        self.calls.append((endpoint, dict(params) if params is not None else None))
        return _response(copy.deepcopy(self.payloads[endpoint]))


def test_worker_stages_and_validates_cup_base_without_a_database(tmp_path: Path) -> None:
    provider = _Provider(_base_payloads())
    worker = CupRawFirstWorker(provider=provider, spool=RawSpool(tmp_path / "spool"))

    report = asyncio.run(worker.capture_and_validate(run_id=7, competition=CupCompetition(2, "Cup One", 2026, False)))

    assert report.validated.scope == CupSeasonScope(2, 2026)
    assert len(report.validated.fixtures) == 1
    assert (report.capture_directory / "fixtures.raw.json").is_file()
    assert [call[0] for call in provider.calls] == ["/leagues", "/teams", "/standings", "/fixtures"]


def test_allow_list_capture_uses_classified_scope_without_global_catalogue_call(tmp_path: Path) -> None:
    path = tmp_path / "cups.txt"; path.write_text("2\tCup One\n", encoding="utf-8")
    with_stats, without_stats = tmp_path / "with.json", tmp_path / "without.json"
    with_stats.write_text("[]", encoding="utf-8")
    without_stats.write_text(_classification(_classified_record(2, 2026, statistics=False)), encoding="utf-8")
    payloads = _base_payloads()
    provider = _Provider(payloads)
    worker = CupRawFirstWorker(provider=provider, spool=RawSpool(tmp_path / "spool"))

    reports = asyncio.run(capture_allow_list(worker=worker, allow_list_path=path, with_fixture_stats_path=with_stats, without_fixture_stats_path=without_stats, run_id=7))

    assert [report.competition.league_external_id for report in reports] == [2]
    assert [call[0] for call in provider.calls] == ["/leagues", "/teams", "/standings", "/fixtures"]
