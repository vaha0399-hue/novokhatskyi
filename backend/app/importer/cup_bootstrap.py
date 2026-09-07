"""Raw-first capture and validation for explicitly classified cup scopes.

This module intentionally has no PostgreSQL implementation.  Cup validation
accepts knockout, group-stage, and no-standings formats, while the existing
canonical writer is explicitly regular-league-only.  A caller supplies the
``CupBaseSink`` that owns the eventual transactional canonical write.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from app.api_football import APIFootballResponse
from app.importer.active_season import (
    ActiveSeasonImportError,
    CupSeasonScope,
    ValidatedCupBase,
    cup_base_requests,
    validate_cup_base_responses,
)
from app.importer.raw_spool import RawSpool, RawSpoolArtifact, RawSpoolError
from app.importer.season_bootstrap import BaseRequest, CollectedBaseResponse, SeasonBootstrapError


class CupBootstrapError(RuntimeError):
    """Cup catalogue, allow-list, or raw capture contract is unsafe."""


@dataclass(frozen=True)
class CupAllowListEntry:
    league_external_id: int
    name: str


@dataclass(frozen=True)
class CupCompetition:
    league_external_id: int
    name: str
    season_start_year: int
    fixture_statistics_coverage: bool

    @property
    def scope(self) -> CupSeasonScope:
        return CupSeasonScope(self.league_external_id, self.season_start_year)


@dataclass(frozen=True)
class CupCaptureReport:
    competition: CupCompetition
    capture_directory: Path
    validated: ValidatedCupBase


class Provider(Protocol):
    async def get(
        self, endpoint: str, *, params: Mapping[str, str | int] | None = None
    ) -> APIFootballResponse: ...


class CupBaseSink(Protocol):
    """Transactional hand-off boundary for a future cup canonical writer."""

    def write_cup_base(
        self, *, validated: ValidatedCupBase, collected: Sequence[CollectedBaseResponse]
    ) -> None: ...


def load_cup_allow_list(path: Path) -> tuple[CupAllowListEntry, ...]:
    """Load the reviewed ``ID<TAB>name`` cup selection without guessing IDs."""
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise CupBootstrapError(f"cannot read cup allow-list: {path}") from error
    entries: list[CupAllowListEntry] = []
    seen: set[int] = set()
    for line_number, line in enumerate(lines, start=1):
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        external_id, separator, name = line.partition("\t")
        if not separator or not name.strip():
            raise CupBootstrapError(f"allow-list line {line_number} must be ID<TAB>name")
        try:
            league_external_id = int(external_id)
        except ValueError as error:
            raise CupBootstrapError(f"allow-list line {line_number} has an invalid ID") from error
        if league_external_id <= 0 or league_external_id in seen:
            raise CupBootstrapError(f"allow-list line {line_number} has a duplicate or non-positive ID")
        seen.add(league_external_id)
        entries.append(CupAllowListEntry(league_external_id, name.strip()))
    if not entries:
        raise CupBootstrapError("cup allow-list is empty")
    return tuple(entries)


def _load_classification_records(path: Path) -> list[Mapping[str, object]]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CupBootstrapError(f"cannot read cup classification: {path}") from error
    if not isinstance(value, list) or not all(isinstance(item, Mapping) for item in value):
        raise CupBootstrapError("cup classification must be a JSON array of objects")
    return list(value)


def load_classified_cup_scopes(
    *, allow_list_path: Path, with_fixture_stats_path: Path, without_fixture_stats_path: Path
) -> tuple[CupCompetition, ...]:
    """Derive exact ``(cup ID, season)`` scope from immutable classifications.

    The input arrays are the catalogue classification artifacts, not a live
    ``current=True`` catalogue query.  This preserves multiple selected seasons
    for one Cup ID (for example 709/2024 and 709/2026).
    """
    wanted = {entry.league_external_id for entry in load_cup_allow_list(allow_list_path)}
    selected: dict[tuple[int, int], CupCompetition] = {}
    for path, expected_statistics in (
        (with_fixture_stats_path, True), (without_fixture_stats_path, False)
    ):
        for record in _load_classification_records(path):
            league_id, name, season, provider_type = (
                record.get("league_id"), record.get("name"), record.get("season"), record.get("type")
            )
            if league_id not in wanted:
                continue
            if (
                not isinstance(league_id, int) or league_id <= 0
                or not isinstance(name, str) or not name.strip()
                or not isinstance(season, int) or season <= 0
            ):
                raise CupBootstrapError("selected cup classification has invalid identity")
            if provider_type != "Cup":
                raise CupBootstrapError(f"selected competition {league_id} is not a classified Cup")
            coverage = record.get("coverage")
            fixtures_coverage = coverage.get("fixtures") if isinstance(coverage, Mapping) else None
            actual_statistics = fixtures_coverage.get("statistics_fixtures") if isinstance(fixtures_coverage, Mapping) else None
            if actual_statistics is not expected_statistics:
                raise CupBootstrapError("classification statistics coverage conflicts with its source array")
            key = (league_id, season)
            if key in selected:
                raise CupBootstrapError(f"selected cup scope {league_id}/{season} is duplicated")
            selected[key] = CupCompetition(league_id, name.strip(), season, expected_statistics)
    missing = sorted(wanted - {competition.league_external_id for competition in selected.values()})
    if missing:
        raise CupBootstrapError(f"selected cup IDs are absent from classification: {','.join(map(str, missing))}")
    return tuple(selected[key] for key in sorted(selected))


class CupRawFirstWorker:
    """Capture four cup endpoints, validate them, and hand them to an injected sink.

    The worker stages every successful source response before validation.  It
    deliberately does not purge captures: until a cup-specific canonical
    writer atomically retains provenance, raw source files remain the safe
    recovery point.
    """

    def __init__(
        self, *, provider: Provider, spool: RawSpool, sink: CupBaseSink | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._provider = provider
        self._spool = spool
        self._sink = sink
        self._clock = clock

    async def _capture(self, directory: Path, request: BaseRequest) -> CollectedBaseResponse:
        try:
            cached = self._spool.load(directory, request)
        except RawSpoolError:
            if not self._spool.discard_partial(directory, request):
                raise
            cached = None
        if cached is None:
            started = self._clock()
            response = await self._provider.get(request.endpoint, params=request.params)
            received = self._clock()
            artifact = RawSpoolArtifact(request, response, started, received)
            self._spool.stage(directory, artifact)
        else:
            artifact = cached
        return CollectedBaseResponse(
            artifact.request, artifact.response, artifact.request_started_at, artifact.response_received_at
        )

    async def capture_and_validate(
        self, *, run_id: int, competition: CupCompetition, generation: int = 1
    ) -> CupCaptureReport:
        scope = competition.scope
        directory = self._spool.capture_directory(
            run_id=run_id, league_external_id=scope.league_external_id,
            season_start_year=scope.season_start_year, generation=generation,
        )
        collected = tuple([
            await self._capture(directory, request) for request in cup_base_requests(scope)
        ])
        try:
            validated = validate_cup_base_responses(collected, scope=scope)
        except (ActiveSeasonImportError, SeasonBootstrapError) as error:
            raise CupBootstrapError("captured cup base responses failed validation") from error
        if self._sink is not None:
            self._sink.write_cup_base(validated=validated, collected=collected)
        return CupCaptureReport(competition, directory, validated)


async def capture_allow_list(
    *, worker: CupRawFirstWorker, allow_list_path: Path, with_fixture_stats_path: Path,
    without_fixture_stats_path: Path, run_id: int
) -> tuple[CupCaptureReport, ...]:
    """Capture exactly the immutable classified Cup scopes in the allow-list."""
    competitions = load_classified_cup_scopes(
        allow_list_path=allow_list_path,
        with_fixture_stats_path=with_fixture_stats_path,
        without_fixture_stats_path=without_fixture_stats_path,
    )
    reports: list[CupCaptureReport] = []
    for competition in competitions:
        reports.append(await worker.capture_and_validate(run_id=run_id, competition=competition))
    return tuple(reports)
