from __future__ import annotations

import copy
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from app.api_football import APIFootballResponse
from app.importer.active_season import (
    ActiveSeasonImportError,
    ActiveSeasonScope,
    base_requests,
    load_replay_collected,
    validate_base_responses,
)
from app.importer.season_bootstrap import CollectedBaseResponse


SAMPLES = Path(__file__).parents[2] / "samples" / "api-football" / "pro-canary-2026-08-29"


def _response(payload: dict) -> APIFootballResponse:
    raw = json.dumps(payload, separators=(",", ":")).encode()
    return APIFootballResponse(payload, raw, 200, {})


def _stored_response(path: Path) -> APIFootballResponse:
    raw = path.read_bytes()
    return APIFootballResponse(json.loads(raw), raw, 200, {})


def _collected() -> tuple[CollectedBaseResponse, ...]:
    scope = ActiveSeasonScope(league_external_id=39, season_start_year=2026, expected_fixture_count=380)
    files = {
        "/leagues": "01-leagues-epl-2026.raw.json",
        "/teams": "02-teams-epl-2026.raw.json",
        "/standings": "04-standings-epl-2026.raw.json",
        "/fixtures": "03-fixtures-epl-2026.raw.json",
    }
    now = datetime.now(UTC)
    return tuple(
        CollectedBaseResponse(
            request=request,
            response=_stored_response(SAMPLES / files[request.endpoint]),
            request_started_at=now - timedelta(seconds=1),
            response_received_at=now,
        )
        for request in base_requests(scope)
    )


def test_real_epl_2026_sample_validates_as_mixed_active_season() -> None:
    scope = ActiveSeasonScope(league_external_id=39, season_start_year=2026, expected_fixture_count=380)

    validated = validate_base_responses(_collected(), scope=scope)

    assert validated.league.name == "Premier League"
    assert len(validated.teams) == 20
    assert len(validated.fixtures) == len(validated.statuses) == 380
    assert {item.status_code for item in validated.fixtures} == {"NS", "FT"}
    assert sum(item.status_code == "FT" for item in validated.fixtures) == 11


def test_active_season_rejects_unapproved_in_progress_status_before_dml() -> None:
    scope = ActiveSeasonScope(league_external_id=39, season_start_year=2026, expected_fixture_count=380)
    collected = list(_collected())
    fixtures = copy.deepcopy(collected[3].response.data)
    fixture = next(item for item in fixtures["response"] if item["fixture"]["status"]["short"] == "NS")
    fixture["fixture"]["status"]["short"] = "1H"
    collected[3] = CollectedBaseResponse(
        request=collected[3].request,
        response=_response(fixtures),
        request_started_at=collected[3].request_started_at,
        response_received_at=collected[3].response_received_at,
    )

    with pytest.raises(ActiveSeasonImportError, match="NS or FT"):
        validate_base_responses(collected, scope=scope)


def test_active_season_rejects_ns_with_a_result_before_dml() -> None:
    scope = ActiveSeasonScope(league_external_id=39, season_start_year=2026, expected_fixture_count=380)
    collected = list(_collected())
    fixtures = copy.deepcopy(collected[3].response.data)
    fixture = next(item for item in fixtures["response"] if item["fixture"]["status"]["short"] == "NS")
    fixture["goals"]["home"] = 1
    collected[3] = CollectedBaseResponse(
        request=collected[3].request,
        response=_response(fixtures),
        request_started_at=collected[3].request_started_at,
        response_received_at=collected[3].response_received_at,
    )

    with pytest.raises(ActiveSeasonImportError, match="must not contain results"):
        validate_base_responses(collected, scope=scope)


def test_saved_canary_replay_artifacts_match_the_requested_scope() -> None:
    scope = ActiveSeasonScope(league_external_id=39, season_start_year=2026, expected_fixture_count=380)

    collected = load_replay_collected(SAMPLES, scope=scope)

    assert [item.request.endpoint for item in collected] == [
        "/leagues", "/teams", "/standings", "/fixtures"
    ]
    assert validate_base_responses(collected, scope=scope).scope == scope


def test_replay_rejects_request_parameters_that_do_not_match_scope(tmp_path: Path) -> None:
    scope = ActiveSeasonScope(league_external_id=39, season_start_year=2026, expected_fixture_count=380)
    for artifact in SAMPLES.glob("0[1-4]-*.request.json"):
        (tmp_path / artifact.name).write_bytes(artifact.read_bytes())
        raw = artifact.with_name(artifact.name.removesuffix(".request.json") + ".raw.json")
        (tmp_path / raw.name).write_bytes(raw.read_bytes())
    request_file = next(tmp_path.glob("*fixtures*.request.json"))
    request = json.loads(request_file.read_text())
    request["parameters"]["season"] = 2025
    request_file.write_text(json.dumps(request))

    with pytest.raises(ActiveSeasonImportError, match="parameters do not match scope"):
        load_replay_collected(tmp_path, scope=scope)
