from __future__ import annotations

import copy
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from app.api_football import APIFootballResponse
from app.importer.season_bootstrap import (
    BaseRequest,
    BootstrapScope,
    CollectedBaseResponse,
    SeasonBootstrapError,
    base_requests,
    validate_base_responses,
)


SAMPLES = Path(__file__).parents[2] / "samples" / "api-football" / "pro-canary-2026-08-22"


def _sample(name: str) -> dict:
    return json.loads((SAMPLES / name).read_text())


def _response(payload: dict) -> APIFootballResponse:
    raw = json.dumps(payload, separators=(",", ":")).encode()
    return APIFootballResponse(payload, raw, 200, {})


def _collected() -> tuple[CollectedBaseResponse, ...]:
    scope = BootstrapScope(league_external_id=39, season_start_year=2025, expected_fixture_count=380)
    payloads = {
        "/leagues": _sample("01-leagues-epl-2025.raw.json"),
        "/teams": _sample("03-teams-epl-2025.raw.json"),
        "/standings": _sample("04-standings-epl-2025.raw.json"),
        "/fixtures": _sample("06-fixtures-epl-2025-completed.raw.json"),
    }
    # The generic season importer requests all fixtures for the season rather
    # than relying on a status filter.  The retained sample proves the same
    # completed 380-fixture contract, so make its request envelope match the
    # future controlled request without changing the response body.
    payloads["/fixtures"] = copy.deepcopy(payloads["/fixtures"])
    payloads["/fixtures"]["parameters"] = {"league": "39", "season": "2025"}
    now = datetime.now(UTC)
    return tuple(
        CollectedBaseResponse(
            request=request,
            response=_response(payloads[request.endpoint]),
            request_started_at=now - timedelta(seconds=1),
            response_received_at=now,
        )
        for request in base_requests(scope)
    )


def test_retained_epl_2025_contract_validates_as_a_completed_season_bootstrap() -> None:
    scope = BootstrapScope(league_external_id=39, season_start_year=2025, expected_fixture_count=380)

    validated = validate_base_responses(_collected(), scope=scope)

    assert validated.league.name == "Premier League"
    assert validated.league.country_external_code == "GB-ENG"
    assert validated.coverage.fixture_statistics_supported is True
    assert validated.coverage.lineups_supported is True
    assert len(validated.teams) == 20
    assert len(validated.fixtures) == len(validated.statuses) == 380
    assert {status.status_code for status in validated.statuses} == {"FT"}


def test_bootstrap_scope_has_a_distinct_per_season_lock() -> None:
    epl_2024 = BootstrapScope(league_external_id=39, season_start_year=2024, expected_fixture_count=380)
    epl_2025 = BootstrapScope(league_external_id=39, season_start_year=2025, expected_fixture_count=380)

    assert epl_2024.lock_key != epl_2025.lock_key
    assert epl_2025.season_scope.preexisting_canary_fixture_external_id is None


def test_team_country_contract_drift_stops_before_canonical_dml() -> None:
    scope = BootstrapScope(league_external_id=39, season_start_year=2025, expected_fixture_count=380)
    collected = list(_collected())
    teams = copy.deepcopy(collected[1].response.data)
    teams["response"][0]["team"]["country"] = "Wrong Country"
    collected[1] = CollectedBaseResponse(
        request=collected[1].request,
        response=_response(teams),
        request_started_at=collected[1].request_started_at,
        response_received_at=collected[1].response_received_at,
    )

    with pytest.raises(SeasonBootstrapError, match="team country"):
        validate_base_responses(collected, scope=scope)


def test_unreviewed_competition_type_stops_before_canonical_dml() -> None:
    scope = BootstrapScope(league_external_id=39, season_start_year=2025, expected_fixture_count=380)
    collected = list(_collected())
    leagues = copy.deepcopy(collected[0].response.data)
    leagues["response"][0]["league"]["type"] = "Cup"
    collected[0] = CollectedBaseResponse(
        request=collected[0].request,
        response=_response(leagues),
        request_started_at=collected[0].request_started_at,
        response_received_at=collected[0].response_received_at,
    )

    with pytest.raises(SeasonBootstrapError, match="unreviewed provider competition type"):
        validate_base_responses(collected, scope=scope)
