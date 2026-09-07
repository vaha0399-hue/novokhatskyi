from __future__ import annotations

import copy
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from app.api_football import APIFootballResponse
from app.importer.active_season import (
    ActiveFixtureOverride,
    ActiveSeasonImportError,
    ActiveSeasonScope,
    CupSeasonScope,
    ENGLISH_CHAMPIONSHIP_2026_ADDITIONAL_TEAM_COUNTRIES,
    ENGLISH_LEAGUE_TWO_2026_ADDITIONAL_TEAM_COUNTRIES,
    base_requests,
    cup_base_requests,
    load_replay_collected,
    validate_base_responses,
    validate_cup_base_responses,
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


def _cup_collected(*, standings: str) -> tuple[CollectedBaseResponse, ...]:
    """Turn retained league raw into a small, structurally valid cup sample."""
    scope = CupSeasonScope(league_external_id=39, season_start_year=2026)
    responses = list(_collected())
    league = copy.deepcopy(responses[0].response.data)
    league["response"][0]["league"]["type"] = "Cup"
    fixtures = copy.deepcopy(responses[3].response.data)
    fixtures["response"] = fixtures["response"][:2]
    fixtures["results"] = len(fixtures["response"])
    if standings == "none":
        standings_payload = copy.deepcopy(responses[2].response.data)
        standings_payload["response"] = []
        standings_payload["results"] = 0
    elif standings == "groups":
        standings_payload = copy.deepcopy(responses[2].response.data)
        rows = standings_payload["response"][0]["league"]["standings"][0]
        groups = [rows[:3], rows[3:6]]
        for group_name, group_rows in zip(("Group A", "Group B"), groups, strict=True):
            for rank, row in enumerate(group_rows, start=1):
                row["group"] = group_name
                row["rank"] = rank
        standings_payload["response"][0]["league"]["standings"] = groups
    else:
        raise AssertionError(f"unknown cup standings shape: {standings}")
    replacements = (league, responses[1].response.data, standings_payload, fixtures)
    now = datetime.now(UTC)
    return tuple(
        CollectedBaseResponse(
            request=request,
            response=_response(payload),
            request_started_at=now - timedelta(seconds=1),
            response_received_at=now,
        )
        for request, payload in zip(cup_base_requests(scope), replacements, strict=True)
    )


def test_cup_scope_has_no_round_robin_fixture_count_and_a_distinct_lock() -> None:
    cup = CupSeasonScope(league_external_id=2, season_start_year=2026)

    assert cup.fixture_request_params == {"league": 2, "season": 2026}
    assert cup.lock_key == "api-football:cup-season:2:2026:v1"
    assert not hasattr(cup, "expected_fixture_count")


def test_cup_validation_accepts_arbitrary_past_and_future_fixtures_with_multiple_groups() -> None:
    scope = CupSeasonScope(league_external_id=39, season_start_year=2026)
    collected = list(_cup_collected(standings="groups"))
    fixtures = copy.deepcopy(collected[3].response.data)
    fixtures["response"][0]["fixture"]["status"]["short"] = "FT"
    fixtures["response"][0]["goals"] = {"home": 2, "away": 1}
    for period in ("halftime", "fulltime", "extratime", "penalty"):
        fixtures["response"][0]["score"][period] = {"home": None, "away": None}
    fixtures["response"][1]["fixture"]["status"]["short"] = "NS"
    fixtures["response"][1]["goals"] = {"home": None, "away": None}
    for period in ("halftime", "fulltime", "extratime", "penalty"):
        fixtures["response"][1]["score"][period] = {"home": None, "away": None}
    collected[3] = CollectedBaseResponse(
        request=collected[3].request,
        response=_response(fixtures),
        request_started_at=collected[3].request_started_at,
        response_received_at=collected[3].response_received_at,
    )

    validated = validate_cup_base_responses(collected, scope=scope)

    assert validated.league.competition_type == "cup"
    assert [item.status_code for item in validated.fixtures] == [
        fixtures["response"][0]["fixture"]["status"]["short"],
        fixtures["response"][1]["fixture"]["status"]["short"],
    ]
    assert [len(group) for group in validated.standings_payload["response"][0]["league"]["standings"]] == [3, 3]


def test_cup_validation_accepts_knockout_cup_without_standings() -> None:
    scope = CupSeasonScope(league_external_id=39, season_start_year=2026)

    validated = validate_cup_base_responses(_cup_collected(standings="none"), scope=scope)

    assert validated.standings_payload["response"] == []
    assert len(validated.fixtures) == 2


def test_cup_validation_accepts_api_football_world_country_without_a_country_code() -> None:
    scope = CupSeasonScope(league_external_id=39, season_start_year=2026)
    collected = list(_cup_collected(standings="none"))
    league = copy.deepcopy(collected[0].response.data)
    league["response"][0]["country"]["code"] = None
    collected[0] = CollectedBaseResponse(
        request=collected[0].request,
        response=_response(league),
        request_started_at=collected[0].request_started_at,
        response_received_at=collected[0].response_received_at,
    )

    validated = validate_cup_base_responses(tuple(collected), scope=scope)

    assert validated.league.country_external_code == "cup-country:england"


def test_real_epl_2026_sample_validates_as_mixed_active_season() -> None:
    scope = ActiveSeasonScope(league_external_id=39, season_start_year=2026, expected_fixture_count=380)

    validated = validate_base_responses(_collected(), scope=scope)

    assert validated.league.name == "Premier League"
    assert len(validated.teams) == 20
    assert len(validated.fixtures) == len(validated.statuses) == 380
    assert {item.status_code for item in validated.fixtures} == {"NS", "FT"}
    assert sum(item.status_code == "FT" for item in validated.fixtures) == 11


def test_active_season_accepts_a_partial_calendar_only_when_explicitly_requested() -> None:
    collected = list(_collected())
    fixtures = copy.deepcopy(collected[3].response.data)
    fixtures["response"] = fixtures["response"][:-1]
    fixtures["results"] = len(fixtures["response"])
    collected[3] = CollectedBaseResponse(
        request=collected[3].request,
        response=_response(fixtures),
        request_started_at=collected[3].request_started_at,
        response_received_at=collected[3].response_received_at,
    )
    partial_scope = ActiveSeasonScope(
        league_external_id=39,
        season_start_year=2026,
        expected_fixture_count=380,
        require_complete_schedule=False,
    )

    validated = validate_base_responses(collected, scope=partial_scope)

    assert len(validated.fixtures) == 379
    with pytest.raises(ActiveSeasonImportError, match="expected complete schedule"):
        validate_base_responses(
            collected,
            scope=ActiveSeasonScope(league_external_id=39, season_start_year=2026, expected_fixture_count=380),
        )


def test_active_season_accepts_penalty_terminal_fixture() -> None:
    collected = list(_collected())
    fixtures = copy.deepcopy(collected[3].response.data)
    fixture = next(item for item in fixtures["response"] if item["fixture"]["status"]["short"] == "FT")
    fixture["fixture"]["status"]["short"] = "PEN"
    collected[3] = CollectedBaseResponse(
        request=collected[3].request,
        response=_response(fixtures),
        request_started_at=collected[3].request_started_at,
        response_received_at=collected[3].response_received_at,
    )

    validated = validate_base_responses(
        collected,
        scope=ActiveSeasonScope(league_external_id=39, season_start_year=2026, expected_fixture_count=380),
    )

    assert any(record.status_code == "PEN" for record in validated.fixtures)


def test_active_season_allows_an_explicitly_unavailable_standings_snapshot() -> None:
    scope = ActiveSeasonScope(
        league_external_id=39,
        season_start_year=2026,
        expected_fixture_count=380,
        allow_empty_standings=True,
    )
    collected = list(_collected())
    standings = copy.deepcopy(collected[2].response.data)
    standings["response"] = []
    standings["results"] = 0
    collected[2] = CollectedBaseResponse(
        request=collected[2].request,
        response=_response(standings),
        request_started_at=collected[2].request_started_at,
        response_received_at=collected[2].response_received_at,
    )

    validated = validate_base_responses(collected, scope=scope)

    assert validated.standings_payload["response"] == []


def test_active_season_accepts_in_progress_status_without_promoting_its_score_to_result() -> None:
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

    validated = validate_base_responses(collected, scope=scope)

    record = next(item for item in validated.fixtures if item.external_id == fixture["fixture"]["id"])
    assert record.status_code == "1H"
    assert (record.home_goals, record.away_goals) == (None, None)


def test_active_season_accepts_repeated_directed_fixture_pairs_by_provider_fixture_id() -> None:
    collected = list(_collected())
    fixtures = copy.deepcopy(collected[3].response.data)
    original, repeated = fixtures["response"][:2]
    repeated["teams"]["home"]["id"] = original["teams"]["home"]["id"]
    repeated["teams"]["away"]["id"] = original["teams"]["away"]["id"]
    collected[3] = CollectedBaseResponse(
        request=collected[3].request,
        response=_response(fixtures),
        request_started_at=collected[3].request_started_at,
        response_received_at=collected[3].response_received_at,
    )

    validated = validate_base_responses(
        collected,
        scope=ActiveSeasonScope(league_external_id=39, season_start_year=2026, expected_fixture_count=380),
    )

    assert {original["fixture"]["id"], repeated["fixture"]["id"]}.issubset(
        {record.external_id for record in validated.fixtures}
    )


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


def test_active_season_accepts_a_postponed_fixture_without_results() -> None:
    scope = ActiveSeasonScope(league_external_id=39, season_start_year=2026, expected_fixture_count=380)
    collected = list(_collected())
    fixtures = copy.deepcopy(collected[3].response.data)
    fixture = next(item for item in fixtures["response"] if item["fixture"]["status"]["short"] == "NS")
    fixture["fixture"]["status"]["short"] = "PST"
    fixture_id = fixture["fixture"]["id"]
    collected[3] = CollectedBaseResponse(
        request=collected[3].request,
        response=_response(fixtures),
        request_started_at=collected[3].request_started_at,
        response_received_at=collected[3].response_received_at,
    )

    validated = validate_base_responses(collected, scope=scope)

    record = next(item for item in validated.fixtures if item.external_id == fixture_id)
    assert record.status_code == "PST"
    assert (record.home_goals, record.away_goals) == (None, None)


def test_active_season_accepts_a_postponed_fixture_without_a_kickoff() -> None:
    scope = ActiveSeasonScope(league_external_id=39, season_start_year=2026, expected_fixture_count=380)
    collected = list(_collected())
    fixtures = copy.deepcopy(collected[3].response.data)
    fixture = next(item for item in fixtures["response"] if item["fixture"]["status"]["short"] == "NS")
    fixture["fixture"].update({"status": {"short": "PST"}, "date": None, "timezone": None})
    fixture_id = fixture["fixture"]["id"]
    collected[3] = CollectedBaseResponse(
        request=collected[3].request,
        response=_response(fixtures),
        request_started_at=collected[3].request_started_at,
        response_received_at=collected[3].response_received_at,
    )

    validated = validate_base_responses(collected, scope=scope)

    record = next(item for item in validated.fixtures if item.external_id == fixture_id)
    assert (record.status_code, record.kickoff_at, record.source_timezone) == ("PST", None, None)


def test_active_season_treats_provider_zero_venue_id_as_unmapped() -> None:
    scope = ActiveSeasonScope(league_external_id=39, season_start_year=2026, expected_fixture_count=380)
    collected = list(_collected())
    fixtures = copy.deepcopy(collected[3].response.data)
    fixture = fixtures["response"][0]
    fixture["fixture"]["venue"]["id"] = 0
    external_fixture_id = fixture["fixture"]["id"]
    collected[3] = CollectedBaseResponse(
        request=collected[3].request,
        response=_response(fixtures),
        request_started_at=collected[3].request_started_at,
        response_received_at=collected[3].response_received_at,
    )

    validated = validate_base_responses(collected, scope=scope)

    record = next(item for item in validated.fixtures if item.external_id == external_fixture_id)
    assert record.venue_external_id is None


def test_active_season_applies_a_reviewed_fixture_override_before_schedule_validation() -> None:
    collected = list(_collected())
    fixtures = copy.deepcopy(collected[3].response.data)
    target = fixtures["response"][0]
    duplicate = fixtures["response"][1]
    original_home_id = target["teams"]["home"]["id"]
    original_away_id = target["teams"]["away"]["id"]
    original_venue_id = target["fixture"]["venue"]["id"]
    target["teams"]["home"]["id"] = duplicate["teams"]["home"]["id"]
    target["teams"]["away"]["id"] = duplicate["teams"]["away"]["id"]
    target["fixture"]["venue"]["id"] = 999_999
    override = ActiveFixtureOverride(
        external_fixture_id=target["fixture"]["id"],
        expected_home_external_id=duplicate["teams"]["home"]["id"],
        expected_away_external_id=duplicate["teams"]["away"]["id"],
        expected_venue_external_id=999_999,
        expected_round_label=target["league"]["round"],
        expected_status_code=target["fixture"]["status"]["short"],
        canonical_home_external_id=original_home_id,
        canonical_away_external_id=original_away_id,
        canonical_venue_external_id=original_venue_id,
        reason="test fixture contract",
    )
    scope = ActiveSeasonScope(
        league_external_id=39,
        season_start_year=2026,
        expected_fixture_count=380,
        fixture_overrides=(override,),
    )
    collected[3] = CollectedBaseResponse(
        request=collected[3].request,
        response=_response(fixtures),
        request_started_at=collected[3].request_started_at,
        response_received_at=collected[3].response_received_at,
    )

    validated = validate_base_responses(collected, scope=scope)

    record = next(item for item in validated.fixtures if item.external_id == override.external_fixture_id)
    assert (record.home_external_id, record.away_external_id, record.venue_external_id) == (
        original_home_id,
        original_away_id,
        original_venue_id,
    )


def test_active_season_rejects_an_override_when_its_source_contract_changes() -> None:
    collected = _collected()
    fixture = collected[3].response.data["response"][0]
    override = ActiveFixtureOverride(
        external_fixture_id=fixture["fixture"]["id"],
        expected_home_external_id=999_999,
        expected_away_external_id=fixture["teams"]["away"]["id"],
        expected_venue_external_id=fixture["fixture"]["venue"]["id"],
        expected_round_label=fixture["league"]["round"],
        expected_status_code=fixture["fixture"]["status"]["short"],
        canonical_home_external_id=fixture["teams"]["home"]["id"],
        canonical_away_external_id=fixture["teams"]["away"]["id"],
        canonical_venue_external_id=fixture["fixture"]["venue"]["id"],
        reason="test stale contract",
    )
    scope = ActiveSeasonScope(
        league_external_id=39,
        season_start_year=2026,
        expected_fixture_count=380,
        fixture_overrides=(override,),
    )

    with pytest.raises(ActiveSeasonImportError, match="source contract changed"):
        validate_base_responses(collected, scope=scope)


def test_active_season_allows_an_explicit_additional_team_country() -> None:
    collected = list(_collected())
    teams = copy.deepcopy(collected[1].response.data)
    teams["response"][0]["team"]["country"] = "Wales"
    scope = ActiveSeasonScope(
        league_external_id=39,
        season_start_year=2026,
        expected_fixture_count=380,
        additional_team_country_names=frozenset({"Wales"}),
    )
    collected[1] = CollectedBaseResponse(
        request=collected[1].request,
        response=_response(teams),
        request_started_at=collected[1].request_started_at,
        response_received_at=collected[1].response_received_at,
    )

    validated = validate_base_responses(collected, scope=scope)

    assert len(validated.teams) == 20


def test_english_wales_policies_are_explicit_per_competition() -> None:
    assert ENGLISH_CHAMPIONSHIP_2026_ADDITIONAL_TEAM_COUNTRIES == frozenset({"Wales"})
    assert ENGLISH_LEAGUE_TWO_2026_ADDITIONAL_TEAM_COUNTRIES == frozenset({"Wales"})


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
