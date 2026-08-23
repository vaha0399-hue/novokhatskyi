import copy
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from app.importer.historical_lineups import (
    FixtureTarget,
    HistoricalLineupsContractError,
    LineupPlayer,
    ParsedLineups,
    TeamLineup,
    _expected_player_values,
    _first_batch_is_fully_complete,
    classify_response,
)

SAMPLE = Path(__file__).parents[2] / "samples" / "api-football" / "lineups.raw.json"


@pytest.fixture
def target() -> FixtureTarget:
    return FixtureTarget(
        fixture_id=1,
        season_id=1,
        external_id=1208021,
        home_team_id=101,
        away_team_id=102,
        home_external_id=33,
        away_external_id=36,
        kickoff_at=datetime(2024, 8, 16, tzinfo=UTC),
        result_finalized_at=datetime(2024, 8, 17, tzinfo=UTC),
    )


@pytest.fixture
def payload() -> dict:
    return json.loads(SAMPLE.read_text())


def test_sample_maps_complete_lineups_and_preserves_substitute_grid_null(payload: dict, target: FixtureTarget) -> None:
    parsed = classify_response(payload, target)

    assert parsed.coverage_state == "complete"
    assert [lineup.external_team_id for lineup in parsed.lineups] == [33, 36]
    assert [lineup.formation for lineup in parsed.lineups] == ["4-2-3-1", "4-2-3-1"]
    assert [sum(player.role == "starter" for player in lineup.players) for lineup in parsed.lineups] == [11, 11]
    assert [sum(player.role == "substitute" for player in lineup.players) for lineup in parsed.lineups] == [9, 9]
    assert all(player.grid is None for lineup in parsed.lineups for player in lineup.players if player.role == "substitute")
    assert len({player.external_id for lineup in parsed.lineups for player in lineup.players}) == 40


def test_empty_and_partial_are_explicit_without_fabricating_team_rows(payload: dict, target: FixtureTarget) -> None:
    empty = {**payload, "results": 0, "response": []}
    partial = {**payload, "results": 1, "response": payload["response"][:1]}

    assert classify_response(empty, target).coverage_state == "empty"
    parsed = classify_response(partial, target)
    assert parsed.coverage_state == "partial"
    assert len(parsed.lineups) == 1


@pytest.mark.parametrize(
    "mutate",
    [
        lambda body: body.update(get="fixtures/statistics"),
        lambda body: body.update(parameters={"fixture": "wrong"}),
        lambda body: body.update(errors={"provider": "error"}),
        lambda body: body.update(results=True),
        lambda body: body.update(paging={"current": 1, "total": 2}),
        lambda body: body.update(response="not-an-array"),
        lambda body: body.update(results=3, response=body["response"] + [copy.deepcopy(body["response"][0])]),
    ],
)
def test_wrapper_contract_drift_fails_closed(payload: dict, target: FixtureTarget, mutate) -> None:
    mutate(payload)
    with pytest.raises(HistoricalLineupsContractError):
        classify_response(payload, target)


def test_nonparticipant_duplicate_team_and_duplicate_player_fail_closed(payload: dict, target: FixtureTarget) -> None:
    nonparticipant = copy.deepcopy(payload)
    nonparticipant["response"][1]["team"]["id"] = 999
    with pytest.raises(HistoricalLineupsContractError, match="non-participant"):
        classify_response(nonparticipant, target)

    duplicate_team = copy.deepcopy(payload)
    duplicate_team["response"][1]["team"]["id"] = 33
    with pytest.raises(HistoricalLineupsContractError, match="duplicate teams"):
        classify_response(duplicate_team, target)

    duplicate_player = copy.deepcopy(payload)
    duplicate_player["response"][1]["startXI"][0]["player"]["id"] = duplicate_player["response"][0]["startXI"][0]["player"]["id"]
    with pytest.raises(HistoricalLineupsContractError, match="player more than once"):
        classify_response(duplicate_player, target)


def test_nullable_lineup_fields_remain_none(payload: dict, target: FixtureTarget) -> None:
    nullable = copy.deepcopy(payload)
    nullable["response"][0]["coach"] = None
    nullable["response"][0]["formation"] = None
    nullable["response"][0]["startXI"][0]["player"].update(number=None, pos=None, grid=None)

    parsed = classify_response(nullable, target)
    lineup = parsed.lineups[0]
    player = lineup.players[0]
    assert lineup.coach_external_id is None
    assert lineup.formation is None
    assert (player.shirt_number, player.position, player.grid) == (None, None, None)


def test_second_batch_gate_requires_five_complete_two_team_results() -> None:
    complete = {
        "fixtures": [
            {"coverage_state": "complete", "team_lineups": 2}
            for _ in range(5)
        ]
    }
    assert _first_batch_is_fully_complete(complete) is True

    for incomplete in (
        {"fixtures": complete["fixtures"][:4]},
        {"fixtures": [*complete["fixtures"][:4], {"coverage_state": "partial", "team_lineups": 1}]},
        {"fixtures": [*complete["fixtures"][:4], {"coverage_state": "complete", "team_lineups": 1}]},
    ):
        assert _first_batch_is_fully_complete(incomplete) is False


def test_verifier_player_values_are_numeric_and_order_independent() -> None:
    parsed = ParsedLineups(
        coverage_state="partial",
        lineups=(
            TeamLineup(
                external_team_id=33,
                coach_external_id=None,
                coach_name=None,
                coach_photo_url=None,
                formation=None,
                players=(
                    LineupPlayer(2, "Two", None, None, None, "starter"),
                    LineupPlayer(10, "Ten", None, None, None, "starter"),
                    LineupPlayer(100, "One Hundred", None, None, None, "starter"),
                ),
            ),
        ),
    )
    actual_in_database_order = frozenset({
        (33, 100, "starter", None, None, None),
        (33, 2, "starter", None, None, None),
        (33, 10, "starter", None, None, None),
    })

    assert actual_in_database_order == _expected_player_values(parsed)


@pytest.mark.parametrize(
    "path,value",
    [
        (("response", 0, "startXI"), "not-an-array"),
        (("response", 0, "startXI", 0, "player", "id"), "not-an-id"),
        (("response", 0, "startXI", 0, "player", "name"), ""),
        (("response", 0, "startXI", 0, "player", "number"), 200),
        (("response", 0, "coach", "id"), None),
    ],
)
def test_malformed_entity_fields_fail_closed(payload: dict, target: FixtureTarget, path, value) -> None:
    node = payload
    for part in path[:-1]:
        node = node[part]
    node[path[-1]] = value
    with pytest.raises(HistoricalLineupsContractError):
        classify_response(payload, target)
