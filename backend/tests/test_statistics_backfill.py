import copy
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from app.importer.statistics_backfill import (
    DEFAULT_RUN_ATTEMPT_CAP,
    ENDPOINT,
    FixtureTarget,
    StatisticsContractError,
    _decimal,
    _integer,
    _params,
    classify_response,
    map_statistics_block,
    run_statistics_backfill,
)


SAMPLE = Path(__file__).parents[2] / "samples" / "api-football" / "fixture-statistics.raw.json"


@pytest.fixture
def target() -> FixtureTarget:
    kickoff = datetime(2024, 8, 16, 19, 0, tzinfo=UTC)
    return FixtureTarget(101, 1208021, 201, 202, kickoff, kickoff + timedelta(hours=3))


@pytest.fixture
def payload() -> dict:
    return json.loads(SAMPLE.read_text())


def test_real_sample_is_complete_and_preserves_typed_values(payload: dict, target: FixtureTarget) -> None:
    state, mapped = classify_response(payload, target)

    assert state == "complete"
    assert [row["external_team_id"] for row in mapped] == [33, 36]
    assert mapped[0]["possession_pct"] == Decimal("55")
    assert mapped[0]["expected_goals"] == Decimal("2.43")
    assert mapped[0]["red_cards"] is None


def test_request_contract_targets_exactly_one_fixture(target: FixtureTarget) -> None:
    assert ENDPOINT == "/fixtures/statistics"
    assert _params(target.external_id) == {"fixture": 1208021}


@pytest.mark.parametrize(
    ("value", "expected"),
    [(None, None), (0, Decimal("0")), (55, Decimal("55")), ("55", Decimal("55")), ("55%", Decimal("55"))],
)
def test_percentage_accepts_provider_number_string_or_null(value, expected) -> None:
    assert _decimal(value, percentage=True, scale=2, maximum=Decimal("100")) == expected


@pytest.mark.parametrize("value", [True, "100.001%", 100.001, "101%", "not-a-number", "-1%"])
def test_percentage_rejects_unsafe_values(value) -> None:
    with pytest.raises(StatisticsContractError):
        _decimal(value, percentage=True, scale=2, maximum=Decimal("100"))


@pytest.mark.parametrize(
    ("value", "expected"),
    [(None, None), (0, 0), (17, 17), ("17", 17)],
)
def test_integer_accepts_provider_integer_string_or_null(value, expected) -> None:
    assert _integer(value) == expected


@pytest.mark.parametrize("value", [True, -1, "-1", 1.5, "1.5"])
def test_integer_rejects_unsafe_values(value) -> None:
    with pytest.raises(StatisticsContractError):
        _integer(value)


def test_empty_and_partial_are_terminal_without_invented_zeroes(payload: dict, target: FixtureTarget) -> None:
    empty = {**payload, "results": 0, "response": []}
    partial = {**payload, "results": 1, "response": payload["response"][:1]}

    assert classify_response(empty, target) == ("empty", [])
    state, mapped = classify_response(partial, target)
    assert state == "partial"
    assert len(mapped) == 1
    assert mapped[0]["red_cards"] is None


def test_duplicate_team_is_contract_failure(payload: dict, target: FixtureTarget) -> None:
    payload["response"][1]["team"]["id"] = payload["response"][0]["team"]["id"]

    with pytest.raises(StatisticsContractError, match="duplicate statistics team"):
        classify_response(payload, target)


def test_duplicate_known_or_unknown_label_is_contract_failure(payload: dict) -> None:
    for label in ("Total Shots", "Future Metric"):
        block = copy.deepcopy(payload["response"][0])
        block["statistics"].extend(
            [{"type": label, "value": 1}, {"type": label, "value": 2}]
        )
        with pytest.raises(StatisticsContractError, match="duplicate"):
            map_statistics_block(block)


def test_unknown_metric_is_preserved_losslessly(payload: dict) -> None:
    block = copy.deepcopy(payload["response"][0])
    block["statistics"].append({"type": "Future Metric", "value": "alpha"})

    mapped = map_statistics_block(block)

    assert mapped["extra_metrics"] == {"Future Metric": "alpha"}


def test_goals_prevented_is_signed_but_expected_goals_remains_nonnegative(payload: dict) -> None:
    signed = copy.deepcopy(payload["response"][0])
    for statistic in signed["statistics"]:
        if statistic["type"] == "goals_prevented":
            statistic["value"] = "-0.30"
    assert map_statistics_block(signed)["goals_prevented"] == Decimal("-0.30")

    invalid_xg = copy.deepcopy(payload["response"][0])
    for statistic in invalid_xg["statistics"]:
        if statistic["type"] == "expected_goals":
            statistic["value"] = "-0.01"
    with pytest.raises(StatisticsContractError, match="outside permitted range"):
        map_statistics_block(invalid_xg)


def test_passes_accurate_cannot_exceed_total_passes(payload: dict) -> None:
    block = copy.deepcopy(payload["response"][0])
    for statistic in block["statistics"]:
        if statistic["type"] == "Total passes":
            statistic["value"] = 1
        elif statistic["type"] == "Passes accurate":
            statistic["value"] = 2

    with pytest.raises(StatisticsContractError, match="passes accurate"):
        map_statistics_block(block)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda body: body.update(parameters={"fixture": "999"}),
        lambda body: body.update(errors={"quota": "error"}),
        lambda body: body.update(results=3),
        lambda body: body.update(results=True, response=body["response"][:1]),
        lambda body: body.update(paging={"current": 1, "total": 2}),
        lambda body: body.update(paging={"current": True, "total": 1}),
        lambda body: body.update(paging={"current": 1, "total": True}),
        lambda body: body.update(response="not-an-array"),
    ],
)
def test_wrapper_schema_drift_fails_closed(payload: dict, target: FixtureTarget, mutation) -> None:
    mutation(payload)
    with pytest.raises(StatisticsContractError):
        classify_response(payload, target)


@pytest.mark.parametrize("max_calls", [0, DEFAULT_RUN_ATTEMPT_CAP + 1])
def test_run_attempt_cap_is_fail_closed_before_environment_access(max_calls: int) -> None:
    with pytest.raises(ValueError, match="between 1 and 90"):
        run_statistics_backfill(max_calls=max_calls)
