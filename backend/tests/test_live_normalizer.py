from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from app.live import (
    LiveFixtureStatus,
    LiveNormalizationError,
    LiveScore,
    normalize_live_fixture,
    normalize_live_response,
)


SAMPLE = (
    Path(__file__).parents[2]
    / "samples"
    / "api-football"
    / "live-fixtures-2026-08-29T0646Z"
    / "fixtures-live-all.raw.json"
)


def _entry(status: str = "1H") -> dict:
    return {
        "fixture": {
            "id": 1557383,
            "status": {"short": status, "long": "status", "elapsed": 45, "extra": 2},
        },
        "league": {"id": 39, "season": 2026},
        "teams": {"home": {"id": 40}, "away": {"id": 65}},
        "goals": {"home": 2, "away": 1},
        "score": {"fulltime": {"home": 99, "away": 98}},
    }


@pytest.mark.parametrize(
    "provider_status,expected",
    [
        ("1H", LiveFixtureStatus.FIRST_HALF),
        ("HT", LiveFixtureStatus.HALF_TIME),
        ("2H", LiveFixtureStatus.SECOND_HALF),
        ("FT", LiveFixtureStatus.FINISHED),
    ],
)
def test_statuses_normalize_to_stable_domain_states(
    provider_status: str, expected: LiveFixtureStatus
) -> None:
    fixture = normalize_live_fixture(_entry(provider_status))

    assert fixture.status is expected
    assert fixture.status.is_terminal is (provider_status == "FT")


def test_current_score_and_time_use_only_live_fields() -> None:
    fixture = normalize_live_fixture(_entry("2H"))

    assert (fixture.score.home, fixture.score.away) == (2, 1)
    assert fixture.elapsed_minute == 45
    assert fixture.added_time == 2


@pytest.mark.parametrize("invalid", [-1, True, "1"])
def test_live_score_rejects_non_integer_or_negative_values(invalid: object) -> None:
    with pytest.raises(ValueError, match="non-negative integers"):
        LiveScore(home=invalid, away=0)  # type: ignore[arg-type]


def test_real_live_sample_normalizes_every_fixture_without_projection() -> None:
    payload = json.loads(SAMPLE.read_bytes())

    fixtures = normalize_live_response(payload)

    assert len(fixtures) == payload["results"] == 13
    assert {fixture.status for fixture in fixtures} == {
        LiveFixtureStatus.FIRST_HALF,
        LiveFixtureStatus.HALF_TIME,
        LiveFixtureStatus.SECOND_HALF,
    }
    assert sorted(fixture.added_time for fixture in fixtures if fixture.added_time is not None) == [1, 2]
    expected_scores = {
        item["fixture"]["id"]: (item["goals"]["home"], item["goals"]["away"])
        for item in payload["response"]
    }
    assert {
        fixture.external_fixture_id: (fixture.score.home, fixture.score.away)
        for fixture in fixtures
    } == expected_scores


def test_expected_league_scope_fails_closed() -> None:
    payload = json.loads(SAMPLE.read_bytes())

    with pytest.raises(LiveNormalizationError, match="escaped the requested league scope"):
        normalize_live_response(payload, expected_league_ids={39})


@pytest.mark.parametrize(
    "mutation,error",
    [
        (("fixture", "status", "short", "LIVE"), "unsupported live fixture status"),
        (("goals", "home", None, -1), "goals.home"),
        (("fixture", "status", "elapsed", True), "fixture.status.elapsed"),
        (("teams", "away", "id", 40), "teams must be distinct"),
    ],
)
def test_malformed_live_values_are_rejected(
    mutation: tuple[str, str, str | None, object], error: str
) -> None:
    payload = deepcopy(_entry())
    first, second, third, value = mutation
    if third is None:
        payload[first][second] = value
    else:
        payload[first][second][third] = value

    with pytest.raises(LiveNormalizationError, match=error):
        normalize_live_fixture(payload)


def test_duplicate_provider_fixture_ids_are_rejected() -> None:
    payload = {
        "get": "fixtures",
        "errors": [],
        "results": 2,
        "paging": {"current": 1, "total": 1},
        "response": [_entry(), _entry("2H")],
    }

    with pytest.raises(LiveNormalizationError, match="duplicate fixture IDs"):
        normalize_live_response(payload)
