import json
from decimal import Decimal
from pathlib import Path

import pytest

from app.importer.canary import (
    CANARY_REQUESTS,
    MAX_API_ATTEMPTS,
    map_fixture_statistics,
    parse_decimal,
    parse_integer,
    request_params_sha256,
)


SAMPLES = Path(__file__).parents[2] / "samples" / "api-football"


def test_canary_plan_is_exactly_six_requests() -> None:
    assert len(CANARY_REQUESTS) == MAX_API_ATTEMPTS == 6
    assert [(request.endpoint, request.params) for request in CANARY_REQUESTS] == [
        ("/fixtures", {"id": 1208021}),
        ("/teams", {"league": 39, "season": 2024}),
        ("/standings", {"league": 39, "season": 2024}),
        ("/fixtures/statistics", {"fixture": 1208021}),
        ("/injuries", {"fixture": 1208021}),
        ("/fixtures/lineups", {"fixture": 1208021}),
    ]


def test_request_hash_is_order_independent_and_fixed_length() -> None:
    first = request_params_sha256({"league": 39, "season": 2024})
    second = request_params_sha256({"season": 2024, "league": 39})
    assert first == second
    assert len(first) == 32


def test_nullable_and_numeric_parsers_do_not_coerce_null_to_zero() -> None:
    assert parse_integer(None) is None
    assert parse_decimal(None) is None
    assert parse_decimal("55%", percentage=True) == Decimal("55")
    assert parse_decimal("2.43") == Decimal("2.43")
    with pytest.raises(ValueError):
        parse_integer(-1)
    with pytest.raises(ValueError):
        parse_decimal("101%", percentage=True)


def test_real_fixture_statistics_mapping() -> None:
    payload = json.loads((SAMPLES / "fixture-statistics.raw.json").read_text())
    mapped = {row["external_team_id"]: row for row in map_fixture_statistics(payload["response"])}

    assert mapped[33]["possession_pct"] == Decimal("55")
    assert mapped[33]["pass_accuracy_pct"] == Decimal("85")
    assert mapped[33]["expected_goals"] == Decimal("2.43")
    assert mapped[33]["goals_prevented"] == Decimal("1.07")
    assert mapped[33]["red_cards"] is None

    assert mapped[36]["possession_pct"] == Decimal("45")
    assert mapped[36]["pass_accuracy_pct"] == Decimal("80")
    assert mapped[36]["expected_goals"] == Decimal("0.44")
    assert mapped[36]["red_cards"] is None
    assert mapped[33]["extra_metrics"] == {}
    assert mapped[36]["extra_metrics"] == {}
