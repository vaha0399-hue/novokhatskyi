import copy
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from app.importer.current_season_statistics import (
    MAX_FIXTURES_PER_REQUEST,
    CurrentSeasonStatisticsScope,
    FixtureTarget,
    _batch_entries,
    _completed_discovery_entries,
    chunk_fixture_targets,
    fixture_ids_parameter,
    select_recent_history,
)
from app.importer.statistics_backfill import StatisticsContractError


SAMPLE = Path(__file__).parents[2] / "samples" / "api-football" / "pro-canary-2026-08-22" / "08-fixtures-batch-three.raw.json"
DISCOVERY_SAMPLE = Path(__file__).parents[2] / "samples" / "api-football" / "pro-canary-2026-08-22" / "06-fixtures-epl-2025-completed.raw.json"


def _target(index: int, *, home_team_id: int | None = None, away_team_id: int | None = None) -> FixtureTarget:
    kickoff = datetime(2026, 8, 1, tzinfo=UTC) + timedelta(days=index)
    home = home_team_id if home_team_id is not None else index * 2 + 1
    away = away_team_id if away_team_id is not None else index * 2 + 2
    return FixtureTarget(
        fixture_id=index + 1,
        external_fixture_id=1000 + index,
        home_team_id=home,
        away_team_id=away,
        home_external_team_id=2000 + index * 2 + 1,
        away_external_team_id=2000 + index * 2 + 2,
        kickoff_at=kickoff,
    )


def _sample_targets() -> tuple[FixtureTarget, ...]:
    return (
        FixtureTarget(1, 1378969, 1, 2, 40, 35, datetime(2026, 8, 15, tzinfo=UTC)),
        FixtureTarget(2, 1378970, 3, 4, 66, 34, datetime(2026, 8, 16, tzinfo=UTC)),
        FixtureTarget(3, 1378974, 5, 6, 47, 44, datetime(2026, 8, 17, tzinfo=UTC)),
    )


def test_fixture_chunking_caps_each_provider_call_at_twenty_ids() -> None:
    batches = chunk_fixture_targets(tuple(_target(index) for index in range(41)))

    assert [len(batch) for batch in batches] == [MAX_FIXTURES_PER_REQUEST, MAX_FIXTURES_PER_REQUEST, 1]
    assert fixture_ids_parameter(target.external_fixture_id for target in batches[0]).count("-") == 19


def test_chunking_deduplicates_same_fixture_before_transport() -> None:
    first = _target(1)
    duplicate = FixtureTarget(**{**first.__dict__, "fixture_id": 999})

    batches = chunk_fixture_targets((first, duplicate, _target(2)))

    assert batches == ((first, _target(2)),)


def test_select_history_unions_each_teams_latest_ten_without_per_team_calls() -> None:
    # The same twelve fixtures serve both teams.  The union therefore has ten
    # ids, not twenty per-team transport requests.
    targets = tuple(
        _target(index, home_team_id=1, away_team_id=2)
        for index in range(12)
    )

    selected = select_recent_history(targets)

    assert len(selected) == 10
    assert {target.fixture_id for target in selected} == set(range(3, 13))


def test_real_batch_sample_normalizes_each_complete_fixture_individually() -> None:
    payload = json.loads(SAMPLE.read_text())

    parsed = _batch_entries(payload, targets=_sample_targets(), league_external_id=39)

    assert parsed.returned_fixture_ids == {1, 2, 3}
    assert all(rows is not None and len(rows) == 2 for rows in parsed.statistics_by_fixture.values())
    assert parsed.statistics_by_fixture[1] is not None
    assert {row["external_team_id"] for row in parsed.statistics_by_fixture[1]} == {40, 35}


def test_partial_statistics_are_skipped_without_inventing_a_half_pair() -> None:
    payload = json.loads(SAMPLE.read_text())
    payload["response"][1]["statistics"] = payload["response"][1]["statistics"][:1]

    parsed = _batch_entries(payload, targets=_sample_targets(), league_external_id=39)

    assert parsed.statistics_by_fixture[2] is None
    assert parsed.statistics_by_fixture[1] is not None and parsed.statistics_by_fixture[3] is not None


def test_batch_provenance_memberships_include_only_provider_returned_fixture_ids() -> None:
    payload = json.loads(SAMPLE.read_text())
    payload["response"] = payload["response"][:2]
    payload["results"] = 2

    parsed = _batch_entries(payload, targets=_sample_targets(), league_external_id=39)

    assert parsed.returned_fixture_ids == {1, 2}
    assert parsed.statistics_by_fixture[3] is None


def test_real_completed_fixture_discovery_is_strict_and_keeps_final_scores() -> None:
    records = _completed_discovery_entries(
        json.loads(DISCOVERY_SAMPLE.read_text()),
        scope=CurrentSeasonStatisticsScope(league_external_id=39, season_start_year=2025),
    )

    assert len(records) == 380
    assert records[0].external_fixture_id == 1378969
    assert records[0].status_code == "FT"
    assert (records[0].home_goals, records[0].away_goals) == (4, 2)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda payload: payload.update(parameters={"ids": "999"}),
        lambda payload: payload["response"][0]["teams"]["home"].update(id=999),
        lambda payload: payload["response"][0]["fixture"]["status"].update(short="NS"),
        lambda payload: payload.update(results=True),
    ],
)
def test_batch_contract_rejects_mismatched_provider_shape(mutate) -> None:
    payload = copy.deepcopy(json.loads(SAMPLE.read_text()))
    mutate(payload)

    with pytest.raises(StatisticsContractError):
        _batch_entries(payload, targets=_sample_targets(), league_external_id=39)
