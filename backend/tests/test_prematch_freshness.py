from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from app.sync.prematch_freshness import (
    PrematchFetchObservation,
    evaluate_prematch_freshness,
)


PROVIDER_ID = 7
FIXTURE_ID = 9001
KICKOFF = datetime(2026, 9, 11, 14, 0, tzinfo=UTC)
T_MINUS_60 = datetime(2026, 9, 11, 13, 0, tzinfo=UTC)
T_MINUS_10 = datetime(2026, 9, 11, 13, 50, tzinfo=UTC)
POLICY_INTERVAL = timedelta(minutes=15)


def _observation(
    fetch_id: int,
    observed_at: datetime,
    *,
    provider_id: int = PROVIDER_ID,
    fixture_id: int = FIXTURE_ID,
    observed_kickoff_at: datetime | None = KICKOFF,
    endpoint: str = "/fixtures",
    fetch_successful: bool = True,
    fetch_normalized: bool = True,
) -> PrematchFetchObservation:
    return PrematchFetchObservation(
        provider_id=provider_id,
        fixture_id=fixture_id,
        fetch_id=fetch_id,
        observed_kickoff_at=observed_kickoff_at,
        observed_at=observed_at,
        endpoint=endpoint,
        fetch_successful=fetch_successful,
        fetch_normalized=fetch_normalized,
    )


@pytest.mark.parametrize("deadline", (T_MINUS_60, T_MINUS_10))
@pytest.mark.parametrize("boundary", ("lower", "upper"))
def test_prematch_freshness_includes_both_boundaries_for_each_event(
    deadline: datetime,
    boundary: str,
) -> None:
    now = deadline + timedelta(minutes=7)
    observed_at = deadline - POLICY_INTERVAL if boundary == "lower" else now

    result = evaluate_prematch_freshness(
        provider_id=PROVIDER_ID,
        fixture_id=FIXTURE_ID,
        current_kickoff_at=KICKOFF,
        now=now,
        deadline=deadline,
        policy_interval=POLICY_INTERVAL,
        observations=[_observation(101, observed_at)],
    )

    assert result.fresh_input is True
    assert result.confirming_fetch_id == 101


def test_prematch_freshness_selects_latest_then_highest_fetch_id() -> None:
    now = T_MINUS_60 + timedelta(minutes=5)
    latest_time = T_MINUS_60 + timedelta(minutes=2)

    result = evaluate_prematch_freshness(
        provider_id=PROVIDER_ID,
        fixture_id=FIXTURE_ID,
        current_kickoff_at=KICKOFF,
        now=now,
        deadline=T_MINUS_60,
        policy_interval=POLICY_INTERVAL,
        observations=[
            _observation(500, T_MINUS_60),
            _observation(501, latest_time),
            _observation(503, latest_time),
            _observation(502, latest_time),
        ],
    )

    assert result.fresh_input is True
    assert result.confirming_fetch_id == 503


def test_prematch_freshness_without_evidence_preserves_normal_planning() -> None:
    result = evaluate_prematch_freshness(
        provider_id=PROVIDER_ID,
        fixture_id=FIXTURE_ID,
        current_kickoff_at=KICKOFF,
        now=T_MINUS_60 + timedelta(minutes=5),
        deadline=T_MINUS_60,
        policy_interval=POLICY_INTERVAL,
        observations=(),
    )

    assert result.fresh_input is False
    assert result.confirming_fetch_id is None


@pytest.mark.parametrize(
    "observed_at",
    (
        T_MINUS_60 - POLICY_INTERVAL - timedelta(microseconds=1),
        T_MINUS_60 + timedelta(minutes=5, microseconds=1),
    ),
)
def test_prematch_freshness_rejects_stale_and_future_observations(
    observed_at: datetime,
) -> None:
    result = evaluate_prematch_freshness(
        provider_id=PROVIDER_ID,
        fixture_id=FIXTURE_ID,
        current_kickoff_at=KICKOFF,
        now=T_MINUS_60 + timedelta(minutes=5),
        deadline=T_MINUS_60,
        policy_interval=POLICY_INTERVAL,
        observations=[_observation(101, observed_at)],
    )

    assert result.fresh_input is False
    assert result.confirming_fetch_id is None


def test_prematch_freshness_rejects_observation_for_previous_kickoff() -> None:
    result = evaluate_prematch_freshness(
        provider_id=PROVIDER_ID,
        fixture_id=FIXTURE_ID,
        current_kickoff_at=KICKOFF + timedelta(days=1),
        now=T_MINUS_60 + timedelta(minutes=5),
        deadline=T_MINUS_60,
        policy_interval=POLICY_INTERVAL,
        observations=[_observation(101, T_MINUS_60)],
    )

    assert result.fresh_input is False
    assert result.confirming_fetch_id is None


@pytest.mark.parametrize(
    ("provider_id", "fixture_id"),
    ((PROVIDER_ID + 1, FIXTURE_ID), (PROVIDER_ID, FIXTURE_ID + 1)),
)
def test_prematch_freshness_rejects_foreign_provider_or_fixture(
    provider_id: int,
    fixture_id: int,
) -> None:
    result = evaluate_prematch_freshness(
        provider_id=PROVIDER_ID,
        fixture_id=FIXTURE_ID,
        current_kickoff_at=KICKOFF,
        now=T_MINUS_60 + timedelta(minutes=5),
        deadline=T_MINUS_60,
        policy_interval=POLICY_INTERVAL,
        observations=[
            _observation(
                101,
                T_MINUS_60,
                provider_id=provider_id,
                fixture_id=fixture_id,
            )
        ],
    )

    assert result.fresh_input is False
    assert result.confirming_fetch_id is None


@pytest.mark.parametrize(
    ("current_kickoff_at", "observed_kickoff_at"),
    ((None, KICKOFF), (KICKOFF, None)),
)
def test_prematch_freshness_requires_known_matching_kickoffs(
    current_kickoff_at: datetime | None,
    observed_kickoff_at: datetime | None,
) -> None:
    result = evaluate_prematch_freshness(
        provider_id=PROVIDER_ID,
        fixture_id=FIXTURE_ID,
        current_kickoff_at=current_kickoff_at,
        now=T_MINUS_60 + timedelta(minutes=5),
        deadline=T_MINUS_60,
        policy_interval=POLICY_INTERVAL,
        observations=[
            _observation(
                101,
                T_MINUS_60,
                observed_kickoff_at=observed_kickoff_at,
            )
        ],
    )

    assert result.fresh_input is False
    assert result.confirming_fetch_id is None


@pytest.mark.parametrize(
    "changes",
    (
        {"endpoint": "/fixtures/statistics"},
        {"fetch_successful": False},
        {"fetch_normalized": False},
    ),
)
def test_prematch_freshness_requires_successful_normalized_fixtures_fetch(
    changes: dict[str, object],
) -> None:
    result = evaluate_prematch_freshness(
        provider_id=PROVIDER_ID,
        fixture_id=FIXTURE_ID,
        current_kickoff_at=KICKOFF,
        now=T_MINUS_10 + timedelta(minutes=5),
        deadline=T_MINUS_10,
        policy_interval=POLICY_INTERVAL,
        observations=[_observation(101, T_MINUS_10, **changes)],  # type: ignore[arg-type]
    )

    assert result.fresh_input is False
    assert result.confirming_fetch_id is None
