"""Pure prematch freshness rule over explicitly supplied fetch observations."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")


@dataclass(frozen=True)
class PrematchFetchObservation:
    """Saved facts needed to prove that one fixture input is recent."""

    provider_id: int
    fixture_id: int
    fetch_id: int
    observed_kickoff_at: datetime | None
    observed_at: datetime
    endpoint: str
    fetch_successful: bool
    fetch_normalized: bool

    def __post_init__(self) -> None:
        if self.provider_id <= 0 or self.fixture_id <= 0 or self.fetch_id <= 0:
            raise ValueError("provider, fixture, and fetch ids must be positive")
        if not self.endpoint.strip():
            raise ValueError("fetch endpoint must not be blank")
        _require_aware(self.observed_at, "fetch observation time")
        if self.observed_kickoff_at is not None:
            _require_aware(self.observed_kickoff_at, "observed fixture kickoff")


@dataclass(frozen=True)
class PrematchFreshnessResult:
    """A matching fetch suppresses prematch work; no match preserves planning."""

    fresh_input: bool
    confirming_fetch_id: int | None

    def __post_init__(self) -> None:
        if self.fresh_input != (self.confirming_fetch_id is not None):
            raise ValueError("fresh input and confirming fetch id must agree")


def evaluate_prematch_freshness(
    *,
    provider_id: int,
    fixture_id: int,
    current_kickoff_at: datetime | None,
    now: datetime,
    deadline: datetime,
    policy_interval: timedelta,
    observations: Iterable[PrematchFetchObservation],
) -> PrematchFreshnessResult:
    """Select the newest valid evidence in ``[deadline - interval, now]``.

    The caller supplies the T-60 or T-10 deadline explicitly.  When receipt
    times tie, the larger fetch id wins so selection is deterministic.
    """
    if provider_id <= 0 or fixture_id <= 0:
        raise ValueError("provider and fixture ids must be positive")
    if policy_interval <= timedelta():
        raise ValueError("prematch policy interval must be positive")
    _require_aware(now, "scheduler time")
    _require_aware(deadline, "prematch deadline")
    if current_kickoff_at is None:
        return PrematchFreshnessResult(False, None)
    _require_aware(current_kickoff_at, "current fixture kickoff")

    current = now.astimezone(UTC)
    lower_bound = deadline.astimezone(UTC) - policy_interval
    current_kickoff = current_kickoff_at.astimezone(UTC)
    eligible = (
        observation
        for observation in observations
        if observation.provider_id == provider_id
        and observation.fixture_id == fixture_id
        and observation.endpoint == "/fixtures"
        and observation.fetch_successful
        and observation.fetch_normalized
        and observation.observed_kickoff_at is not None
        and observation.observed_kickoff_at.astimezone(UTC) == current_kickoff
        and lower_bound <= observation.observed_at.astimezone(UTC) <= current
    )
    confirming = max(
        eligible,
        key=lambda observation: (
            observation.observed_at.astimezone(UTC),
            observation.fetch_id,
        ),
        default=None,
    )
    if confirming is None:
        return PrematchFreshnessResult(False, None)
    return PrematchFreshnessResult(True, confirming.fetch_id)
