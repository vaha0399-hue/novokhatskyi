from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta

import pytest

from app.sync.policies import (
    CompetitionSyncPolicy,
    CoverageObservation,
    CoverageState,
    PolicyCheckedEnqueuer,
    PolicyCheckedExecutor,
    PolicyDenialReason,
    RefreshInterval,
    SyncPolicyDenied,
    SyncPolicyGate,
    SyncWorkRequest,
    _coverage_from_json,
    _intervals_from_json,
)


NOW = datetime(2026, 9, 8, tzinfo=UTC)
COVERAGE_REQUEST = SyncWorkRequest(7, 42, "coverage_refresh")
FIXTURES_REQUEST = SyncWorkRequest(7, 42, "fixtures_refresh")


class MemoryReader:
    def __init__(self, policy: CompetitionSyncPolicy | None) -> None:
        self.policy, self.calls = policy, 0

    def get(self, *, provider_id: int, season_id: int) -> CompetitionSyncPolicy | None:
        self.calls += 1
        assert (provider_id, season_id) == (7, 42)
        return self.policy


def _policy(**changes: object) -> CompetitionSyncPolicy:
    values: dict[str, object] = {
        "provider_id": 7, "season_id": 42, "policy_instance_id": 101, "enabled": True,
        "allowed_work_types": frozenset({"coverage_refresh", "fixtures_refresh"}),
        "coverage": {
            "coverage_refresh": CoverageObservation(CoverageState.UNKNOWN, date(2026, 9, 7)),
            "fixtures_refresh": CoverageObservation(CoverageState.COVERED, date(2026, 9, 7)),
        },
        "refresh_intervals": {
            "coverage_refresh": RefreshInterval(25, "second"),
            "fixtures_refresh": RefreshInterval(1, "hour"),
        },
        "priority": 0, "history_depth_seasons": 2, "policy_version": 1, "paused_until": None,
    }
    values.update(changes)
    return CompetitionSyncPolicy(**values)  # type: ignore[arg-type]


def _gate(policy: CompetitionSyncPolicy | None) -> tuple[SyncPolicyGate, MemoryReader]:
    reader = MemoryReader(policy)
    return SyncPolicyGate(reader, now=lambda: NOW), reader


@pytest.mark.parametrize(
    ("policy", "reason"),
    [
        (None, PolicyDenialReason.MISSING),
        (_policy(enabled=False), PolicyDenialReason.DISABLED),
        (_policy(paused_until=NOW + timedelta(minutes=1)), PolicyDenialReason.PAUSED),
        (_policy(allowed_work_types=frozenset({"fixtures_refresh"}), refresh_intervals={"fixtures_refresh": RefreshInterval(1, "hour")}), PolicyDenialReason.WORK_TYPE_NOT_ALLOWED),
    ],
)
def test_enqueue_is_denied_before_enqueue_callback(policy: CompetitionSyncPolicy | None, reason: PolicyDenialReason) -> None:
    gate, _ = _gate(policy)
    called: list[object] = []
    with pytest.raises(SyncPolicyDenied) as error:
        PolicyCheckedEnqueuer(gate).enqueue(COVERAGE_REQUEST, lambda authorization: called.append(authorization))
    assert error.value.reason is reason
    assert called == []


@pytest.mark.parametrize("state", [CoverageState.UNKNOWN, CoverageState.NOT_COVERED])
def test_unknown_or_negative_coverage_blocks_ordinary_work_before_enqueue(state: CoverageState) -> None:
    coverage = {"coverage_refresh": CoverageObservation(state, date(2026, 9, 7)), "fixtures_refresh": CoverageObservation(state, date(2026, 9, 7))}
    gate, _ = _gate(_policy(coverage=coverage))
    called: list[object] = []
    with pytest.raises(SyncPolicyDenied, match="coverage_not_confirmed"):
        PolicyCheckedEnqueuer(gate).enqueue(FIXTURES_REQUEST, lambda value: called.append(value))
    assert called == []


@pytest.mark.parametrize("state", [CoverageState.UNKNOWN, CoverageState.NOT_COVERED])
def test_unknown_or_negative_coverage_stays_observed_and_allows_an_explicit_freshness_check(state: CoverageState) -> None:
    coverage = {"coverage_refresh": CoverageObservation(state, date(2026, 9, 7)), "fixtures_refresh": CoverageObservation(state, date(2026, 9, 7))}
    gate, _ = _gate(_policy(coverage=coverage))
    authorization = gate.before_enqueue(COVERAGE_REQUEST)
    assert authorization.coverage == CoverageObservation(state, date(2026, 9, 7))
    assert authorization.refresh_interval == RefreshInterval(25, "second")


def test_execution_rereads_version_and_does_not_call_executor_after_disabling() -> None:
    gate, reader = _gate(_policy())
    authorization = gate.before_enqueue(COVERAGE_REQUEST)
    reader.policy = replace(reader.policy, enabled=False, policy_version=2)  # type: ignore[arg-type]
    called: list[object] = []
    with pytest.raises(SyncPolicyDenied, match="version_changed"):
        PolicyCheckedExecutor(gate).execute(authorization, lambda value: called.append(value))
    assert called == []


def test_execution_rejects_a_stale_version_even_when_changed_policy_stays_enabled() -> None:
    gate, reader = _gate(_policy())
    authorization = gate.before_enqueue(COVERAGE_REQUEST)
    reader.policy = replace(reader.policy, priority=50, policy_version=2)  # type: ignore[arg-type]
    called: list[object] = []
    with pytest.raises(SyncPolicyDenied, match="version_changed"):
        PolicyCheckedExecutor(gate).execute(authorization, lambda value: called.append(value))
    assert called == []


def test_execution_rejects_a_recreated_policy_instance_before_callback() -> None:
    gate, reader = _gate(_policy())
    authorization = gate.before_enqueue(COVERAGE_REQUEST)
    reader.policy = replace(reader.policy, policy_instance_id=102, policy_version=1)  # type: ignore[arg-type]
    called: list[object] = []
    with pytest.raises(SyncPolicyDenied, match="instance_changed"):
        PolicyCheckedExecutor(gate).execute(authorization, lambda value: called.append(value))
    assert called == []


@pytest.mark.parametrize("observed_on", ["20260908", "infinity", "tomorrow", "2026-02-30"])
def test_python_coverage_reader_rejects_noncanonical_or_invalid_dates(observed_on: str) -> None:
    with pytest.raises(ValueError, match="coverage observation"):
        _coverage_from_json({"fixtures_refresh": {"state": "covered", "observed_on": observed_on}})


def test_python_refresh_interval_reader_accepts_seconds_and_rejects_nonpositive_values() -> None:
    assert _intervals_from_json({"coverage_refresh": {"value": 25, "unit": "second"}}) == {
        "coverage_refresh": RefreshInterval(25, "second")
    }
    for value in (0, -1):
        with pytest.raises(ValueError, match="refresh interval"):
            _intervals_from_json({"coverage_refresh": {"value": value, "unit": "second"}})


def test_execution_calls_executor_only_after_a_fresh_authorized_read() -> None:
    gate, reader = _gate(_policy())
    authorization = gate.before_enqueue(FIXTURES_REQUEST)
    assert PolicyCheckedExecutor(gate).execute(authorization, lambda value: value.policy_version) == 1
    assert reader.calls == 2
