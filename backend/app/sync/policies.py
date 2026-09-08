"""Fail-closed, reusable permission gates for future sync queue adapters."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from re import fullmatch
from typing import Any, Protocol, TypeVar

from psycopg import Connection


class CoverageState(StrEnum):
    UNKNOWN = "unknown"
    COVERED = "covered"
    NOT_COVERED = "not_covered"


@dataclass(frozen=True)
class CoverageObservation:
    state: CoverageState
    observed_on: date | None


@dataclass(frozen=True)
class RefreshInterval:
    value: int
    unit: str


class PolicyDenialReason(StrEnum):
    MISSING = "missing"
    DISABLED = "disabled"
    PAUSED = "paused"
    WORK_TYPE_NOT_ALLOWED = "work_type_not_allowed"
    COVERAGE_NOT_CONFIRMED = "coverage_not_confirmed"
    INSTANCE_CHANGED = "instance_changed"
    VERSION_CHANGED = "version_changed"


class SyncPolicyDenied(RuntimeError):
    """Raised before enqueue or executor invocation when a policy disallows work."""

    def __init__(self, reason: PolicyDenialReason) -> None:
        self.reason = reason
        super().__init__(f"competition sync policy denied: {reason.value}")


@dataclass(frozen=True)
class CompetitionSyncPolicy:
    provider_id: int
    season_id: int
    policy_instance_id: int
    enabled: bool
    allowed_work_types: frozenset[str]
    coverage: Mapping[str, CoverageObservation]
    refresh_intervals: Mapping[str, RefreshInterval]
    priority: int
    history_depth_seasons: int
    policy_version: int
    paused_until: datetime | None

    def coverage_for(self, work_type: str) -> CoverageObservation:
        return self.coverage.get(work_type, CoverageObservation(CoverageState.UNKNOWN, None))


@dataclass(frozen=True)
class SyncWorkRequest:
    provider_id: int
    season_id: int
    work_type: str

    def __post_init__(self) -> None:
        if self.provider_id <= 0 or self.season_id <= 0 or not self.work_type.strip():
            raise ValueError("provider, season, and nonblank work type are required")


@dataclass(frozen=True)
class AuthorizedSyncWork:
    request: SyncWorkRequest
    policy_instance_id: int
    policy_version: int
    coverage: CoverageObservation
    refresh_interval: RefreshInterval


class CompetitionSyncPolicyReader(Protocol):
    def get(self, *, provider_id: int, season_id: int) -> CompetitionSyncPolicy | None: ...


def _coverage_from_json(value: object) -> dict[str, CoverageObservation]:
    if not isinstance(value, Mapping):
        raise ValueError("competition sync policy coverage is malformed")
    result: dict[str, CoverageObservation] = {}
    for work_type, item in value.items():
        if not isinstance(work_type, str) or not isinstance(item, Mapping):
            raise ValueError("competition sync policy coverage is malformed")
        try:
            observed_on = item.get("observed_on")
            if not isinstance(observed_on, str) or fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", observed_on) is None:
                raise ValueError
            parsed_date = date.fromisoformat(observed_on)
            if parsed_date.isoformat() != observed_on:
                raise ValueError
            result[work_type] = CoverageObservation(
                CoverageState(str(item.get("state"))), parsed_date
            )
        except ValueError as exc:
            raise ValueError("competition sync policy coverage observation is malformed") from exc
    return result


def _intervals_from_json(value: object) -> dict[str, RefreshInterval]:
    if not isinstance(value, Mapping):
        raise ValueError("competition sync policy refresh intervals are malformed")
    result: dict[str, RefreshInterval] = {}
    for work_type, item in value.items():
        if not isinstance(work_type, str) or not isinstance(item, Mapping):
            raise ValueError("competition sync policy refresh interval is malformed")
        interval_value, unit = item.get("value"), item.get("unit")
        if not isinstance(interval_value, int) or isinstance(interval_value, bool) or interval_value < 1 or unit not in {
            "second", "minute", "hour", "day", "week"
        }:
            raise ValueError("competition sync policy refresh interval is malformed")
        result[work_type] = RefreshInterval(interval_value, unit)
    return result


class PostgresCompetitionSyncPolicyReader:
    """Read-only policy reader suitable for queue and executor adapters."""

    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection

    def get(self, *, provider_id: int, season_id: int) -> CompetitionSyncPolicy | None:
        row = self._connection.execute(
            """SELECT provider_id,season_id,policy_instance_id,enabled,allowed_work_types,coverage,refresh_intervals,
                      priority,history_depth_seasons,policy_version,paused_until
                 FROM ops.competition_sync_policies WHERE provider_id=%s AND season_id=%s""",
            (provider_id, season_id),
        ).fetchone()
        if row is None:
            return None
        allowed_work_types = row[4]
        if not isinstance(allowed_work_types, Sequence) or isinstance(allowed_work_types, str):
            raise ValueError("competition sync policy allowed work types are malformed")
        try:
            return CompetitionSyncPolicy(
                provider_id=int(row[0]), season_id=int(row[1]), policy_instance_id=int(row[2]), enabled=bool(row[3]),
                allowed_work_types=frozenset(str(value) for value in allowed_work_types),
                coverage=_coverage_from_json(row[5]), refresh_intervals=_intervals_from_json(row[6]),
                priority=int(row[7]), history_depth_seasons=int(row[8]), policy_version=int(row[9]), paused_until=row[10],
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("competition sync policy row is malformed") from exc


class SyncPolicyGate:
    """One fail-closed decision path shared by enqueue and execution adapters."""

    coverage_refresh_work_type = "coverage_refresh"

    def __init__(self, reader: CompetitionSyncPolicyReader, *, now: Callable[[], datetime]) -> None:
        self._reader, self._now = reader, now

    def before_enqueue(self, request: SyncWorkRequest) -> AuthorizedSyncWork:
        return self._authorize(request, expected_policy_instance_id=None, expected_policy_version=None)

    def before_execution(self, authorization: AuthorizedSyncWork) -> AuthorizedSyncWork:
        return self._authorize(
            authorization.request,
            expected_policy_instance_id=authorization.policy_instance_id,
            expected_policy_version=authorization.policy_version,
        )

    def _authorize(
        self,
        request: SyncWorkRequest,
        *,
        expected_policy_instance_id: int | None,
        expected_policy_version: int | None,
    ) -> AuthorizedSyncWork:
        policy = self._reader.get(provider_id=request.provider_id, season_id=request.season_id)
        if policy is None:
            raise SyncPolicyDenied(PolicyDenialReason.MISSING)
        if expected_policy_instance_id is not None and policy.policy_instance_id != expected_policy_instance_id:
            raise SyncPolicyDenied(PolicyDenialReason.INSTANCE_CHANGED)
        if expected_policy_version is not None and policy.policy_version != expected_policy_version:
            raise SyncPolicyDenied(PolicyDenialReason.VERSION_CHANGED)
        if not policy.enabled:
            raise SyncPolicyDenied(PolicyDenialReason.DISABLED)
        if policy.paused_until is not None and policy.paused_until > self._now():
            raise SyncPolicyDenied(PolicyDenialReason.PAUSED)
        if request.work_type not in policy.allowed_work_types:
            raise SyncPolicyDenied(PolicyDenialReason.WORK_TYPE_NOT_ALLOWED)
        coverage = policy.coverage_for(request.work_type)
        if request.work_type != self.coverage_refresh_work_type and coverage.state is not CoverageState.COVERED:
            raise SyncPolicyDenied(PolicyDenialReason.COVERAGE_NOT_CONFIRMED)
        try:
            interval = policy.refresh_intervals[request.work_type]
        except KeyError as exc:
            raise ValueError("allowed work type lacks a refresh interval") from exc
        return AuthorizedSyncWork(request, policy.policy_instance_id, policy.policy_version, coverage, interval)


T = TypeVar("T")


class PolicyCheckedEnqueuer:
    """Adapter that proves a rejected request cannot reach an enqueue callback."""

    def __init__(self, gate: SyncPolicyGate) -> None:
        self._gate = gate

    def enqueue(self, request: SyncWorkRequest, enqueue: Callable[[AuthorizedSyncWork], T]) -> T:
        return enqueue(self._gate.before_enqueue(request))


class PolicyCheckedExecutor:
    """Adapter that rereads policy before a claimed item's executor is called."""

    def __init__(self, gate: SyncPolicyGate) -> None:
        self._gate = gate

    def execute(self, authorization: AuthorizedSyncWork, executor: Callable[[AuthorizedSyncWork], T]) -> T:
        return executor(self._gate.before_execution(authorization))
