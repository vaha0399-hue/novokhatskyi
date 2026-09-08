"""Pure, fail-closed contract for provider fixture states and match data.

This is deliberately a policy boundary for future importers and scanners.  It
does not parse provider JSON, write a database row, or alter the current live
pipeline.  Unknown input remains unknown and must be retained as raw evidence
for review rather than coerced into a result or a zero-valued statistic.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class MatchState(StrEnum):
    SCHEDULED = "scheduled"
    IN_PROGRESS = "in_progress"
    PAUSED = "paused"
    SUSPENDED = "suspended"
    INTERRUPTED = "interrupted"
    POSTPONED = "postponed"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    ABANDONED = "abandoned"
    ADMINISTRATIVE = "administrative"
    UNKNOWN = "unknown"


class StatusAction(StrEnum):
    STORE_SCHEDULE = "store_schedule"
    TRACK_LIVE = "track_live"
    HOLD_AND_RECHECK = "hold_and_recheck"
    AWAIT_REPLACEMENT_KICKOFF = "await_replacement_kickoff"
    RECONCILE_PLAYED_RESULT = "reconcile_played_result"
    MARK_NOT_PLAYED = "mark_not_played"
    PRESERVE_RAW_AND_REVIEW = "preserve_raw_and_review"


class ResultKind(StrEnum):
    REGULATION = "regulation"
    AFTER_EXTRA_TIME = "after_extra_time"
    PENALTY_SHOOTOUT = "penalty_shootout"
    ADMINISTRATIVE = "administrative"
    UNRESOLVED = "unresolved"


class StatisticsPeriod(StrEnum):
    REGULATION_90 = "regulation_90"
    FIRST_HALF = "first_half"
    SECOND_HALF = "second_half"
    EXTRA_TIME = "extra_time"
    PENALTY_SHOOTOUT = "penalty_shootout"
    UNKNOWN = "unknown"


class PhaseTransitionAction(StrEnum):
    ACCEPT = "accept"
    ACCEPT_AMBIGUOUS_LIVE = "accept_ambiguous_live"
    REVIEW_REGRESSION = "review_regression"
    REVIEW_CONFLICT = "review_conflict"


class ObservationAction(StrEnum):
    APPLY = "apply"
    APPLY_CORRECTION = "apply_correction"
    NO_CHANGE = "no_change"
    IGNORE_OLDER_REQUEST = "ignore_older_request"
    REVIEW_PHASE_REGRESSION = "review_phase_regression"
    REVIEW_CONFLICT = "review_conflict"


@dataclass(frozen=True)
class StatusRule:
    provider_code: str
    state: MatchState
    action: StatusAction
    is_terminal: bool
    is_played_result: bool


@dataclass(frozen=True)
class ScorePair:
    home: int
    away: int

    def __post_init__(self) -> None:
        if any(not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in (self.home, self.away)):
            raise ValueError("score values must be non-negative integers")


@dataclass(frozen=True)
class ResultObservation:
    """Provider scores, preserved by their source field rather than inferred."""

    overall: ScorePair | None
    fulltime: ScorePair | None
    extratime: ScorePair | None
    penalty: ScorePair | None


@dataclass(frozen=True)
class ResolvedResult:
    kind: ResultKind
    regulation_90: ScorePair | None
    provider_overall: ScorePair | None
    provider_extratime: ScorePair | None
    penalty_shootout: ScorePair | None
    eligible_for_played_match_analytics: bool


@dataclass(frozen=True)
class PollObservation:
    """One immutable per-fixture response snapshot from a local poll request.

    ``request_sequence`` is allocated locally when a request is sent.  It is
    the sole freshness order at this boundary; ``received_at`` is retained for
    audit and latency analysis only.  The fingerprint must cover the complete
    retained per-fixture content, including status and all score fields.
    """

    provider_code: str
    content_fingerprint: str
    request_sequence: int
    received_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.provider_code, str):
            raise ValueError("provider status code must be a string")
        if not isinstance(self.request_sequence, int) or isinstance(self.request_sequence, bool) or self.request_sequence < 0:
            raise ValueError("request sequence must be a non-negative integer")
        if not isinstance(self.content_fingerprint, str) or not _SHA256_RE.fullmatch(self.content_fingerprint):
            raise ValueError("content fingerprint must be a SHA-256 hex digest")
        if not isinstance(self.received_at, datetime) or self.received_at.tzinfo is None:
            raise ValueError("received_at must be timezone-aware")


_RULES = (
    StatusRule("TBD", MatchState.SCHEDULED, StatusAction.STORE_SCHEDULE, False, False),
    StatusRule("NS", MatchState.SCHEDULED, StatusAction.STORE_SCHEDULE, False, False),
    StatusRule("1H", MatchState.IN_PROGRESS, StatusAction.TRACK_LIVE, False, False),
    StatusRule("HT", MatchState.PAUSED, StatusAction.HOLD_AND_RECHECK, False, False),
    StatusRule("2H", MatchState.IN_PROGRESS, StatusAction.TRACK_LIVE, False, False),
    StatusRule("ET", MatchState.IN_PROGRESS, StatusAction.TRACK_LIVE, False, False),
    StatusRule("BT", MatchState.PAUSED, StatusAction.HOLD_AND_RECHECK, False, False),
    StatusRule("P", MatchState.IN_PROGRESS, StatusAction.TRACK_LIVE, False, False),
    StatusRule("LIVE", MatchState.IN_PROGRESS, StatusAction.TRACK_LIVE, False, False),
    StatusRule("SUSP", MatchState.SUSPENDED, StatusAction.HOLD_AND_RECHECK, False, False),
    StatusRule("INT", MatchState.INTERRUPTED, StatusAction.HOLD_AND_RECHECK, False, False),
    StatusRule("FT", MatchState.COMPLETED, StatusAction.RECONCILE_PLAYED_RESULT, True, True),
    StatusRule("AET", MatchState.COMPLETED, StatusAction.RECONCILE_PLAYED_RESULT, True, True),
    StatusRule("PEN", MatchState.COMPLETED, StatusAction.RECONCILE_PLAYED_RESULT, True, True),
    StatusRule("PST", MatchState.POSTPONED, StatusAction.AWAIT_REPLACEMENT_KICKOFF, False, False),
    StatusRule("CANC", MatchState.CANCELLED, StatusAction.MARK_NOT_PLAYED, True, False),
    StatusRule("ABD", MatchState.ABANDONED, StatusAction.PRESERVE_RAW_AND_REVIEW, True, False),
    StatusRule("AWD", MatchState.ADMINISTRATIVE, StatusAction.PRESERVE_RAW_AND_REVIEW, True, False),
    StatusRule("WO", MatchState.ADMINISTRATIVE, StatusAction.PRESERVE_RAW_AND_REVIEW, True, False),
)
_BY_CODE = {rule.provider_code: rule for rule in _RULES}
UNKNOWN_STATUS = StatusRule("UNKNOWN", MatchState.UNKNOWN, StatusAction.PRESERVE_RAW_AND_REVIEW, False, False)
_SHA256_RE = re.compile(r"[a-f0-9]{64}")


def status_rule(provider_code: object) -> StatusRule:
    """Return a documented rule or an explicit review-only unknown rule."""
    if not isinstance(provider_code, str):
        return UNKNOWN_STATUS
    return _BY_CODE.get(provider_code, UNKNOWN_STATUS)


def resolve_result(provider_code: object, observed: ResultObservation) -> ResolvedResult:
    """Classify score fields without treating a missing period as a zero.

    ``score.fulltime`` is the only source for the 90-minute score, including
    stoppage time.  ``goals``/``overall`` is never substituted into that
    field.  Extra-time and shootout fields retain their provider labels; this
    contract deliberately does not calculate an aggregate-round winner.
    """
    rule = status_rule(provider_code)
    if not rule.is_played_result:
        kind = ResultKind.ADMINISTRATIVE if rule.state is MatchState.ADMINISTRATIVE else ResultKind.UNRESOLVED
        return ResolvedResult(kind, None, observed.overall, observed.extratime, observed.penalty, False)
    if observed.fulltime is None:
        return ResolvedResult(ResultKind.UNRESOLVED, None, observed.overall, observed.extratime, observed.penalty, False)
    if provider_code == "FT":
        if observed.overall is not None and observed.overall != observed.fulltime:
            return ResolvedResult(ResultKind.UNRESOLVED, observed.fulltime, observed.overall, observed.extratime, observed.penalty, False)
        return ResolvedResult(ResultKind.REGULATION, observed.fulltime, observed.overall, observed.extratime, observed.penalty, True)
    if provider_code == "AET":
        return ResolvedResult(ResultKind.AFTER_EXTRA_TIME, observed.fulltime, observed.overall, observed.extratime, observed.penalty, True)
    return ResolvedResult(ResultKind.PENALTY_SHOOTOUT, observed.fulltime, observed.overall, observed.extratime, observed.penalty, True)


def regulation_statistics_bucket(period: StatisticsPeriod) -> StatisticsPeriod | None:
    """Return the only period permitted in 90-minute aggregate metrics."""
    return StatisticsPeriod.REGULATION_90 if period is StatisticsPeriod.REGULATION_90 else None


_PRECISE_PHASE_ORDER = {"1H": 10, "HT": 20, "2H": 30, "ET": 40, "BT": 45, "P": 50}
_SCHEDULE_CODES = frozenset({"TBD", "NS", "PST"})
_INTERRUPTION_CODES = frozenset({"SUSP", "INT"})


def observation_fingerprint(provider_code: str, snapshot: bytes) -> str:
    """Return an immutable fixture identity that always includes its status."""
    if not isinstance(provider_code, str):
        raise ValueError("provider status code must be a string")
    if not isinstance(snapshot, bytes):
        raise ValueError("fixture snapshot must be bytes")
    return hashlib.sha256(provider_code.encode("utf-8") + b"\0" + snapshot).hexdigest()


def phase_transition_action(previous_code: object, incoming_code: object) -> PhaseTransitionAction:
    """Evaluate football phases only; this function has no polling-time input.

    A poll may miss phases, so any forward jump from a schedule to a terminal
    phase is valid.  Precise live phases cannot regress.  ``LIVE`` is an
    in-progress provider signal with no reliable ordinal phase: it can update
    a live snapshot, but cannot claim a precise phase rollback.
    """
    previous = status_rule(previous_code)
    incoming = status_rule(incoming_code)
    if previous.state is MatchState.UNKNOWN or incoming.state is MatchState.UNKNOWN:
        return PhaseTransitionAction.REVIEW_CONFLICT
    if previous.provider_code == incoming.provider_code:
        return PhaseTransitionAction.ACCEPT
    if previous.is_terminal:
        return PhaseTransitionAction.REVIEW_CONFLICT
    if incoming.is_terminal:
        return PhaseTransitionAction.ACCEPT
    if previous.provider_code in _SCHEDULE_CODES:
        return PhaseTransitionAction.ACCEPT
    if incoming.provider_code in _SCHEDULE_CODES:
        return PhaseTransitionAction.REVIEW_REGRESSION
    if previous.provider_code in _INTERRUPTION_CODES or incoming.provider_code in _INTERRUPTION_CODES:
        return PhaseTransitionAction.ACCEPT
    if incoming.provider_code == "LIVE":
        return PhaseTransitionAction.ACCEPT_AMBIGUOUS_LIVE
    if previous.provider_code == "LIVE":
        return PhaseTransitionAction.ACCEPT
    previous_phase = _PRECISE_PHASE_ORDER.get(previous.provider_code)
    incoming_phase = _PRECISE_PHASE_ORDER.get(incoming.provider_code)
    if previous_phase is None or incoming_phase is None:
        return PhaseTransitionAction.REVIEW_CONFLICT
    if previous.provider_code == "BT" and incoming.provider_code == "ET":
        return PhaseTransitionAction.ACCEPT
    if incoming_phase < previous_phase:
        return PhaseTransitionAction.REVIEW_REGRESSION
    return PhaseTransitionAction.ACCEPT


def observation_action(current: PollObservation, incoming: PollObservation) -> ObservationAction:
    """Combine local request order, identity, then football phase semantics.

    A response from an earlier dispatched request is ignored even if it arrives
    later.  Receipt time is intentionally absent from ordering.  Equal status
    does not mean no change: only the same immutable content fingerprint does.
    """
    if incoming.request_sequence < current.request_sequence:
        return ObservationAction.IGNORE_OLDER_REQUEST
    if incoming.content_fingerprint == current.content_fingerprint:
        return ObservationAction.NO_CHANGE
    if incoming.request_sequence == current.request_sequence:
        return ObservationAction.REVIEW_CONFLICT
    if status_rule(current.provider_code).is_terminal and status_rule(incoming.provider_code).is_terminal:
        return ObservationAction.APPLY_CORRECTION
    phase = phase_transition_action(current.provider_code, incoming.provider_code)
    if phase is PhaseTransitionAction.REVIEW_REGRESSION:
        return ObservationAction.REVIEW_PHASE_REGRESSION
    if phase is PhaseTransitionAction.REVIEW_CONFLICT:
        return ObservationAction.REVIEW_CONFLICT
    return ObservationAction.APPLY
