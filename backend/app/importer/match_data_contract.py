"""Pure, fail-closed contract for provider fixture states and match data.

This is deliberately a policy boundary for future importers and scanners.  It
does not parse provider JSON, write a database row, or alter the current live
pipeline.  Unknown input remains unknown and must be retained as raw evidence
for review rather than coerced into a result or a zero-valued statistic.
"""

from __future__ import annotations

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


class TransitionAction(StrEnum):
    ACCEPT = "accept"
    IGNORE_STALE = "ignore_stale"
    RECONCILE_RESULT_CORRECTION = "reconcile_result_correction"
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


_FORWARD_STATES = {
    MatchState.SCHEDULED: frozenset({MatchState.SCHEDULED, MatchState.IN_PROGRESS, MatchState.POSTPONED, MatchState.CANCELLED, MatchState.ADMINISTRATIVE}),
    MatchState.POSTPONED: frozenset({MatchState.POSTPONED, MatchState.SCHEDULED, MatchState.CANCELLED, MatchState.ADMINISTRATIVE}),
    MatchState.IN_PROGRESS: frozenset({MatchState.IN_PROGRESS, MatchState.PAUSED, MatchState.SUSPENDED, MatchState.INTERRUPTED, MatchState.COMPLETED, MatchState.ABANDONED}),
    MatchState.PAUSED: frozenset({MatchState.PAUSED, MatchState.IN_PROGRESS, MatchState.SUSPENDED, MatchState.INTERRUPTED, MatchState.COMPLETED, MatchState.ABANDONED}),
    MatchState.SUSPENDED: frozenset({MatchState.SUSPENDED, MatchState.IN_PROGRESS, MatchState.POSTPONED, MatchState.COMPLETED, MatchState.CANCELLED, MatchState.ABANDONED}),
    MatchState.INTERRUPTED: frozenset({MatchState.INTERRUPTED, MatchState.IN_PROGRESS, MatchState.POSTPONED, MatchState.COMPLETED, MatchState.CANCELLED, MatchState.ABANDONED}),
}


def transition_action(
    previous_code: object,
    incoming_code: object,
    *,
    previous_observed_at: datetime,
    incoming_observed_at: datetime,
) -> TransitionAction:
    """Decide whether a newer provider observation may advance a fixture."""
    if incoming_observed_at < previous_observed_at:
        return TransitionAction.IGNORE_STALE
    previous = status_rule(previous_code)
    incoming = status_rule(incoming_code)
    if incoming.state is MatchState.UNKNOWN or previous.state is MatchState.UNKNOWN:
        return TransitionAction.REVIEW_CONFLICT
    if incoming_observed_at == previous_observed_at and incoming.provider_code != previous.provider_code:
        return TransitionAction.REVIEW_CONFLICT
    if previous.is_terminal:
        if incoming_observed_at > previous_observed_at:
            return TransitionAction.RECONCILE_RESULT_CORRECTION
        return TransitionAction.REVIEW_CONFLICT
    if incoming.state in _FORWARD_STATES.get(previous.state, frozenset()):
        return TransitionAction.ACCEPT
    return TransitionAction.REVIEW_CONFLICT
