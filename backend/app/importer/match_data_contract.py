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
    PROVIDER_FULLTIME = "provider_fulltime"
    AFTER_EXTRA_TIME = "after_extra_time"
    PENALTY_SHOOTOUT = "penalty_shootout"
    ADMINISTRATIVE = "administrative"
    UNRESOLVED = "unresolved"


class ProviderPeriodSemantics(StrEnum):
    """Meaning of a provider score field when its temporal scope is unproven."""

    UNCONFIRMED = "unconfirmed"


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
    provider_fulltime: ScorePair | None
    fulltime_period_semantics: ProviderPeriodSemantics
    provider_overall: ScorePair | None
    provider_extratime: ScorePair | None
    penalty_shootout: ScorePair | None
    eligible_for_regulation_90_analytics: bool


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


@dataclass(frozen=True)
class PollProjection:
    """Current snapshot plus per-fixture request watermark and precise phase."""

    observation: PollObservation
    processed_request_sequence: int
    last_precise_phase: str | None

    def __post_init__(self) -> None:
        if self.processed_request_sequence < self.observation.request_sequence:
            raise ValueError("processed request sequence cannot precede current observation")
        if self.last_precise_phase is not None and self.last_precise_phase not in _PRECISE_PHASE_ORDER:
            raise ValueError("last precise phase must be a documented precise phase")

    @classmethod
    def from_observation(cls, observation: PollObservation) -> PollProjection:
        return cls(
            observation=observation,
            processed_request_sequence=observation.request_sequence,
            last_precise_phase=_precise_phase(observation.provider_code),
        )


@dataclass(frozen=True)
class ObservationDecision:
    action: ObservationAction
    next_projection: PollProjection

    @property
    def next_processed_request_sequence(self) -> int:
        return self.next_projection.processed_request_sequence


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
    """Preserve provider score fields without assigning unconfirmed periods."""
    rule = status_rule(provider_code)
    if not rule.is_played_result:
        kind = ResultKind.ADMINISTRATIVE if rule.state is MatchState.ADMINISTRATIVE else ResultKind.UNRESOLVED
        return ResolvedResult(
            kind,
            None,
            ProviderPeriodSemantics.UNCONFIRMED,
            observed.overall,
            observed.extratime,
            observed.penalty,
            False,
        )
    if observed.fulltime is None:
        return ResolvedResult(
            ResultKind.UNRESOLVED,
            None,
            ProviderPeriodSemantics.UNCONFIRMED,
            observed.overall,
            observed.extratime,
            observed.penalty,
            False,
        )
    if provider_code == "FT":
        if observed.overall is not None and observed.overall != observed.fulltime:
            return ResolvedResult(
                ResultKind.UNRESOLVED,
                observed.fulltime,
                ProviderPeriodSemantics.UNCONFIRMED,
                observed.overall,
                observed.extratime,
                observed.penalty,
                False,
            )
        return ResolvedResult(
            ResultKind.PROVIDER_FULLTIME,
            observed.fulltime,
            ProviderPeriodSemantics.UNCONFIRMED,
            observed.overall,
            observed.extratime,
            observed.penalty,
            False,
        )
    if provider_code == "AET":
        kind = ResultKind.AFTER_EXTRA_TIME
    else:
        kind = ResultKind.PENALTY_SHOOTOUT
    return ResolvedResult(
        kind,
        observed.fulltime,
        ProviderPeriodSemantics.UNCONFIRMED,
        observed.overall,
        observed.extratime,
        observed.penalty,
        False,
    )


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
    if previous.provider_code in _INTERRUPTION_CODES:
        return PhaseTransitionAction.ACCEPT
    if incoming.provider_code in _INTERRUPTION_CODES:
        return PhaseTransitionAction.ACCEPT
    if previous.provider_code in _SCHEDULE_CODES:
        return PhaseTransitionAction.ACCEPT
    if incoming.provider_code in _SCHEDULE_CODES:
        return PhaseTransitionAction.REVIEW_REGRESSION
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


def _precise_phase(provider_code: str) -> str | None:
    return provider_code if provider_code in _PRECISE_PHASE_ORDER else None


def _next_precise_phase(current: PollProjection, incoming: PollObservation) -> str | None:
    if incoming.provider_code in {"LIVE", "SUSP", "INT", "BT"}:
        return current.last_precise_phase
    if incoming.provider_code in _SCHEDULE_CODES or status_rule(incoming.provider_code).is_terminal:
        return None
    return _precise_phase(incoming.provider_code)


def _with_watermark(projection: PollProjection, sequence: int) -> PollProjection:
    return PollProjection(
        observation=projection.observation,
        processed_request_sequence=sequence,
        last_precise_phase=projection.last_precise_phase,
    )


def decide_poll_observation(current: PollProjection, incoming: PollObservation) -> ObservationDecision:
    """Decide a poll response and always return its next local watermark.

    Receipt time is intentionally absent from freshness. Every response that is
    not older than the watermark advances it, including ``NO_CHANGE`` and a
    response rejected for review. This prevents an earlier in-flight request
    from becoming acceptable after a newer one has already been handled.
    """
    if incoming.request_sequence < current.processed_request_sequence:
        return ObservationDecision(ObservationAction.IGNORE_OLDER_REQUEST, current)
    handled = _with_watermark(current, incoming.request_sequence)
    if incoming.content_fingerprint == current.observation.content_fingerprint:
        return ObservationDecision(ObservationAction.NO_CHANGE, handled)
    if incoming.request_sequence == current.processed_request_sequence:
        return ObservationDecision(ObservationAction.REVIEW_CONFLICT, handled)
    if current.observation.provider_code in _INTERRUPTION_CODES and incoming.provider_code == "PST":
        current_phase = current.observation.provider_code
    else:
        current_phase = current.last_precise_phase or current.observation.provider_code
    if status_rule(current.observation.provider_code).is_terminal and status_rule(incoming.provider_code).is_terminal:
        next_projection = PollProjection(incoming, incoming.request_sequence, _next_precise_phase(current, incoming))
        return ObservationDecision(ObservationAction.APPLY_CORRECTION, next_projection)
    phase = phase_transition_action(current_phase, incoming.provider_code)
    if phase is PhaseTransitionAction.REVIEW_REGRESSION:
        return ObservationDecision(ObservationAction.REVIEW_PHASE_REGRESSION, handled)
    if phase is PhaseTransitionAction.REVIEW_CONFLICT:
        return ObservationDecision(ObservationAction.REVIEW_CONFLICT, handled)
    next_projection = PollProjection(incoming, incoming.request_sequence, _next_precise_phase(current, incoming))
    return ObservationDecision(ObservationAction.APPLY, next_projection)


def observation_action(current: PollObservation, incoming: PollObservation) -> ObservationAction:
    """Compatibility shorthand for pairwise callers without projection state."""
    return decide_poll_observation(PollProjection.from_observation(current), incoming).action
