from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from app.importer.match_data_contract import (
    MatchState,
    ObservationAction,
    ObservationDecision,
    PollObservation,
    PollProjection,
    PhaseTransitionAction,
    ProviderPeriodSemantics,
    ResultKind,
    ResultObservation,
    ScorePair,
    StatisticsPeriod,
    StatusAction,
    regulation_statistics_bucket,
    observation_fingerprint,
    decide_poll_observation,
    phase_transition_action,
    resolve_result,
    status_rule,
)


@pytest.mark.parametrize(
    ("code", "state", "action", "played"),
    [
        ("TBD", MatchState.SCHEDULED, StatusAction.STORE_SCHEDULE, False),
        ("NS", MatchState.SCHEDULED, StatusAction.STORE_SCHEDULE, False),
        ("1H", MatchState.IN_PROGRESS, StatusAction.TRACK_LIVE, False),
        ("HT", MatchState.PAUSED, StatusAction.HOLD_AND_RECHECK, False),
        ("2H", MatchState.IN_PROGRESS, StatusAction.TRACK_LIVE, False),
        ("ET", MatchState.IN_PROGRESS, StatusAction.TRACK_LIVE, False),
        ("BT", MatchState.PAUSED, StatusAction.HOLD_AND_RECHECK, False),
        ("P", MatchState.IN_PROGRESS, StatusAction.TRACK_LIVE, False),
        ("LIVE", MatchState.IN_PROGRESS, StatusAction.TRACK_LIVE, False),
        ("SUSP", MatchState.SUSPENDED, StatusAction.HOLD_AND_RECHECK, False),
        ("INT", MatchState.INTERRUPTED, StatusAction.HOLD_AND_RECHECK, False),
        ("FT", MatchState.COMPLETED, StatusAction.RECONCILE_PLAYED_RESULT, True),
        ("AET", MatchState.COMPLETED, StatusAction.RECONCILE_PLAYED_RESULT, True),
        ("PEN", MatchState.COMPLETED, StatusAction.RECONCILE_PLAYED_RESULT, True),
        ("PST", MatchState.POSTPONED, StatusAction.AWAIT_REPLACEMENT_KICKOFF, False),
        ("CANC", MatchState.CANCELLED, StatusAction.MARK_NOT_PLAYED, False),
        ("ABD", MatchState.ABANDONED, StatusAction.PRESERVE_RAW_AND_REVIEW, False),
        ("AWD", MatchState.ADMINISTRATIVE, StatusAction.PRESERVE_RAW_AND_REVIEW, False),
        ("WO", MatchState.ADMINISTRATIVE, StatusAction.PRESERVE_RAW_AND_REVIEW, False),
    ],
)
def test_documented_statuses_have_explicit_contracts(
    code: str, state: MatchState, action: StatusAction, played: bool
) -> None:
    rule = status_rule(code)
    assert (rule.state, rule.action, rule.is_played_result) == (state, action, played)


def test_unknown_status_is_never_silently_normalized() -> None:
    rule = status_rule("VAR_DELAY")
    assert rule.state is MatchState.UNKNOWN
    assert rule.action is StatusAction.PRESERVE_RAW_AND_REVIEW


@pytest.mark.parametrize(
    ("status", "observation", "kind", "provider_fulltime", "eligible"),
    [
        ("FT", ResultObservation(ScorePair(2, 1), ScorePair(2, 1), None, None), ResultKind.PROVIDER_FULLTIME, ScorePair(2, 1), False),
        ("AET", ResultObservation(ScorePair(3, 2), ScorePair(2, 2), ScorePair(3, 2), None), ResultKind.AFTER_EXTRA_TIME, ScorePair(2, 2), False),
        ("PEN", ResultObservation(ScorePair(1, 1), ScorePair(1, 1), None, ScorePair(5, 4)), ResultKind.PENALTY_SHOOTOUT, ScorePair(1, 1), False),
        ("AWD", ResultObservation(ScorePair(3, 0), None, None, None), ResultKind.ADMINISTRATIVE, None, False),
    ],
)
def test_result_examples_preserve_each_provider_period(
    status: str,
    observation: ResultObservation,
    kind: ResultKind,
    provider_fulltime: ScorePair | None,
    eligible: bool,
) -> None:
    resolved = resolve_result(status, observation)
    assert (resolved.kind, resolved.provider_fulltime, resolved.eligible_for_regulation_90_analytics) == (
        kind,
        provider_fulltime,
        eligible,
    )


def test_missing_fulltime_is_not_replaced_by_provider_overall_goals() -> None:
    resolved = resolve_result("FT", ResultObservation(ScorePair(4, 0), None, None, None))
    assert resolved.kind is ResultKind.UNRESOLVED
    assert resolved.provider_fulltime is None
    assert resolved.provider_overall == ScorePair(4, 0)
    assert resolved.eligible_for_regulation_90_analytics is False


def test_unknown_or_extra_time_statistics_never_enter_regulation_bucket() -> None:
    assert regulation_statistics_bucket(StatisticsPeriod.REGULATION_90) is StatisticsPeriod.REGULATION_90
    assert regulation_statistics_bucket(StatisticsPeriod.EXTRA_TIME) is None
    assert regulation_statistics_bucket(StatisticsPeriod.PENALTY_SHOOTOUT) is None
    assert regulation_statistics_bucket(StatisticsPeriod.UNKNOWN) is None


@pytest.mark.parametrize(
    ("previous", "incoming", "expected"),
    [
        ("NS", "HT", PhaseTransitionAction.ACCEPT),
        ("NS", "FT", PhaseTransitionAction.ACCEPT),
        ("2H", "1H", PhaseTransitionAction.REVIEW_REGRESSION),
        ("ET", "2H", PhaseTransitionAction.REVIEW_REGRESSION),
        ("ET", "BT", PhaseTransitionAction.ACCEPT),
        ("BT", "ET", PhaseTransitionAction.ACCEPT),
        ("ET", "ET", PhaseTransitionAction.ACCEPT),
        ("2H", "LIVE", PhaseTransitionAction.ACCEPT_AMBIGUOUS_LIVE),
        ("LIVE", "ET", PhaseTransitionAction.ACCEPT),
    ],
)
def test_phase_transition_allows_skipped_polls_but_not_phase_regressions(
    previous: str, incoming: str, expected: PhaseTransitionAction
) -> None:
    assert phase_transition_action(previous, incoming) is expected


def _poll(code: str, payload: bytes, request_sequence: int, received_at: datetime) -> PollObservation:
    return PollObservation(
        provider_code=code,
        content_fingerprint=observation_fingerprint(code, payload),
        request_sequence=request_sequence,
        received_at=received_at,
    )


def test_identical_terminal_observation_is_no_change_by_content_not_status() -> None:
    received = datetime(2026, 9, 8, 12, tzinfo=UTC)
    current = _poll("FT", b'{"goals":{"home":1,"away":0}}', 10, received)
    repeated = _poll("FT", b'{"goals":{"home":1,"away":0}}', 11, received + timedelta(seconds=1))
    corrected = _poll("FT", b'{"goals":{"home":2,"away":0}}', 12, received + timedelta(seconds=2))
    projection = PollProjection.from_observation(current)
    repeated_decision = decide_poll_observation(projection, repeated)
    corrected_decision = decide_poll_observation(repeated_decision.next_projection, corrected)
    assert repeated_decision.action is ObservationAction.NO_CHANGE
    assert corrected_decision.action is ObservationAction.APPLY_CORRECTION


def test_newer_changed_terminal_status_is_also_a_result_correction() -> None:
    received = datetime(2026, 9, 8, 12, tzinfo=UTC)
    current = _poll("FT", b'{"score":"1-1"}', 10, received)
    corrected = _poll("AET", b'{"score":"2-1"}', 11, received + timedelta(seconds=1))
    decision = decide_poll_observation(PollProjection.from_observation(current), corrected)
    assert decision.action is ObservationAction.APPLY_CORRECTION


def test_late_response_from_an_older_dispatched_request_cannot_rollback_projection() -> None:
    received = datetime(2026, 9, 8, 12, tzinfo=UTC)
    newer_response = _poll("2H", b'{"score":"1-0"}', 8, received)
    older_response_arriving_later = _poll("1H", b'{"score":"0-0"}', 7, received + timedelta(seconds=5))
    decision = decide_poll_observation(PollProjection.from_observation(newer_response), older_response_arriving_later)
    assert decision.action is ObservationAction.IGNORE_OLDER_REQUEST


def test_same_request_sequence_with_different_content_requires_review() -> None:
    received = datetime(2026, 9, 8, 12, tzinfo=UTC)
    current = _poll("2H", b'{"score":"1-0"}', 8, received)
    conflicting_response = _poll("2H", b'{"score":"1-1"}', 8, received + timedelta(seconds=2))
    decision = decide_poll_observation(PollProjection.from_observation(current), conflicting_response)
    assert decision.action is ObservationAction.REVIEW_CONFLICT


def test_newer_phase_regression_requires_review_even_when_response_is_newer() -> None:
    received = datetime(2026, 9, 8, 12, tzinfo=UTC)
    current = _poll("ET", b'{"score":"2-2"}', 20, received)
    regression = _poll("2H", b'{"score":"1-1"}', 21, received + timedelta(seconds=1))
    decision = decide_poll_observation(PollProjection.from_observation(current), regression)
    assert decision.action is ObservationAction.REVIEW_PHASE_REGRESSION


def test_newer_terminal_to_live_observation_requires_review() -> None:
    received = datetime(2026, 9, 8, 12, tzinfo=UTC)
    current = _poll("FT", b'{"score":"1-0"}', 30, received)
    repair = _poll("SUSP", b'{"score":"1-0"}', 31, received + timedelta(seconds=1))
    decision = decide_poll_observation(PollProjection.from_observation(current), repair)
    assert decision.action is ObservationAction.REVIEW_CONFLICT


@pytest.mark.parametrize("interruption", ["SUSP", "INT"])
def test_interrupted_fixture_may_be_postponed(interruption: str) -> None:
    assert phase_transition_action(interruption, "PST") is PhaseTransitionAction.ACCEPT


def test_live_and_interruption_preserve_precise_phase_across_a_chain() -> None:
    received = datetime(2026, 9, 8, 12, tzinfo=UTC)
    start = PollProjection.from_observation(_poll("2H", b'{"score":"1-0"}', 1, received))
    live = decide_poll_observation(start, _poll("LIVE", b'{"score":"1-0"}', 2, received + timedelta(seconds=1)))
    assert live.action is ObservationAction.APPLY
    assert live.next_projection.last_precise_phase == "2H"
    regression = decide_poll_observation(live.next_projection, _poll("1H", b'{"score":"1-0"}', 3, received + timedelta(seconds=2)))
    assert regression.action is ObservationAction.REVIEW_PHASE_REGRESSION
    assert regression.next_projection.last_precise_phase == "2H"


@pytest.mark.parametrize("interruption", ["SUSP", "INT"])
def test_temporary_interruption_preserves_precise_phase(interruption: str) -> None:
    received = datetime(2026, 9, 8, 12, tzinfo=UTC)
    start = PollProjection.from_observation(_poll("2H", b'{"score":"1-0"}', 1, received))
    paused = decide_poll_observation(start, _poll(interruption, b'{"score":"1-0"}', 2, received + timedelta(seconds=1)))
    regression = decide_poll_observation(paused.next_projection, _poll("1H", b'{"score":"1-0"}', 3, received + timedelta(seconds=2)))
    assert paused.next_projection.last_precise_phase == "2H"
    assert regression.action is ObservationAction.REVIEW_PHASE_REGRESSION


def test_postponement_after_interruption_clears_precise_phase_memory() -> None:
    received = datetime(2026, 9, 8, 12, tzinfo=UTC)
    start = PollProjection.from_observation(_poll("2H", b'{"score":"1-0"}', 1, received))
    suspended = decide_poll_observation(start, _poll("SUSP", b'{"score":"1-0"}', 2, received + timedelta(seconds=1)))
    postponed = decide_poll_observation(suspended.next_projection, _poll("PST", b'{"score":"1-0"}', 3, received + timedelta(seconds=2)))
    assert postponed.action is ObservationAction.APPLY
    assert postponed.next_projection.last_precise_phase is None


def test_extra_time_break_chain_keeps_and_advances_precise_phase() -> None:
    received = datetime(2026, 9, 8, 12, tzinfo=UTC)
    start = PollProjection.from_observation(_poll("ET", b'{"score":"2-2"}', 1, received))
    break_time = decide_poll_observation(start, _poll("BT", b'{"score":"2-2"}', 2, received + timedelta(seconds=1)))
    resumed = decide_poll_observation(break_time.next_projection, _poll("ET", b'{"score":"2-2","minute":105}', 3, received + timedelta(seconds=2)))
    assert (break_time.action, resumed.action, resumed.next_projection.last_precise_phase) == (
        ObservationAction.APPLY,
        ObservationAction.APPLY,
        "ET",
    )


def test_no_change_advances_processed_request_watermark() -> None:
    received = datetime(2026, 9, 8, 12, tzinfo=UTC)
    first = PollProjection.from_observation(_poll("2H", b'{"score":"1-0"}', 1, received))
    identical = decide_poll_observation(first, _poll("2H", b'{"score":"1-0"}', 3, received + timedelta(seconds=1)))
    late_changed = decide_poll_observation(identical.next_projection, _poll("2H", b'{"score":"2-0"}', 2, received + timedelta(seconds=2)))
    assert identical.action is ObservationAction.NO_CHANGE
    assert identical.next_processed_request_sequence == 3
    assert late_changed.action is ObservationAction.IGNORE_OLDER_REQUEST
    assert late_changed.next_processed_request_sequence == 3


def test_provider_fulltime_period_is_unconfirmed_and_not_eligible_for_90_minute_analytics() -> None:
    resolved = resolve_result("FT", ResultObservation(ScorePair(2, 1), ScorePair(2, 1), None, None))
    assert resolved.provider_fulltime == ScorePair(2, 1)
    assert resolved.fulltime_period_semantics is ProviderPeriodSemantics.UNCONFIRMED
    assert resolved.eligible_for_regulation_90_analytics is False
